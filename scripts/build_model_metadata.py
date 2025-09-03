#!/usr/bin/env python3
"""
Build a clean, sortable CSV (and optional Parquet/JSONL) of Hugging Face model metadata
from cached HF API JSONs and an enriched CSV. Optionally upsert into the SQLite DB.

Inputs
------
- Enriched CSV (default: data/models_enriched.csv)
  Columns may include: model_name, url, updated_url, model_description, params, model_size
- Cache directory (default: cache/)
  Contains responses from https://huggingface.co/api/models/<repo_id> as JSON. Supports:
  "<org>__<repo>.json" and "model_<org>__<repo>.json" naming.

Outputs
-------
- Model metadata CSV (default: data/model_metadata.csv)
- Optional: model files CSV (default: data/model_files.csv) when --emit-files is set
- Optional: Parquet/JSONL mirrors
- Optional: upsert to SQLite (tables typically created by scripts.models_db)

Highlights
---------
- Robust URL normalization and repo_id resolution
- Graceful handling of missing or partial cache JSONs
- Derives file format flags (safetensors/bin/gguf/onnx/tflite/pt) from siblings
- Computes total file count and total size (best-effort)
- Parses parameter counts from multiple sources (safetensors.total, "14m", "7B", etc.)
- Aggregates tags & languages, extracts license from tag or cardData
- Summarizes MTEB/model-index as a compact JSON string (optional)
- Nice progress bars; clear logging

Usage
-----
  python build_model_metadata.py \
      --input data/models_enriched.csv \
      --cache cache \
      --output-csv data/model_metadata.csv \
      --emit-files data/model_files.csv \
      --parquet data/model_metadata.parquet \
      --jsonl data/model_metadata.jsonl \
      --write-db --db-path /app/db/models.db
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

# Optional progress bar
try:
    from tqdm import tqdm
    _tqdm = tqdm
except Exception:  # pragma: no cover
    def _tqdm(x, **kwargs):
        return x

# ---- Optional DB integration -------------------------------------------------
# We only import if available; otherwise DB steps are skipped.
_init_db = None
_upsert_model = None
_upsert_file = None
try:
    from scripts.models_db import init_db as _init_db, upsert_model as _upsert_model
    # upsert_file may or may not exist depending on your version; guard it.
    try:
        from scripts.models_db import upsert_file as _upsert_file
    except Exception:
        _upsert_file = None
except ModuleNotFoundError:
    # Try parent for local runs
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    try:
        from scripts.models_db import init_db as _init_db, upsert_model as _upsert_model
        try:
            from scripts.models_db import upsert_file as _upsert_file
        except Exception:
            _upsert_file = None
    except Exception:
        pass


# ---- Helpers -----------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, file=sys.stderr)

def is_clean_hf_url(url: str) -> bool:
    try:
        if not isinstance(url, str) or not url:
            return False
        if not url.startswith("https://huggingface.co/"):
            return False
        tail = url.split("huggingface.co/")[-1].strip("/")
        if not tail:
            return False
        parts = tail.split("/")
        # Allow single-tenant (bert-base-uncased) or org/repo
        return len(parts) in (1, 2)
    except Exception:
        return False

def extract_repo_id_from_url(url: str) -> Optional[str]:
    if not isinstance(url, str) or not url:
        return None
    if "huggingface.co/" not in url:
        return None
    tail = url.split("huggingface.co/")[-1].split("?")[0].split("#")[0].strip("/")
    parts = [p for p in tail.split("/") if p]
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    if len(parts) == 1:
        return parts[0]
    return None

def best_repo_id_for_row(row: pd.Series) -> Optional[str]:
    upd = row.get("updated_url")
    if isinstance(upd, str) and is_clean_hf_url(upd):
        rid = extract_repo_id_from_url(upd)
        if rid:
            return rid

    url = row.get("url")
    if isinstance(url, str) and is_clean_hf_url(url):
        rid = extract_repo_id_from_url(url)
        if rid:
            return rid

    mn = row.get("model_name")
    if isinstance(mn, str) and "/" in mn:
        return mn.strip().strip("/")

    return None

def choose_preferred_row(rows: pd.DataFrame) -> pd.Series:
    # prefer rows whose updated_url is clean → then url → else first
    candidates = rows[rows.get("updated_url_clean", False) == True]  # noqa: E712
    if len(candidates) > 0:
        return candidates.iloc[0]
    candidates = rows[rows.get("url_clean", False) == True]          # noqa: E712
    if len(candidates) > 0:
        return candidates.iloc[0]
    return rows.iloc[0]

def cache_json_for_repo(cache_dir: Path, repo_id: str) -> Optional[dict]:
    safe = repo_id.replace("/", "__")
    for name in (f"{safe}.json", f"model_{safe}.json"):
        p = cache_dir / name
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None

def list_from(value) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []

def extract_license(top_tags: List[str], card: dict) -> str:
    lic = ""
    if isinstance(card, dict):
        raw = card.get("license")
        if isinstance(raw, str):
            lic = raw
    if not lic and isinstance(top_tags, list):
        for t in top_tags:
            if isinstance(t, str) and t.lower().startswith("license:"):
                lic = t.split(":", 1)[-1]
                break
    return lic or ""

def parse_params_to_int(*vals) -> Optional[int]:
    def _from_str(s: str) -> Optional[int]:
        t = s.strip().lower().replace(",", "")
        if not t:
            return None
        if t.endswith("k"):
            return int(float(t[:-1]) * 1_000)
        if t.endswith("m"):
            return int(float(t[:-1]) * 1_000_000)
        if t.endswith("b"):
            return int(float(t[:-1]) * 1_000_000_000)
        return int(float(t))

    for v in vals:
        if v is None:
            continue
        if isinstance(v, int):
            return v
        if isinstance(v, float) and not math.isnan(v):
            return int(v)
        if isinstance(v, str) and v.strip():
            try:
                parsed = _from_str(v)
                if parsed is not None:
                    return parsed
            except Exception:
                continue
    return None

def readable_int(n: Optional[int]) -> str:
    if n is None:
        return ""
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return str(n)

def summarize_model_index(card: dict) -> str:
    """
    Compact JSON string summarizing model-index entries (task→#datasets).
    Keeps it small for storing in 'benchmarks_json'.
    """
    try:
        idx = card.get("model-index") or []
        task_counts: Dict[str, int] = {}
        for entry in idx:
            results = entry.get("results") or []
            for r in results:
                task = (r.get("task") or {}).get("type") or ""
                if not task:
                    continue
                task_counts[task] = task_counts.get(task, 0) + 1
        return json.dumps({"task_counts": task_counts}, separators=(",", ":"))
    except Exception:
        return ""

def derive_file_flags_and_sizes(siblings: List[dict]) -> Tuple[Dict[str, int], Dict[str, bool], int]:
    """
    Returns:
      sizes: {"count": int, "bytes": int}
      flags: {"has_safetensors": bool, "has_bin": bool, ...}
      known_bytes_sum: int
    """
    count = 0
    total_bytes = 0
    flags = {
        "has_safetensors": False,
        "has_bin": False,
        "has_pt": False,
        "has_onnx": False,
        "has_tflite": False,
        "has_gguf": False,
    }
    for s in siblings or []:
        name = s.get("rfilename") or ""
        if not name:
            continue
        count += 1
        sz = s.get("size")
        if isinstance(sz, (int, float)) and sz >= 0:
            total_bytes += int(sz)
        lower = name.lower()
        if lower.endswith(".safetensors") or lower.endswith(".safetensors.index.json"):
            flags["has_safetensors"] = True
        if lower.endswith(".bin") or lower.endswith(".bin.index.json"):
            flags["has_bin"] = True
        if lower.endswith(".pt"):
            flags["has_pt"] = True
        if lower.endswith(".onnx"):
            flags["has_onnx"] = True
        if lower.endswith(".tflite"):
            flags["has_tflite"] = True
        if lower.endswith(".gguf"):
            flags["has_gguf"] = True
    return {"count": count, "bytes": total_bytes}, flags, total_bytes

def parse_metadata(repo_id: str, js: dict, enrich_row: pd.Series) -> Dict[str, object]:
    # Basic identity
    model_id = js.get("id") or js.get("modelId") or repo_id
    author = js.get("author") or (repo_id.split("/")[0] if "/" in repo_id else "")
    pipeline_tag = js.get("pipeline_tag") \
        or js.get("transformersInfo", {}).get("pipeline_tag") \
        or js.get("cardData", {}).get("pipeline_tag") \
        or ""

    # Tags / languages
    top_tags = list_from(js.get("tags"))
    card = js.get("cardData") or {}
    card_tags = list_from(card.get("tags"))
    tags = list(dict.fromkeys(top_tags + card_tags))
    languages = list_from(card.get("language"))

    # License
    license_spdx = extract_license(top_tags, card)

    # Popularity & flags
    downloads = js.get("downloads") or 0
    likes = js.get("likes") or 0
    created_at = js.get("createdAt") or ""
    last_modified = js.get("lastModified") or ""
    private = bool(js.get("private", False))
    gated = bool(js.get("gated", False))
    disabled = bool(js.get("disabled", False))
    sha = js.get("sha") or ""

    # Config/architectures
    cfg = js.get("config") or {}
    architectures = list_from(cfg.get("architectures"))
    model_type = cfg.get("model_type") or ""

    # Transformers helpers
    tinfo = js.get("transformersInfo") or {}
    auto_model = tinfo.get("auto_model") or ""
    processor = tinfo.get("processor") or ""

    # Regions / arxiv from tags
    region = ""
    arxiv_ids: List[str] = []
    for t in tags:
        tt = (t or "").lower()
        if tt.startswith("region:"):
            region = t.split(":", 1)[-1]
        if tt.startswith("arxiv:"):
            arxiv_ids.append(t.split(":", 1)[-1])

    # Params (safetensors.total & enrichment fallback)
    st = js.get("safetensors") or {}
    st_total = st.get("total")
    params_raw = enrich_row.get("params")
    params_int = parse_params_to_int(st_total, params_raw)
    params_readable = readable_int(params_int)

    # Storage & files
    used_storage = js.get("usedStorage") or None
    siblings = js.get("siblings") or []
    sizes, flags, known_bytes_sum = derive_file_flags_and_sizes(siblings)
    # prefer usedStorage if present; else sum of siblings sizes (best-effort)
    used_storage_bytes = int(used_storage) if isinstance(used_storage, (int, float)) else (known_bytes_sum if known_bytes_sum else "")

    # Enrichment fields
    model_description = enrich_row.get("model_description") or ""
    model_size_raw = enrich_row.get("model_size") or ""

    # Canonical URLs / slugs
    canonical_url = f"https://huggingface.co/{repo_id}"
    repo_tail = repo_id.split("/")[-1]
    org = repo_id.split("/")[0] if "/" in repo_id else ""
    name_hf_slug = f"huggingface-{org}-{repo_tail}" if org else f"huggingface-{repo_tail}"

    # Benchmarks summary (tiny JSON string)
    benchmarks_json = summarize_model_index(card)

    # HF Spaces that reference this model (small list)
    spaces = list_from(js.get("spaces"))
    spaces_join = ";".join(spaces) if spaces else ""

    return {
        "repo_id": model_id if model_id else repo_id,
        "canonical_url": canonical_url,
        "name_hf_slug": name_hf_slug,
        "model_name": enrich_row.get("model_name") or "",
        "author": author,
        "pipeline_tag": pipeline_tag,
        "library_name": js.get("library_name") or "",
        "license": license_spdx,
        "languages": ";".join(languages) if languages else "",
        "tags": ";".join(tags) if tags else "",
        "downloads": int(downloads) if isinstance(downloads, (int, float)) else 0,
        "likes": int(likes) if isinstance(likes, (int, float)) else 0,
        "created_at": created_at,
        "last_modified": last_modified,
        "private": private,
        "gated": gated,
        "disabled": disabled,
        "sha": sha,
        "model_type": model_type,
        "architectures": ";".join(architectures) if architectures else "",
        "auto_model": auto_model,
        "processor": processor,
        # Params/size
        "parameters": params_int if params_int is not None else "",
        "parameters_readable": params_readable,
        "used_storage_bytes": used_storage_bytes,
        # Derived from siblings
        "file_count": sizes["count"],
        "has_safetensors": flags["has_safetensors"],
        "has_bin": flags["has_bin"],
        "has_pt": flags["has_pt"],
        "has_onnx": flags["has_onnx"],
        "has_tflite": flags["has_tflite"],
        "has_gguf": flags["has_gguf"],
        # Benchmarks / spaces
        "benchmarks_json": benchmarks_json,
        "spaces": spaces_join,
        # From enrichment
        "model_description": model_description,
        "params_raw": params_raw if isinstance(params_raw, str) else (str(params_raw) if params_raw else ""),
        "model_size_raw": model_size_raw if isinstance(model_size_raw, str) else (str(model_size_raw) if model_size_raw else ""),
    }

def minimal_row_for(rid: str, enrich_row: pd.Series) -> Dict[str, object]:
    canonical_url = f"https://huggingface.co/{rid}"
    return {
        "repo_id": rid,
        "canonical_url": canonical_url,
        "name_hf_slug": f"huggingface-{rid.replace('/', '-')}" if rid else "",
        "model_name": enrich_row.get("model_name") or "",
        "author": rid.split("/")[0] if "/" in rid else "",
        "pipeline_tag": "",
        "library_name": "",
        "license": "",
        "languages": "",
        "tags": "",
        "downloads": "",
        "likes": "",
        "created_at": "",
        "last_modified": "",
        "private": "",
        "gated": "",
        "disabled": "",
        "sha": "",
        "model_type": "",
        "architectures": "",
        "auto_model": "",
        "processor": "",
        "parameters": "",
        "parameters_readable": "",
        "used_storage_bytes": "",
        "file_count": 0,
        "has_safetensors": False,
        "has_bin": False,
        "has_pt": False,
        "has_onnx": False,
        "has_tflite": False,
        "has_gguf": False,
        "benchmarks_json": "",
        "spaces": "",
        "model_description": enrich_row.get("model_description") or "",
        "params_raw": enrich_row.get("params") or "",
        "model_size_raw": enrich_row.get("model_size") or "",
    }

def collect_file_rows(repo_id: str, js: dict) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for s in js.get("siblings") or []:
        name = s.get("rfilename")
        if not name:
            continue
        out.append({
            "repo_id": repo_id,
            "rfilename": name,
            "size": s.get("size") if isinstance(s.get("size"), (int, float)) else "",
        })
    return out


# ---- Main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Build model metadata from cached HF model JSONs + enriched CSV.")
    ap.add_argument("--input", default="data/models_enriched.csv", help="Path to enriched CSV (default: data/models_enriched.csv)")
    ap.add_argument("--cache", default="cache", help="Directory with cached model JSONs (default: cache)")
    ap.add_argument("--output-csv", default="data/model_metadata.csv", help="Output CSV path (default: data/model_metadata.csv)")
    ap.add_argument("--emit-files", default=None, help="Optional CSV to emit per-file rows from siblings (e.g., data/model_files.csv)")
    ap.add_argument("--parquet", default=None, help="Optional Parquet path to mirror the CSV")
    ap.add_argument("--jsonl", default=None, help="Optional JSONL path to mirror the CSV")
    ap.add_argument("--write-db", action="store_true", help="Upsert rows into SQLite models/files tables (if scripts.models_db is available)")
    ap.add_argument("--db-path", default="/app/db/models.db", help="SQLite path (default: /app/db/models.db)")
    args = ap.parse_args()

    cache_dir = Path(args.cache)
    if not cache_dir.exists():
        raise SystemExit(f"Cache dir not found: {cache_dir}")

    df = pd.read_csv(args.input)

    # Mark clean URL flags (safe even if cols missing)
    df["updated_url_clean"] = df.get("updated_url", pd.Series([None] * len(df))).apply(
        lambda u: is_clean_hf_url(u) if isinstance(u, str) else False
    )
    df["url_clean"] = df.get("url", pd.Series([None] * len(df))).apply(
        lambda u: is_clean_hf_url(u) if isinstance(u, str) else False
    )

    # Compute repo_id per row; drop unresolved
    repo_ids: List[Optional[str]] = []
    for _, row in df.iterrows():
        repo_ids.append(best_repo_id_for_row(row))
    df["repo_id"] = repo_ids
    df_valid = df[~df["repo_id"].isna()].copy()

    rows_out: List[Dict[str, object]] = []
    files_out: List[Dict[str, object]] = []

    for rid, group in _tqdm(list(df_valid.groupby("repo_id", sort=True)), desc="Models"):
        best_row = choose_preferred_row(group)
        js = cache_json_for_repo(cache_dir, rid)
        if not js:
            rows_out.append(minimal_row_for(rid, best_row))
            continue

        # Main metadata
        row_meta = parse_metadata(rid, js, best_row)
        rows_out.append(row_meta)

        # Optional per-file rows
        if args.emit_files:
            files_out.extend(collect_file_rows(rid, js))

    out_df = pd.DataFrame(rows_out)

    # Friendly column order (extras get appended)
    col_order = [
        "repo_id", "canonical_url", "name_hf_slug",
        "model_name", "author",
        "pipeline_tag", "library_name", "license", "languages", "tags",
        "downloads", "likes", "created_at", "last_modified",
        "private", "gated", "disabled", "sha",
        "model_type", "architectures", "auto_model", "processor",
        "parameters", "parameters_readable", "used_storage_bytes",
        "file_count", "has_safetensors", "has_bin", "has_pt", "has_onnx", "has_tflite", "has_gguf",
        "benchmarks_json", "spaces",
        "model_description", "params_raw", "model_size_raw",
    ]
    final_cols = [c for c in col_order if c in out_df.columns] + [c for c in out_df.columns if c not in col_order]
    out_df = out_df[final_cols]

    # Write CSV
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.output_csv, index=False)
    log(f"Wrote {args.output_csv} with {len(out_df)} models.")

    # Optional mirrors
    if args.parquet:
        out_df.to_parquet(args.parquet, index=False)
        log(f"Wrote {args.parquet}")
    if args.jsonl:
        with open(args.jsonl, "w", encoding="utf-8") as f:
            for _, r in out_df.iterrows():
                f.write(json.dumps({k: (None if (pd.isna(v) if not isinstance(v, (list, dict)) else False) else v)
                                    for k, v in r.to_dict().items()}, ensure_ascii=False) + "\n")
        log(f"Wrote {args.jsonl}")

    # Optional per-file CSV
    if args.emit_files and files_out:
        files_df = pd.DataFrame(files_out)[["repo_id", "rfilename", "size"]]
        Path(args.emit_files).parent.mkdir(parents=True, exist_ok=True)
        files_df.to_csv(args.emit_files, index=False)
        log(f"Wrote {args.emit_files} with {len(files_df)} files.")

    # Optional DB write
    if args.write_db:
        if _init_db is None or _upsert_model is None:
            log("[DB] scripts.models_db not available; skipping DB write.")
        else:
            _init_db(args.db_path)
            up_cnt = 0
            for _, row in out_df.iterrows():
                repo_id = row.get("repo_id")
                if not isinstance(repo_id, str) or not repo_id:
                    continue
                fields = {
                    "canonical_url": row.get("canonical_url"),
                    "model_name": row.get("model_name"),
                    "author": row.get("author"),
                    "pipeline_tag": row.get("pipeline_tag"),
                    "library_name": row.get("library_name"),
                    "license": row.get("license"),
                    "languages": row.get("languages"),
                    "tags": row.get("tags"),
                    "downloads": row.get("downloads"),
                    "likes": row.get("likes"),
                    "created_at": row.get("created_at"),
                    "last_modified": row.get("last_modified"),
                    "private": row.get("private"),
                    "gated": row.get("gated"),
                    "disabled": row.get("disabled"),
                    "sha": row.get("sha"),
                    "model_type": row.get("model_type"),
                    "architectures": row.get("architectures"),
                    "auto_model": row.get("auto_model"),
                    "processor": row.get("processor"),
                    "parameters": row.get("parameters"),
                    "parameters_readable": row.get("parameters_readable"),
                    "used_storage_bytes": row.get("used_storage_bytes"),
                    "file_count": row.get("file_count"),
                    "has_safetensors": row.get("has_safetensors"),
                    "has_bin": row.get("has_bin"),
                    "has_pt": row.get("has_pt"),
                    "has_onnx": row.get("has_onnx"),
                    "has_tflite": row.get("has_tflite"),
                    "has_gguf": row.get("has_gguf"),
                    "benchmarks_json": row.get("benchmarks_json"),
                    "spaces": row.get("spaces"),
                    "model_description": row.get("model_description"),
                    "params_raw": row.get("params_raw"),
                    "model_size_raw": row.get("model_size_raw"),
                }
                _upsert_model(args.db_path, repo_id, fields)
                up_cnt += 1
            log(f"[DB] upserted {up_cnt} models into {args.db_path}")

            # Optional file rows into DB (if function exists and we emitted them)
            if _upsert_file and args.emit_files and files_out:
                inserted = 0
                for rec in files_out:
                    try:
                        _upsert_file(args.db_path, rec["repo_id"], rec["rfilename"], rec.get("size"))
                        inserted += 1
                    except Exception:
                        continue
                log(f"[DB] upserted {inserted} files into {args.db_path}")


if __name__ == "__main__":
    main()
