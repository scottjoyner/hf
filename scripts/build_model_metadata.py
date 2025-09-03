#!/usr/bin/env python3
# (User-provided script, kept intact except for default paths)
import argparse
import json
import math
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

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
    candidates = rows[rows["updated_url_clean"] == True]
    if len(candidates) > 0:
        return candidates.iloc[0]
    candidates = rows[rows["url_clean"] == True]
    if len(candidates) > 0:
        return candidates.iloc[0]
    return rows.iloc[0]

def cache_json_for_repo(cache_dir: Path, repo_id: str) -> Optional[dict]:
    safe = repo_id.replace("/", "__")
    paths = [
        cache_dir / f"{safe}.json",
        cache_dir / f"model_{safe}.json",
    ]
    for p in paths:
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
    return None

def parse_params_to_int(*vals) -> Optional[int]:
    def _from_str(s: str) -> Optional[int]:
        t = s.strip().lower().replace(",", "")
        if t.endswith("k"):
            n = t[:-1]
            return int(float(n) * 1_000)
        if t.endswith("m"):
            n = t[:-1]
            return int(float(n) * 1_000_000)
        if t.endswith("b"):
            n = t[:-1]
            return int(float(n) * 1_000_000_000)
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
                return _from_str(v)
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

def extract_license(top_tags: List[str], card: dict) -> str:
    lic = ""
    if isinstance(card, dict):
        lic = (card.get("license") or "") if isinstance(card.get("license"), str) else ""
    if not lic and isinstance(top_tags, list):
        for t in top_tags:
            if isinstance(t, str) and t.lower().startswith("license:"):
                lic = t.split(":", 1)[-1]
                break
    return lic or ""

def list_from(value) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []

def parse_metadata(repo_id: str, js: dict, enrich_row: pd.Series) -> Dict[str, object]:
    model_id = js.get("id") or js.get("modelId") or repo_id
    author = js.get("author") or (repo_id.split("/")[0] if "/" in repo_id else "")
    pipeline_tag = js.get("pipeline_tag") or js.get("transformersInfo", {}).get("pipeline_tag") \
                   or js.get("cardData", {}).get("pipeline_tag") or ""
    library_name = js.get("library_name") or ""

    top_tags = list_from(js.get("tags"))
    card_tags = list_from(js.get("cardData", {}).get("tags"))
    tags = list(dict.fromkeys(top_tags + card_tags))

    languages = list_from(js.get("cardData", {}).get("language"))
    license_spdx = extract_license(top_tags, js.get("cardData", {}))

    downloads = js.get("downloads") or 0
    likes = js.get("likes") or 0
    created_at = js.get("createdAt") or ""
    last_modified = js.get("lastModified") or ""
    private = bool(js.get("private", False))
    gated = bool(js.get("gated", False))
    disabled = bool(js.get("disabled", False))
    sha = js.get("sha") or ""

    cfg = js.get("config") or {}
    architectures = list_from(cfg.get("architectures"))
    model_type = cfg.get("model_type") or ""

    tinfo = js.get("transformersInfo") or {}
    auto_model = tinfo.get("auto_model") or ""
    processor = tinfo.get("processor") or ""

    region = ""
    arxiv_ids = []
    for t in tags:
        tt = t.lower()
        if tt.startswith("region:"):
            region = t.split(":", 1)[-1]
        if tt.startswith("arxiv:"):
            arxiv_ids.append(t.split(":", 1)[-1])

    st = js.get("safetensors") or {}
    st_total = st.get("total")
    params_raw = enrich_row.get("params")
    params_int = parse_params_to_int(st_total, params_raw)
    params_readable = readable_int(params_int)

    used_storage = js.get("usedStorage") or None

    model_description = enrich_row.get("model_description") or ""
    model_size_raw = enrich_row.get("model_size") or ""

    canonical_url = f"https://huggingface.co/{repo_id}"
    repo_tail = repo_id.split("/")[-1]
    org = repo_id.split("/")[0] if "/" in repo_id else ""
    if org:
        name_hf_slug = f"huggingface-{org}-{repo_tail}"
    else:
        name_hf_slug = f"huggingface-{repo_tail}"

    return {
        "repo_id": model_id if model_id else repo_id,
        "canonical_url": canonical_url,
        "name_hf_slug": name_hf_slug,
        "model_name": enrich_row.get("model_name") or "",
        "author": author,
        "pipeline_tag": pipeline_tag,
        "library_name": library_name,
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
        "parameters": params_int if params_int is not None else "",
        "parameters_readable": params_readable,
        "used_storage_bytes": int(used_storage) if isinstance(used_storage, (int, float)) else "",
        "region": region,
        "arxiv_ids": ";".join(arxiv_ids) if arxiv_ids else "",
        "model_description": model_description,
        "params_raw": params_raw if isinstance(params_raw, str) else (str(params_raw) if params_raw else ""),
        "model_size_raw": model_size_raw if isinstance(model_size_raw, str) else (str(model_size_raw) if model_size_raw else ""),
    }

def main():
    ap = argparse.ArgumentParser(description="Build model_metadata.csv from cached HF model JSONs.")
    ap.add_argument("--input", default="models_enriched.csv", help="Path to models_enriched.csv")
    ap.add_argument("--cache", default="cache", help="Directory with cached model JSONs")
    ap.add_argument("--output", default="model_metadata.csv", help="Output CSV path")
    args = ap.parse_args()

    cache_dir = Path(args.cache)
    if not cache_dir.exists():
        raise SystemExit(f"Cache dir not found: {cache_dir}")

    df = pd.read_csv(args.input)

    def series_or_none(df, col):
        return df[col] if col in df.columns else pd.Series([None] * len(df))

    upd_series = series_or_none(df, "updated_url")
    url_series = series_or_none(df, "url")

    df["updated_url_clean"] = upd_series.apply(lambda u: is_clean_hf_url(u) if isinstance(u, str) else False)
    df["url_clean"] = url_series.apply(lambda u: is_clean_hf_url(u) if isinstance(u, str) else False)

    repo_ids = []
    for _, row in df.iterrows():
        rid = best_repo_id_for_row(row)
        repo_ids.append(rid)
    df["repo_id"] = repo_ids

    df_valid = df[~df["repo_id"].isna()].copy()

    rows_out = []
    for rid, group in df_valid.groupby("repo_id", sort=True):
        best_row = choose_preferred_row(group)
        js = cache_json_for_repo(cache_dir, rid)
        if not js:
            minimal = {
                "repo_id": rid,
                "canonical_url": f"https://huggingface.co/{rid}",
                "name_hf_slug": (f"huggingface-{rid.replace('/', '-')}" if rid else ""),
                "model_name": best_row.get("model_name") or "",
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
                "region": "",
                "arxiv_ids": "",
                "model_description": best_row.get("model_description") or "",
                "params_raw": best_row.get("params") or "",
                "model_size_raw": best_row.get("model_size") or "",
            }
            rows_out.append(minimal)
            continue

        rows_out.append(parse_metadata(rid, js, best_row))

    out_df = pd.DataFrame(rows_out)

    col_order = [
        "repo_id", "canonical_url", "name_hf_slug",
        "model_name", "author",
        "pipeline_tag", "library_name", "license", "languages", "tags",
        "downloads", "likes", "created_at", "last_modified",
        "private", "gated", "disabled", "sha",
        "model_type", "architectures", "auto_model", "processor",
        "parameters", "parameters_readable", "used_storage_bytes",
        "region", "arxiv_ids",
        "model_description", "params_raw", "model_size_raw",
    ]
    final_cols = [c for c in col_order if c in out_df.columns] + [c for c in out_df.columns if c not in col_order]
    out_df = out_df[final_cols]

    out_df.to_csv(args.output, index=False)
    print(f"Wrote {args.output} with {len(out_df)} models.")

if __name__ == "__main__":
    main()
