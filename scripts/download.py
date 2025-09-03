#!/usr/bin/env python3
"""
Download Hugging Face model binaries for a list of models.
(Adapted to run under Docker.)
"""

import argparse
import csv
import fnmatch
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests
from tqdm import tqdm

# --------------------------
# Configuration
# --------------------------
CACHE_DIR = "cache"  # must match your loader's cache dir
DEFAULT_OUT_DIR = "hf_models"
DEFAULT_REVISION = "main"

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", "").strip()
BASE_HEADERS = {"User-Agent": "hf-downloader/1.0"}
if HUGGINGFACE_TOKEN:
    BASE_HEADERS["Authorization"] = f"Bearer {HUGGINGFACE_TOKEN}"

# Reasonable default set focused on binaries + required runtime files
DEFAULT_WEIGHT_PATTERNS = [
    # Weights / shards
    "*.safetensors", "*.bin", "*safetensors.index.json", "*bin.index.json",
    # Alt formats
    "*.onnx", "*.tflite", "*.gguf", "*.pt",
    # Core configs & tokenizer
    "config.json", "generation_config.json", "preprocessor_config.json",
    "tokenizer.json", "tokenizer.model", "spiece.model", "vocab.*", "merges.txt",
    "special_tokens_map.json",
]

def sanitize_repo_id(repo_id: str) -> str:
    return repo_id.strip().strip("/")

def is_hf_url(url: str) -> bool:
    return isinstance(url, str) and url.startswith("https://huggingface.co/")

def extract_repo_id_from_url(url: str) -> Optional[str]:
    if not is_hf_url(url):
        return None
    parts = url.split("huggingface.co/")[-1].split("?")[0].split("#")[0].strip("/")
    bits = parts.split("/")
    if len(bits) >= 2:
        return f"{bits[0]}/{bits[1]}"
    return None

def resolve_repo_id(row: dict) -> Optional[str]:
    if "updated_url" in row and isinstance(row["updated_url"], str):
        rid = extract_repo_id_from_url(row["updated_url"])
        if rid:
            return sanitize_repo_id(rid)
    for key in ("repo_id", "model_id"):
        if key in row and isinstance(row[key], str) and "/" in row[key]:
            return sanitize_repo_id(row[key])
    if "url" in row and isinstance(row["url"], str):
        rid = extract_repo_id_from_url(row["url"])
        if rid:
            return sanitize_repo_id(rid)
    if "model_name" in row and isinstance(row["model_name"], str) and "/" in row["model_name"]:
        return sanitize_repo_id(row["model_name"])
    return None

def cache_path_for_repo(repo_id: str) -> Path:
    safe = repo_id.replace("/", "__")
    return Path(CACHE_DIR) / f"{safe}.json"

def load_or_fetch_model_json(repo_id: str, revision: str, sleep_s: float = 0.5) -> Optional[dict]:
    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    cpath = cache_path_for_repo(repo_id)
    if cpath.exists():
        try:
            return json.loads(cpath.read_text())
        except Exception:
            pass
    api_url = f"https://huggingface.co/api/models/{repo_id}"
    try:
        r = requests.get(api_url, headers=BASE_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        cpath.write_text(json.dumps(data, indent=2))
        time.sleep(sleep_s)
        return data
    except requests.HTTPError as e:
        print(f"[WARN] API error for {repo_id}: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[WARN] Failed to fetch {repo_id}: {e}", file=sys.stderr)
        return None

def list_siblings(model_json: dict) -> List[dict]:
    if not model_json:
        return []
    sibs = model_json.get("siblings") or []
    out = []
    for s in sibs:
        rf = s.get("rfilename")
        if not rf:
            continue
        out.append({"rfilename": rf, "size": s.get("size")})
    return out

def choose_files(siblings: List[dict], patterns: List[str]) -> List[dict]:
    selected = []
    for s in siblings:
        name = s["rfilename"]
        if any(fnmatch.fnmatch(name, pat) for pat in patterns):
            selected.append(s)
    return selected

def make_dest_path(root: Path, repo_id: str, rfilename: str) -> Path:
    return root / repo_id / rfilename

def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def link_or_copy(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    try:
        if dst.exists():
            return
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)

def download_file(repo_id: str, rfilename: str, dest: Path, headers: Dict[str, str], revision: str, expected_size: Optional[int] = None) -> bool:
    ensure_parent(dest)
    url = f"https://huggingface.co/{repo_id}/resolve/{revision}/{rfilename}"
    tmp = dest.with_suffix(dest.suffix + ".part")

    resume_pos = 0
    if tmp.exists():
        resume_pos = tmp.stat().st_size
    elif dest.exists():
        if expected_size and dest.stat().st_size == expected_size:
            return True
        if expected_size is None:
            return True

    h = dict(headers)
    if resume_pos > 0:
        h["Range"] = f"bytes={resume_pos}-"

    with requests.get(url, headers=h, stream=True, timeout=60) as r:
        if r.status_code == 416:
            tmp.rename(dest)
            return True
        if r.status_code in (401, 403):
            print(f"[WARN] Access denied for {repo_id}:{rfilename} (auth required?)", file=sys.stderr)
            return False
        if r.status_code not in (200, 206):
            print(f"[WARN] HTTP {r.status_code} for {url}", file=sys.stderr)
            return False

        mode = "ab" if r.status_code == 206 and resume_pos > 0 else "wb"
        if mode == "wb" and tmp.exists():
            tmp.unlink(missing_ok=True)

        total = None
        if expected_size is not None:
            total = expected_size
            if r.status_code == 206:
                total = expected_size - resume_pos
        else:
            if "Content-Length" in r.headers:
                try:
                    clen = int(r.headers["Content-Length"])
                    total = clen
                except Exception:
                    total = None

        chunk_iter = r.iter_content(chunk_size=1024 * 1024)
        from tqdm import tqdm
        with open(tmp, mode) as f, tqdm(
            total=total if total and total > 0 else None,
            unit="B", unit_scale=True, unit_divisor=1024,
            desc=f"{repo_id}/{Path(rfilename).name}",
            leave=False,
        ) as pbar:
            for chunk in chunk_iter:
                if not chunk:
                    continue
                f.write(chunk)
                if pbar.total is not None:
                    pbar.update(len(chunk))

    if expected_size and tmp.stat().st_size != expected_size:
        print(f"[INFO] Size mismatch for {dest.name}: got {tmp.stat().st_size}, expected {expected_size}", file=sys.stderr)
    tmp.rename(dest)
    return True

def main():
    ap = argparse.ArgumentParser(description="Download HF model binaries for a CSV of models.")
    ap.add_argument("--input", default="data/models.csv", help="Path to input CSV")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Root folder to place downloaded files.")
    ap.add_argument("--copy-dir", default=None, help="Optional second directory to also copy/hardlink files to.")
    ap.add_argument("--revision", default=DEFAULT_REVISION, help="Branch/tag/commit to resolve from (default: main).")
    ap.add_argument("--patterns", default="weights", help='One of: "weights", "all", or comma-separated globs.')
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep between model metadata fetches (seconds).")
    ap.add_argument("--dry-run", action="store_true", help="List what would be downloaded without fetching files.")
    args = ap.parse_args()

    out_root = Path(args.out_dir)
    copy_root = Path(args.copy_dir) if args.copy_dir else None
    out_root.mkdir(parents=True, exist_ok=True)
    if copy_root:
        copy_root.mkdir(parents=True, exist_ok=True)

    if args.patterns == "weights":
        patterns = DEFAULT_WEIGHT_PATTERNS
    elif args.patterns == "all":
        patterns = ["*"]
    else:
        patterns = [p.strip() for p in args.patterns.split(",") if p.strip()]

    try:
        df = pd.read_csv(args.input)
    except Exception:
        rows = list(csv.DictReader(open(args.input, newline="", encoding="utf-8")))
        import pandas as pd
        df = pd.DataFrame(rows)

    processed: Set[str] = set()
    ok_models, fail_models = 0, 0
    total_rows = len(df)

    for _, row in tqdm(df.iterrows(), total=total_rows, desc="Models"):
        rid = resolve_repo_id(row.to_dict())
        if not rid:
            fail_models += 1
            continue
        if rid in processed:
            continue
        processed.add(rid)

        model_json = load_or_fetch_model_json(rid, args.revision, sleep_s=args.sleep)
        if not model_json:
            print(f"[WARN] Skipping {rid} (no metadata).", file=sys.stderr)
            fail_models += 1
            continue

        siblings = list_siblings(model_json)
        if not siblings:
            print(f"[WARN] No files listed for {rid}.", file=sys.stderr)
            fail_models += 1
            continue

        files = siblings if patterns == ["*"] else choose_files(siblings, patterns)
        if not files:
            print(f"[WARN] Patterns matched no files for {rid}.", file=sys.stderr)
            ok_models += 1
            continue

        if args.dry_run:
            print(f"[DRY] {rid}:")
            for s in files:
                print(f"   - {s['rfilename']}  ({s.get('size','?')} bytes)")
            ok_models += 1
            continue

        all_ok = True
        for s in files:
            rfilename = s["rfilename"]
            size = s.get("size")
            dest = make_dest_path(out_root, rid, rfilename)
            ok = download_file(rid, rfilename, dest, BASE_HEADERS, args.revision, expected_size=size)
            if not ok:
                all_ok = False
                continue
            if copy_root:
                mirror = make_dest_path(copy_root, rid, rfilename)
                try:
                    link_or_copy(dest, mirror)
                except Exception as e:
                    print(f"[WARN] Mirror copy failed for {mirror}: {e}", file=sys.stderr)

        if all_ok:
            ok_models += 1
        else:
            fail_models += 1

    print(f"Done. Models OK: {ok_models} | Models with issues: {fail_models} | Unique models processed: {len(processed)}")
    if not HUGGINGFACE_TOKEN:
        print("Note: Set HUGGINGFACE_TOKEN for private/gated repos.", file=sys.stderr)

if __name__ == "__main__":
    main()
"""
"""
