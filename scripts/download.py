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
try:
    from scripts.hf_normalize import canonical_repo_id_from_url, is_hf_url
except ModuleNotFoundError:
    # allow running as "python scripts/download.py" or different CWDs
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from scripts.hf_normalize import canonical_repo_id_from_url, is_hf_url

# from scripts.hf_normalize import canonical_repo_id_from_url, is_hf_url

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
    return canonical_repo_id_from_url(url) if is_hf_url(url) else None

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


# --- Direct upload + DB integration additions ---
import boto3
from botocore.exceptions import ClientError
from scripts.models_db import init_db, upsert_model, upsert_file, record_upload, compute_sha256

def extract_basic_model_fields(repo_id: str, js: dict) -> dict:
    # Minimal parse to populate 'models' table
    author = js.get("author") or (repo_id.split("/")[0] if "/" in repo_id else "")
    pipeline_tag = js.get("pipeline_tag") or js.get("transformersInfo", {}).get("pipeline_tag") \
                   or js.get("cardData", {}).get("pipeline_tag") or ""
    downloads = js.get("downloads") or 0
    likes = js.get("likes") or 0
    created_at = js.get("createdAt") or ""
    last_modified = js.get("lastModified") or ""
    tags = js.get("tags") or []
    languages = js.get("cardData", {}).get("language") or []
    license_spdx = ""
    if isinstance(tags, list):
        for t in tags:
            if isinstance(t, str) and t.lower().startswith("license:"):
                license_spdx = t.split(":",1)[-1]
                break
    # params
    st = js.get("safetensors") or {}
    params_int = st.get("total") if isinstance(st.get("total"), int) else None
    params_readable = ""
    if isinstance(params_int, int):
        if params_int >= 1_000_000_000:
            params_readable = f"{params_int/1_000_000_000:.2f}B"
        elif params_int >= 1_000_000:
            params_readable = f"{params_int/1_000_000:.2f}M"
        elif params_int >= 1_000:
            params_readable = f"{params_int/1_000:.2f}K"
        else:
            params_readable = str(params_int)

    return {
        "canonical_url": f"https://huggingface.co/{repo_id}",
        "model_name": repo_id.split("/")[-1],
        "author": author,
        "pipeline_tag": pipeline_tag,
        "license": license_spdx,
        "parameters": params_int,
        "parameters_readable": params_readable,
        "downloads": int(downloads) if isinstance(downloads, (int,float)) else 0,
        "likes": int(likes) if isinstance(likes, (int,float)) else 0,
        "created_at": created_at,
        "last_modified": last_modified,
        "languages": ";".join(languages) if isinstance(languages, list) else "",
        "tags": ";".join([t for t in tags if isinstance(t,str)]),
        "model_description": js.get("cardData", {}).get("summary","") if isinstance(js.get("cardData"), dict) else "",
        "params_raw": "",
        "model_size_raw": "",
    }

def ensure_bucket(s3_client, bucket: str, region: str, endpoint_url: Optional[str] = None):
    try:
        s3_client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if code not in (403, 404):
            raise
    create_params = {"Bucket": bucket}
    if region and (endpoint_url is None or "amazonaws.com" in (endpoint_url or "")):
        if region != "us-east-1":
            create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        s3_client.create_bucket(**create_params)
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise

def upload_minio(client, bucket: str, key: str, file_path: str) -> Optional[str]:
    try:
        client.upload_file(file_path, bucket, key)
        # Head to get ETag
        resp = client.head_object(Bucket=bucket, Key=key)
        return resp.get("ETag")
    except Exception as e:
        print(f"[WARN] MinIO upload failed for {key}: {e}", file=sys.stderr)
        return None

def get_env(name: str, default: Optional[str] = None):
    v = os.getenv(name)
    return v if v not in (None, "") else default

def main_with_db_and_upload():
    # Parse args again to extract custom flags, or rely on envs
    import argparse
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--db-path", default=os.getenv("DB_PATH", "/app/db/models.db"))
    ap.add_argument("--direct-upload", action="store_true", default=os.getenv("DIRECT_UPLOAD","0") == "1")
    ap.add_argument("--minio-url", default=get_env("MINIO_URL","http://minio:9000"))
    ap.add_argument("--minio-bucket", default=get_env("MINIO_BUCKET","models"))
    ap.add_argument("--minio-region", default=get_env("MINIO_REGION","us-east-1"))
    ap.add_argument("--minio-prefix", default=get_env("MINIO_KEY_PREFIX",""))
    ap.add_argument("--minio-access-key", default=get_env("MINIO_ROOT_USER", get_env("MINIO_ACCESS_KEY")))
    ap.add_argument("--minio-secret-key", default=get_env("MINIO_ROOT_PASSWORD", get_env("MINIO_SECRET_KEY")))
    # We accept unknown args to avoid clashing with original parser
    flags, unknown = ap.parse_known_args()

    # Initialize DB
    init_db(flags.db_path)

    # If direct upload desired, set up client + bucket
    minio_client = None
    if flags.direct_upload and flags.minio_access_key and flags.minio_secret_key and flags.minio_bucket:
        minio_client = boto3.client(
            "s3",
            endpoint_url=flags.minio_url,
            aws_access_key_id=flags.minio_access_key,
            aws_secret_access_key=flags.minio_secret_key,
            region_name=flags.minio_region,
        )
        ensure_bucket(minio_client, flags.minio_bucket, flags.minio_region, endpoint_url=flags.minio_url)

    # Re-run original main() but intercept loop via a tiny hook.
    # We'll monkeypatch download_file to compute sha + upload + DB writes.
    original_download_file = download_file

    def download_file_hook(repo_id, rfilename, dest, headers, revision, expected_size=None):
        ok = original_download_file(repo_id, rfilename, dest, headers, revision, expected_size)
        # Record local file info
        size = None
        if dest.exists():
            size = dest.stat().st_size
            sha256 = compute_sha256(str(dest))
        else:
            sha256 = None
        upsert_file(flags.db_path, repo_id, rfilename, size, sha256, str(dest) if dest.exists() else None)
        # Upload to MinIO
        if ok and minio_client is not None and dest.exists():
            rel = dest.relative_to(Path(args.out_dir)).as_posix() if 'args' in globals() else dest.name
            key = f"{flags.minio_prefix}/{rel}" if flags.minio_prefix else rel
            etag = upload_minio(minio_client, flags.minio_bucket, key, str(dest))
            record_upload(flags.db_path, repo_id, rfilename, "minio", flags.minio_bucket, key, etag)
        return ok

    globals()['download_file'] = download_file_hook

    # Wrap load_or_fetch_model_json to also upsert basic model row
    original_load = load_or_fetch_model_json
    def load_or_fetch_with_upsert(repo_id: str, revision: str, sleep_s: float = 0.5):
        js = original_load(repo_id, revision, sleep_s)
        if js:
            fields = extract_basic_model_fields(repo_id, js)
            upsert_model(flags.db_path, repo_id, fields)
        return js
    globals()['load_or_fetch_model_json'] = load_or_fetch_with_upsert

    # Call the original main()
    return main()

if __name__ == "__main__":
    # If invoked normally, run enhanced main to support DB+upload
    main_with_db_and_upload()
