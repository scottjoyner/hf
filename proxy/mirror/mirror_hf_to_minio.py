#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mirror approved Hugging Face repos into an internal S3/MinIO bucket with manifest & hashing.

- Respects LICENSE_ALLOWLIST
- Optional deny-lists (repos/files)
- Retries & resumable downloads via huggingface_hub
- Writes manifest.csv with SHA256, size, license, revision
- Uploads to MinIO/S3 using MinIO SDK
"""
from __future__ import annotations

import csv
import hashlib
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from huggingface_hub import HfApi, snapshot_download, hf_hub_url, list_repo_files, ModelInfo
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

# ---------- Config ----------

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

HF_TOKEN = os.getenv("HF_TOKEN", "").strip()
HF_REPOS = [x.strip() for x in os.getenv("HF_REPOS", "").split(",") if x.strip()]
HF_REVISIONS = [x.strip() for x in os.getenv("HF_REVISIONS", "").split(",") if x.strip()]
LICENSE_ALLOWLIST = {x.strip().lower() for x in os.getenv("LICENSE_ALLOWLIST", "").split(",") if x.strip()}
REPO_DENYLIST = os.getenv("REPO_DENYLIST", "").strip()
FILE_DENYLIST = os.getenv("FILE_DENYLIST", "").strip()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "models").strip()
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "mirrors/hf").strip().strip("/")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "").strip()
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in {"1","true","yes"}

HTTP_PROXY = os.getenv("HTTP_PROXY", "").strip() or None
HTTPS_PROXY = os.getenv("HTTPS_PROXY", "").strip() or None
NO_PROXY = os.getenv("NO_PROXY", "").strip() or None

DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1","true","yes"}

# ---------- Utilities ----------

def _regex_or_none(pat: str) -> Optional[re.Pattern]:
    return re.compile(pat) if pat else None

REPO_DENY = _regex_or_none(REPO_DENYLIST)
FILE_DENY = _regex_or_none(FILE_DENYLIST)

def sha256_of_file(path: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def s3_key_for(repo_id: str, relpath: Path) -> str:
    # Store under: <prefix>/<repo_id>/<relpath>
    repo_id_clean = repo_id.replace(":", "_")
    return "/".join([MINIO_PREFIX, repo_id_clean, relpath.as_posix()]).strip("/")

def ensure_bucket(client: Minio, bucket: str):
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket, location=MINIO_REGION)

# ---------- Main ----------

def main() -> int:
    if not HF_REPOS:
        print("No HF_REPOS provided. Set HF_REPOS in .env", file=sys.stderr)
        return 2

    if not HF_TOKEN:
        print("Warning: No HF_TOKEN set; public repos only.", file=sys.stderr)

    api = HfApi(token=HF_TOKEN if HF_TOKEN else None)

    client = Minio(
        endpoint=MINIO_ENDPOINT.replace("http://","").replace("https://",""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
        region=MINIO_REGION,
    )

    manifest_path = DATA_DIR / "manifest.csv"
    fieldnames = [
        "repo_id","revision","license","filename","size_bytes","sha256","s3_key"
    ]

    rows: List[dict] = []
    ensure_bucket(client, MINIO_BUCKET)

    for idx, repo in enumerate(HF_REPOS):
        if REPO_DENY and REPO_DENY.search(repo):
            print(f"[skip] {repo} matches REPO_DENYLIST", file=sys.stderr)
            continue

        revision = HF_REVISIONS[idx] if idx < len(HF_REVISIONS) and HF_REVISIONS[idx] else None

        # Get license & sanity check
        try:
            info = api.model_info(repo, token=HF_TOKEN)  # Model-only; if datasets, switch to dataset_info
            license_id = (info.license or "").lower()
        except Exception as e:
            print(f"[warn] Could not fetch model_info for {repo}: {e}", file=sys.stderr)
            license_id = ""

        if LICENSE_ALLOWLIST and license_id and license_id not in LICENSE_ALLOWLIST:
            print(f"[skip] {repo} license '{license_id}' not in allowlist", file=sys.stderr)
            continue

        print(f"[sync] {repo} (license={license_id or 'unknown'}) revision={revision or 'default'}")

        local_dir = snapshot_download(
            repo_id=repo,
            revision=revision,
            token=HF_TOKEN if HF_TOKEN else None,
            resume_download=True,
            local_dir=DATA_DIR / "cache" / repo.replace("/","___"),
            local_dir_use_symlinks=False,
        )

        basepath = Path(local_dir)
        files = [p for p in basepath.rglob("*") if p.is_file()]
        for p in tqdm(files, desc=f"Uploading {repo}"):
            rel = p.relative_to(basepath)
            if FILE_DENY and FILE_DENY.search(str(rel)):
                continue

            sha = sha256_of_file(p)
            size = p.stat().st_size
            key = s3_key_for(repo, rel)

            if not DRY_RUN:
                # Upload if not present or size differs
                try:
                    # Quick existence check
                    st = client.stat_object(MINIO_BUCKET, key)
                    if st.size == size:
                        # assume OK (could harden by storing sha in metadata and comparing)
                        pass
                    else:
                        client.fput_object(MINIO_BUCKET, key, str(p))
                except S3Error:
                    client.fput_object(MINIO_BUCKET, key, str(p))

            rows.append({
                "repo_id": repo,
                "revision": revision or "",
                "license": license_id,
                "filename": str(rel).replace("\\","/"),
                "size_bytes": size,
                "sha256": sha,
                "s3_key": key,
            })

        # TODO: SBOM/provenance/cosign hook here if you need attestations

    # Write manifest
    with manifest_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"[ok] Wrote manifest: {manifest_path}")
    print(f"[ok] Mirrored {len(rows)} objects.")
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
