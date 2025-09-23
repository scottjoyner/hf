#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mirror approved Hugging Face repos into an internal S3/MinIO bucket.

- Respects LICENSE_ALLOWLIST
- Optional deny-lists (repos/files)
- Streams downloads through the Kerberos-aware proxy without local copies
- Writes ``manifest.csv`` with SHA256, size, license, revision
- Uploads to MinIO/S3 using MinIO SDK
"""
from __future__ import annotations

import csv
import hashlib
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from scripts.http_client import create_session_from_env

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

HF_ENDPOINT = os.getenv("HF_ENDPOINT", "https://huggingface.co").rstrip("/")
HF_API_BASE = os.getenv("HF_API_BASE", f"{HF_ENDPOINT}/api")
HF_TIMEOUT = float(os.getenv("HF_HTTP_TIMEOUT", "30"))
DOWNLOAD_CHUNK_SIZE = int(os.getenv("DOWNLOAD_CHUNK_SIZE", str(1 << 20)))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "models").strip()
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "mirrors/hf").strip().strip("/")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "").strip()
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in {"1","true","yes"}

DRY_RUN = os.getenv("DRY_RUN", "").lower() in {"1","true","yes"}

_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = create_session_from_env()
    return _SESSION


def _auth_headers(token: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": os.getenv("HF_USER_AGENT", "hf-mirror/1.0"),
        "Accept": "application/json",
    }
    tok = token or HF_TOKEN
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    return headers


def _hf_get(path: str, *, params: Optional[Dict[str, str]] = None, token: Optional[str] = None) -> requests.Response:
    url = f"{HF_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    session = _get_session()
    resp = session.get(url, params=params or {}, headers=_auth_headers(token), timeout=HF_TIMEOUT)
    resp.raise_for_status()
    return resp


def _hf_download_url(repo_id: str, revision: Optional[str], filename: str) -> str:
    repo = quote(repo_id, safe="")
    rev = quote(revision or "main", safe="")
    parts = [quote(part, safe="") for part in filename.split("/")]
    path = "/".join(parts)
    return f"{HF_ENDPOINT}/{repo}/resolve/{rev}/{path}"


class _HashingStream:
    def __init__(self, response: requests.Response, chunk_size: int = DOWNLOAD_CHUNK_SIZE) -> None:
        self._raw = response.raw
        self._raw.decode_content = True
        self._chunk = chunk_size
        self.sha256 = hashlib.sha256()
        self.total = 0

    def read(self, size: Optional[int] = None) -> bytes:
        chunk_size = size if size and size > 0 else self._chunk
        data = self._raw.read(chunk_size)
        if data:
            self.sha256.update(data)
            self.total += len(data)
        return data or b""

# ---------- Utilities ----------

def _regex_or_none(pat: str) -> Optional[re.Pattern]:
    return re.compile(pat) if pat else None

REPO_DENY = _regex_or_none(REPO_DENYLIST)
FILE_DENY = _regex_or_none(FILE_DENYLIST)

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

    session = _get_session()

    for idx, repo in enumerate(HF_REPOS):
        if REPO_DENY and REPO_DENY.search(repo):
            print(f"[skip] {repo} matches REPO_DENYLIST", file=sys.stderr)
            continue

        revision = HF_REVISIONS[idx] if idx < len(HF_REVISIONS) and HF_REVISIONS[idx] else None

        try:
            manifest = _hf_get(f"models/{quote(repo, safe='')}", params={"revision": revision} if revision else None).json()
        except Exception as e:
            print(f"[warn] Could not fetch manifest for {repo}: {e}", file=sys.stderr)
            continue

        license_id = (manifest.get("license") or "").lower()

        if LICENSE_ALLOWLIST and license_id and license_id not in LICENSE_ALLOWLIST:
            print(f"[skip] {repo} license '{license_id}' not in allowlist", file=sys.stderr)
            continue

        print(f"[sync] {repo} (license={license_id or 'unknown'}) revision={revision or 'default'}")
        siblings = manifest.get("siblings") or []
        resolved_revision = revision or manifest.get("sha") or "main"

        for file_info in tqdm(siblings, desc=f"Uploading {repo}"):
            rfilename = file_info.get("rfilename") if isinstance(file_info, dict) else None
            if not rfilename:
                continue
            if FILE_DENY and FILE_DENY.search(rfilename):
                continue

            relpath = Path(rfilename)
            key = s3_key_for(repo, relpath)
            expected_sha = file_info.get("sha256") or ((file_info.get("lfs") or {}).get("sha256") if isinstance(file_info, dict) else None)
            expected_size = file_info.get("size") or ((file_info.get("lfs") or {}).get("size") if isinstance(file_info, dict) else None)

            size_bytes = expected_size or 0
            sha_hex = expected_sha or ""

            if DRY_RUN:
                rows.append({
                    "repo_id": repo,
                    "revision": revision or "",
                    "license": license_id,
                    "filename": rfilename,
                    "size_bytes": size_bytes,
                    "sha256": sha_hex,
                    "s3_key": key,
                })
                continue

            skip_existing = False
            if expected_size:
                try:
                    st = client.stat_object(MINIO_BUCKET, key)
                    if st.size == expected_size:
                        size_bytes = st.size
                        skip_existing = True
                except S3Error:
                    pass

            if not skip_existing:
                url = _hf_download_url(repo, resolved_revision, rfilename)
                headers = _auth_headers()
                headers.pop("Accept", None)
                headers["Accept"] = "application/octet-stream"
                with session.get(url, headers=headers, stream=True, timeout=max(600.0, HF_TIMEOUT)) as resp:
                    resp.raise_for_status()
                    stream = _HashingStream(resp, DOWNLOAD_CHUNK_SIZE)
                    length_header = resp.headers.get("Content-Length")
                    try:
                        length = int(length_header) if length_header is not None else -1
                    except ValueError:
                        length = -1
                    part_size = max(DOWNLOAD_CHUNK_SIZE, 10 * 1024 * 1024)
                    if length >= 0:
                        client.put_object(MINIO_BUCKET, key, stream, length)
                    else:
                        client.put_object(MINIO_BUCKET, key, stream, length, part_size=part_size)
                    sha_hex = stream.sha256.hexdigest()
                    size_bytes = stream.total

            rows.append({
                "repo_id": repo,
                "revision": revision or "",
                "license": license_id,
                "filename": rfilename,
                "size_bytes": size_bytes,
                "sha256": sha_hex,
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
