#!/usr/bin/env python3
import argparse
import os
import pathlib
import sys
import concurrent.futures as cf
from typing import Optional
import boto3
from botocore.exceptions import ClientError

def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def iter_files(root: pathlib.Path):
    for p in root.rglob("*"):
        if p.is_file():
            yield p

def ensure_bucket(s3_client, bucket: str, region: str, endpoint_url: Optional[str] = None):
    try:
        s3_client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        if code not in (403, 404):
            raise
    # Create
    create_params = {"Bucket": bucket}
    if region and (endpoint_url is None or "amazonaws.com" in endpoint_url):
        # Only include CreateBucketConfiguration for AWS regions != us-east-1
        if region != "us-east-1":
            create_params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        s3_client.create_bucket(**create_params)
        print(f"[sync] Created bucket: {bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise

def upload_dir(s3_client, bucket: str, key_prefix: str, src_root: pathlib.Path):
    files = list(iter_files(src_root))
    total = len(files)
    print(f"[sync] Uploading {total} files to s3://{bucket}/{key_prefix or ''}")

    def _upload(p: pathlib.Path):
        rel = p.relative_to(src_root).as_posix()
        key = f"{key_prefix}/{rel}" if key_prefix else rel
        s3_client.upload_file(str(p), bucket, key)
        return key

    # Threaded uploads
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for i, key in enumerate(ex.map(_upload, files), start=1):
            if i % 50 == 0 or i == total:
                print(f"[sync] {i}/{total} uploaded...")

def main():
    ap = argparse.ArgumentParser(description="Sync a directory to MinIO and AWS S3 (optional).")
    ap.add_argument("--src", default="hf_models", help="Source directory to upload recursively")
    args = ap.parse_args()

    src_root = pathlib.Path(args.src).resolve()
    if not src_root.exists():
        print(f"[sync] Source not found: {src_root}", file=sys.stderr)
        sys.exit(1)

    # MinIO config (S3-compatible)
    MINIO_URL = env("MINIO_URL", "http://minio:9000")
    MINIO_BUCKET = env("MINIO_BUCKET", "models")
    MINIO_REGION = env("MINIO_REGION", "us-east-1")
    MINIO_KEY_PREFIX = env("MINIO_KEY_PREFIX", "")
    MINIO_ACCESS_KEY = env("MINIO_ROOT_USER", env("MINIO_ACCESS_KEY"))
    MINIO_SECRET_KEY = env("MINIO_ROOT_PASSWORD", env("MINIO_SECRET_KEY"))

    # AWS S3 backup (optional)
    AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = env("AWS_REGION", "us-east-1")
    AWS_S3_BUCKET = env("AWS_S3_BUCKET")
    AWS_S3_KEY_PREFIX = env("AWS_S3_KEY_PREFIX", "")

    # MinIO upload
    if MINIO_ACCESS_KEY and MINIO_SECRET_KEY and MINIO_BUCKET:
        print(f"[sync] Uploading to MinIO bucket '{MINIO_BUCKET}' at {MINIO_URL}")
        minio_s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
        )
        ensure_bucket(minio_s3, MINIO_BUCKET, MINIO_REGION, endpoint_url=MINIO_URL)
        upload_dir(minio_s3, MINIO_BUCKET, MINIO_KEY_PREFIX, src_root)
    else:
        print("[sync] MinIO environment not fully configured, skipping MinIO upload.")

    # AWS S3 upload (optional)
    if AWS_S3_BUCKET and AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        print(f"[sync] Uploading to AWS S3 bucket '{AWS_S3_BUCKET}' in {AWS_REGION}")
        aws_s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        ensure_bucket(aws_s3, AWS_S3_BUCKET, AWS_REGION)
        upload_dir(aws_s3, AWS_S3_BUCKET, AWS_S3_KEY_PREFIX, src_root)
    else:
        print("[sync] AWS S3 environment not fully configured, skipping AWS upload.")

if __name__ == "__main__":
    main()
