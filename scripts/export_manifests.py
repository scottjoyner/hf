#!/usr/bin/env python3
import argparse, json, os, sqlite3, time
from pathlib import Path
from typing import Dict, List, Any, Optional

from minio import Minio
from minio.error import S3Error

DB_PATH = os.environ.get("DB_PATH", "/app/db/models.db")

def connect(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    return c

def fetch_models(c: sqlite3.Connection):
    return c.execute("""
      SELECT repo_id, COALESCE(last_update_ts,0) AS updated_ts
        FROM models ORDER BY repo_id ASC;""").fetchall()

def fetch_files(c: sqlite3.Connection, repo_id: str):
    return c.execute("""
      SELECT rfilename, size, sha256, updated_ts
        FROM files WHERE repo_id=? ORDER BY rfilename ASC;""", (repo_id,)).fetchall()

def object_key_for(c: sqlite3.Connection, repo_id: str, rfilename: str) -> str:
    row = c.execute("""
      SELECT object_key FROM uploads
       WHERE repo_id=? AND rfilename=? AND target IN ('minio','s3')
       ORDER BY uploaded_ts DESC LIMIT 1;""", (repo_id, rfilename)).fetchone()
    if row and row["object_key"]:
        return row["object_key"]
    return f"hf/{repo_id.strip('/')}/{rfilename}"

def write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))

def ensure_bucket(mc: Minio, bucket: str):
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)

def upload_json(mc: Minio, bucket: str, object_key: str, data: Any):
    from io import BytesIO
    blob = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    mc.put_object(bucket, object_key, BytesIO(blob), length=len(blob),
                  content_type="application/json")

def run_once(out_dir: Path, publish: bool,
             minio_endpoint: str, access_key: str, secret_key: str, secure: bool, bucket: str):
    c = connect(DB_PATH)
    models = fetch_models(c)
    index: List[Dict[str, Any]] = []
    mc: Optional[Minio] = None
    if publish:
        mc = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        ensure_bucket(mc, bucket)

    for m in models:
        rid = m["repo_id"]; uts = int(m["updated_ts"] or 0)
        files = fetch_files(c, rid)
        items = []
        for f in files:
            ok = object_key_for(c, rid, f["rfilename"])
            items.append({
                "rfilename": f["rfilename"],
                "size": f["size"],
                "sha256": f["sha256"],
                "updated_ts": f["updated_ts"],
                "object_key": ok
            })
        manifest = {"schema": "hf-model-manifest/v1", "repo_id": rid, "updated_ts": uts, "files": items}
        # write locally
        local_path = out_dir / "manifests" / f"{rid.replace('/','__')}.json"
        write_json(local_path, manifest)
        # publish to MinIO
        if mc:
            upload_json(mc, bucket, f"manifests/{rid.replace('/','__')}.json", manifest)

        index.append({"repo_id": rid, "updated_ts": uts})

    write_json(out_dir / "index.json", {"generated_ts": int(time.time()), "items": index})
    if mc:
        upload_json(mc, bucket, "manifests/index.json", {"generated_ts": int(time.time()), "items": index})
    print(f"[export] wrote {len(index)} manifests to {out_dir} {'and MinIO' if mc else ''}")

def main():
    ap = argparse.ArgumentParser(description="Export per-model manifests and index.")
    ap.add_argument("--out-dir", default="/app/data/manifests")
    ap.add_argument("--publish", action="store_true", help="Also publish to MinIO")
    ap.add_argument("--every", type=int, default=0, help="Loop and run every N seconds (0=once)")
    ap.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "minio:9000"))
    ap.add_argument("--minio-access",   default=os.environ.get("MINIO_ROOT_USER", ""))
    ap.add_argument("--minio-secret",   default=os.environ.get("MINIO_ROOT_PASSWORD", ""))
    ap.add_argument("--minio-bucket",   default=os.environ.get("MINIO_BUCKET", "models"))
    ap.add_argument("--minio-secure",   action="store_true")
    args = ap.parse_args()

    out = Path(args.out_dir)
    if args.every <= 0:
        run_once(out, args.publish, args.minio_endpoint, args.minio_access, args.minio_secret, args.minio_secure, args.minio_bucket)
    else:
        while True:
            try:
                run_once(out, args.publish, args.minio_endpoint, args.minio_access, args.minio_secret, args.minio_secure, args.minio_bucket)
            except S3Error as e:
                print(f"[export] minio error: {e}", flush=True)
            except Exception as e:
                print(f"[export] error: {e}", flush=True)
            time.sleep(args.every)

if __name__ == "__main__":
    main()
