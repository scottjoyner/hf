#!/usr/bin/env python3
import argparse
import os
import shlex
import sys
from pathlib import Path
from subprocess import CalledProcessError, run

# ---------- config helpers ----------

def env_path(name: str, default: str) -> Path:
    return Path(os.getenv(name, default)).resolve()

DATA_DIR   = env_path("DATA_DIR",   "/app/data")
CACHE_DIR  = env_path("CACHE_DIR",  "/app/cache")
OUT_DIR    = env_path("OUT_DIR",    "/app/hf_models")
DB_PATH    = os.getenv("DB_PATH",   "/app/db/models.db")

MODELS_CSV         = (DATA_DIR / "models.csv")
MODELS_ENRICHED_CSV= (DATA_DIR / "models_enriched.csv")
MODEL_META_CSV     = (DATA_DIR / "model_metadata.csv")
MODEL_FILES_CSV    = (DATA_DIR / "model_files.csv")

# S3 (optional)
S3_BUCKET  = os.getenv("S3_BUCKET", "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))

# MinIO (optional; S3 compatible)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "")).strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "")).strip()

# ---------- utils ----------

def info(msg: str) -> None:
    print(f"[worker] {msg}", flush=True)

def ensure_dirs():
    for p in [DATA_DIR, CACHE_DIR, OUT_DIR, Path(DB_PATH).parent]:
        p.mkdir(parents=True, exist_ok=True)

def sh(cmd, check=True, env=None, cwd=None):
    """
    Run a shell command, streaming output. Accepts list or string.
    """
    if isinstance(cmd, str):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = cmd
    info(f"$ {' '.join(shlex.quote(c) for c in cmd_list)}")
    return run(cmd_list, check=check, env=env, cwd=cwd)

# ---------- steps ----------

def step_db_init():
    """
    Create the SQLite schema if needed.
    """
    ensure_dirs()
    try:
        # use the python API directly for reliability
        from scripts.models_db import init_db
    except Exception as e:
        info(f"models_db import failed: {e}")
        sys.exit(1)

    init_db(DB_PATH)
    info(f"DB ready at {DB_PATH}")

def step_scrape():
    """
    Build data/models_enriched.csv from data/models.csv and cache/.
    """
    ensure_dirs()
    if not MODELS_CSV.exists():
        info(f"Input CSV not found: {MODELS_CSV}")
        sys.exit(2)

    sh([
        "python", "scripts/scrape.py",
        "--input", str(MODELS_CSV),
        "--output", str(MODELS_ENRICHED_CSV),
    ])

def step_download():
    """
    Download model binaries listed in models_enriched.csv to OUT_DIR.
    """
    ensure_dirs()
    if not MODELS_ENRICHED_CSV.exists():
        info(f"Need {MODELS_ENRICHED_CSV}. Run 'worker scrape' first.")
        sys.exit(3)

    sh([
        "python", "scripts/download.py",
        "--input", str(MODELS_ENRICHED_CSV),
        "--out-dir", str(OUT_DIR),
        "--patterns", os.getenv("DOWNLOAD_PATTERNS", "weights"),
        "--revision", os.getenv("HF_REVISION", "main"),
    ])

def step_metadata(write_db: bool = True):
    """
    Build model metadata CSV (and optionally write into SQLite).
    """
    ensure_dirs()
    if not MODELS_ENRICHED_CSV.exists():
        info(f"Need {MODELS_ENRICHED_CSV}. Run 'worker scrape' first.")
        sys.exit(4)

    cmd = [
        "python", "scripts/build_model_metadata.py",
        "--input", str(MODELS_ENRICHED_CSV),
        "--cache", str(CACHE_DIR),
        "--output-csv", str(MODEL_META_CSV),
        "--emit-files", str(MODEL_FILES_CSV),
    ]
    if write_db:
        cmd += ["--write-db", "--db-path", DB_PATH]
    sh(cmd)

def step_sync():
    """
    Push OUT_DIR (hf_models) to:
      - MinIO bucket (via mc), if MINIO_BUCKET + creds are present
      - S3 bucket (via awscli), if S3_BUCKET + creds are present
    """
    ensure_dirs()

    # ---- MinIO via mc ----
    if MINIO_BUCKET and MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
        try:
            # Configure mc alias "local"
            env = {
                **os.environ,
                "MC_HOST_local": f"{MINIO_ENDPOINT}",
                "MC_ACCESS_KEY": MINIO_ACCESS_KEY,
                "MC_SECRET_KEY": MINIO_SECRET_KEY,
            }
            # Safer: set alias with explicit creds
            sh([
                "mc", "alias", "set", "local", MINIO_ENDPOINT,
                MINIO_ACCESS_KEY, MINIO_SECRET_KEY
            ], check=False, env=env)

            # Ensure bucket exists, then mirror
            sh(["mc", "mb", "-p", f"local/{MINIO_BUCKET}"], check=False, env=env)
            sh([
                "mc", "mirror",
                "--overwrite",
                "--remove",
                str(OUT_DIR) + "/",         # trailing slash: copy contents
                f"local/{MINIO_BUCKET}/"    # trailing slash too
            ], env=env)
            info(f"MinIO sync complete → {MINIO_ENDPOINT}/{MINIO_BUCKET}")
        except FileNotFoundError:
            info("mc not found in image; skipping MinIO sync")

    else:
        info("MinIO env not fully set; skipping MinIO sync")

    # ---- AWS S3 via awscli ----
    if S3_BUCKET and os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        try:
            sh([
                "aws", "s3", "sync",
                str(OUT_DIR) + "/", f"s3://{S3_BUCKET}/",
                "--no-progress",
                "--only-show-errors",
                "--region", AWS_REGION,
            ])
            info(f"S3 sync complete → s3://{S3_BUCKET}/")
        except FileNotFoundError:
            info("aws CLI not found in image; skipping S3 sync")
    else:
        info("S3 env not fully set; skipping S3 sync")

def step_db_web(host="0.0.0.0", port=8080):
    """
    Serve the DB with sqlite-web (lightweight UI).
    """
    ensure_dirs()
    if not Path(DB_PATH).exists():
        step_db_init()

    try:
        sh([
            "sqlite_web",
            "--host", host,
            "--port", str(port),
            "--no-browser",
            DB_PATH
        ])
    except FileNotFoundError:
        info("sqlite_web is not installed in the image.")
        sys.exit(5)

# ---------- cli ----------

def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    ap = argparse.ArgumentParser(prog="worker", description="Models pipeline worker")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("help")

    sub.add_parser("db-init")

    sub.add_parser("scrape")
    sub.add_parser("download")

    p_meta = sub.add_parser("metadata")
    p_meta.add_argument("--no-db", action="store_true", help="Do not write to DB")

    sub.add_parser("sync")

    p_web = sub.add_parser("db-web")
    p_web.add_argument("--host", default="0.0.0.0")
    p_web.add_argument("--port", default="8080")

    sub.add_parser("all")

    args = ap.parse_args(argv)

    if args.cmd in (None, "help"):
        ap.print_help()
        print("""
Examples:
  worker db-init
  worker scrape
  worker download
  worker metadata
  worker sync
  worker db-web --port 8080
  worker all   # run the whole pipeline
""")
        return 0

    try:
        if args.cmd == "db-init":
            step_db_init()
        elif args.cmd == "scrape":
            step_scrape()
        elif args.cmd == "download":
            step_download()
        elif args.cmd == "metadata":
            step_metadata(write_db=not getattr(args, "no-db", False))
        elif args.cmd == "sync":
            step_sync()
        elif args.cmd == "db-web":
            step_db_web(host=args.host, port=int(args.port))
        elif args.cmd == "all":
            info("pipeline: db-init -> scrape -> download -> metadata -> sync")
            step_db_init()
            step_scrape()
            step_download()
            step_metadata(write_db=True)
            step_sync()
        else:
            print(f"Unknown subcommand: {args.cmd}", file=sys.stderr)
            return 2
    except CalledProcessError as e:
        return e.returncode
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
