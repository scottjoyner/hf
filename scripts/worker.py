#!/usr/bin/env python3
import argparse
import os
import shlex
import sys
import csv
from pathlib import Path
from urllib.parse import quote
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

MINIO_ALIAS     = os.getenv("MINIO_ALIAS", "minio").strip()  # alias name to use in mc (not a hostname)
MINIO_PREFIX    = os.getenv("MINIO_PREFIX", "").strip()      # optional subfolder inside the bucket
MINIO_SECURE    = (os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes"))

# ---------- utils ----------

def _csv_has_rows(path: Path) -> bool:
    """Return True if CSV has at least one non-empty, non-comment data row beyond the header."""
    try:
        if not path.exists():
            return False
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return False
            for row in reader:
                if row and not str(row[0]).strip().startswith("#"):
                    # has at least one data row
                    return True
        return False
    except Exception as e:
        info(f"csv_has_rows error for {path}: {e}")
        return False

def _pick_models_input() -> Path:
    """Prefer models_enriched.csv if it has data, else fallback to models.csv; allow override via MODELS_INPUT."""
    override = os.getenv("MODELS_INPUT", "").strip()
    if override:
        p = Path(override)
        info(f"MODELS_INPUT override set: {p}")
        return p

    enriched = DATA_DIR / "models_enriched.csv"
    base = DATA_DIR / "models.csv"

    if _csv_has_rows(enriched):
        info(f"Using enriched models file: {enriched}")
        return enriched
    if _csv_has_rows(base):
        info(f"Using base models file: {base}")
        return base

    # Neither has rows; still return enriched (consistent path) but warn loudly
    info(f"⚠️ No data rows found in {enriched} or {base}. Downloader may exit with warning.")
    return enriched

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
    Download model binaries listed in models_enriched.csv (preferred) or models.csv to OUT_DIR.
    """
    ensure_dirs()
    input_path = _pick_models_input()

    info(f"Downloader input: {input_path}")
    sh([
        "python", "scripts/download.py",
        "--input", str(input_path),
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


def _normalize_minio_endpoint(ep: str) -> str:
    """
    Accepts 'minio:9000', 'http://minio:9000', or 'https://minio:9000' and returns a proper URL.
    Respects MINIO_SECURE if scheme is missing.
    """
    ep = (ep or "").strip()
    if ep.startswith("http://") or ep.startswith("https://"):
        return ep
    scheme = "https" if MINIO_SECURE else "http"
    return f"{scheme}://{ep}"

def _minio_env_with_alias(url: str, access_key: str, secret_key: str):
    """
    Build MC_HOST_<ALIAS> env so we don't depend on persisted mc config in the container.
    """
    ak = quote(access_key, safe="")
    sk = quote(secret_key, safe="")
    # url is like http(s)://host:port — we embed creds: http(s)://AK:SK@host:port
    if "://" not in url:
        raise ValueError(f"bad MINIO_ENDPOINT (no scheme): {url}")
    scheme, rest = url.split("://", 1)
    with_creds = f"{scheme}://{ak}:{sk}@{rest}"
    env = os.environ.copy()
    env[f"MC_HOST_{MINIO_ALIAS}"] = with_creds
    return env

def _path_join_bucket(alias: str, bucket: str, prefix: str = "") -> str:
    """
    Build 'alias/bucket[/prefix]' for mc.
    """
    if prefix:
        prefix = prefix.strip("/")
        return f"{alias}/{bucket}/{prefix}"
    return f"{alias}/{bucket}"

def _count_local_files(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(1 for _ in root.rglob("*") if _.is_file())

# ---------- REPLACED SYNC STEP ----------

def step_sync():
    """
    Push OUT_DIR (/app/hf_models) to MinIO and/or S3.
    - MinIO via mc using ephemeral env alias (no persistent config).
    - Optional subfolder prefix via MINIO_PREFIX.
    Fails fast on auth/permission issues.
    """
    ensure_dirs()

    # --- sanity: do we actually have files to push? ---
    file_count = _count_local_files(OUT_DIR)
    info(f"local OUT_DIR={OUT_DIR} files={file_count}")
    if file_count == 0:
        info("Nothing to upload from OUT_DIR; skipping sync.")
        # do not return nonzero; a 'no-op' sync is acceptable
        # (uncomment next line if you prefer hard failure)
        # sys.exit(20)

    # ---- MinIO via mc (recommended path) ----
    if MINIO_BUCKET and MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
        endpoint_url = _normalize_minio_endpoint(MINIO_ENDPOINT)
        env = _minio_env_with_alias(endpoint_url, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

        # Compose remote like:  <ALIAS>/<BUCKET>[/<PREFIX>]
        remote_root = _path_join_bucket(MINIO_ALIAS, MINIO_BUCKET, "")
        remote_path = _path_join_bucket(MINIO_ALIAS, MINIO_BUCKET, MINIO_PREFIX)

        info(f"MinIO alias={MINIO_ALIAS} endpoint={endpoint_url} bucket={MINIO_BUCKET} prefix={MINIO_PREFIX or '(none)'}")

        # 1) connectivity check (lists buckets or returns nonzero if no auth)
        try:
            sh(["mc", "ls", MINIO_ALIAS], check=True, env=env)
        except CalledProcessError as e:
            info(f"❌ Cannot reach MinIO alias '{MINIO_ALIAS}' at {endpoint_url}: {e}")
            sys.exit(e.returncode)

        # 2) ensure bucket exists (do NOT ignore auth failures)
        mb = sh(["mc", "mb", "-p", remote_root], check=False, env=env)
        # exit codes: 0 -> created; nonzero may be 'already exists' or 'denied'
        if mb.returncode != 0:
            # Distinguish 'already owned by you' vs auth. Try stat/ls to verify access.
            ls = sh(["mc", "ls", remote_root], check=False, env=env)
            if ls.returncode != 0:
                info("❌ Access denied or bucket not accessible with provided credentials.")
                info("    Double-check MINIO_* env vars inside the worker container.")
                # helpful dump (without secrets)
                info(f"    Debug: endpoint={endpoint_url} bucket={MINIO_BUCKET} alias={MINIO_ALIAS}")
                sys.exit(21)
            else:
                info(f"Bucket exists: {remote_root}")

        # 3) mirror local -> remote (respect prefix if provided)
        src = str(OUT_DIR) + "/"
        dst = remote_path + "/"
        info(f"Mirroring: {src}  →  {dst}")
        try:
            sh([
                "mc", "mirror",
                "--overwrite",
                "--remove",
                src, dst
            ], check=True, env=env)
            info(f"✅ MinIO sync complete → {endpoint_url}/{MINIO_BUCKET}{('/'+MINIO_PREFIX) if MINIO_PREFIX else ''}")
        except CalledProcessError as e:
            info(f"❌ MinIO mirror failed: {e}")
            sys.exit(e.returncode)
    else:
        info("MinIO env not fully set; skipping MinIO sync")

    # ---- Optional: S3 via awscli ----
    if S3_BUCKET and os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        try:
            sh([
                "aws", "s3", "sync",
                str(OUT_DIR) + "/", f"s3://{S3_BUCKET}/",
                "--no-progress",
                "--only-show-errors",
                "--region", AWS_REGION,
            ], check=True)
            info(f"✅ S3 sync complete → s3://{S3_BUCKET}/")
        except FileNotFoundError:
            info("aws CLI not found in image; skipping S3 sync")
        except CalledProcessError as e:
            info(f"❌ S3 sync failed: {e}")
            sys.exit(e.returncode)
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
