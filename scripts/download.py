#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import hashlib
import logging
import os
import sqlite3
import sys
import time
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, List
# --- Hugging Face imports (version-agnostic) ---
try:
    from huggingface_hub import snapshot_download
    try:
        # Newer public path
        from huggingface_hub.errors import HfHubHTTPError
    except Exception:
        try:
            # Older internal path
            from huggingface_hub.utils._errors import HfHubHTTPError
        except Exception:
            # Fallback: define a compatible exception
            class HfHubHTTPError(Exception):
                pass
except Exception:
    snapshot_download = None
    class HfHubHTTPError(Exception):
        pass



# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
LOG = logging.getLogger("download")
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ---------------------------------------------------------------------
# Config / defaults
# ---------------------------------------------------------------------
DEFAULT_DB_PATH = os.getenv("DB_PATH", "/app/db/models.db")
_CANON_RE = re.compile(r"https?://(?:www\.)?huggingface\.co/([^/\s]+)/([^/\s]+)")
_HF_URL_PATTERNS = [
    re.compile(r"^https?://(?:www\.)?huggingface\.co/([^/\s]+)/([^/\s?#]+)"),
    re.compile(r"^https?://(?:www\.)?hf\.co/([^/\s]+)/([^/\s?#]+)"),
]

# ---------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------
def connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _now_epoch() -> int:
    return int(time.time())

# ---------------------------------------------------------------------
# Desired schemas (authoritative shape)
# ---------------------------------------------------------------------
MODELS_COLUMNS: Dict[str, str] = {
    "repo_id": "TEXT PRIMARY KEY",
    "canonical_url": "TEXT",
    "name_hf_slug": "TEXT",
    "model_name": "TEXT",
    "author": "TEXT",
    "pipeline_tag": "TEXT",
    "library_name": "TEXT",
    "license": "TEXT",
    "languages": "TEXT",
    "tags": "TEXT",
    "downloads": "INTEGER",
    "likes": "INTEGER",
    "created_at": "TEXT",
    "last_modified": "TEXT",
    "private": "INTEGER",
    "gated": "INTEGER",
    "disabled": "INTEGER",
    "sha": "TEXT",
    "model_type": "TEXT",
    "architectures": "TEXT",
    "auto_model": "TEXT",
    "processor": "TEXT",
    "parameters": "INTEGER",
    "parameters_readable": "TEXT",
    "used_storage_bytes": "INTEGER",
    "region": "TEXT",
    "arxiv_ids": "TEXT",
    "model_description": "TEXT",
    "params_raw": "TEXT",
    "model_size_raw": "TEXT",
    "first_seen_ts": "INTEGER",
    "last_update_ts": "INTEGER",
}

FILES_COLUMNS: Dict[str, str] = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "repo_id": "TEXT NOT NULL",
    "rfilename": "TEXT NOT NULL",
    "size": "INTEGER",
    "sha256": "TEXT",
    "local_path": "TEXT",
    "storage_root": "TEXT",
    "created_ts": "INTEGER",
    "updated_ts": "INTEGER",
}

UPLOADS_COLUMNS: Dict[str, str] = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "repo_id": "TEXT NOT NULL",
    "rfilename": "TEXT NOT NULL",
    "target": "TEXT NOT NULL",
    "bucket": "TEXT NOT NULL",
    "object_key": "TEXT NOT NULL",
    "etag": "TEXT",
    "uploaded_ts": "INTEGER",
}

# ---------------------------------------------------------------------
# Introspection / migration utilities
# ---------------------------------------------------------------------
def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone())

def _columns(conn: sqlite3.Connection, table: str) -> Iterable[str]:
    return [r["name"] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]

def _add_column(conn: sqlite3.Connection, table: str, col: str, decl: str, default_sql: Optional[str] = None):
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
    if default_sql is not None:
        conn.execute(f"UPDATE {table} SET {col}={default_sql} WHERE {col} IS NULL;")

def _ensure_unique_index(conn: sqlite3.Connection, name: str, table: str, cols: str):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?;", (name,)).fetchone():
        conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {name} ON {table}({cols});")

def _ensure_index(conn: sqlite3.Connection, name: str, table: str, cols: str):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?;", (name,)).fetchone():
        conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({cols});")

def _drop_and_create(conn: sqlite3.Connection, table: str, columns: Dict[str, str], uniques: Tuple[Tuple[str, str], ...] = ()):
    conn.execute(f"DROP TABLE IF EXISTS {table};")
    cols_sql = ", ".join([f"{k} {v}" for k, v in columns.items()])
    conn.execute(f"CREATE TABLE {table} ({cols_sql});")
    for iname, icols in uniques:
        _ensure_unique_index(conn, iname, table, icols)

def _rebuild_table(conn: sqlite3.Connection, table: str, columns: Dict[str, str], insert_map: Dict[str, str], uniques: Tuple[Tuple[str, str], ...] = ()):
    new_table = f"{table}__new"
    conn.execute(f"DROP TABLE IF EXISTS {new_table};")
    cols_sql = ", ".join([f"{k} {v}" for k, v in columns.items()])
    conn.execute(f"CREATE TABLE {new_table} ({cols_sql});")
    for iname, icols in uniques:
        _ensure_unique_index(conn, iname, new_table, icols)

    src_cols = []
    dst_cols = []
    for dst, src_expr in insert_map.items():
        if dst in columns:
            dst_cols.append(dst)
            src_cols.append(src_expr)
    if dst_cols:
        conn.execute(
            f"INSERT OR IGNORE INTO {new_table} ({', '.join(dst_cols)}) "
            f"SELECT {', '.join(src_cols)} FROM {table};"
        )

    conn.execute(f"DROP TABLE {table};")
    conn.execute(f"ALTER TABLE {new_table} RENAME TO {table};")

# ---------------------------------------------------------------------
# init / migrate
# ---------------------------------------------------------------------
def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with connect(db_path) as conn:
        # models
        if not _table_exists(conn, "models"):
            conn.execute("CREATE TABLE models (repo_id TEXT PRIMARY KEY);")
        existing = set(_columns(conn, "models"))
        for col, decl in MODELS_COLUMNS.items():
            if col not in existing:
                if col == "repo_id":
                    continue  # PK exists by creation above
                default_sql = "strftime('%s','now')" if col in ("first_seen_ts", "last_update_ts") else None
                _add_column(conn, "models", col, decl, default_sql)

        # files
        uniques_files = (("uniq_files_repo_rfn", "repo_id, rfilename"),)
        if not _table_exists(conn, "files"):
            _drop_and_create(conn, "files", FILES_COLUMNS, uniques_files)
        else:
            cols = set(_columns(conn, "files"))
            if "id" not in cols:
                cnt = conn.execute("SELECT COUNT(1) AS n FROM files;").fetchone()["n"]
                if cnt == 0:
                    _drop_and_create(conn, "files", FILES_COLUMNS, uniques_files)
                else:
                    insert_map = {
                        "repo_id": "repo_id",
                        "rfilename": "rfilename",
                        "size": "COALESCE(size, NULL)",
                        "sha256": "COALESCE(sha256, NULL)",
                        "local_path": "COALESCE(local_path, NULL)",
                        "storage_root": "COALESCE(storage_root, NULL)",
                        "created_ts": "COALESCE(created_ts, strftime('%s','now'))",
                        "updated_ts": "COALESCE(updated_ts, strftime('%s','now'))",
                    }
                    _rebuild_table(conn, "files", FILES_COLUMNS, insert_map, uniques_files)
            cols = set(_columns(conn, "files"))
            for col, decl in FILES_COLUMNS.items():
                if col not in cols:
                    if col == "id":
                        continue  # cannot add PK via ALTER
                    default_sql = "strftime('%s','now')" if col in ("created_ts", "updated_ts") else None
                    _add_column(conn, "files", col, decl, default_sql)
            _ensure_index(conn, "idx_files_repo", "files", "repo_id")
            _ensure_unique_index(conn, "uniq_files_repo_rfn", "files", "repo_id, rfilename")

        # uploads
        uniques_uploads = (("uniq_upload_target_bucket_key", "target, bucket, object_key"),)
        if not _table_exists(conn, "uploads"):
            _drop_and_create(conn, "uploads", UPLOADS_COLUMNS, uniques_uploads)
        else:
            cols = set(_columns(conn, "uploads"))
            if "id" not in cols:
                cnt = conn.execute("SELECT COUNT(1) AS n FROM uploads;").fetchone()["n"]
                if cnt == 0:
                    _drop_and_create(conn, "uploads", UPLOADS_COLUMNS, uniques_uploads)
                else:
                    insert_map = {
                        "repo_id": "repo_id",
                        "rfilename": "rfilename",
                        "target": "target",
                        "bucket": "bucket",
                        "object_key": "object_key",
                        "etag": "COALESCE(etag, NULL)",
                        "uploaded_ts": "COALESCE(uploaded_ts, strftime('%s','now'))",
                    }
                    _rebuild_table(conn, "uploads", UPLOADS_COLUMNS, insert_map, uniques_uploads)
            cols = set(_columns(conn, "uploads"))
            for col, decl in UPLOADS_COLUMNS.items():
                if col not in cols:
                    if col == "id":
                        continue
                    default_sql = "strftime('%s','now')" if col == "uploaded_ts" else None
                    _add_column(conn, "uploads", col, decl, default_sql)
            _ensure_unique_index(conn, "uniq_upload_target_bucket_key", "uploads", "target, bucket, object_key")

        conn.commit()

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def compute_sha256_and_size(path: str | Path, chunk_size: int = 1024 * 1024) -> Tuple[str, int]:
    """Return (hex_sha256, size_bytes) for a local file."""
    p = Path(path)
    h = hashlib.sha256()
    size = 0
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size

def compute_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Backward-compatible helper that returns ONLY the hex digest.
    (download.py expects a string.)
    """
    digest, _ = compute_sha256_and_size(path, chunk_size=chunk_size)
    return digest

def _columns_for_upsert(fields: Dict[str, Any]) -> Tuple[str, str, list]:
    cols, qs, vals = [], [], []
    for k, v in fields.items():
        cols.append(k); qs.append("?"); vals.append(v)
    return ",".join(cols), ",".join(qs), vals

# ---------------------------------------------------------------------
# Upserts / Queries
# ---------------------------------------------------------------------
def upsert_model(db_path: str, repo_id: str, fields: Dict[str, Any]) -> None:
    if not repo_id:
        return
    init_db(db_path)
    all_fields = dict(fields)
    all_fields["repo_id"] = repo_id
    all_fields["last_update_ts"] = _now_epoch()
    cols, qs, vals = _columns_for_upsert(all_fields)
    set_parts = [f"{k}=excluded.{k}" for k in all_fields.keys() if k not in ("repo_id", "first_seen_ts")]
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO models(repo_id, first_seen_ts, last_update_ts) VALUES (?, ?, ?) "
            "ON CONFLICT(repo_id) DO NOTHING;",
            (repo_id, _now_epoch(), _now_epoch()),
        )
        conn.execute(
            f"INSERT INTO models ({cols}) VALUES ({qs}) "
            f"ON CONFLICT(repo_id) DO UPDATE SET {', '.join(set_parts)};",
            vals,
        )
        conn.commit()

def upsert_file(
    db_path: str,
    repo_id: str,
    rfilename: str,
    size: Optional[int] = None,
    sha256: Optional[str] = None,
    local_path: Optional[str] = None,
    storage_root: Optional[str] = None,
) -> int:
    """
    Positional-friendly signature to match callers:
      upsert_file(db_path, repo_id, rfilename, size, sha256, local_path[, storage_root])
    """
    init_db(db_path)

    # If the caller accidentally passed (hex,size) tuple as sha256, unpack.
    if isinstance(sha256, tuple) and len(sha256) == 2:
        sha_hex, sz = sha256
        try:
            sha256 = str(sha_hex)
        except Exception:
            sha256 = None
        if size is None:
            try:
                size = int(sz)
            except Exception:
                pass

    # compute sha if missing but we have a local file
    if not sha256 and local_path and Path(local_path).exists():
        try:
            sha256_hex, sz = compute_sha256_and_size(local_path)
            sha256 = sha256_hex
            size = size or sz
        except Exception:
            pass

    with connect(db_path) as conn:
        conn.execute("INSERT INTO models(repo_id) VALUES (?) ON CONFLICT(repo_id) DO NOTHING;", (repo_id,))
        conn.execute("""
        INSERT INTO files (repo_id, rfilename, size, sha256, local_path, storage_root, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, COALESCE(strftime('%s','now'), 0), COALESCE(strftime('%s','now'), 0))
        ON CONFLICT(repo_id, rfilename) DO UPDATE SET
          size=COALESCE(excluded.size, files.size),
          sha256=COALESCE(excluded.sha256, files.sha256),
          local_path=COALESCE(excluded.local_path, files.local_path),
          storage_root=COALESCE(excluded.storage_root, files.storage_root),
          updated_ts=excluded.updated_ts;
        """, (repo_id, rfilename, size, sha256, local_path, storage_root))
        row = conn.execute(
            "SELECT id FROM files WHERE repo_id=? AND rfilename=?;",
            (repo_id, rfilename)
        ).fetchone()
        conn.commit()
        return int(row["id"]) if row else -1

def record_upload(
    db_path: str,
    repo_id: str,
    rfilename: str,
    *,
    target: str,
    bucket: str,
    object_key: str,
    etag: Optional[str] = None,
) -> int:
    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute("INSERT INTO models(repo_id) VALUES (?) ON CONFLICT(repo_id) DO NOTHING;", (repo_id,))
        conn.execute("INSERT INTO files(repo_id, rfilename) VALUES (?, ?) ON CONFLICT(repo_id, rfilename) DO NOTHING;",
                     (repo_id, rfilename))
        conn.execute("""
        INSERT INTO uploads (repo_id, rfilename, target, bucket, object_key, etag, uploaded_ts)
        VALUES (?, ?, ?, ?, ?, ?, COALESCE(strftime('%s','now'), 0))
        ON CONFLICT(target, bucket, object_key) DO UPDATE SET
          etag=COALESCE(excluded.etag, uploads.etag),
          uploaded_ts=excluded.uploaded_ts;
        """, (repo_id, rfilename, target, bucket, object_key, etag))
        row = conn.execute(
            "SELECT id FROM uploads WHERE target=? AND bucket=? AND object_key=?;",
            (target, bucket, object_key)
        ).fetchone()
        conn.commit()
        return int(row["id"]) if row else -1

def get_models(db_path: str = DEFAULT_DB_PATH, limit: int = 200, offset: int = 0):
    init_db(db_path)
    with connect(db_path) as conn:
        cur = conn.execute("""
        SELECT repo_id, model_name, author, pipeline_tag, license,
               parameters, parameters_readable, downloads, likes,
               created_at, last_modified, canonical_url
        FROM models
        ORDER BY last_update_ts DESC
        LIMIT ? OFFSET ?;
        """, (limit, offset))
        return [dict(r) for r in cur.fetchall()]

def get_files_for_repo(db_path: str, repo_id: str):
    init_db(db_path)
    with connect(db_path) as conn:
        cur = conn.execute("""
        SELECT rfilename, size, sha256, local_path, storage_root, updated_ts
        FROM files WHERE repo_id=? ORDER BY rfilename ASC;
        """, (repo_id,))
        return [dict(r) for r in cur.fetchall()]

# ---------------------------------------------------------------------
# NEW: downloader CLI wired into your DB
# ---------------------------------------------------------------------

PRESETS: Dict[str, List[str]] = {
    "weights": ["*.bin", "*.safetensors", "*.pt", "*.pth", "*.ckpt", "*.onnx"],
    "gguf": ["*.gguf"],
    "tokenizer": [
        "tokenizer*.json", "vocab.*", "*merges.txt", "*vocab.json",
        "spiece.model", "*.model"
    ],
    "config": [
        "config.json", "*config.json", "*.yaml", "*.yml", "generation_config.json"
    ],
    "text": ["README.md", "*.md", "LICENSE*", "NOTICE*", "COPYING*"],
    "core": [
        "config.json", "generation_config.json",
        "tokenizer*.json", "vocab.*", "*merges.txt", "*vocab.json",
        "spiece.model", "*.model",
        "*.safetensors", "*.bin"
    ],
}

def expand_patterns(spec: str) -> List[str]:
    if not spec:
        return []
    tokens = [t.strip() for t in spec.split(",") if t.strip()]
    out: List[str] = []
    for t in tokens:
        key = t.lower()
        if key in PRESETS:
            out.extend(PRESETS[key])
        elif any(ch in t for ch in ["*", "?", "[", "]"]) or "." in t:
            out.append(t)  # raw glob
        else:
            out.append(f"*{t}*")
    # dedupe preserve order
    seen = set(); uniq: List[str] = []
    for p in out:
        if p not in seen:
            seen.add(p); uniq.append(p)
    return uniq

def _safe_repo_folder(repo_id: str) -> str:
    return repo_id.replace("/", "__")

def _extract_repo_id(row: Dict[str, str]) -> str:
    """
    Return 'owner/repo' from a row using generous heuristics:
    - direct columns: repo_id, model_id, name_hf_slug, id, hf_repo, huggingface_repo
    - URL columns: updated_url, url, canonical_url -> parse owner/repo
    - model_name if it already looks like owner/repo
    """
    # 1) direct ids
    for key in ("repo_id", "model_id", "name_hf_slug", "id", "hf_repo", "huggingface_repo"):
        v = (row.get(key) or "").strip()
        if v:
            return v

    # 2) URL-derived ids
    for key in ("updated_url", "url", "canonical_url"):
        url = (row.get(key) or "").strip()
        if not url:
            continue
        for pat in _HF_URL_PATTERNS:
            m = pat.match(url)
            if m:
                return f"{m.group(1)}/{m.group(2)}"

    # 3) model_name sometimes already has owner/repo
    mn = (row.get("model_name") or "").strip()
    if mn and "/" in mn and not mn.startswith("http"):
        return mn

    return ""

def _read_rows(path: Path) -> List[Dict[str, str]]:
    """
    Read CSV/TXT and return rows with a resolved 'repo_id'.
    For CSV, we accept flexible columns and try to parse HF URLs.
    """
    if not path.exists():
        raise FileNotFoundError(f"input not found: {path}")

    # TXT input: one repo_id per line
    if path.suffix.lower() == ".txt":
        rows: List[Dict[str, str]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            rows.append({"repo_id": line})
        return rows

    # CSV input
    kept: List[Dict[str, str]] = []
    total = 0
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            total += 1
            # normalize strings
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
            rid = _extract_repo_id(row)
            if rid:
                row["repo_id"] = rid
                kept.append(row)

    LOG.info("Input file: %s", path)
    LOG.info("Rows detected: %d (kept from %d total)", len(kept), total)
    return kept

def _walk_and_upsert(db_path: str, repo_id: str, local_root: Path, storage_root: Path) -> int:
    """
    Walk local_root; upsert every file into DB with relative rfilename.
    Returns number of files recorded.
    """
    n = 0
    for p in local_root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(local_root)
        rfilename = str(rel).replace("\\", "/")
        # compute sha+size once
        try:
            sha, size = compute_sha256_and_size(p)
        except Exception:
            sha, size = None, p.stat().st_size if p.exists() else None
        upsert_file(
            db_path=db_path,
            repo_id=repo_id,
            rfilename=rfilename,
            size=size,
            sha256=sha,
            local_path=str(p),
            storage_root=str(storage_root),
        )
        n += 1
    return n

def download_one(
    repo_id: str,
    target_dir: Path,
    allow_patterns: List[str],
    revision: Optional[str],
) -> Path:
    if snapshot_download is None:
        raise RuntimeError(
            "huggingface_hub is not available. Add `huggingface_hub` to requirements.txt."
        )
    target_dir.mkdir(parents=True, exist_ok=True)
    LOG.info("Downloading %s (rev=%s) patterns=%s -> %s", repo_id, revision, "(all)", target_dir)
    local_path = snapshot_download(
        repo_id=repo_id,
        repo_type="model",
        revision=revision,
        local_dir=str(target_dir),
        local_dir_use_symlinks=False,
        allow_patterns=allow_patterns if allow_patterns else None,
        ignore_patterns=None,
        max_workers=8,
        etag_timeout=20,
    )
    return Path(local_path)

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Download models and record files in DB")
    ap.add_argument("--input", required=True, help="CSV/TXT with repo IDs (columns: repo_id|model_id|name_hf_slug)")
    ap.add_argument("--out-dir", required=True, help="Directory to store downloaded files")
    ap.add_argument("--patterns", default="weights", help="Preset/patterns, e.g. 'weights,tokenizer,config' or '*.bin,*.json'")
    ap.add_argument("--revision", default=os.getenv("HF_REVISION", "main"), help="Default revision (per-row 'revision' overrides)")
    ap.add_argument("--layout", choices=("by_repo","flat"), default="by_repo", help="by_repo -> <out>/<owner__repo>/...")
    ap.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite DB path")
    args = ap.parse_args(argv)

    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    init_db(args.db_path)

    default_patterns = expand_patterns(args.patterns)
    LOG.info("Using patterns: %s", default_patterns or "(all files)")
    rows = _read_rows(input_path)
    LOG.info("Input file: %s", input_path)
    LOG.info("Rows detected: %d", len(rows))
    if not rows:
        LOG.error(
            "No rows found in %s. Expect a repo id column or a URL column like 'updated_url'/'url' "
            "pointing to https://huggingface.co/<owner>/<repo>.",
            input_path,
        )
        return 1
    if not rows:
        LOG.warning("No rows found in %s", input_path)
        return 0

    ok = 0
    fail = 0
    for row in rows:
        repo_id = row["repo_id"]
        rev = (row.get("revision") or args.revision or "").strip() or None
        per_row_patterns = row.get("allow_patterns", "").strip()
        allow_patterns = expand_patterns(per_row_patterns) if per_row_patterns else default_patterns

        # choose local target dir
        if args.layout == "by_repo":
            subfolder = (row.get("subfolder") or _safe_repo_folder(repo_id)).strip()
            target = out_dir / subfolder
        else:
            target = out_dir

        try:
            local_root = download_one(repo_id, target, allow_patterns, rev)
            count = _walk_and_upsert(
                db_path=args.db_path,
                repo_id=repo_id,
                local_root=local_root,
                storage_root=out_dir if args.layout == "by_repo" else out_dir,
            )
            LOG.info("✅ %s -> %s (files recorded: %d)", repo_id, local_root, count)
            ok += 1
        except HfHubHTTPError as e:
            LOG.error("❌ %s (HTTP): %s", repo_id, e)
            fail += 1
        except Exception as e:
            LOG.error("❌ %s: %s", repo_id, e)
            fail += 1

    LOG.info("Done. success=%d failed=%d total=%d", ok, fail, len(rows))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
