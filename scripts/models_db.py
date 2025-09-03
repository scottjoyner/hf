#!/usr/bin/env python3
from __future__ import annotations
import hashlib
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

DEFAULT_DB_PATH = os.getenv("DB_PATH", "/app/db/models.db")

# ------------------ connection ------------------
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

# ------------------ desired schemas ------------------
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

# ------------------ helpers ------------------
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
    # Create shadow table with desired schema
    new_table = f"{table}__new"
    conn.execute(f"DROP TABLE IF EXISTS {new_table};")
    cols_sql = ", ".join([f"{k} {v}" for k, v in columns.items()])
    conn.execute(f"CREATE TABLE {new_table} ({cols_sql});")
    for iname, icols in uniques:
        _ensure_unique_index(conn, iname, new_table, icols)
    # Copy data with mapping of common columns
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
    # Swap
    conn.execute(f"DROP TABLE {table};")
    conn.execute(f"ALTER TABLE {new_table} RENAME TO {table};")

# ------------------ init / migrate ------------------
def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with connect(db_path) as conn:
        # models
        if not _table_exists(conn, "models"):
            conn.execute("CREATE TABLE models (repo_id TEXT PRIMARY KEY);")
        existing = set(_columns(conn, "models"))
        for col, decl in MODELS_COLUMNS.items():
            if col not in existing:
                default_sql = "strftime('%s','now')" if col in ("first_seen_ts", "last_update_ts") else None
                _add_column(conn, "models", col, decl, default_sql)

        # files
        uniques_files = (("uniq_files_repo_rfn", "repo_id, rfilename"),)
        if not _table_exists(conn, "files"):
            _drop_and_create(conn, "files", FILES_COLUMNS, uniques_files)
        else:
            cols = set(_columns(conn, "files"))
            if "id" not in cols:
                # If table empty, drop & create. Else rebuild.
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
            # add any other missing columns
            cols = set(_columns(conn, "files"))
            for col, decl in FILES_COLUMNS.items():
                if col not in cols:
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
                    default_sql = "strftime('%s','now')" if col == "uploaded_ts" else None
                    _add_column(conn, "uploads", col, decl, default_sql)
            _ensure_unique_index(conn, "uniq_upload_target_bucket_key", "uploads", "target, bucket, object_key")

        conn.commit()

# ------------------ utilities ------------------
def compute_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def _columns_for_upsert(fields: Dict[str, Any]) -> Tuple[str, str, list]:
    cols, qs, vals = [], [], []
    for k, v in fields.items():
        cols.append(k); qs.append("?"); vals.append(v)
    return ",".join(cols), ",".join(qs), vals

# ------------------ upserts / queries ------------------
def upsert_model(db_path: str, repo_id: str, fields: Dict[str, Any]) -> None:
    if not repo_id:
        return
    init_db(db_path)
    all_fields = dict(fields)
    all_fields["repo_id"] = repo_id
    all_fields["last_update_ts"] = _now_epoch()
    cols, qs, vals = _columns_for_upsert(all_fields)
    set_parts = [f"{k}=excluded.{k}" for k in all_fields.keys() if k != "repo_id"]
    sql = f"INSERT INTO models ({cols}) VALUES ({qs}) ON CONFLICT(repo_id) DO UPDATE SET {', '.join(set_parts)};"
    with connect(db_path) as conn:
        conn.execute(sql, vals)
        conn.commit()

def upsert_file(
    db_path: str,
    repo_id: str,
    rfilename: str,
    *,
    size: Optional[int] = None,
    sha256: Optional[str] = None,
    local_path: Optional[str] = None,
    storage_root: Optional[str] = None,
) -> int:
    init_db(db_path)
    if not sha256 and local_path and Path(local_path).exists():
        try:
            sha256 = compute_sha256(local_path)
        except Exception:
            sha256 = None
    with connect(db_path) as conn:
        conn.execute("""
        INSERT INTO files (repo_id, rfilename, size, sha256, local_path, storage_root, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, COALESCE(strftime('%s','now'), 0), COALESCE(strftime('%s','now'), 0))
        ON CONFLICT(repo_id, rfilename) DO UPDATE SET
          size=excluded.size,
          sha256=excluded.sha256,
          local_path=excluded.local_path,
          storage_root=excluded.storage_root,
          updated_ts=excluded.updated_ts;
        """, (repo_id, rfilename, size, sha256, local_path, storage_root))
        row = conn.execute("SELECT id FROM files WHERE repo_id=? AND rfilename=?;", (repo_id, rfilename)).fetchone()
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
