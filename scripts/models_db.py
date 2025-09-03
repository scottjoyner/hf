#!/usr/bin/env python3
"""
SQLite helpers for the HF pipeline.

Tables
------
models(repo_id PK, ...metadata...)
files(id PK, repo_id, rfilename, size, sha256, local_path, storage_root, ...)
uploads(id PK, repo_id, rfilename, target, bucket, object_key, etag, ...)

Public API
----------
connect(db_path) -> sqlite3.Connection
init_db(db_path) -> None
upsert_model(db_path, repo_id, fields: dict) -> None
upsert_file(db_path, repo_id, rfilename, *, size=None, sha256=None,
            local_path=None, storage_root=None) -> int
record_upload(db_path, repo_id, rfilename, *, target, bucket, object_key, etag=None) -> int
compute_sha256(path) -> str
"""

from __future__ import annotations
import hashlib
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

DEFAULT_DB_PATH = os.getenv("DB_PATH", "/app/db/models.db")

# ---------- connection & schema ----------

def connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Light WAL tuning for concurrent readers/writers
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _now_epoch() -> int:
    return int(time.time())

def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS models (
          repo_id TEXT PRIMARY KEY,
          canonical_url TEXT,
          name_hf_slug TEXT,
          model_name TEXT,
          author TEXT,
          pipeline_tag TEXT,
          library_name TEXT,
          license TEXT,
          languages TEXT,
          tags TEXT,
          downloads INTEGER,
          likes INTEGER,
          created_at TEXT,
          last_modified TEXT,
          private INTEGER,
          gated INTEGER,
          disabled INTEGER,
          sha TEXT,
          model_type TEXT,
          architectures TEXT,
          auto_model TEXT,
          processor TEXT,
          parameters INTEGER,
          parameters_readable TEXT,
          used_storage_bytes INTEGER,
          region TEXT,
          arxiv_ids TEXT,
          model_description TEXT,
          params_raw TEXT,
          model_size_raw TEXT,
          first_seen_ts INTEGER DEFAULT (strftime('%s','now')),
          last_update_ts INTEGER DEFAULT (strftime('%s','now'))
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          repo_id TEXT NOT NULL,
          rfilename TEXT NOT NULL,
          size INTEGER,
          sha256 TEXT,
          local_path TEXT,
          storage_root TEXT,
          created_ts INTEGER DEFAULT (strftime('%s','now')),
          updated_ts INTEGER DEFAULT (strftime('%s','now')),
          UNIQUE(repo_id, rfilename)
        );
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          repo_id TEXT NOT NULL,
          rfilename TEXT NOT NULL,
          target TEXT NOT NULL,           -- e.g., 'minio' or 's3'
          bucket TEXT NOT NULL,
          object_key TEXT NOT NULL,
          etag TEXT,
          uploaded_ts INTEGER DEFAULT (strftime('%s','now')),
          UNIQUE(target, bucket, object_key)
        );
        """)

        conn.commit()

# ---------- helpers ----------

def compute_sha256(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def _columns_for_upsert(table: str, fields: Dict[str, Any]) -> Tuple[str, str, list]:
    cols = []
    vals = []
    qs = []
    for k, v in fields.items():
        cols.append(k)
        vals.append(v)
        qs.append("?")
    return ",".join(cols), ",".join(qs), vals

# ---------- upserts ----------

def upsert_model(db_path: str, repo_id: str, fields: Dict[str, Any]) -> None:
    if not repo_id:
        return
    all_fields = dict(fields)
    all_fields["repo_id"] = repo_id
    all_fields["last_update_ts"] = _now_epoch()

    # Build sets for ON CONFLICT
    set_parts = [f"{k}=excluded.{k}" for k in all_fields.keys() if k != "repo_id"]

    cols, qs, vals = _columns_for_upsert("models", all_fields)
    sql = f"""
    INSERT INTO models ({cols}) VALUES ({qs})
    ON CONFLICT(repo_id) DO UPDATE SET {", ".join(set_parts)};
    """
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
    """
    Insert/update a file row. Returns the file id.
    """
    if not sha256 and local_path and Path(local_path).exists():
        try:
            sha256 = compute_sha256(local_path)
        except Exception:
            sha256 = None

    fields = {
        "repo_id": repo_id,
        "rfilename": rfilename,
        "size": size,
        "sha256": sha256,
        "local_path": local_path,
        "storage_root": storage_root,
        "updated_ts": _now_epoch(),
    }

    # Upsert by (repo_id, rfilename)
    with connect(db_path) as conn:
        conn.execute("""
        INSERT INTO files (repo_id, rfilename, size, sha256, local_path, storage_root, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(repo_id, rfilename) DO UPDATE SET
          size=excluded.size,
          sha256=excluded.sha256,
          local_path=excluded.local_path,
          storage_root=excluded.storage_root,
          updated_ts=excluded.updated_ts;
        """, (fields["repo_id"], fields["rfilename"], fields["size"], fields["sha256"],
              fields["local_path"], fields["storage_root"], fields["updated_ts"]))
        # Return id
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
    target: str,         # 'minio' or 's3'
    bucket: str,
    object_key: str,
    etag: Optional[str] = None,
) -> int:
    """
    Record that a file was uploaded to an external object store.
    Unique on (target,bucket,object_key). Returns upload id.
    """
    with connect(db_path) as conn:
        conn.execute("""
        INSERT INTO uploads (repo_id, rfilename, target, bucket, object_key, etag, uploaded_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target, bucket, object_key) DO UPDATE SET
          etag=COALESCE(excluded.etag, uploads.etag),
          uploaded_ts=excluded.uploaded_ts;
        """, (repo_id, rfilename, target, bucket, object_key, etag, _now_epoch()))
        row = conn.execute(
            "SELECT id FROM uploads WHERE target=? AND bucket=? AND object_key=?;",
            (target, bucket, object_key)
        ).fetchone()
        conn.commit()
        return int(row["id"]) if row else -1

# ---------- convenience (used by seeding / web) ----------

def get_models(db_path: str = DEFAULT_DB_PATH, limit: int = 200, offset: int = 0):
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
    with connect(db_path) as conn:
        cur = conn.execute("""
        SELECT rfilename, size, sha256, local_path, storage_root, updated_ts
        FROM files WHERE repo_id=? ORDER BY rfilename ASC;
        """, (repo_id,))
        return [dict(r) for r in cur.fetchall()]
