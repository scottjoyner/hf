# scripts/models_db.py
from __future__ import annotations
import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Tuple

MODELS_COLS: Dict[str, str] = {
    # identity & basic
    "repo_id": "TEXT PRIMARY KEY",
    "canonical_url": "TEXT",
    "name_hf_slug": "TEXT",
    "model_name": "TEXT",
    "author": "TEXT",
    # hf metadata
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
    # params & size
    "parameters": "INTEGER",
    "parameters_readable": "TEXT",
    "used_storage_bytes": "INTEGER",
    # files summary
    "file_count": "INTEGER",
    "has_safetensors": "INTEGER",
    "has_bin": "INTEGER",
    "has_pt": "INTEGER",
    "has_onnx": "INTEGER",
    "has_tflite": "INTEGER",
    "has_gguf": "INTEGER",
    # extras
    "benchmarks_json": "TEXT",
    "spaces": "TEXT",
    # enrichment echoes
    "model_description": "TEXT",
    "params_raw": "TEXT",
    "model_size_raw": "TEXT",
}

FILES_COLS: Dict[str, str] = {
    "repo_id": "TEXT",
    "rfilename": "TEXT",
    "size": "INTEGER",
}

def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def _cols_in_table(conn: sqlite3.Connection, table: str) -> set:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}  # 2nd col is name

def _ensure_models_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "models"):
        cols_sql = ", ".join([f"{k} {v}" for k, v in MODELS_COLS.items()])
        conn.execute(f"CREATE TABLE models ({cols_sql});")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_models_author ON models(author);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_models_pipeline ON models(pipeline_tag);")
        conn.commit()
        return

    # migrate missing columns
    existing = _cols_in_table(conn, "models")
    for col, typ in MODELS_COLS.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE models ADD COLUMN {col} {typ};")
    conn.commit()

def _ensure_files_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "files"):
        cols_sql = ", ".join([f"{k} {v}" for k, v in FILES_COLS.items()])
        conn.execute(f"""
            CREATE TABLE files (
                {cols_sql},
                PRIMARY KEY (repo_id, rfilename),
                FOREIGN KEY (repo_id) REFERENCES models(repo_id) ON DELETE CASCADE
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id);")
        conn.commit()
        return

    existing = _cols_in_table(conn, "files")
    for col, typ in FILES_COLS.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE files ADD COLUMN {col} {typ};")
    conn.commit()

def init_db(db_path: str) -> None:
    with _connect(db_path) as conn:
        _ensure_models_table(conn)
        _ensure_files_table(conn)

def upsert_model(db_path: str, repo_id: str, fields: Dict[str, object]) -> None:
    if not repo_id:
        return
    # keep only known columns (besides PK repo_id)
    clean = {k: fields.get(k) for k in MODELS_COLS.keys() if k != "repo_id" and k in fields}
    cols = ["repo_id"] + list(clean.keys())
    vals = [repo_id] + list(clean.values())
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join([f"{c}=excluded.{c}" for c in clean.keys()])

    sql = f"""
        INSERT INTO models ({", ".join(cols)}) VALUES ({placeholders})
        ON CONFLICT(repo_id) DO UPDATE SET {updates};
    """
    with _connect(db_path) as conn:
        conn.execute(sql, vals)
        conn.commit()

def upsert_file(db_path: str, repo_id: str, rfilename: str, size: int | None) -> None:
    if not (repo_id and rfilename):
        return
    sql = """
        INSERT INTO files (repo_id, rfilename, size) VALUES (?, ?, ?)
        ON CONFLICT(repo_id, rfilename) DO UPDATE SET size=excluded.size;
    """
    with _connect(db_path) as conn:
        conn.execute(sql, (repo_id, rfilename, size if isinstance(size, int) else None))
        conn.commit()
