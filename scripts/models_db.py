#!/usr/bin/env python3
import sqlite3
import pathlib
import hashlib
import os
from typing import Optional, Dict, Any

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS models (
  repo_id TEXT PRIMARY KEY,
  canonical_url TEXT,
  model_name TEXT,
  author TEXT,
  pipeline_tag TEXT,
  license TEXT,
  parameters INTEGER,
  parameters_readable TEXT,
  downloads INTEGER,
  likes INTEGER,
  created_at TEXT,
  last_modified TEXT,
  languages TEXT,
  tags TEXT,
  model_description TEXT,
  params_raw TEXT,
  model_size_raw TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  repo_id TEXT NOT NULL,
  rfilename TEXT NOT NULL,
  size INTEGER,
  sha256 TEXT,
  local_path TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(repo_id, rfilename)
);
CREATE TABLE IF NOT EXISTS uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  repo_id TEXT,
  rfilename TEXT,
  storage TEXT,
  bucket TEXT,
  key TEXT,
  etag TEXT,
  uploaded_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id);
CREATE INDEX IF NOT EXISTS idx_uploads_repo ON uploads(repo_id);
"""

def connect(db_path: str):
  p = pathlib.Path(db_path)
  p.parent.mkdir(parents=True, exist_ok=True)
  conn = sqlite3.connect(str(p))
  conn.execute("PRAGMA foreign_keys=ON;")
  return conn

def init_db(db_path: str):
  conn = connect(db_path)
  try:
    conn.executescript(SCHEMA)
    conn.commit()
  finally:
    conn.close()

def compute_sha256(path: str, chunk: int = 1024*1024) -> Optional[str]:
  try:
    h = hashlib.sha256()
    with open(path, "rb") as f:
      while True:
        b = f.read(chunk)
        if not b:
          break
        h.update(b)
    return h.hexdigest()
  except Exception:
    return None

def upsert_model(db_path: str, repo_id: str, fields: Dict[str, Any]):
  keys = ["canonical_url","model_name","author","pipeline_tag","license","parameters",
          "parameters_readable","downloads","likes","created_at","last_modified",
          "languages","tags","model_description","params_raw","model_size_raw"]
  placeholders = ",".join(f"{k}=excluded.{k}" for k in keys)
  cols = ",".join(["repo_id"] + keys)
  qs = ",".join(["?"]*(1+len(keys)))
  vals = [repo_id] + [fields.get(k) for k in keys]
  conn = connect(db_path)
  try:
    conn.execute(f"""
      INSERT INTO models ({cols}) VALUES ({qs})
      ON CONFLICT(repo_id) DO UPDATE SET {placeholders}, updated_at=datetime('now')
    """, vals)
    conn.commit()
  finally:
    conn.close()

def upsert_file(db_path: str, repo_id: str, rfilename: str, size: Optional[int], sha256: Optional[str], local_path: Optional[str]):
  conn = connect(db_path)
  try:
    conn.execute("""
      INSERT INTO files (repo_id, rfilename, size, sha256, local_path)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(repo_id, rfilename) DO UPDATE SET
        size=excluded.size,
        sha256=COALESCE(excluded.sha256, files.sha256),
        local_path=COALESCE(excluded.local_path, files.local_path)
    """, (repo_id, rfilename, int(size) if size is not None else None, sha256, local_path))
    conn.commit()
  finally:
    conn.close()

def record_upload(db_path: str, repo_id: str, rfilename: str, storage: str, bucket: str, key: str, etag: Optional[str]):
  conn = connect(db_path)
  try:
    conn.execute("""
      INSERT INTO uploads (repo_id, rfilename, storage, bucket, key, etag)
      VALUES (?, ?, ?, ?, ?, ?)
    """, (repo_id, rfilename, storage, bucket, key, etag))
    conn.commit()
  finally:
    conn.close()
