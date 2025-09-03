#!/usr/bin/env python3
import os, sqlite3, secrets, time, hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_PATH = os.environ.get("DB_PATH", "/app/db/models.db")

# ------------------ Connection helpers ------------------

def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Reasonable pragmas for SQLite in-prod single node
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)
    ).fetchone())

def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r["name"] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]

def _add_column(conn: sqlite3.Connection, table: str, col: str, decl: str, default_sql: Optional[str] = None):
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl};")
    if default_sql is not None:
        conn.execute(f"UPDATE {table} SET {col}={default_sql} WHERE {col} IS NULL;")

def _ensure_index(conn: sqlite3.Connection, name: str, table: str, cols: str, unique: bool = False):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?;", (name,)).fetchone():
        conn.execute(f"CREATE {'UNIQUE ' if unique else ''}INDEX IF NOT EXISTS {name} ON {table}({cols});")

# ------------------ Schema bootstrap / migrations ------------------

def ensure_registry_tables() -> None:
    with connect() as c:
        # users, api_keys
        if not _table_exists(c, "users"):
            c.execute("""
            CREATE TABLE users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT UNIQUE NOT NULL,
              name TEXT NOT NULL,
              role TEXT NOT NULL DEFAULT 'developer',  -- 'developer' | 'platform' | 'admin'
              created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              updated_ts INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );""")
        cols = set(_columns(c, "users"))
        if "role" not in cols:
            _add_column(c, "users", "role", "TEXT NOT NULL DEFAULT 'developer'")
        _ensure_index(c, "idx_users_role", "users", "role")

        if not _table_exists(c, "api_keys"):
            c.execute("""
            CREATE TABLE api_keys (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              api_key TEXT UNIQUE NOT NULL,
              created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              revoked_ts INTEGER
            );""")
        _ensure_index(c, "idx_api_keys_user", "api_keys", "user_id")
        _ensure_index(c, "idx_api_keys_revoked", "api_keys", "revoked_ts")

        # models (extends your existing)
        if not _table_exists(c, "models"):
            c.execute("CREATE TABLE models (repo_id TEXT PRIMARY KEY);")
        cols = set(_columns(c, "models"))
        if "owner_user_id" not in cols:
            _add_column(c, "models", "owner_user_id", "INTEGER REFERENCES users(id)")
        for col, decl in [
            ("canonical_url", "TEXT"),
            ("model_name", "TEXT"),
            ("author", "TEXT"),
            ("pipeline_tag", "TEXT"),
            ("license", "TEXT"),
            ("parameters", "INTEGER"),
            ("parameters_readable", "TEXT"),
            ("downloads", "INTEGER"),
            ("likes", "INTEGER"),
            ("created_at", "TEXT"),
            ("last_modified", "TEXT"),
            ("languages", "TEXT"),
            ("tags", "TEXT"),
            ("last_update_ts", "INTEGER"),
            ("file_count", "INTEGER"),
            ("has_safetensors", "INTEGER"),
            ("has_bin", "INTEGER"),
            ("visibility", "TEXT DEFAULT 'private'")  # 'private'|'public' (optional)
        ]:
            if col not in cols:
                _add_column(c, "models", col, decl)
        _ensure_index(c, "idx_models_owner", "models", "owner_user_id")

        # model_versions
        if not _table_exists(c, "model_versions"):
            c.execute("""
            CREATE TABLE model_versions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              repo_id TEXT NOT NULL REFERENCES models(repo_id) ON DELETE CASCADE,
              version TEXT NOT NULL,          -- e.g. 'v1', '2025-09-03', '1.2.0'
              notes TEXT,
              created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              UNIQUE(repo_id, version)
            );""")
        _ensure_index(c, "idx_versions_repo", "model_versions", "repo_id")

        # files (extend with optional version)
        if not _table_exists(c, "files"):
            c.execute("""
            CREATE TABLE files (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              repo_id TEXT NOT NULL,
              rfilename TEXT NOT NULL,
              size INTEGER, sha256 TEXT,
              local_path TEXT, storage_root TEXT,
              created_ts INTEGER, updated_ts INTEGER
            );""")
        cols = set(_columns(c, "files"))
        if "version" not in cols:
            _add_column(c, "files", "version", "TEXT")
        _ensure_index(c, "idx_files_repo", "files", "repo_id")
        _ensure_index(c, "uniq_files_repo_rfn_ver", "files", "repo_id, rfilename, COALESCE(version,'')", unique=True)

        # uploads (extend with optional version)
        if not _table_exists(c, "uploads"):
            c.execute("""
            CREATE TABLE uploads (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              repo_id TEXT NOT NULL,
              rfilename TEXT NOT NULL,
              target TEXT NOT NULL,
              bucket TEXT NOT NULL,
              object_key TEXT NOT NULL,
              etag TEXT, uploaded_ts INTEGER
            );""")
        cols = set(_columns(c, "uploads"))
        if "version" not in cols:
            _add_column(c, "uploads", "version", "TEXT")
        _ensure_index(c, "uniq_upload_target_bucket_key", "uploads", "target, bucket, object_key", unique=True)

        # platform grants
        if not _table_exists(c, "platform_model_grants"):
            c.execute("""
            CREATE TABLE platform_model_grants (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              platform_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              repo_id TEXT NOT NULL REFERENCES models(repo_id) ON DELETE CASCADE,
              permitted_from_ts INTEGER NOT NULL,
              permitted_until_ts INTEGER,
              created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              UNIQUE(platform_user_id, repo_id)
            );""")
        _ensure_index(c, "idx_grants_platform", "platform_model_grants", "platform_user_id")
        _ensure_index(c, "idx_grants_repo", "platform_model_grants", "repo_id")

        # access logs (as in your app)
        if not _table_exists(c, "access_logs"):
            c.execute("""
            CREATE TABLE access_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              user_id INTEGER,
              api_key_id INTEGER,
              event_type TEXT,
              repo_id TEXT,
              rfilename TEXT,
              object_key TEXT,
              size INTEGER,
              status TEXT,
              remote_addr TEXT,
              user_agent TEXT
            );""")
        _ensure_index(c, "idx_logs_user", "access_logs", "user_id")
        _ensure_index(c, "idx_logs_repo", "access_logs", "repo_id")

        c.commit()

# ------------------ Users & Auth ------------------

def _generate_key() -> str:
    # 32 bytes → 43 chars base64url roughly; prefix for clarity
    return "sk_live_" + secrets.token_urlsafe(32)

def create_user(email: str, name: str, role: str = "developer") -> Tuple[int, str]:
    role = (role or "developer").strip().lower()
    if role not in ("developer", "platform", "admin"):
        role = "developer"
    key = _generate_key()
    with connect() as c:
        c.execute("INSERT INTO users(email, name, role) VALUES (?,?,?)", (email.strip(), name.strip(), role))
        uid = int(c.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        c.execute("INSERT INTO api_keys(user_id, api_key) VALUES (?,?)", (uid, key))
        c.commit()
    return uid, key

def user_from_api_key(api_key: str) -> Optional[Dict[str, Any]]:
    with connect() as c:
        row = c.execute("""
            SELECT u.id AS user_id, u.email, u.name, u.role, k.id AS api_key_id
              FROM api_keys k JOIN users u ON u.id=k.user_id
             WHERE k.api_key=? AND k.revoked_ts IS NULL
        """, (api_key,)).fetchone()
        return dict(row) if row else None

def rotate_key(user_id: int) -> str:
    new_key = _generate_key()
    now = int(time.time())
    with connect() as c:
        c.execute("UPDATE api_keys SET revoked_ts=? WHERE user_id=? AND revoked_ts IS NULL", (now, user_id))
        c.execute("INSERT INTO api_keys(user_id, api_key) VALUES (?,?)", (user_id, new_key))
        c.commit()
    return new_key

def log_access(*, user_id: Optional[int], api_key_id: Optional[int], event_type: str,
               repo_id: Optional[str], rfilename: Optional[str],
               object_key: Optional[str], size: Optional[int], status: str,
               remote_addr: Optional[str], user_agent: Optional[str]) -> None:
    with connect() as c:
        c.execute("""
        INSERT INTO access_logs(user_id, api_key_id, event_type, repo_id, rfilename, object_key, size, status, remote_addr, user_agent)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                  (user_id, api_key_id, event_type, repo_id, rfilename, object_key, size, status, remote_addr, user_agent))
        c.commit()

# ------------------ Models & Versions ------------------

def upsert_model(*, repo_id: str, owner_user_id: int, fields: Dict[str, Any]) -> None:
    with connect() as c:
        # ensure model row exists
        c.execute("INSERT INTO models(repo_id, owner_user_id, last_update_ts) VALUES (?,?,strftime('%s','now')) "
                  "ON CONFLICT(repo_id) DO NOTHING", (repo_id, owner_user_id))
        # update metadata
        sets = []
        vals = []
        for k, v in fields.items():
            sets.append(f"{k}=?")
            vals.append(v)
        if sets:
            vals += [repo_id]
            c.execute(f"UPDATE models SET {', '.join(sets)}, last_update_ts=strftime('%s','now') WHERE repo_id=?", vals)
        c.commit()

def create_version(repo_id: str, version: str, notes: Optional[str] = None) -> int:
    with connect() as c:
        c.execute("INSERT OR IGNORE INTO model_versions(repo_id, version, notes) VALUES (?,?,?)",
                  (repo_id, version, notes))
        row = c.execute("SELECT id FROM model_versions WHERE repo_id=? AND version=?", (repo_id, version)).fetchone()
        c.commit()
        return int(row["id"]) if row else -1

def upsert_file(repo_id: str, rfilename: str, *, version: Optional[str] = None,
                size: Optional[int] = None, sha256: Optional[str] = None,
                storage_root: Optional[str] = "minio") -> int:
    with connect() as c:
        c.execute("""
        INSERT INTO files(repo_id, rfilename, version, size, sha256, storage_root, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'), strftime('%s','now'))
        ON CONFLICT(repo_id, rfilename, COALESCE(version,'')) DO UPDATE SET
          size=COALESCE(excluded.size, files.size),
          sha256=COALESCE(excluded.sha256, files.sha256),
          storage_root=COALESCE(excluded.storage_root, files.storage_root),
          updated_ts=strftime('%s','now')
        """, (repo_id, rfilename, version, size, sha256, storage_root))
        row = c.execute("SELECT id FROM files WHERE repo_id=? AND rfilename=? AND COALESCE(version,'')=COALESCE(?, '')",
                        (repo_id, rfilename, version)).fetchone()
        c.commit()
        return int(row["id"]) if row else -1

def record_upload(repo_id: str, rfilename: str, *, object_key: str,
                  bucket: str, target: str = "minio", version: Optional[str] = None,
                  etag: Optional[str] = None) -> int:
    with connect() as c:
        c.execute("""
        INSERT INTO uploads(repo_id, rfilename, version, target, bucket, object_key, etag, uploaded_ts)
        VALUES (?,?,?,?,?,?,?,strftime('%s','now'))
        ON CONFLICT(target, bucket, object_key) DO UPDATE SET etag=COALESCE(excluded.etag, uploads.etag),
                                                            uploaded_ts=strftime('%s','now')
        """, (repo_id, rfilename, version, target, bucket, object_key, etag))
        row = c.execute("SELECT id FROM uploads WHERE target=? AND bucket=? AND object_key=?",
                        (target, bucket, object_key)).fetchone()
        c.commit()
        return int(row["id"]) if row else -1

# ------------------ Grants / Access Control ------------------

def grant_platform_access(platform_user_id: int, repo_id: str, permitted_from_ts: int,
                          permitted_until_ts: Optional[int] = None) -> None:
    with connect() as c:
        c.execute("""
        INSERT INTO platform_model_grants(platform_user_id, repo_id, permitted_from_ts, permitted_until_ts)
        VALUES (?,?,?,?)
        ON CONFLICT(platform_user_id, repo_id) DO UPDATE SET
          permitted_from_ts=excluded.permitted_from_ts,
          permitted_until_ts=excluded.permitted_until_ts
        """, (platform_user_id, repo_id, permitted_from_ts, permitted_until_ts))
        c.commit()

def revoke_platform_access(platform_user_id: int, repo_id: str) -> None:
    with connect() as c:
        c.execute("DELETE FROM platform_model_grants WHERE platform_user_id=? AND repo_id=?",
                  (platform_user_id, repo_id)); c.commit()

def list_grants_for_user(platform_user_id: int) -> List[sqlite3.Row]:
    with connect() as c:
        return c.execute("""
        SELECT repo_id, permitted_from_ts, permitted_until_ts
          FROM platform_model_grants
         WHERE platform_user_id=?""", (platform_user_id,)).fetchall()

def user_can_access_repo(user: Dict[str, Any], repo_id: str, *, at_ts: Optional[int] = None) -> bool:
    if user["role"] == "admin":
        return True
    with connect() as c:
        row = c.execute("SELECT owner_user_id, visibility FROM models WHERE repo_id=?", (repo_id,)).fetchone()
        if not row:
            return False
        owner_id = row["owner_user_id"]
        visibility = (row["visibility"] or "private").lower()
        if user["role"] == "developer":
            # owner or public
            return (owner_id and int(owner_id) == int(user["user_id"])) or visibility == "public"
        if user["role"] == "platform":
            now = int(time.time()) if at_ts is None else int(at_ts)
            g = c.execute("""
            SELECT 1 FROM platform_model_grants
             WHERE platform_user_id=? AND repo_id=? AND permitted_from_ts<=?
               AND (permitted_until_ts IS NULL OR permitted_until_ts>=?)""",
                          (int(user["user_id"]), repo_id, now, now)).fetchone()
            return bool(g)
    return False

def resolve_object_key(conn: sqlite3.Connection, repo_id: str, rfilename: str, *, version: Optional[str]) -> str:
    # Prefer last uploaded key if exists; else default scheme includes version
    row = conn.execute("""
      SELECT object_key FROM uploads
       WHERE repo_id=? AND rfilename=? AND COALESCE(version,'')=COALESCE(?, '')
         AND target IN ('minio','s3') ORDER BY uploaded_ts DESC LIMIT 1
    """, (repo_id, rfilename, version)).fetchone()
    if row and row["object_key"]:
        return row["object_key"]
    # default path
    if version:
        return f"hf/{repo_id.strip('/')}/versions/{version}/{rfilename}"
    return f"hf/{repo_id.strip('/')}/{rfilename}"
