#!/usr/bin/env python3
import hashlib, os, secrets, sqlite3, time
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

DB_PATH = os.environ.get("DB_PATH", "/app/db/models.db")

def _now() -> int: return int(time.time())

def conn():
    p = Path(DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(p))
    c.row_factory = sqlite3.Row
    return c

# --- schema for users/api keys/access logs ---
def ensure_registry_tables() -> None:
    with conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT UNIQUE,
          name TEXT,
          is_active INTEGER DEFAULT 1,
          created_ts INTEGER
        );""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS api_keys(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          key_hash TEXT UNIQUE NOT NULL,
          created_ts INTEGER,
          last_used_ts INTEGER,
          revoked_ts INTEGER,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);")
        c.execute("""
        CREATE TABLE IF NOT EXISTS access_logs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          api_key_id INTEGER,
          event_type TEXT,
          repo_id TEXT,
          rfilename TEXT,
          object_key TEXT,
          size INTEGER,
          status TEXT,
          remote_addr TEXT,
          user_agent TEXT,
          ts INTEGER
        );""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON access_logs(ts);")
        c.commit()

# --- helpers ---
def _hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()

def _new_key() -> str:
    return "sk-" + secrets.token_hex(24)

# --- user & key management ---
def create_user(email: str, name: str) -> Tuple[int, str]:
    """Create user and first API key; returns (user_id, api_key_plaintext)."""
    now = _now()
    with conn() as c:
        c.execute("INSERT OR IGNORE INTO users(email, name, is_active, created_ts) VALUES(?,?,1,?)",
                  (email, name, now))
        row = c.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
        uid = int(row["id"])
        # rotate: revoke existing keys
        c.execute("UPDATE api_keys SET revoked_ts=? WHERE user_id=? AND revoked_ts IS NULL", (now, uid))
        plain = _new_key(); kh = _hash_key(plain)
        c.execute("INSERT INTO api_keys(user_id, key_hash, created_ts) VALUES(?,?,?)", (uid, kh, now))
        c.commit()
        return uid, plain

def rotate_key(user_id: int) -> str:
    now = _now()
    plain = _new_key(); kh = _hash_key(plain)
    with conn() as c:
        c.execute("UPDATE api_keys SET revoked_ts=? WHERE user_id=? AND revoked_ts IS NULL", (now, user_id))
        c.execute("INSERT INTO api_keys(user_id, key_hash, created_ts) VALUES(?,?,?)", (user_id, kh, now))
        c.commit()
    return plain

def user_from_api_key(plaintext: str) -> Optional[Dict[str, Any]]:
    kh = _hash_key(plaintext)
    with conn() as c:
        a = c.execute("""
          SELECT ak.id AS api_key_id, ak.user_id, ak.revoked_ts,
                 u.email, u.name, u.is_active
            FROM api_keys ak JOIN users u ON u.id=ak.user_id
           WHERE ak.key_hash=? LIMIT 1""", (kh,)).fetchone()
        if not a or a["revoked_ts"] is not None or not a["is_active"]:
            return None
        c.execute("UPDATE api_keys SET last_used_ts=? WHERE id=?", (_now(), a["api_key_id"]))
        return dict(a)

# --- access logging ---
def log_access(user_id: Optional[int], api_key_id: Optional[int], event_type: str,
               repo_id: Optional[str], rfilename: Optional[str],
               object_key: Optional[str], size: Optional[int],
               status: str, remote_addr: Optional[str], user_agent: Optional[str]) -> None:
    with conn() as c:
        c.execute("""
        INSERT INTO access_logs(user_id, api_key_id, event_type, repo_id, rfilename, object_key,
                                size, status, remote_addr, user_agent, ts)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (user_id, api_key_id, event_type, repo_id, rfilename, object_key,
         size, status, remote_addr, user_agent, _now()))
        c.commit()

def ensure_registry_tables() -> None:
    with conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS users(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email TEXT UNIQUE,
          name TEXT,
          is_active INTEGER DEFAULT 1,
          created_ts INTEGER
        );""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS api_keys(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          key_hash TEXT UNIQUE NOT NULL,
          created_ts INTEGER,
          last_used_ts INTEGER,
          revoked_ts INTEGER,
          FOREIGN KEY(user_id) REFERENCES users(id)
        );""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);")
        c.execute("""
        CREATE TABLE IF NOT EXISTS access_logs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          api_key_id INTEGER,
          event_type TEXT,
          repo_id TEXT,
          rfilename TEXT,
          object_key TEXT,
          size INTEGER,
          status TEXT,
          remote_addr TEXT,
          user_agent TEXT,
          ts INTEGER
        );""")
        # NEW: extra indexes for usage analytics
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON access_logs(ts);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_user_ts ON access_logs(user_id, ts);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_event_ts ON access_logs(event_type, ts);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_logs_repo_ts ON access_logs(repo_id, ts);")
        c.commit()