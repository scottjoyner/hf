# tests/conftest.py (replace app_client fixture’s import logic)
import os, sys, importlib, importlib.util
from pathlib import Path
import pytest

def _load_app_module():
    # project root = parent of tests/
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    # 1) If APP_MODULE_FILE is provided, use it
    env_path = os.getenv("APP_MODULE_FILE")
    if env_path:
        p = Path(env_path)
        if p.exists():
            spec = importlib.util.spec_from_file_location("main", str(p))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod

    # 2) Try common locations
    candidates = [
        root / "main.py",
        root / "app.py",
        root / "db_web.py",
        root / "registry" / "main.py",
    ]
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("main", str(p))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod

    # 3) Fallback to plain import if a package exists
    try:
        return importlib.import_module("main")
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "Could not locate your Flask app module. "
            "Set APP_MODULE_FILE=/path/to/your_main.py or run pytest from repo root with PYTHONPATH=."
        ) from e

@pytest.fixture()
def app_client(env):
    # import AFTER env so app reads temp DB paths
    appmod = _load_app_module()
    app = appmod.app
    app.testing = True
    # create base tables (as before) ...
    db = appmod.get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS models (
      repo_id TEXT PRIMARY KEY,
      author TEXT, model TEXT,
      last_modified TEXT, downloads INTEGER
    );
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      repo_id TEXT NOT NULL,
      author TEXT, model TEXT,
      rfilename TEXT NOT NULL,
      size INTEGER,
      sha256 TEXT,
      local_path TEXT,
      storage_root TEXT,
      created_ts INTEGER DEFAULT (strftime('%s','now')),
      updated_ts INTEGER DEFAULT (strftime('%s','now'))
    );
    CREATE TABLE IF NOT EXISTS uploads (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      repo_id TEXT NOT NULL,
      rfilename TEXT,
      uploaded_ts INTEGER DEFAULT (strftime('%s','now'))
    );
    """)
    db.commit()
    yield app, appmod
