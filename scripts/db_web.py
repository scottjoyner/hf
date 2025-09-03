#!/usr/bin/env python3
import os, sqlite3, time, hashlib, hmac, re
from pathlib import Path
from flask import Flask, g, render_template, request, abort, redirect, url_for, session, send_file, jsonify, make_response

# ----------------- Config -----------------
DB_PATH      = os.getenv("DB_PATH", "/app/db/models.db")
SECRET_KEY   = os.getenv("SECRET_KEY", "dev-secret-change-me")  # set in prod
SESSION_NAME = os.getenv("SESSION_COOKIE_NAME", "reg_sess")
# Optional: Enable presign mode later (requires boto3 or minio client)
ENABLE_PRESIGN = (os.getenv("ENABLE_PRESIGN", "0").lower() in ("1", "true", "yes"))

app = Flask(__name__, template_folder="webapp/templates", static_folder="webapp/static")
app.secret_key = SECRET_KEY
app.config["SESSION_COOKIE_NAME"] = SESSION_NAME
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "0") in ("1","true","yes")

# ----------------- DB helpers -----------------
def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
        db = g._db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        _ensure_schema(db)
    return db

@app.teardown_appcontext
def close_db(_exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

def _ensure_schema(db: sqlite3.Connection):
    # Existing tables assumed: models, files, uploads
    # New tables: users, api_keys, permissions, downloads_audit
    db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        name TEXT,
        role TEXT NOT NULL DEFAULT 'developer', -- 'admin' | 'platform' | 'developer'
        pw_hash TEXT,                            -- for session login (UI); optional for API-only users
        active INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now'))
    );

    CREATE TABLE IF NOT EXISTS api_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        key_hash TEXT UNIQUE NOT NULL,          -- SHA256 hex of the raw API key
        label TEXT,
        created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
        last_used_ts INTEGER,
        revoked INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        repo_id TEXT NOT NULL,                 -- exact repo ('owner/repo') or '*' for all
        path_prefix TEXT,                      -- optional subpath constraint within repo (prefix match on files.rfilename)
        can_download INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS downloads_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,                       -- nullable for anonymous (should be rare)
        api_key_id INTEGER,
        repo_id TEXT NOT NULL,
        rfilename TEXT NOT NULL,
        ts INTEGER NOT NULL DEFAULT (strftime('%s','now')),
        ip TEXT,
        user_agent TEXT,
        via TEXT,                              -- 'session' | 'api_key'
        outcome TEXT,                          -- 'ALLOW' | 'DENY'
        permission_id INTEGER,                 -- the matched permissions.id (if any)
        permission_source TEXT,                -- e.g., 'role:admin' | 'role:platform' | 'explicit'
        message TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(api_key_id) REFERENCES api_keys(id)
    );
    """)
    db.commit()

# ----------------- Auth utils -----------------
def _now() -> int: return int(time.time())

def _hash_api_key(raw: str) -> str:
    # Store SHA256 hex of key; lookup compares deterministic hash
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def current_user():
    uid = session.get("uid")
    if not uid:
        return None
    db = get_db()
    u = db.execute("SELECT * FROM users WHERE id=? AND active=1", (uid,)).fetchone()
    return u

def require_login():
    u = current_user()
    if not u:
        return redirect(url_for("login", next=request.path))
    return u

def _check_password(pw_hash: str, password: str) -> bool:
    # Simple PBKDF2 via werkzeug (fallback manual) to avoid new deps:
    try:
        from werkzeug.security import check_password_hash
        return check_password_hash(pw_hash or "", password or "")
    except Exception:
        # Very basic fallback: treat stored as 'sha256$hex'
        if not pw_hash or not password or not pw_hash.startswith("sha256$"):
            return False
        _, hexv = pw_hash.split("$", 1)
        return hmac.compare_digest(hashlib.sha256(password.encode()).hexdigest(), hexv)

def _make_password_hash(password: str) -> str:
    try:
        from werkzeug.security import generate_password_hash
        return generate_password_hash(password)
    except Exception:
        return "sha256$" + hashlib.sha256(password.encode()).hexdigest()

def _auth_from_api_key():
    # Accept 'Authorization: Bearer <key>' or 'X-API-Key: <key>'
    raw = None
    auth = request.headers.get("Authorization","")
    if auth.startswith("Bearer "):
        raw = auth.split(" ",1)[1].strip()
    if not raw:
        raw = request.headers.get("X-API-Key","").strip()
    if not raw:
        return (None, None)  # (user, api_key_row)

    db = get_db()
    key_hash = _hash_api_key(raw)
    k = db.execute("SELECT * FROM api_keys WHERE key_hash=? AND revoked=0", (key_hash,)).fetchone()
    if not k:
        return (None, None)
    u = db.execute("SELECT * FROM users WHERE id=? AND active=1", (k["user_id"],)).fetchone()
    if not u:
        return (None, None)
    # touch last_used_ts
    db.execute("UPDATE api_keys SET last_used_ts=? WHERE id=?", (_now(), k["id"]))
    db.commit()
    return (u, k)

# ----------------- Permission check + audit -----------------
def _match_permission(db, user, repo_id: str, rfilename: str):
    """
    Returns (allowed:bool, permission_id:int|None, source:str)
    Priority:
      - admin role => allow (source 'role:admin')
      - explicit permission rows (most specific path_prefix)
      - platform role => allow (source 'role:platform')  [bulk access]
      - else deny
    """
    if not user:
        return (False, None, None)

    role = (user["role"] or "developer").lower()
    if role == "admin":
        return (True, None, "role:admin")

    # Explicit rows sorted by specificity (exact repo over '*', longer path_prefix first)
    rows = db.execute("""
        SELECT id, repo_id, path_prefix, can_download
        FROM permissions
        WHERE user_id=? AND can_download=1
          AND (repo_id=? OR repo_id='*')
        ORDER BY (CASE WHEN repo_id=? THEN 0 ELSE 1 END),
                 (CASE WHEN path_prefix IS NULL THEN 0 ELSE 1 END) DESC,
                 LENGTH(COALESCE(path_prefix,'')) DESC
    """, (user["id"], repo_id, repo_id)).fetchall()

    for pr in rows:
        pfx = pr["path_prefix"]
        if (pfx is None) or rfilename.startswith(pfx):
            return (True, pr["id"], "explicit")

    if role == "platform":
        return (True, None, "role:platform")

    return (False, None, None)

def _audit(db, *, user_id, api_key_id, repo_id, rfilename, outcome, via, permission_id, permission_source, message=None):
    db.execute("""
        INSERT INTO downloads_audit (user_id, api_key_id, repo_id, rfilename, ts, ip, user_agent, via, outcome, permission_id, permission_source, message)
        VALUES (?, ?, ?, ?, strftime('%s','now'), ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, api_key_id, repo_id, rfilename,
        request.remote_addr, request.headers.get("User-Agent",""),
        via, outcome, permission_id, permission_source, message
    ))
    db.commit()

# ----------------- Core pages -----------------
@app.route("/")
def index():
    db = get_db()
    m_count = db.execute("SELECT COUNT(*) c FROM models").fetchone()["c"]
    f_count = db.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
    u_count = db.execute("SELECT COUNT(*) c FROM uploads").fetchone()["c"]
    latest = db.execute("SELECT repo_id, last_modified, downloads FROM models ORDER BY last_modified DESC LIMIT 10").fetchall()
    top = db.execute("SELECT repo_id, downloads FROM models ORDER BY downloads DESC LIMIT 10").fetchall()
    user = current_user()
    return render_template("index.html", m_count=m_count, f_count=f_count, u_count=u_count, latest=latest, top=top, user=user)

@app.route("/models")
def models():
    q = request.args.get("q","").strip()
    db = get_db()
    if q:
        rows = db.execute("SELECT * FROM models WHERE repo_id LIKE ? OR model_name LIKE ? ORDER BY last_modified DESC",
                          (f"%{q}%", f"%{q}%")).fetchall()
    else:
        rows = db.execute("SELECT * FROM models ORDER BY last_modified DESC LIMIT 200").fetchall()
    return render_template("models.html", rows=rows, q=q, user=current_user())

@app.route("/models/<path:repo_id>")
def model_detail(repo_id):
    db = get_db()
    m = db.execute("SELECT * FROM models WHERE repo_id=?", (repo_id,)).fetchone()
    if not m:
        abort(404)
    files = db.execute("SELECT * FROM files WHERE repo_id=? ORDER BY rfilename", (repo_id,)).fetchall()
    # your original query used 'uploaded_at' but schema uses 'uploaded_ts'
    uploads = db.execute("SELECT * FROM uploads WHERE repo_id=? ORDER BY uploaded_ts DESC", (repo_id,)).fetchall()
    return render_template("model_detail.html", m=m, files=files, uploads=uploads, user=current_user())

@app.route("/files")
def files_page():
    db = get_db()
    rows = db.execute("SELECT * FROM files ORDER BY created_ts DESC LIMIT 500").fetchall()
    return render_template("files.html", rows=rows, user=current_user())

# ----------------- UI Auth -----------------
@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "GET":
        return render_template("login.html", next=request.args.get("next","/"), user=current_user())
    email = (request.form.get("email","") or "").strip().lower()
    password = request.form.get("password","") or ""
    next_url = request.form.get("next","/")

    db = get_db()
    u = db.execute("SELECT * FROM users WHERE email=? AND active=1", (email,)).fetchone()
    if not u or not _check_password(u["pw_hash"] or "", password):
        return render_template("login.html", error="Invalid credentials", next=next_url, user=None), 401

    session["uid"] = u["id"]
    return redirect(next_url or "/")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ----------------- UI Download (session) -----------------
@app.route("/download/<path:repo_id>/<path:rfilename>")
def ui_download(repo_id, rfilename):
    user = require_login()
    if not isinstance(user, sqlite3.Row):
        return user  # redirect to login

    db = get_db()
    f = db.execute("SELECT * FROM files WHERE repo_id=? AND rfilename=?", (repo_id, rfilename)).fetchone()
    if not f:
        abort(404)

    allowed, perm_id, source = _match_permission(db, user, repo_id, rfilename)
    if not allowed:
        _audit(db, user_id=user["id"], api_key_id=None, repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="session", permission_id=perm_id, permission_source=source, message="UI download denied")
        abort(403)

    # For now, stream from local path; later you can presign to S3/MinIO and redirect.
    local_path = f["local_path"]
    if not local_path or not Path(local_path).exists():
        _audit(db, user_id=user["id"], api_key_id=None, repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="session", permission_id=perm_id, permission_source=source, message="File missing on disk")
        abort(404)

    _audit(db, user_id=user["id"], api_key_id=None, repo_id=repo_id, rfilename=rfilename,
           outcome="ALLOW", via="session", permission_id=perm_id, permission_source=source, message="UI download")
    # You can add etag/last-mod headers if desired
    return send_file(local_path, as_attachment=True)

# ----------------- API Download (API key) -----------------
@app.route("/api/files/<path:repo_id>/<path:rfilename>/download", methods=["GET"])
def api_download(repo_id, rfilename):
    user, key = _auth_from_api_key()
    db = get_db()

    if not user or not key:
        # audit anonymous attempt with DENY if file exists
        _audit(db, user_id=None, api_key_id=None, repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="api_key", permission_id=None, permission_source=None, message="Missing/invalid API key")
        return jsonify({"error":"unauthorized"}), 401

    f = db.execute("SELECT * FROM files WHERE repo_id=? AND rfilename=?", (repo_id, rfilename)).fetchone()
    if not f:
        _audit(db, user_id=user["id"], api_key_id=key["id"], repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="api_key", permission_id=None, permission_source=None, message="File not found")
        return jsonify({"error":"not_found"}), 404

    allowed, perm_id, source = _match_permission(db, user, repo_id, rfilename)
    if not allowed:
        _audit(db, user_id=user["id"], api_key_id=key["id"], repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="api_key", permission_id=perm_id, permission_source=source, message="Policy denies download")
        return jsonify({"error":"forbidden"}), 403

    local_path = f["local_path"]
    if not local_path or not Path(local_path).exists():
        _audit(db, user_id=user["id"], api_key_id=key["id"], repo_id=repo_id, rfilename=rfilename,
               outcome="DENY", via="api_key", permission_id=perm_id, permission_source=source, message="File missing on disk")
        return jsonify({"error":"not_found"}), 404

    # Option A (default): stream file (keeps lineage in this service)
    _audit(db, user_id=user["id"], api_key_id=key["id"], repo_id=repo_id, rfilename=rfilename,
           outcome="ALLOW", via="api_key", permission_id=perm_id, permission_source=source, message="API download")
    return send_file(local_path, as_attachment=True)

    # Option B: if you enable presign later, audit here and return JSON with URL or 302 redirect


# ----------------- Admin helpers (optional) -----------------
@app.route("/admin/seed", methods=["POST"])
def admin_seed():
    """
    One-time helper to create a first admin and (optionally) a platform user + wildcard permission.
    Env:
      SEED_ADMIN_EMAIL, SEED_ADMIN_PASSWORD
    """
    db = get_db()
    email = (os.getenv("SEED_ADMIN_EMAIL","") or "").strip().lower()
    pw    = os.getenv("SEED_ADMIN_PASSWORD","") or ""
    if not email or not pw:
        return jsonify({"error":"set SEED_ADMIN_EMAIL and SEED_ADMIN_PASSWORD envs"}), 400

    existing = db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
    if existing:
        return jsonify({"status":"exists", "user_id": existing["id"]}), 200

    db.execute("INSERT INTO users(email, name, role, pw_hash, active) VALUES(?,?,?,?,1)",
               (email, email.split("@")[0], "admin", _make_password_hash(pw)))
    db.commit()
    uid = db.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()["id"]
    return jsonify({"status":"created", "user_id": uid}), 201

@app.route("/admin/api-keys", methods=["POST"])
def admin_create_api_key():
    """
    Admin can mint an API key for a user.
    JSON body: { "user_email": "...", "label": "build-agent-1" }
    Returns: {"api_key": "<raw-once>", "key_id": ...}
    """
    u = current_user()
    if not u or (u["role"] or "") not in ("admin","platform"):
        return jsonify({"error":"admin_only"}), 403

    data = request.get_json(force=True, silent=True) or {}
    email = (data.get("user_email","") or "").strip().lower()
    label = (data.get("label","") or "").strip()
    if not email:
        return jsonify({"error":"user_email required"}), 400

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email=? AND active=1", (email,)).fetchone()
    if not user:
        return jsonify({"error":"no such user"}), 404

    # generate raw key and hash
    raw = hashlib.sha256(os.urandom(32)).hexdigest()  # 64-hex chars, good enough
    key_hash = _hash_api_key(raw)
    db.execute("INSERT INTO api_keys(user_id, key_hash, label) VALUES(?,?,?)", (user["id"], key_hash, label))
    db.commit()
    kid = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    return jsonify({"api_key": raw, "key_id": kid}), 201

# ----------------- Minimal JSON view for audits (optional) -----------------
@app.route("/api/audits", methods=["GET"])
def api_audits():
    u = current_user()
    if not u or (u["role"] or "") not in ("admin","platform"):
        return jsonify({"error":"forbidden"}), 403
    db = get_db()
    rows = db.execute("""
      SELECT a.id, a.ts, a.repo_id, a.rfilename, a.via, a.outcome, a.permission_source,
             u.email AS user_email, k.label AS api_key_label, a.ip
      FROM downloads_audit a
      LEFT JOIN users u ON a.user_id=u.id
      LEFT JOIN api_keys k ON a.api_key_id=k.id
      ORDER BY a.ts DESC LIMIT 500
    """).fetchall()
    return jsonify([dict(r) for r in rows])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
