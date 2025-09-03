#!/usr/bin/env python3
import os, sqlite3
from flask import Flask, g, render_template, request, abort

DB_PATH = os.getenv("DB_PATH", "/app/db/models.db")

app = Flask(__name__, template_folder="webapp/templates", static_folder="webapp/static")

def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

@app.route("/")
def index():
    db = get_db()
    m_count = db.execute("SELECT COUNT(*) c FROM models").fetchone()["c"]
    f_count = db.execute("SELECT COUNT(*) c FROM files").fetchone()["c"]
    u_count = db.execute("SELECT COUNT(*) c FROM uploads").fetchone()["c"]
    latest = db.execute("SELECT repo_id, last_modified, downloads FROM models ORDER BY last_modified DESC LIMIT 10").fetchall()
    top = db.execute("SELECT repo_id, downloads FROM models ORDER BY downloads DESC LIMIT 10").fetchall()
    return render_template("index.html", m_count=m_count, f_count=f_count, u_count=u_count, latest=latest, top=top)

@app.route("/models")
def models():
    q = request.args.get("q","").strip()
    db = get_db()
    if q:
        rows = db.execute("SELECT * FROM models WHERE repo_id LIKE ? OR model_name LIKE ? ORDER BY last_modified DESC",
                          (f"%{q}%", f"%{q}%")).fetchall()
    else:
        rows = db.execute("SELECT * FROM models ORDER BY last_modified DESC LIMIT 200").fetchall()
    return render_template("models.html", rows=rows, q=q)

@app.route("/models/<path:repo_id>")
def model_detail(repo_id):
    db = get_db()
    m = db.execute("SELECT * FROM models WHERE repo_id=?", (repo_id,)).fetchone()
    if not m:
        abort(404)
    files = db.execute("SELECT * FROM files WHERE repo_id=? ORDER BY rfilename", (repo_id,)).fetchall()
    uploads = db.execute("SELECT * FROM uploads WHERE repo_id=? ORDER BY uploaded_at DESC", (repo_id,)).fetchall()
    return render_template("model_detail.html", m=m, files=files, uploads=uploads)

@app.route("/files")
def files():
    db = get_db()
    rows = db.execute("SELECT * FROM files ORDER BY created_at DESC LIMIT 500").fetchall()
    return render_template("files.html", rows=rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
