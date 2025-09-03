#!/usr/bin/env python3
import os, sqlite3, time
from pathlib import Path
from typing import List, Optional, Any, Dict

from fastapi import FastAPI, HTTPException, Query, Depends, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from minio import Minio

# local
from registry_app.db import (
    ensure_registry_tables, log_access, user_from_api_key,
    create_user, rotate_key
)

DB_PATH = os.environ.get("DB_PATH", "/app/db/models.db")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "models")
MINIO_SECURE     = os.environ.get("MINIO_SECURE", "false").strip().lower() == "true"
ADMIN_TOKEN      = os.environ.get("REGISTRY_ADMIN_TOKEN", "").strip()

def get_conn() -> sqlite3.Connection:
    p = Path(DB_PATH); p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(p)); c.row_factory = sqlite3.Row
    return c

_minio: Optional[Minio] = None
def get_minio() -> Minio:
    global _minio
    if _minio is None:
        _minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    return _minio

# ---------- models ----------
class ModelRow(BaseModel):
    repo_id: str
    canonical_url: Optional[str] = None
    model_name: Optional[str] = None
    author: Optional[str] = None
    pipeline_tag: Optional[str] = None
    license: Optional[str] = None
    parameters: Optional[int] = None
    parameters_readable: Optional[str] = None
    downloads: Optional[int] = None
    likes: Optional[int] = None
    created_at: Optional[str] = None
    last_modified: Optional[str] = None
    languages: Optional[str] = None
    tags: Optional[str] = None
    last_update_ts: Optional[int] = None
    file_count: Optional[int] = None
    has_safetensors: Optional[int] = None
    has_bin: Optional[int] = None

class FileRow(BaseModel):
    rfilename: str
    size: Optional[int] = None
    sha256: Optional[str] = None
    updated_ts: Optional[int] = None
    object_key: Optional[str] = None
    presigned_url: Optional[str] = None

class Manifest(BaseModel):
    schema: str = "hf-model-manifest/v1"
    repo_id: str
    updated_ts: int
    files: List[FileRow]

class ChangeRow(BaseModel):
    repo_id: str
    last_update_ts: int

# ---------- auth dependencies ----------
def current_user(x_api_key: Optional[str] = Header(default=None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="missing x-api-key")
    user = user_from_api_key(x_api_key)
    if not user:
        raise HTTPException(status_code=401, detail="invalid or revoked api key")
    return user

def admin_required(x_admin_token: Optional[str] = Header(default=None)):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="admin token not configured")
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="invalid admin token")
    return {"role": "admin"}

# ---------- helpers ----------
def _window(since: Optional[int], until: Optional[int], default_days: int = 30) -> (int, int):
    now = int(time.time())
    if until is None: until = now
    if since is None: since = until - default_days * 86400
    return int(since), int(until)

def _object_key_for(c: sqlite3.Connection, repo_id: str, rfilename: str) -> str:
    row = c.execute("""
      SELECT object_key FROM uploads
       WHERE repo_id=? AND rfilename=? AND target IN ('minio','s3')
       ORDER BY uploaded_ts DESC LIMIT 1""", (repo_id, rfilename)).fetchone()
    if row and row["object_key"]:
        return row["object_key"]
    return f"hf/{repo_id.strip('/')}/{rfilename}"

# ---------- FastAPI ----------
app = FastAPI(title="Model Registry API", version="0.3.0")

@app.on_event("startup")
def _startup():
    ensure_registry_tables()

@app.get("/healthz")
def healthz():
    return {"ok": True, "time": int(time.time())}

# ---------- user endpoints ----------
class RegisterIn(BaseModel):
    email: str
    name: str

class RegisterOut(BaseModel):
    user_id: int
    api_key: str

@app.post("/v1/users/register", response_model=RegisterOut)
def register(body: RegisterIn):
    uid, api_key = create_user(body.email.strip(), body.name.strip())
    return RegisterOut(user_id=uid, api_key=api_key)

@app.post("/v1/users/rotate-key")
def rotate(user=Depends(current_user)):
    new_key = rotate_key(int(user["user_id"]))
    return {"user_id": int(user["user_id"]), "api_key": new_key}

@app.get("/v1/users/me")
def me(user=Depends(current_user)):
    return {"user_id": int(user["user_id"]), "email": user["email"], "name": user["name"]}

# ---------- model catalog ----------
@app.get("/v1/models", response_model=List[ModelRow])
def list_models(
    q: Optional[str] = Query(None), updated_since: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0),
    user=Depends(current_user)
):
    sql = """
    SELECT repo_id, canonical_url, model_name, author, pipeline_tag, license,
           parameters, parameters_readable, downloads, likes, created_at, last_modified,
           languages, tags, last_update_ts,
           COALESCE(file_count, NULL) AS file_count,
           COALESCE(has_safetensors, NULL) AS has_safetensors,
           COALESCE(has_bin, NULL) AS has_bin
    FROM models WHERE 1=1
    """
    params: List[Any] = []
    if q:
        like = f"%{q}%"
        sql += " AND (repo_id LIKE ? OR model_name LIKE ? OR author LIKE ?)"
        params += [like, like, like]
    if updated_since:
        sql += " AND (last_update_ts IS NOT NULL AND last_update_ts > ?)"
        params.append(updated_since)
    sql += " ORDER BY last_update_ts DESC NULLS LAST, repo_id ASC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with get_conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [ModelRow(**dict(r)) for r in rows]

@app.get("/v1/models/{repo_id}", response_model=ModelRow)
def get_model(repo_id: str, user=Depends(current_user)):
    with get_conn() as c:
        r = c.execute("""
        SELECT repo_id, canonical_url, model_name, author, pipeline_tag, license,
               parameters, parameters_readable, downloads, likes, created_at, last_modified,
               languages, tags, last_update_ts,
               COALESCE(file_count, NULL) AS file_count,
               COALESCE(has_safetensors, NULL) AS has_safetensors,
               COALESCE(has_bin, NULL) AS has_bin
        FROM models WHERE repo_id=?""", (repo_id,)).fetchone()
        if not r: raise HTTPException(status_code=404, detail="model not found")
        return ModelRow(**dict(r))

@app.get("/v1/models/{repo_id}/files", response_model=List[FileRow])
def list_files(repo_id: str, presign: bool = Query(False), expires: int = Query(3600, ge=60, le=86400),
               user=Depends(current_user), request: Request = None):
    with get_conn() as c:
        files = c.execute("""
         SELECT rfilename, size, sha256, updated_ts FROM files
          WHERE repo_id=? ORDER BY rfilename ASC""", (repo_id,)).fetchall()
        mc = get_minio() if presign else None
        out: List[FileRow] = []
        for r in files:
            obj = _object_key_for(c, repo_id, r["rfilename"])
            url = None
            if presign and mc:
                try: url = mc.presigned_get_object(MINIO_BUCKET, obj, expires=expires)
                except Exception: url = None
            fr = FileRow(rfilename=r["rfilename"], size=r["size"], sha256=r["sha256"],
                         updated_ts=r["updated_ts"], object_key=obj, presigned_url=url)
            out.append(fr)
        # log event
        ra = request.headers.get("x-forwarded-for") or request.client.host if request else None
        ua = request.headers.get("user-agent") if request else None
        log_access(user_id=int(user["user_id"]), api_key_id=int(user["api_key_id"]),
                   event_type="files_list", repo_id=repo_id, rfilename=None,
                   object_key=None, size=None, status="ok", remote_addr=ra, user_agent=ua)
        return out

@app.get("/v1/files/{repo_id:path}/{rfilename:path}/download")
def direct_download(repo_id: str, rfilename: str, expires: int = Query(3600, ge=60, le=86400),
                    user=Depends(current_user), request: Request = None):
    with get_conn() as c:
        row = c.execute("""
          SELECT rfilename, size, sha256, updated_ts FROM files
           WHERE repo_id=? AND rfilename=? LIMIT 1""", (repo_id, rfilename)).fetchone()
        if not row: raise HTTPException(status_code=404, detail="file not found")
        obj = _object_key_for(c, repo_id, row["rfilename"])
        try:
            url = get_minio().presigned_get_object(MINIO_BUCKET, obj, expires=expires)
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"object not accessible: {obj}") from e
        ra = request.headers.get("x-forwarded-for") or request.client.host if request else None
        ua = request.headers.get("user-agent") if request else None
        log_access(user_id=int(user["user_id"]), api_key_id=int(user["api_key_id"]),
                   event_type="download_url_issued", repo_id=repo_id, rfilename=row["rfilename"],
                   object_key=obj, size=row["size"], status="ok", remote_addr=ra, user_agent=ua)
        return JSONResponse({"repo_id": repo_id, "rfilename": row["rfilename"],
                             "size": row["size"], "sha256": row["sha256"],
                             "object_key": obj, "url": url, "expires_in": expires})

@app.get("/v1/manifest/{repo_id}", response_model=Manifest)
def manifest(repo_id: str, presign: bool = Query(False), expires: int = Query(3600, ge=60, le=86400),
             user=Depends(current_user), request: Request = None):
    with get_conn() as c:
        m = c.execute("SELECT last_update_ts FROM models WHERE repo_id=?", (repo_id,)).fetchone()
        if not m: raise HTTPException(status_code=404, detail="model not found")
        files = list_files.__wrapped__(repo_id=repo_id, presign=presign, expires=expires, user=user, request=request)
        ra = request.headers.get("x-forwarded-for") or request.client.host if request else None
        ua = request.headers.get("user-agent") if request else None
        log_access(user_id=int(user["user_id"]), api_key_id=int(user["api_key_id"]),
                   event_type="manifest", repo_id=repo_id, rfilename=None,
                   object_key=None, size=None, status="ok", remote_addr=ra, user_agent=ua)
        return Manifest(repo_id=repo_id, updated_ts=m["last_update_ts"] or 0, files=files)

@app.get("/v1/changes", response_model=List[ChangeRow])
def changes(since: int = Query(...), limit: int = Query(500, ge=1, le=2000), user=Depends(current_user)):
    with get_conn() as c:
        rows = c.execute("""
          SELECT repo_id, last_update_ts FROM models
           WHERE last_update_ts IS NOT NULL AND last_update_ts > ?
           ORDER BY last_update_ts ASC LIMIT ?""", (since, limit)).fetchall()
        return [ChangeRow(**dict(r)) for r in rows]

# ---------- NEW: per-user usage ----------
@app.get("/v1/users/me/usage")
def my_usage(
    since: Optional[int] = Query(None),
    until: Optional[int] = Query(None),
    top_models_limit: int = Query(20, ge=1, le=200),
    user=Depends(current_user)
):
    since, until = _window(since, until, default_days=30)
    uid = int(user["user_id"])
    out: Dict[str, Any] = {"window": {"since": since, "until": until}}

    with get_conn() as c:
        # totals
        t = c.execute("""
          SELECT COUNT(*) AS events,
                 SUM(CASE WHEN event_type='manifest' THEN 1 ELSE 0 END) AS manifests,
                 SUM(CASE WHEN event_type='files_list' THEN 1 ELSE 0 END) AS files_list,
                 SUM(CASE WHEN event_type='download_url_issued' THEN 1 ELSE 0 END) AS downloads,
                 MAX(ts) AS last_seen_ts
            FROM access_logs
           WHERE user_id=? AND ts BETWEEN ? AND ?;
        """, (uid, since, until)).fetchone()
        out["totals"] = {k: int(t[k] or 0) for k in ["events","manifests","files_list","downloads"]}
        out["last_seen_ts"] = int(t["last_seen_ts"] or 0)

        # distincts
        d_repo = c.execute("""
          SELECT COUNT(DISTINCT repo_id) AS n FROM access_logs
           WHERE user_id=? AND repo_id IS NOT NULL AND ts BETWEEN ? AND ?;
        """, (uid, since, until)).fetchone()
        out["distinct_models"] = int(d_repo["n"] or 0)

        # top models (by downloads)
        top = c.execute("""
          SELECT repo_id, COUNT(*) AS downloads
            FROM access_logs
           WHERE user_id=? AND event_type='download_url_issued' AND ts BETWEEN ? AND ?
           GROUP BY repo_id ORDER BY downloads DESC, repo_id ASC LIMIT ?;
        """, (uid, since, until, top_models_limit)).fetchall()
        out["top_models"] = [{"repo_id": r["repo_id"], "downloads": int(r["downloads"])} for r in top]

        # timeseries (daily)
        ts_rows = c.execute("""
          SELECT date(ts,'unixepoch') AS day,
                 COUNT(*) AS events,
                 SUM(CASE WHEN event_type='download_url_issued' THEN 1 ELSE 0 END) AS downloads
            FROM access_logs
           WHERE user_id=? AND ts BETWEEN ? AND ?
           GROUP BY day ORDER BY day ASC;
        """, (uid, since, until)).fetchall()
        out["timeseries_daily"] = [
            {"day": r["day"], "events": int(r["events"]), "downloads": int(r["downloads"] or 0)} for r in ts_rows
        ]
    return out

# ---------- NEW: admin usage dashboard ----------
@app.get("/v1/admin/usage")
def admin_usage(
    since: Optional[int] = Query(None),
    until: Optional[int] = Query(None),
    top_users_limit: int = Query(50, ge=1, le=500),
    top_models_limit: int = Query(100, ge=1, le=1000),
    filter_user_id: Optional[int] = Query(None),
    filter_email: Optional[str] = Query(None),
    admin=Depends(admin_required)
):
    since, until = _window(since, until, default_days=30)
    out: Dict[str, Any] = {"window": {"since": since, "until": until}}

    with get_conn() as c:
        user_clause = ""
        params: List[Any] = [since, until]

        # resolve user filter
        if filter_email and not filter_user_id:
            row = c.execute("SELECT id FROM users WHERE email=?", (filter_email.strip(),)).fetchone()
            if row: filter_user_id = int(row["id"])
        if filter_user_id:
            user_clause = " AND l.user_id=? "
            params.append(int(filter_user_id))

        # totals over selection
        t = c.execute(f"""
          SELECT COUNT(*) AS events,
                 SUM(CASE WHEN l.event_type='manifest' THEN 1 ELSE 0 END) AS manifests,
                 SUM(CASE WHEN l.event_type='files_list' THEN 1 ELSE 0 END) AS files_list,
                 SUM(CASE WHEN l.event_type='download_url_issued' THEN 1 ELSE 0 END) AS downloads,
                 MAX(l.ts) AS last_seen_ts
            FROM access_logs l
           WHERE l.ts BETWEEN ? AND ? {user_clause};
        """, tuple(params)).fetchone()
        out["totals"] = {k: int(t[k] or 0) for k in ["events","manifests","files_list","downloads"]}
        out["last_seen_ts"] = int(t["last_seen_ts"] or 0)

        # top users (ignored if filtering a single user)
        if not filter_user_id:
            rows = c.execute("""
              SELECT u.id AS user_id, u.email, u.name,
                     COUNT(*) AS events,
                     SUM(CASE WHEN l.event_type='download_url_issued' THEN 1 ELSE 0 END) AS downloads,
                     MAX(l.ts) AS last_seen_ts
                FROM access_logs l JOIN users u ON u.id=l.user_id
               WHERE l.ts BETWEEN ? AND ?
               GROUP BY u.id, u.email, u.name
               ORDER BY downloads DESC, events DESC
               LIMIT ?;
            """, (since, until, top_users_limit)).fetchall()
            out["top_users"] = [
                {"user_id": int(r["user_id"]), "email": r["email"], "name": r["name"],
                 "events": int(r["events"]), "downloads": int(r["downloads"] or 0),
                 "last_seen_ts": int(r["last_seen_ts"] or 0)}
                for r in rows
            ]

        # top models in selection
        rows = c.execute(f"""
          SELECT l.repo_id, COUNT(*) AS downloads
            FROM access_logs l
           WHERE l.event_type='download_url_issued' AND l.ts BETWEEN ? AND ? {user_clause}
           GROUP BY l.repo_id
           ORDER BY downloads DESC, l.repo_id ASC
           LIMIT ?;
        """, tuple(params + [top_models_limit])).fetchall()
        out["top_models"] = [{"repo_id": r["repo_id"], "downloads": int(r["downloads"])} for r in rows]

        # timeseries (daily)
        rows = c.execute(f"""
          SELECT date(l.ts,'unixepoch') AS day,
                 COUNT(*) AS events,
                 SUM(CASE WHEN l.event_type='download_url_issued' THEN 1 ELSE 0 END) AS downloads
            FROM access_logs l
           WHERE l.ts BETWEEN ? AND ? {user_clause}
           GROUP BY day ORDER BY day ASC;
        """, tuple(params)).fetchall()
        out["timeseries_daily"] = [
            {"day": r["day"], "events": int(r["events"]), "downloads": int(r["downloads"] or 0)}
            for r in rows
        ]

    # include filter echo
    if filter_user_id or filter_email:
        out["filter"] = {"user_id": filter_user_id, "email": filter_email}
    return out
