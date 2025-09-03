#!/usr/bin/env python3
import os, time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from minio import Minio

from registry_app.db import (
    ensure_registry_tables, connect, log_access,
    user_from_api_key, create_user, rotate_key,
    upsert_model, create_version, upsert_file, record_upload,
    grant_platform_access, revoke_platform_access, list_grants_for_user,
    user_can_access_repo, resolve_object_key
)

# ------------------ Settings ------------------
DB_PATH = os.environ.get("DB_PATH", "/app/db/models.db")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "models")
MINIO_SECURE     = os.environ.get("MINIO_SECURE", "false").strip().lower() == "true"
ADMIN_TOKEN      = os.environ.get("REGISTRY_ADMIN_TOKEN", "").strip()
DEFAULT_PRESIGN_EXP = int(os.environ.get("DEFAULT_PRESIGN_EXP", "3600"))

app = FastAPI(title="Model Registry API", version="1.0.0")

# CORS (tighten for prod domains)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_minio: Optional[Minio] = None
def get_minio() -> Minio:
    global _minio
    if _minio is None:
        _minio = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
    return _minio

@app.on_event("startup")
def _startup():
    ensure_registry_tables()

# ------------------ Auth dependencies ------------------

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

def role_required(role: str):
    def _dep(user=Depends(current_user)):
        if user["role"] != role and user["role"] != "admin":
            raise HTTPException(status_code=403, detail=f"{role} role required")
        return user
    return _dep

# ------------------ Schemas ------------------

class RegisterIn(BaseModel):
    email: str
    name: str
    # role omitted here (defaults to developer). Admin endpoint can create platform/admin users.

class RegisterOut(BaseModel):
    user_id: int
    api_key: str

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
    visibility: Optional[str] = "private"

class FileRow(BaseModel):
    rfilename: str
    version: Optional[str] = None
    size: Optional[int] = None
    sha256: Optional[str] = None
    updated_ts: Optional[int] = None
    object_key: Optional[str] = None
    presigned_url: Optional[str] = None

class ManifestEntry(BaseModel):
    repo_id: str
    allowed_from_ts: int
    files: List[FileRow] = Field(default_factory=list)

class PlatformManifest(BaseModel):
    schema: str = "hf-platform-manifest/v1"
    generated_ts: int
    items: List[ManifestEntry]

# ------------------ Utility ------------------

def _window(since: Optional[int], until: Optional[int], default_days: int = 30) -> (int, int):
    now = int(time.time())
    if until is None: until = now
    if since is None: since = until - default_days * 86400
    return int(since), int(until)

# ------------------ Health ------------------

@app.get("/healthz")
def healthz():
    return {"ok": True, "time": int(time.time())}

# ------------------ Users ------------------

@app.post("/v1/users/register", response_model=RegisterOut, tags=["users"])
def register(body: RegisterIn):
    # Public self-serve for developer users
    uid, api_key = create_user(body.email.strip(), body.name.strip(), role="developer")
    return RegisterOut(user_id=uid, api_key=api_key)

@app.post("/v1/users/rotate-key", tags=["users"])
def rotate(user=Depends(current_user)):
    new_key = rotate_key(int(user["user_id"]))
    return {"user_id": int(user["user_id"]), "api_key": new_key}

@app.get("/v1/users/me", tags=["users"])
def me(user=Depends(current_user)):
    return {"user_id": int(user["user_id"]), "email": user["email"], "name": user["name"], "role": user["role"]}

# ------------------ Admin: users & grants ------------------

class AdminCreateUserIn(BaseModel):
    email: str
    name: str
    role: str = Field(regex="^(developer|platform|admin)$")

@app.post("/v1/admin/users", tags=["admin"])
def admin_create_user(body: AdminCreateUserIn, admin=Depends(admin_required)):
    uid, key = create_user(body.email.strip(), body.name.strip(), body.role)
    return {"user_id": uid, "api_key": key, "role": body.role}

class GrantIn(BaseModel):
    platform_user_id: int
    repo_id: str
    permitted_from_ts: int
    permitted_until_ts: Optional[int] = None

@app.post("/v1/admin/grants", tags=["admin"])
def admin_grant_access(body: GrantIn, admin=Depends(admin_required)):
    grant_platform_access(body.platform_user_id, body.repo_id, body.permitted_from_ts, body.permitted_until_ts)
    return {"ok": True}

@app.delete("/v1/admin/grants", tags=["admin"])
def admin_revoke_access(platform_user_id: int = Query(...), repo_id: str = Query(...), admin=Depends(admin_required)):
    revoke_platform_access(platform_user_id, repo_id); return {"ok": True}

@app.get("/v1/admin/grants", tags=["admin"])
def admin_list_grants(platform_user_id: Optional[int] = Query(None), admin=Depends(admin_required)):
    if not platform_user_id:
        return {"detail": "provide platform_user_id"}
    rows = list_grants_for_user(platform_user_id)
    return [{"repo_id": r["repo_id"], "permitted_from_ts": int(r["permitted_from_ts"] or 0),
             "permitted_until_ts": int(r["permitted_until_ts"] or 0) if r["permitted_until_ts"] else None}
            for r in rows]

# ------------------ Developer: models, versions, uploads ------------------

class DevModelCreate(BaseModel):
    repo_id: str
    model_name: Optional[str] = None
    canonical_url: Optional[str] = None
    visibility: Optional[str] = Field(default="private", regex="^(private|public)$")

@app.post("/v1/dev/models", tags=["developer"])
def dev_create_model(body: DevModelCreate, user=Depends(role_required("developer"))):
    fields = {}
    if body.model_name: fields["model_name"] = body.model_name
    if body.canonical_url: fields["canonical_url"] = body.canonical_url
    if body.visibility: fields["visibility"] = body.visibility
    upsert_model(repo_id=body.repo_id.strip(), owner_user_id=int(user["user_id"]), fields=fields)
    return {"ok": True, "repo_id": body.repo_id}

class DevVersionCreate(BaseModel):
    version: str
    notes: Optional[str] = None

@app.post("/v1/dev/models/{repo_id}/versions", tags=["developer"])
def dev_create_version(repo_id: str, body: DevVersionCreate, user=Depends(role_required("developer"))):
    # ensure ownership
    if not user_can_access_repo(user, repo_id):  # developers can access their own
        raise HTTPException(status_code=403, detail="not owner or model missing")
    vid = create_version(repo_id, body.version, body.notes)
    if vid < 0:
        raise HTTPException(status_code=400, detail="could not create version")
    return {"ok": True, "version": body.version}

class UploadInitOut(BaseModel):
    object_key: str
    url: str
    expires_in: int

@app.post("/v1/dev/models/{repo_id}/versions/{version}/uploads/initiate", response_model=UploadInitOut, tags=["developer"])
def dev_upload_initiate(repo_id: str, version: str, filename: str = Query(...), expires: int = Query(DEFAULT_PRESIGN_EXP, ge=60, le=86400),
                        user=Depends(role_required("developer"))):
    if not user_can_access_repo(user, repo_id):
        raise HTTPException(status_code=403, detail="not owner or model missing")
    # deterministic object path (developers)
    object_key = f"hf/{repo_id.strip('/')}/versions/{version}/{filename}"
    try:
        url = get_minio().presigned_put_object(MINIO_BUCKET, object_key, expires=expires)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"minio presign failed: {e}") from e
    return UploadInitOut(object_key=object_key, url=url, expires_in=expires)

class UploadCompleteIn(BaseModel):
    rfilename: str
    size: Optional[int] = None
    sha256: Optional[str] = None

@app.post("/v1/dev/models/{repo_id}/versions/{version}/uploads/complete", tags=["developer"])
def dev_upload_complete(repo_id: str, version: str, body: UploadCompleteIn,
                        user=Depends(role_required("developer"))):
    if not user_can_access_repo(user, repo_id):
        raise HTTPException(status_code=403, detail="not owner or model missing")
    upsert_file(repo_id, body.rfilename, version=version, size=body.size, sha256=body.sha256, storage_root="minio")
    record_upload(repo_id, body.rfilename, version=version, object_key=f"hf/{repo_id.strip('/')}/versions/{version}/{body.rfilename}",
                  bucket=MINIO_BUCKET, target="minio", etag=None)
    return {"ok": True}

# ------------------ Catalog (role-aware) ------------------

@app.get("/v1/models", response_model=List[ModelRow], tags=["catalog"])
def list_models(
    q: Optional[str] = Query(None), updated_since: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0),
    user=Depends(current_user)
):
    sql = """
    SELECT repo_id, canonical_url, model_name, author, pipeline_tag, license,
           parameters, parameters_readable, downloads, likes, created_at, last_modified,
           languages, tags, last_update_ts, file_count, has_safetensors, has_bin, visibility, owner_user_id
      FROM models WHERE 1=1
    """
    params: List[Any] = []
    if q:
        like = f"%{q}%"; sql += " AND (repo_id LIKE ? OR model_name LIKE ? OR author LIKE ?)"; params += [like, like, like]
    if updated_since:
        sql += " AND (last_update_ts IS NOT NULL AND last_update_ts > ?)"; params.append(updated_since)

    # Role-aware visibility
    rows: List[Dict[str, Any]] = []
    with connect(DB_PATH) as c:
        sql2 = sql + " ORDER BY last_update_ts DESC NULLS LAST, repo_id ASC LIMIT ? OFFSET ?"
        params2 = params + [limit, offset]
        for r in c.execute(sql2, params2).fetchall():
            r = dict(r)
            if user["role"] == "admin":
                rows.append(r); continue
            if user["role"] == "developer":
                if (r["owner_user_id"] and int(r["owner_user_id"]) == int(user["user_id"])) or (r["visibility"] == "public"):
                    rows.append(r)
            elif user["role"] == "platform":
                if user_can_access_repo(user, r["repo_id"]):
                    rows.append(r)
    # Strip owner_user_id before returning
    return [ModelRow(**{k: v for k, v in row.items() if k != "owner_user_id"}) for row in rows]

@app.get("/v1/models/{repo_id}", response_model=ModelRow, tags=["catalog"])
def get_model(repo_id: str, user=Depends(current_user)):
    if not user_can_access_repo(user, repo_id):
        raise HTTPException(status_code=403, detail="not authorized for this model")
    with connect(DB_PATH) as c:
        r = c.execute("""
        SELECT repo_id, canonical_url, model_name, author, pipeline_tag, license,
               parameters, parameters_readable, downloads, likes, created_at, last_modified,
               languages, tags, last_update_ts, file_count, has_safetensors, has_bin, visibility
          FROM models WHERE repo_id=?""", (repo_id,)).fetchone()
        if not r: raise HTTPException(status_code=404, detail="model not found")
        return ModelRow(**dict(r))

@app.get("/v1/models/{repo_id}/files", response_model=List[FileRow], tags=["catalog"])
def list_files(repo_id: str, version: Optional[str] = Query(None),
               presign: bool = Query(False), expires: int = Query(DEFAULT_PRESIGN_EXP, ge=60, le=86400),
               user=Depends(current_user), request: Request = None):
    if not user_can_access_repo(user, repo_id):
        raise HTTPException(status_code=403, detail="not authorized for this model")
    out: List[FileRow] = []
    with connect(DB_PATH) as c:
        if version:
            rows = c.execute("""
               SELECT rfilename, version, size, sha256, updated_ts
                 FROM files WHERE repo_id=? AND COALESCE(version,'')=?
                 ORDER BY rfilename ASC
            """, (repo_id, version)).fetchall()
        else:
            rows = c.execute("""
               SELECT rfilename, version, size, sha256, updated_ts
                 FROM files WHERE repo_id=? ORDER BY rfilename ASC
            """, (repo_id,)).fetchall()

        mc = get_minio() if presign else None
        for r in rows:
            obj = resolve_object_key(c, repo_id, r["rfilename"], version=r["version"])
            url = None
            if presign and mc:
                try: url = mc.presigned_get_object(MINIO_BUCKET, obj, expires=expires)
                except Exception: url = None
            out.append(FileRow(rfilename=r["rfilename"], version=r["version"], size=r["size"],
                               sha256=r["sha256"], updated_ts=r["updated_ts"], object_key=obj, presigned_url=url))

        # log
        if request:
            ra = request.headers.get("x-forwarded-for") or request.client.host
            ua = request.headers.get("user-agent")
            log_access(user_id=int(user["user_id"]), api_key_id=int(user["api_key_id"]),
                       event_type="files_list", repo_id=repo_id, rfilename=None,
                       object_key=None, size=None, status="ok", remote_addr=ra, user_agent=ua)
    return out

@app.get("/v1/files/{repo_id:path}/{rfilename:path}/download", tags=["catalog"])
def direct_download(repo_id: str, rfilename: str, version: Optional[str] = Query(None),
                    expires: int = Query(DEFAULT_PRESIGN_EXP, ge=60, le=86400),
                    user=Depends(current_user), request: Request = None):
    if not user_can_access_repo(user, repo_id):
        raise HTTPException(status_code=403, detail="not authorized for this model")
    with connect(DB_PATH) as c:
        row = c.execute("""
          SELECT rfilename, version, size, sha256, updated_ts FROM files
           WHERE repo_id=? AND rfilename=? AND COALESCE(version,'')=COALESCE(?, '')
           LIMIT 1""", (repo_id, rfilename, version)).fetchone()
        if not row: raise HTTPException(status_code=404, detail="file not found")
        obj = resolve_object_key(c, repo_id, row["rfilename"], version=row["version"])
        try:
            url = get_minio().presigned_get_object(MINIO_BUCKET, obj, expires=expires)
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"object not accessible: {obj}") from e
        if request:
            ra = request.headers.get("x-forwarded-for") or request.client.host
            ua = request.headers.get("user-agent")
            log_access(user_id=int(user["user_id"]), api_key_id=int(user["api_key_id"]),
                       event_type="download_url_issued", repo_id=repo_id, rfilename=row["rfilename"],
                       object_key=obj, size=row["size"], status="ok", remote_addr=ra, user_agent=ua)
        return JSONResponse({"repo_id": repo_id, "rfilename": row["rfilename"], "version": row["version"],
                             "size": row["size"], "sha256": row["sha256"],
                             "object_key": obj, "url": url, "expires_in": expires})

# ------------------ Platform: bulk manifest ------------------

@app.get("/v1/platform/manifest", response_model=PlatformManifest, tags=["platform"])
def platform_manifest(presign: bool = Query(False), expires: int = Query(DEFAULT_PRESIGN_EXP, ge=60, le=86400),
                      user=Depends(role_required("platform"))):
    now = int(time.time())
    items: List[ManifestEntry] = []
    with connect(DB_PATH) as c:
        # all active grants
        for g in c.execute("""
          SELECT repo_id, permitted_from_ts, permitted_until_ts
            FROM platform_model_grants
           WHERE platform_user_id=? AND permitted_from_ts<=? AND (permitted_until_ts IS NULL OR permitted_until_ts>=?)
           ORDER BY repo_id ASC
        """, (int(user["user_id"]), now, now)).fetchall():
            repo_id = g["repo_id"]
            if not user_can_access_repo(user, repo_id, at_ts=now):
                continue
            files: List[FileRow] = []
            rows = c.execute("""
              SELECT rfilename, version, size, sha256, updated_ts
                FROM files WHERE repo_id=? ORDER BY rfilename ASC
            """, (repo_id,)).fetchall()
            mc = get_minio() if presign else None
            for r in rows:
                obj = resolve_object_key(c, repo_id, r["rfilename"], version=r["version"])
                url = None
                if presign and mc:
                    try: url = mc.presigned_get_object(MINIO_BUCKET, obj, expires=expires)
                    except Exception: url = None
                files.append(FileRow(rfilename=r["rfilename"], version=r["version"], size=r["size"],
                                     sha256=r["sha256"], updated_ts=r["updated_ts"],
                                     object_key=obj, presigned_url=url))
            items.append(ManifestEntry(repo_id=repo_id, allowed_from_ts=int(g["permitted_from_ts"]), files=files))
    return PlatformManifest(generated_ts=now, items=items)

# ------------------ Changes (kept) ------------------

class ChangeRow(BaseModel):
    repo_id: str
    last_update_ts: int

@app.get("/v1/changes", response_model=List[ChangeRow], tags=["catalog"])
def changes(since: int = Query(...), limit: int = Query(500, ge=1, le=2000), user=Depends(current_user)):
    with connect(DB_PATH) as c:
        rows = c.execute("""
          SELECT repo_id, last_update_ts FROM models
           WHERE last_update_ts IS NOT NULL AND last_update_ts > ?
           ORDER BY last_update_ts ASC LIMIT ?""", (since, limit)).fetchall()
        # role-aware filter
        out = []
        for r in rows:
            if user_can_access_repo(user, r["repo_id"]):
                out.append(ChangeRow(repo_id=r["repo_id"], last_update_ts=int(r["last_update_ts"] or 0)))
        return out
