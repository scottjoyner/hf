# Model Registry API

**Version:** 0.3.0  
**Base URL:** `https://<your-host>` (dev: `http://localhost:8081`)  
**Auth:** User key via `x-api-key` header.  
**Admin:** `x-admin-token: <REGISTRY_ADMIN_TOKEN>`.  
**Time fields:** Unix epoch seconds.

- Swagger UI (when server is running): `/docs`
- ReDoc: `/redoc`
- OpenAPI JSON: `/openapi.json`

> This document describes the HTTP API exposed by the Registry app (FastAPI). It covers authentication, endpoints, request/response shapes, error handling, and security notes.

---

## Quick Start

1. **Register to get a key**
   ```bash
   BASE="http://localhost:8081"
   curl -sS -X POST "$BASE/v1/users/register" \
     -H 'content-type: application/json' \
     -d '{"email":"you@example.com","name":"You"}'
   # → { "user_id": 42, "api_key": "sk_live_xxx" }
   ```

2. **Use your key**
   ```bash
   API_KEY="sk_live_xxx"
   curl -sS "$BASE/v1/models?limit=5" -H "x-api-key: $API_KEY"
   ```

---

## Authentication

### User auth
Send your key in the `x-api-key` header on **all** endpoints (except `/healthz` and `/v1/users/register`).

Common errors:
- `401 {"detail":"missing x-api-key"}` – header absent
- `401 {"detail":"invalid or revoked api key"}` – key unknown or rotated

### Admin auth
Admin-only endpoints require:
```
x-admin-token: <REGISTRY_ADMIN_TOKEN>
```
Common errors:
- `503 {"detail":"admin token not configured"}` – server missing token
- `401 {"detail":"invalid admin token"}` – wrong token provided

---

## Conventions

- **Pagination**: Use `limit` and `offset` where supported.
- **Times**: Specify `since`/`until` as Unix seconds.
- **Paths**: `repo_id` and `rfilename` may contain slashes; URL-encode as needed.
- **Storage**: Objects are served from MinIO with optional presigned URLs.

---

## Endpoints

### Health

#### `GET /healthz`
Returns a minimal liveness payload.

**Response**
```json
{ "ok": true, "time": 1699999999 }
```

---

### Users

#### `POST /v1/users/register`
Create a user and provision an API key.

**Request body**
```json
{ "email": "you@example.com", "name": "You" }
```

**Response**
```json
{ "user_id": 42, "api_key": "sk_live_xxx" }
```

> Anyone can self-register as currently implemented. Consider adding allowlists or approval flows for public deployments.

---

#### `POST /v1/users/rotate-key` (auth)
Rotate your API key. The previous key is revoked immediately.

**Headers**: `x-api-key: <current key>`

**Response**
```json
{ "user_id": 42, "api_key": "sk_live_new" }
```

---

#### `GET /v1/users/me` (auth)
Return the current user.

**Headers**: `x-api-key: <key>`

**Response**
```json
{ "user_id": 42, "email": "you@example.com", "name": "You" }
```

---

### Catalog

#### `GET /v1/models` (auth)
List models with optional search and time filtering.

**Query params**
- `q` *(optional)* – substring on `repo_id`, `model_name`, or `author`
- `updated_since` *(optional)* – epoch seconds
- `limit` *(default 100, max 500)*
- `offset` *(default 0)*

**Example**
```bash
curl -sS "$BASE/v1/models?q=llama&limit=10" -H "x-api-key: $API_KEY"
```

**Sample item**
```json
{
  "repo_id": "meta-llama/Llama-3-8B",
  "canonical_url": "https://huggingface.co/meta-llama/Llama-3-8B",
  "model_name": "Llama-3-8B",
  "author": "meta-llama",
  "pipeline_tag": "text-generation",
  "license": "llama3",
  "parameters": 8000000000,
  "parameters_readable": "8B",
  "downloads": 12345,
  "likes": 678,
  "created_at": "2024-01-10T00:00:00Z",
  "last_modified": "2024-04-01T12:00:00Z",
  "languages": "en",
  "tags": "llama3, meta",
  "last_update_ts": 1711972800,
  "file_count": 23,
  "has_safetensors": 1,
  "has_bin": 0
}
```

---

#### `GET /v1/models/{repo_id}` (auth)
Fetch a single model by ID.

**Errors**: `404 {"detail":"model not found"}`

---

#### `GET /v1/models/{repo_id}/files` (auth)
List files for a model; optionally include presigned URLs.

**Query params**
- `presign` *(bool, default `false`)*
- `expires` *(seconds, min 60, max 86400; default 3600)*

**Sample item**
```json
{
  "rfilename": "tokenizer.json",
  "size": 123456,
  "sha256": "abcd...ef",
  "updated_ts": 1711972800,
  "object_key": "hf/meta-llama/Llama-3-8B/tokenizer.json",
  "presigned_url": "https://minio...&X-Amz-Expires=900"
}
```

---

#### `GET /v1/files/{repo_id}/{rfilename}/download` (auth)
Return a presigned URL for a single file.

**Query params**
- `expires` *(seconds, min 60, max 86400; default 3600)*

**Response**
```json
{
  "repo_id": "meta-llama/Llama-3-8B",
  "rfilename": "tokenizer.json",
  "size": 123456,
  "sha256": "abcd...ef",
  "object_key": "hf/meta-llama/Llama-3-8B/tokenizer.json",
  "url": "https://minio...sig...",
  "expires_in": 900
}
```

**Errors**
- `404 {"detail":"file not found"}` – unknown file
- `404 {"detail":"object not accessible: <key>"}` – storage error

---

#### `GET /v1/manifest/{repo_id}` (auth)
Return a manifest of all files; optionally presigned.

**Query params**
- `presign` *(bool, default `false`)*
- `expires` *(seconds, min 60, max 86400; default 3600)*

**Response**
```json
{
  "schema": "hf-model-manifest/v1",
  "repo_id": "meta-llama/Llama-3-8B",
  "updated_ts": 1711972800,
  "files": [
    { "rfilename": "tokenizer.json", "size": 123, "sha256": "…", "object_key": "…", "presigned_url": null },
    { "rfilename": "model.safetensors", "size": 456, "sha256": "…", "object_key": "…", "presigned_url": null }
  ]
}
```

---

#### `GET /v1/changes` (auth)
Change feed since a given timestamp.

**Query params**
- `since` *(required, epoch seconds)* – return rows with `last_update_ts > since`
- `limit` *(default 500, max 2000)*

**Example**
```bash
SINCE=$(date -d '1 day ago' +%s 2>/dev/null || date -v -1d +%s)
curl -sS "$BASE/v1/changes?since=$SINCE&limit=1000" -H "x-api-key: $API_KEY"
```

**Item**
```json
{ "repo_id": "meta-llama/Llama-3-8B", "last_update_ts": 1711972800 }
```

---

### Usage & Analytics

#### `GET /v1/users/me/usage` (auth)
Your usage over a window; includes totals, distinct models, top models, daily series.

**Query params**
- `since`, `until` *(epoch; default last 30 days)*
- `top_models_limit` *(default 20, max 200)*

**Response (truncated)**
```json
{
  "window": { "since": 1710800000, "until": 1713400000 },
  "totals": { "events": 42, "manifests": 5, "files_list": 10, "downloads": 27 },
  "last_seen_ts": 1713399999,
  "distinct_models": 7,
  "top_models": [ { "repo_id": "meta-llama/Llama-3-8B", "downloads": 12 } ],
  "timeseries_daily": [ { "day": "2024-04-01", "events": 7, "downloads": 4 } ]
}
```

---

#### `GET /v1/admin/usage` (admin)
Admin overview with optional user filtering.

**Headers**: `x-admin-token: <REGISTRY_ADMIN_TOKEN>`

**Query params**
- `since`, `until` *(epoch; default last 30 days)*
- `top_users_limit` *(default 50, max 500)*
- `top_models_limit` *(default 100, max 1000)*
- `filter_user_id` or `filter_email` *(optional)*

**Response (truncated)**
```json
{
  "window": { "since": 1710800000, "until": 1713400000 },
  "totals": { "events": 420, "manifests": 50, "files_list": 100, "downloads": 270 },
  "last_seen_ts": 1713399999,
  "top_users": [
    { "user_id": 42, "email": "you@example.com", "name": "You",
      "events": 100, "downloads": 60, "last_seen_ts": 1713399999 }
  ],
  "top_models": [ { "repo_id": "meta-llama/Llama-3-8B", "downloads": 123 } ],
  "timeseries_daily": [ { "day": "2024-04-01", "events": 70, "downloads": 40 } ]
}
```

---

## Storage & Presigned URLs

- Backed by MinIO using envs:
  - `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET`, `MINIO_SECURE`
- Default object key pattern:
  - `hf/{repo_id}/{rfilename}`
- If an entry exists in the `uploads` table, that `object_key` is used instead.
- `expires` bounds for presigned URLs: **60–86400** seconds.

---

## Error Handling

Common statuses:
- `200 OK` – success
- `401 Unauthorized` – bad or missing auth
- `404 Not Found` – model/file/object missing
- `503 Service Unavailable` – admin token not configured (admin endpoints)

Error payload shape:
```json
{ "detail": "message here" }
```

---

## Security Recommendations

- Serve behind **HTTPS**.
- Keep `REGISTRY_ADMIN_TOKEN` secret.
- Rotate user API keys regularly (`/v1/users/rotate-key`).
- Consider:
  - Rate limiting and WAF
  - Restricting `/v1/users/register` (allowlist/approval) for public use
  - Auditing via `access_logs`

---

## Postman

This API ships with a Postman collection and environment:
- **Collection:** `Model Registry API.postman_collection.json`
- **Environment:** `Model Registry - Local.postman_environment.json`

Import both into Postman, set `{{baseUrl}}`, then run **Users → Register** to auto-populate `{{apiKey}}`.






# Model Registry API (v1)

**Version:** 1.0.0  
**Base URL:** `https://<your-host>` (dev: `http://localhost:8081`)  
**Auth:** User key via `x-api-key` header.  
**Roles:** `developer`, `platform`, `admin`  
**Admin header:** `x-admin-token: <REGISTRY_ADMIN_TOKEN>`  
**Time fields:** Unix epoch seconds.

- Swagger UI (when server is running): `/docs`
- ReDoc: `/redoc`
- OpenAPI JSON: `/openapi.json`

> Production-ready Registry API: roles, developer uploads & versions, platform grants & manifests, MinIO storage, presigned URLs.

---

## Quick Start

### Developer
```bash
BASE=http://localhost:8081
curl -sS -X POST $BASE/v1/users/register -H 'content-type: application/json'   -d '{"email":"dev@you.com","name":"Dev"}' | tee /tmp/dev.json
DEV_KEY=$(jq -r .api_key /tmp/dev.json)

curl -sS -X POST $BASE/v1/dev/models -H "x-api-key: $DEV_KEY" -H 'content-type: application/json'   -d '{"repo_id":"acme/finetune-bert","model_name":"Finetune BERT","visibility":"private"}'

curl -sS -X POST $BASE/v1/dev/models/acme/finetune-bert/versions   -H "x-api-key: $DEV_KEY" -H 'content-type: application/json'   -d '{"version":"v1"}'
```

**Upload via presigned PUT**
```bash
curl -sS -X POST "$BASE/v1/dev/models/acme/finetune-bert/versions/v1/uploads/initiate?filename=model.safetensors&expires=900"   -H "x-api-key: $DEV_KEY" | tee /tmp/up.json
PUT_URL=$(jq -r .url /tmp/up.json)
curl -sS -X PUT --data-binary @model.safetensors "$PUT_URL"
curl -sS -X POST "$BASE/v1/dev/models/acme/finetune-bert/versions/v1/uploads/complete"   -H "x-api-key: $DEV_KEY" -H 'content-type: application/json'   -d '{"rfilename":"model.safetensors","size":123456789,"sha256":"deadbeef..."}'
```

### Admin (platform user & grant)
```bash
ADMIN=changeme
curl -sS -X POST $BASE/v1/admin/users -H "x-admin-token: $ADMIN"   -H 'content-type: application/json'   -d '{"email":"plat@partner.com","name":"Plat","role":"platform"}' | tee /tmp/plat.json
PLAT_KEY=$(jq -r .api_key /tmp/plat.json); PLAT_ID=$(jq -r .user_id /tmp/plat.json)

NOW=$(date +%s)
curl -sS -X POST $BASE/v1/admin/grants -H "x-admin-token: $ADMIN"   -H 'content-type: application/json'   -d "{"platform_user_id":$PLAT_ID,"repo_id":"acme/finetune-bert","permitted_from_ts":$NOW}"
```

### Platform (manifest & download)
```bash
curl -sS "$BASE/v1/platform/manifest?presign=true&expires=600" -H "x-api-key: $PLAT_KEY" | jq .items[0]
curl -sS "$BASE/v1/files/acme/finetune-bert/model.safetensors/download?version=v1&expires=900" -H "x-api-key: $PLAT_KEY"
```

---

## Endpoints Summary

- **Users:** register, rotate-key, me
- **Admin:** create users (roles), grants CRUD
- **Developer:** create models, versions, initiate/complete uploads
- **Catalog (role-aware):** list models, model detail, list files (presign), direct download, changes
- **Platform:** bulk manifest (optional presigned URLs)

See full endpoint details in code or Swagger UI.