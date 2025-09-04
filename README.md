# Models Pipeline (Docker + MinIO + S3 backup)

This repo packages your model ingestion toolchain into containers, integrates MinIO for on-cluster storage (PVC-ready), and optionally syncs to an external S3 bucket for backup.

## What’s inside
- **scripts/**: your Python tools plus a new `sync_to_s3.py` and a small `worker` entrypoint.
- **docker-compose.yml**: MinIO + per-task workers. MinIO uses a persistent volume. A bootstrap container (`mc`) creates the bucket.
- **k8s/**: Optional Kubernetes manifests for a PVC-backed MinIO and a CronJob to run the ingestion on a schedule.
- **.env.example**: All environment variables in one place.
- **requirements.txt**: Python deps for the image.
- **Makefile**: Friendly commands for common workflows.

## Quick start (Docker Compose)
1) Copy env:
```bash
cp .env.example .env
# Edit .env with your values (HUGGINGFACE_TOKEN optional)
```

2) Bring up MinIO & create the bucket:
```bash
docker compose up -d minio minio-console mc
```

3) Run the pipeline steps (you can run independently or all-in-one):

```bash
# A) Enrich models.csv -> models_enriched.csv (uses HF API with caching)
docker compose run --rm scraper

# B) Download binaries into ./hf_models (shared volume)
docker compose run --rm downloader

# C) Build sortable metadata CSV ./model_metadata.csv
docker compose run --rm metadata

# D) Sync ./hf_models to both MinIO and S3 (if AWS env vars set)
docker compose run --rm sync
```

Or all-in-one:
```bash
docker compose run --rm pipeline        # runs A -> B -> C -> D
```

4) Open MinIO console at http://localhost:9001 (default creds from `.env`) and check the `{MINIO_BUCKET}` bucket.

## Files written
- `cache/` (volume): HF API JSON caches
- `hf_models/` (volume): downloaded model files organized by `<org>/<repo>/…`
- `models_enriched.csv`, `model_metadata.csv` in the container workdir (persist if you bind-mount or copy out)

## Kubernetes (optional)
- PVC-backed MinIO via `k8s/minio.yaml`
- A CronJob `k8s/cronjob.yaml` to run the pipeline image on schedule
Adjust namespaces, storage classes, and image names to your environment.

## Notes
- Set `HUGGINGFACE_TOKEN` in `.env` for private/gated repos (optional).
- `sync_to_s3.py` uploads to both MinIO **and** external S3 if the corresponding environment variables are present.
- `models.csv` lives in `./data/models.csv` — edit to add/remove repos.

# HOW TO RUN PIPELINE
```
# 1) Just see what would be downloaded (no pipeline run)
./run_pipeline.sh --dry-run-models

# 2) Validate (fail if zero models), then run pipeline once
./run_pipeline.sh --validate-models --run-once

# 3) Upload a new list and run the pipeline
./run_pipeline.sh --set-models ./my_models.csv --run-once

# 4) Full: update list, validate, remove orphans, run, then tail logs
./run_pipeline.sh --set-models ./my_models.csv --validate-models --remove-orphans --run-once --logs
```
# 🧠 HuggingFace Model Pipeline

A full-stack, containerized pipeline for scraping, downloading, and organizing Hugging Face models locally. Models are enriched with metadata, saved to SQLite, and synced to a local S3-compatible MinIO bucket. Optionally exposed via a model registry dashboard.

---

## 🚀 Components

| Service             | Description                                                  |
|---------------------|--------------------------------------------------------------|
| `scraper`           | Enriches models from `models.csv` into `models_enriched.csv` |
| `downloader`        | Downloads model files via HuggingFace Hub                    |
| `metadata`          | Builds metadata and saves to CSV and SQLite (`models.db`)    |
| `sync`              | Uploads model files to MinIO S3 bucket                       |
| `registry`          | RESTful API registry and manifest exporter                   |
| `db-web`            | Local web viewer for `models.db`                             |
| `minio`             | Local S3-compatible object storage                           |
| `mc`                | MinIO Client (used to alias/set up buckets)                  |
| `manifest-exporter` | Periodic manifest file exporter                              |

---

## 🌐 Application URLs

| App               | URL                             | Purpose                         |
|------------------|----------------------------------|----------------------------------|
| 🔹 MinIO Console  | http://localhost:19001          | Visual dashboard for S3 bucket  |
| 🔹 MinIO S3       | http://localhost:19000/models   | Files served from bucket        |
| 🔹 DB Web Viewer  | http://localhost:8080           | View SQLite `models.db`         |
| 🔹 Registry API   | http://localhost:8081           | Registry service for models     |

---

## 📦 Folder Structure

| Host Path         | Container Path      | Purpose                              |
|------------------|---------------------|--------------------------------------|
| `./data/`        | `/app/data`         | Input/output CSVs & manifests        |
| `./hf_models/`   | `/app/hf_models`    | Downloaded model files               |
| `./db/`          | `/app/db`           | SQLite DB `models.db`                |
| `./cache/`       | `/app/cache`        | Temporary cache                      |

---

## ⚙️ .env Configuration (Simplified)

```env
# Basic
TZ=America/New_York

# Hugging Face
HUGGINGFACE_TOKEN=

# MinIO S3 (Local)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=models
MINIO_ENDPOINT=minio:9000
MINIO_SECURE=false
MINIO_ALIAS=minio

# Local Paths
DATA_DIR=/app/data
CACHE_DIR=/app/cache
OUT_DIR=/app/hf_models
DB_PATH=/app/db/models.db

# Sync flags
DIRECT_UPLOAD=1
REGISTRY_API_KEY=dev-please-change
```

---

## 🧪 Quickstart

### 1. Build and run all containers

```bash
docker compose build
docker compose up
```

> On first run, MinIO will create the bucket and populate it via the sync step.

---

### 2. Run full pipeline (manual trigger)

```bash
docker compose run --rm pipeline-image all
```

This will execute:
- `scrape`
- `download`
- `metadata`
- `sync`

---

### 3. View Data

- Visit [MinIO Console](http://localhost:19001)
- Explore your local SQLite DB at [http://localhost:8080](http://localhost:8080)
- Access the model registry at [http://localhost:8081](http://localhost:8081)

---

## 🔁 Scheduled Tasks

- `manifest-exporter` runs every 86400s (24h) to export manifests.

---

## 📂 Output Artifacts

| File                            | Description                                 |
|---------------------------------|---------------------------------------------|
| `models.csv`                    | Initial list of models to process           |
| `models_enriched.csv`          | Enriched with metadata from HF Hub          |
| `model_metadata.csv`           | Output metadata per model                   |
| `model_files.csv`              | Detailed file manifest                      |
| `models.db`                    | SQLite database for use in registry         |
| `hf_models/<org>/<model>/`     | Raw downloaded model files                  |
| `data/manifests/`              | Periodic exported JSON manifests            |

---

## 🛠️ Debugging

To enter a container and run a task manually:

```bash
docker compose run --rm worker bash
```

To inspect DB contents:

```bash
sqlite3 db/models.db ".tables"
```

---

## 🧹 Tear Down

```bash
docker compose down
```

To remove volumes and cached files:

```bash
docker system prune -af --volumes
```

---

## 🧪 Testing Tips

You can run individual tasks:

```bash
docker compose run --rm scraper
docker compose run --rm downloader
docker compose run --rm metadata
docker compose run --rm sync
```

---
# OPENSHIFT

# 0) Build & push your images to a cluster-accessible registry
export PIPELINE_IMAGE="quay.io/YOU/models-pipeline:1.0.0"
export REGISTRY_IMAGE="quay.io/YOU/models-registry:1.0.0"

# 1) Have a .env with at least:
# MINIO_ROOT_USER=...
# MINIO_ROOT_PASSWORD=...
# MINIO_BUCKET=models
# REGISTRY_ADMIN_TOKEN=...   (for your registry app)

# 2) Log into your OpenShift cluster
oc login ...

# 3) Apply core resources and run the pipeline once
bash run_os_pipeline.sh \
  --namespace models-pipeline \
  --apply \
  --run-once \
  --show-urls \
  --pipeline-image "$PIPELINE_IMAGE" \
  --registry-image "$REGISTRY_IMAGE"


---
