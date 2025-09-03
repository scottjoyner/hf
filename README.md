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