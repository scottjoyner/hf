#!/usr/bin/env bash
set -euo pipefail

cmd=${1:-help}

case "$cmd" in
  db-init)
    echo "[worker] initializing DB"
    python3 -c "from scripts.models_db import init_db; import os; init_db(os.getenv('DB_PATH','/app/db/models.db'))"
    ;;
  db-web)
    echo "[worker] starting DB web on :8080"
    python3 scripts/db_web.py
    ;;
  help)
    echo "Usage: worker [db-init|db-web|scrape|download|metadata|sync|all]"
    ;;
  scrape)
    echo "[worker] scrape -> models_enriched.csv"
    python3 scripts/scrape.py --input data/models.csv --output models_enriched.csv
    ;;
  download)
    echo "[worker] download -> hf_models"
    python3 scripts/download.py --input data/models.csv --out-dir hf_models --patterns weights
    ;;
  metadata)
    echo "[worker] build model_metadata.csv"
    if [[ ! -f "models_enriched.csv" ]]; then
      echo "models_enriched.csv not found; running scrape first"
      python3 scripts/scrape.py --input data/models.csv --output models_enriched.csv
    fi
    python3 scripts/build_model_metadata.py --input models_enriched.csv --cache cache --output model_metadata.csv
    ;;
  sync)
    echo "[worker] sync hf_models -> MinIO (+ S3 if configured)"
    python3 scripts/sync_to_s3.py --src hf_models
    ;;
  all)
    echo "[worker] pipeline: db-init -> scrape -> download -> metadata -> sync"
    python3 -c "from scripts.models_db import init_db; import os; init_db(os.getenv('DB_PATH','/app/db/models.db'))"
    python3 scripts/scrape.py --input data/models.csv --output models_enriched.csv
    python3 scripts/download.py --input data/models.csv --out-dir hf_models --patterns weights
    python3 scripts/build_model_metadata.py --input models_enriched.csv --cache cache --output model_metadata.csv
    python3 scripts/sync_to_s3.py --src hf_models
    ;;
  *)
    echo "Unknown command: $cmd"
    exit 1
    ;;
esac
