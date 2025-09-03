cp .env.example .env    # fill in any secrets you want (HUGGINGFACE_TOKEN, AWS creds, etc.)

# Bring up MinIO and create the bucket
docker compose up -d minio minio-console mc

# Run steps independently:
docker compose run --rm scraper     # enrich models.csv -> models_enriched.csv
docker compose run --rm downloader  # download binaries -> hf_models volume
docker compose run --rm metadata    # build model_metadata.csv
docker compose run --rm sync        # upload hf_models to MinIO + AWS S3 (if configured)

# Or do everything in one shot:
docker compose run --rm pipeline
