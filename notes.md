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



#TODO
 Currently the logic associated with the model_id column from the build_model_metadata.csv has significant consequences as it currently uses the <AUTHOR>/<MODEL_ID> tag as the column name, and the entire application is depentnant on this
 