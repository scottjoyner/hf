SHELL := /bin/bash

.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  build         - Build the pipeline image"
	@echo "  up            - Start MinIO and initialize bucket"
	@echo "  scrape        - Enrich models.csv -> models_enriched.csv"
	@echo "  download      - Download model files into hf_models volume"
	@echo "  metadata      - Build model_metadata.csv"
	@echo "  sync          - Upload hf_models to MinIO and S3"
	@echo "  all           - Run entire pipeline"

build:
	docker compose build

up:
	docker compose up -d minio minio-console mc

scrape:
	docker compose run --rm scraper

download:
	docker compose run --rm downloader

metadata:
	docker compose run --rm metadata

sync:
	docker compose run --rm sync

all:
	docker compose run --rm pipeline
