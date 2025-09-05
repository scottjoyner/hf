
# SQLite → SQL Server Starter (v2)

Production-minded starter for migrating from SQLite to SQL Server (local or Azure), including:
- A partitioned example for `LogEvent`
- Full **auto-generation** of DDL/constraints/bulk-load scripts from your actual SQLite DB
- CSV export with **manifest (row counts + SHA-256)**
- Validations and idempotent local runs

## Quickstart (demo tables)

```bash
docker compose up -d
docker compose run --rm db-init

# Optionally load demo CSVs if you place UserProfile.csv / LogEvent.csv in ./import
docker compose run --rm db-init sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /sql/03_bulkload.sql
```

## Full auto-migration from an existing SQLite schema

```bash
# 0) start SQL Server and initialize base DB
docker compose up -d
docker compose run --rm db-init

# 1) Generate MSSQL scripts from your SQLite DB (ALL tables)
python3 scripts/generate_mssql_from_sqlite.py --sqlite /path/to/app.db --out generated --db appdb

# 2) Export CSVs + manifest for all tables
python3 scripts/sqlite_export_plus.py --sqlite /path/to/app.db --out import

# 3) Apply generated schema + bulk load + add constraints
docker compose run --rm db-init
```

### Validate row counts

```bash
docker compose run --rm db-init sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /sql/validate_counts.sql
# Compare with ./import/manifest.json
```

## Best-practice choices baked in

- **Type mapping:** `DATETIME2(3)` for time, `NVARCHAR` for text, `DECIMAL(18,6)` for numeric, `FLOAT` for real, `VARBINARY(MAX)` for blobs.
- **Identity & PKs:** single-column INTEGER PKs become `BIGINT IDENTITY(1,1)`. Bulk loader preserves IDs via `IDENTITY_INSERT`.
- **Foreign keys & indexes** created **after** data load (`WITH CHECK`) for speed + integrity.
- **Load order** via FK topology sort.
- **UTF‑8 CSV + manifest** with per-table SHA‑256 and counts.
- **Partitioning pattern** for `LogEvent` (consider dedicated logging backend for raw firehose).
- **Idempotent local runs**: dev-friendly drop/create; for prod, move to controlled migrations (Alembic / EF / Liquibase).

## Notes / knobs to tune

- Adjust partition boundaries in `sql/01_schema.sql` or generate your own partitioned table for large time-series.
- If you don’t want to preserve identities, drop `IDENTITY_INSERT` and omit ID columns from inserts.
- Collation defaults to server. Set Unicode/case-sensitivity per your needs.
- For extremely large loads: disable nonclustered indexes before load, then rebuild (not included by default).

## Files

- `docker-compose.yml` — SQL Server + tools
- `sql/01_schema.sql` — Example schema (UserProfile + partitioned LogEvent)
- `sql/02_sample_upsert.sql` — Upsert patterns
- `sql/03_bulkload.sql` — Demo BULK INSERT
- `sql/04_partition_retention_examples.sql` — Partition retention
- `sql/validate_counts.sql` — Verify counts post-load
- `sql/init.sh` — Orchestrates DB init, generated DDL, bulk load, constraints
- `scripts/sqlite_export.py` — Simple CSV export
- `scripts/sqlite_export_plus.py` — CSV export with manifest
- `scripts/generate_mssql_from_sqlite.py` — Generates DDL + constraints + bulkload for ALL tables
- `generated/` — Place for generated scripts
- `import/` — CSVs + `manifest.json`
