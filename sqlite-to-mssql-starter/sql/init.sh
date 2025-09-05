
#!/usr/bin/env bash
set -euo pipefail

echo "⏳ Waiting for SQL Server to be available..."
until sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" -b -o /dev/null; do
  sleep 2
done

echo "✅ Connected. Ensuring database 'appdb' exists..."
sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -Q "IF DB_ID('appdb') IS NULL CREATE DATABASE appdb;" -b -e

echo "📜 Applying base schema (01_schema.sql)..."
sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /sql/01_schema.sql -b -e

# Apply generated schema if present
if [ -f /generated/01_schema_generated.sql ]; then
  echo "📜 Applying generated schema (generated/01_schema_generated.sql)..."
  sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /generated/01_schema_generated.sql -b -e
fi

# Load generated bulk data if present
if [ -f /var/opt/mssql/import/manifest.json ] && [ -f /generated/03_bulkload_generated.sql ]; then
  echo "🚚 Bulk loading generated CSVs (generated/03_bulkload_generated.sql)..."
  sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /generated/03_bulkload_generated.sql -b -e
fi

# Add constraints after data load if present
if [ -f /generated/02_constraints_generated.sql ]; then
  echo "🔗 Adding FK constraints (generated/02_constraints_generated.sql)..."
  sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -d appdb -i /generated/02_constraints_generated.sql -b -e
fi

echo "✅ Initialization complete."
