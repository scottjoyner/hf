#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

# Flags
SKIP_BUILD=false
RUN_ONCE=false
SHOW_LOGS=false
REMOVE_ORPHANS=false
SET_MODELS_FILE=""
VALIDATE_MODELS=false
DRY_RUN_MODELS=false

print_usage() {
  cat <<'USAGE'
Usage: ./run_pipeline.sh [OPTIONS]

Options:
  --skip-build             Skip docker compose build
  --run-once               Run: scraper -> downloader -> metadata -> sync (once), then exit
  --logs                   Tail logs after execution
  --remove-orphans         Remove orphan containers on down/up
  --set-models <FILE>      Replace ./data/models.csv atomically with FILE before running
  --validate-models        Validate models list (host-level + container parse); fail if zero rows
  --dry-run-models         Print which input will be used, count rows, and sample repo ids; then exit
  --compose-file <FILE>    Use alternate compose file (default: docker-compose.yml)
  --menu                   Launch interactive menu
  --help                   Show this help message

Environment:
  COMPOSE_FILE             Alternate compose file path (same as --compose-file)
  MINIO_HEALTH_URL         Override MinIO health URL (default: http://127.0.0.1:19000/minio/health/ready)
USAGE
}

check_env_file() {
  if [[ ! -f ".env" ]]; then
    echo "❌ .env file not found. Please create one before running this script."
    exit 1
  fi
}

ensure_data_dir() {
  mkdir -p ./data
}

build_images() {
  echo "🔧 Building Docker images..."
  docker compose -f "$COMPOSE_FILE" build
}

down_services() {
  if [[ "$REMOVE_ORPHANS" == true ]]; then
    docker compose -f "$COMPOSE_FILE" down --remove-orphans
  else
    docker compose -f "$COMPOSE_FILE" down
  fi
}

start_services() {
  echo "🚀 Starting all services..."
  if [[ "$REMOVE_ORPHANS" == true ]]; then
    docker compose -f "$COMPOSE_FILE" up -d --remove-orphans
  else
    docker compose -f "$COMPOSE_FILE" up -d
  fi
}

run_task() {
  local task=$1; shift || true
  echo "▶️ Running task: $task $*"
  docker compose -f "$COMPOSE_FILE" run --rm "$task" "$@"
}

show_logs() {
  echo "📄 Tailing logs (Ctrl+C to exit)..."
  docker compose -f "$COMPOSE_FILE" logs -f registry pipeline worker
}

# Basic CSV validation on host: header has a known column; at least 1 data row
validate_models_csv_host() {
  local src="$1"
  [[ -f "$src" ]] || return 1
  awk 'BEGIN{hdr=0; ok=0}
       NR==1 { if ($0 ~ /(repo_id|url|updated_url|model_name)/) hdr=1; next }
       NR>1 && $0 !~ /^[[:space:]]*(#|$)/ { ok=1 }
       END { exit (hdr && ok) ? 0 : 1 }' "$src"
}

set_models() {
  local src="$1"
  [[ -f "$src" ]] || { echo "❌ File not found: $src"; exit 1; }
  if ! validate_models_csv_host "$src"; then
    echo "❌ $src looks empty or missing expected columns (need one of: repo_id, url, updated_url, model_name)"
    exit 1
  fi
  mkdir -p ./data
  cp "$src" ./data/models.csv.tmp && mv ./data/models.csv.tmp ./data/models.csv
  echo "✅ Updated ./data/models.csv"
}

# Container-level parse using your scripts/download.py logic.
# Prints FILE, COUNT, and up to 10 SAMPLE lines.
dry_run_models() {
  echo "🔎 Dry-run: parsing model list as downloader would..."
  docker compose -f "$COMPOSE_FILE" run --rm \
    --entrypoint bash pipeline-image -lc '
python - <<PY
from pathlib import Path
import sys
try:
    from scripts.download import _read_rows
except Exception as e:
    print("ERROR: cannot import scripts.download:", e)
    sys.exit(3)

def rows_for(path):
    p = Path(path)
    if not p.exists():
        return None
    try:
        return _read_rows(p)
    except Exception:
        return None

enriched = "/app/data/models_enriched.csv"
base = "/app/data/models.csv"

rows = rows_for(enriched)
which = None
if rows:
    which = enriched
else:
    rows = rows_for(base)
    if rows:
        which = base

if which is None or rows is None:
    print("FILE: none")
    print("COUNT: 0")
    sys.exit(0)

print("FILE:", which)
print("COUNT:", len(rows))
for r in rows[:10]:
    rid = (r.get("repo_id") or "").strip()
    if rid:
        print("SAMPLE:", rid)
PY'
}

# Extract COUNT from dry_run_models output (returns 0 if not found)
dry_run_models_count() {
  local out
  out="$(dry_run_models || true)"
  echo "$out"
  local c
  c="$(echo "$out" | awk -F': ' '/^COUNT:/{print $2; exit}')"
  [[ -n "$c" ]] || c=0
  echo "$c"
}

# MinIO health: default maps 19000->9000 from compose
wait_for_minio() {
  local url="${MINIO_HEALTH_URL:-http://127.0.0.1:19000/minio/health/ready}"
  local max_tries=60
  local delay=2
  echo "⏳ Waiting for MinIO to be ready at: $url"
  for ((i=1; i<=max_tries; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "✅ MinIO is ready."
      return 0
    fi
    printf "… (%d/%d)\r" "$i" "$max_tries"
    sleep "$delay"
  done
  echo ""
  echo "❌ MinIO not ready after $((max_tries*delay))s. Check 'docker compose logs minio'."
  exit 1
}

run_pipeline_once() {
  echo "🔁 Running one-time pipeline: scraper → downloader → metadata → sync"
  run_task scraper
  run_task downloader
  run_task metadata
  run_task sync
  echo "✅ One-time pipeline completed."
}

interactive_menu() {
  while true; do
    echo ""
    echo "📦 === MODEL PIPELINE MENU ==="
    echo "1) Build Docker images"
    echo "2) Start all services"
    echo "3) Run scraper"
    echo "4) Run downloader"
    echo "5) Run metadata"
    echo "6) Run sync"
    echo "7) Run pipeline (service 'pipeline' = 'all')"
    echo "8) One-time pipeline (scraper → downloader → metadata → sync)"
    echo "9) Update models.csv (host path)"
    echo "10) Validate models (host + container parse)"
    echo "11) Dry-run models (show file, count, sample) and exit"
    echo "12) Stop all services"
    echo "13) Show logs"
    echo "14) Exit"
    read -rp "Choose [1-14]: " choice
    case "$choice" in
      1) build_images ;;
      2) start_services; wait_for_minio ;;
      3) run_task scraper ;;
      4) run_task downloader ;;
      5) run_task metadata ;;
      6) run_task sync ;;
      7) run_task pipeline ;;
      8) run_pipeline_once ;;
      9)
        read -rp "Enter path to models.csv: " p
        set_models "$p"
        ;;
      10)
        # Quick host validation then container parse
        if [[ -f ./data/models_enriched.csv ]] && validate_models_csv_host ./data/models_enriched.csv; then
          echo "✅ Host check: ./data/models_enriched.csv looks ok"
        elif [[ -f ./data/models.csv ]] && validate_models_csv_host ./data/models.csv; then
          echo "✅ Host check: ./data/models.csv looks ok"
        else
          echo "⚠️ Host check inconclusive; proceeding to container parse…"
        fi
        cnt="$(dry_run_models_count)"
        if [[ "$cnt" -eq 0 ]]; then
          echo "❌ Container parse found 0 models."
          exit 2
        else
          echo "✅ Container parse found $cnt models."
        fi
        ;;
      11)
        dry_run_models
        exit 0
        ;;
      12) down_services ;;
      13) show_logs ;;
      14) echo "👋 Bye"; exit 0 ;;
      *) echo "❌ Invalid option." ;;
    esac
  done
}

# ---------------- Argument parsing ----------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=true; shift ;;
    --run-once) RUN_ONCE=true; shift ;;
    --logs) SHOW_LOGS=true; shift ;;
    --remove-orphans) REMOVE_ORPHANS=true; shift ;;
    --set-models) SET_MODELS_FILE="${2:-}"; shift 2 ;;
    --compose-file) COMPOSE_FILE="${2:-}"; shift 2 ;;
    --validate-models) VALIDATE_MODELS=true; shift ;;
    --dry-run-models) DRY_RUN_MODELS=true; shift ;;
    --menu) interactive_menu; exit 0 ;;
    --help) print_usage; exit 0 ;;
    *) echo "❌ Unknown option: $1"; print_usage; exit 1 ;;
  esac
done

# ---------------- Execution flow ----------------
check_env_file
ensure_data_dir

# Allow atomic update of models.csv before anything else
if [[ -n "$SET_MODELS_FILE" ]]; then
  set_models "$SET_MODELS_FILE"
fi

# We need images built before container-level parsing
if [[ "$SKIP_BUILD" == false ]]; then
  build_images
fi

# Optional validation and/or dry-run (no services required)
if [[ "$VALIDATE_MODELS" == true ]]; then
  # Host-level quick check
  if [[ -f ./data/models_enriched.csv ]] && validate_models_csv_host ./data/models_enriched.csv; then
    echo "✅ Host check: ./data/models_enriched.csv looks ok"
  elif [[ -f ./data/models.csv ]] && validate_models_csv_host ./data/models.csv; then
    echo "✅ Host check: ./data/models.csv looks ok"
  else
    echo "⚠️ Host check inconclusive; proceeding to container parse…"
  fi
  cnt="$(dry_run_models_count)"
  if [[ "$cnt" -eq 0 ]]; then
    echo "❌ Container parse found 0 models."
    exit 2
  else
    echo "✅ Container parse found $cnt models."
  fi
fi

if [[ "$DRY_RUN_MODELS" == true ]]; then
  dry_run_models
  exit 0
fi

# Normal run flow
start_services
wait_for_minio

if [[ "$RUN_ONCE" == true ]]; then
  run_pipeline_once
  echo "✅ Pipeline run complete. Core services are still running."
  if [[ "$SHOW_LOGS" == true ]]; then
    show_logs
  fi
  exit 0
fi

# Default: run the one-time pipeline sequence
run_pipeline_once

if [[ "$SHOW_LOGS" == true ]]; then
  show_logs
fi
