#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.yml"

# Flags
SKIP_BUILD=false
RUN_ONCE=false
SHOW_LOGS=false
REMOVE_ORPHANS=false

# --- Helper Functions ---

print_usage() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --skip-build          Skip docker compose build"
  echo "  --run-once            Start services, then run: scraper -> downloader -> metadata -> sync (once), then exit"
  echo "  --logs                Tail logs after execution"
  echo "  --remove-orphans      Remove orphan containers on down/up"
  echo "  --menu                Launch interactive menu"
  echo "  --help                Show this help message"
  echo ""
}

check_env_file() {
  if [[ ! -f ".env" ]]; then
    echo "❌ .env file not found. Please create one before running this script."
    exit 1
  fi
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
  local task=$1
  shift || true
  echo "▶️ Running task: $task $*"
  docker compose -f "$COMPOSE_FILE" run --rm "$task" "$@"
}

show_logs() {
  echo "📄 Tailing logs (Ctrl+C to exit)..."
  docker compose -f "$COMPOSE_FILE" logs -f registry pipeline worker
}

# Health check for MinIO via host-mapped port (default: 19000->9000)
wait_for_minio() {
  local url="${MINIO_HEALTH_URL:-http://127.0.0.1:19000/minio/health/ready}"
  local max_tries=60
  local delay=2

  echo "⏳ Waiting for MinIO to be ready at: $url"
  for ((i=1; i<=max_tries; i++)); do
    if curl -fsS "$url" > /dev/null 2>&1; then
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
    echo "1. Build Docker images"
    echo "2. Start all services"
    echo "3. Run scraper"
    echo "4. Run downloader"
    echo "5. Run metadata"
    echo "6. Run sync"
    echo "7. Run pipeline (service 'pipeline' = 'all')"
    echo "8. One-time pipeline (scraper → downloader → metadata → sync)"
    echo "9. Stop all services"
    echo "10. Show logs"
    echo "11. Exit"
    echo "=============================="
    read -rp "Choose an option [1-11]: " choice
    case "$choice" in
      1) build_images ;;
      2) start_services; wait_for_minio ;;
      3) run_task scraper ;;
      4) run_task downloader ;;
      5) run_task metadata ;;
      6) run_task sync ;;
      7) run_task pipeline ;;   # runs the 'all' command from your image
      8) run_pipeline_once ;;
      9) down_services ;;
      10) show_logs ;;
      11) echo "👋 Exiting."; exit 0 ;;
      *) echo "❌ Invalid option." ;;
    esac
  done
}

# --- Argument Parser ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=true; shift ;;
    --run-once) RUN_ONCE=true; shift ;;
    --logs) SHOW_LOGS=true; shift ;;
    --remove-orphans) REMOVE_ORPHANS=true; shift ;;
    --menu) interactive_menu; exit 0 ;;
    --help) print_usage; exit 0 ;;
    *) echo "❌ Unknown option: $1"; print_usage; exit 1 ;;
  esac
done

# --- Execution Flow ---
check_env_file

if [[ "$SKIP_BUILD" == false ]]; then
  build_images
fi

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

# Default: keep behavior explicit and stepwise
run_pipeline_once

if [[ "$SHOW_LOGS" == true ]]; then
  show_logs
fi
