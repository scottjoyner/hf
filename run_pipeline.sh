#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.yml"

# Flags
SKIP_BUILD=false
RUN_ONCE=false
SHOW_LOGS=false

# --- Helper Functions ---

print_usage() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --skip-build         Skip docker compose build"
  echo "  --run-once           Start core services, run pipeline once, then exit"
  echo "  --logs               Tail logs after execution"
  echo "  --menu               Launch interactive menu"
  echo "  --help               Show this help message"
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

start_services() {
  echo "🚀 Starting all services..."
  docker compose -f "$COMPOSE_FILE" up -d
}

run_task() {
  local task=$1
  echo "▶️ Running task: $task"
  docker compose -f "$COMPOSE_FILE" run --rm "$task"
}

show_logs() {
  echo "📄 Tailing logs (Ctrl+C to exit)..."
  docker compose -f "$COMPOSE_FILE" logs -f registry pipeline worker
}

interactive_menu() {
  while true; do
    echo ""
    echo "📦 === MODEL PIPELINE MENU ==="
    echo "1. Build Docker images"
    echo "2. Start all services"
    echo "3. Run sync"
    echo "4. Run pipeline"
    echo "5. Run scraper"
    echo "6. Run metadata"
    echo "7. Stop all services"
    echo "8. Show logs"
    echo "9. Exit"
    echo "=============================="
    read -rp "Choose an option [1-9]: " choice
    case "$choice" in
      1) build_images ;;
      2) start_services ;;
      3) run_task sync ;;
      4) run_task pipeline ;;
      5) run_task scraper ;;
      6) run_task metadata ;;
      7) docker compose -f "$COMPOSE_FILE" down ;;
      8) show_logs ;;
      9) echo "👋 Exiting."; exit 0 ;;
      *) echo "❌ Invalid option." ;;
    esac
  done
}

wait_for_minio() {
  echo "⏳ Waiting for MinIO to be ready..."
  sleep 5  # basic wait; can replace with curl health check if needed
}

# --- Argument Parser ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=true; shift ;;
    --run-once) RUN_ONCE=true; shift ;;
    --logs) SHOW_LOGS=true; shift ;;
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
  echo "🔁 Running one-time pipeline..."
  run_task sync
  run_task pipeline
  echo "✅ Pipeline run complete. Core services are still running."
  if [[ "$SHOW_LOGS" == true ]]; then
    show_logs
  fi
  exit 0
fi

# Default: Full flow with sync + pipeline + logs
run_task sync
run_task pipeline

if [[ "$SHOW_LOGS" == true ]]; then
  show_logs
fi
