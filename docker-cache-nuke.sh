#!/usr/bin/env bash
set -euo pipefail

# docker-cache-nuke.sh
# Purge Docker caches (and optionally EVERYTHING) safely and reproducibly.
# Modes:
#   --dry-run        : show what would run, do nothing
#   --caches-only    : prune build caches + tmp data; keep images/volumes
#   --all            : remove all unused data (images, containers, networks) + volumes
#   --hard           : stop Docker and delete /var/lib/docker + /var/lib/containerd (irreversible)
#   --force          : skip confirmation prompts
#
# Examples:
#   bash docker-cache-nuke.sh --caches-only
#   sudo bash docker-cache-nuke.sh --all --force
#   sudo bash docker-cache-nuke.sh --hard   # full reset (DANGER: deletes everything)

DRY_RUN=0
MODE=""
FORCE=0

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --caches-only) MODE="caches" ;;
    --all) MODE="all" ;;
    --hard) MODE="hard" ;;
    --force) FORCE=1 ;;
    *) echo "Unknown arg: $arg" >&2; exit 2;;
  esac
done

if [[ -z "${MODE}" ]]; then
  echo "Choose one mode: --caches-only | --all | --hard" >&2
  exit 2
fi

run() {
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY] $*"
  else
    echo "+ $*"
    eval "$@"
  fi
}

pause_confirm() {
  local msg="$1"
  if [[ $FORCE -eq 1 ]]; then
    return
  fi
  echo ">>> $msg"
  read -r -p "Proceed? [y/N] " ans
  if [[ ! "$ans" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
  fi
}

header() {
  echo
  echo "=============================================="
  echo "$1"
  echo "=============================================="
}

header "Docker Disk Usage BEFORE"
run "docker system df -v || true"

if [[ "$MODE" == "caches" ]]; then
  header "Pruning build caches (builder + buildx) and temporary data"
  pause_confirm "This will remove build caches. Images/volumes remain."
  # BuildKit/Builder caches
  run "docker builder prune -a -f || true"
  run "docker buildx prune -a -f || true"

  # Lightweight system prune without touching images/volumes
  run "docker system prune -f || true"

  # Truncate huge container JSON logs (safe)
  if [[ -d /var/lib/docker/containers ]]; then
    pause_confirm "Optionally truncate large container logs in /var/lib/docker/containers/*.log"
    run "sudo find /var/lib/docker/containers -name \"*.log\" -type f -print -exec truncate -s 0 {} \\; || true"
  fi

elif [[ "$MODE" == "all" ]]; then
  header "Removing ALL unused Docker data (images, containers, networks) and VOLUMES"
  echo "WARNING: --all will delete dangling AND unused images/containers/networks."
  echo "         With --volumes, anonymous and unused named volumes are also removed."
  echo "         This can remove databases or MinIO buckets stored in volumes."
  pause_confirm "Continue with full prune including volumes?"

  # Stop any orphaned compose resources just in case (no-op if none)
  run "docker compose ls >/dev/null 2>&1 && docker compose ls || true"

  # Remove stopped containers, dangling networks/images, build cache, AND volumes
  run "docker system prune -a -f --volumes || true"

  # Extra: prune builder/buildx caches explicitly
  run "docker builder prune -a -f || true"
  run "docker buildx prune -a -f || true"

elif [[ "$MODE" == "hard" ]]; then
  header "HARD RESET: stop Docker and nuke /var/lib/docker + /var/lib/containerd"
  echo "DANGER: This will delete *everything* Docker knows about: images, layers, containers, networks, volumes, caches."
  echo "        Use only if Docker state is corrupted or you want a factory reset."
  pause_confirm "Final confirmation for HARD RESET?"

  # Attempt a graceful stop
  if command -v systemctl >/dev/null 2>&1; then
    run "sudo systemctl stop docker || true"
    run "sudo systemctl stop docker.socket || true"
    run "sudo systemctl stop containerd || true"
  else
    run "sudo service docker stop || true"
    run "sudo service containerd stop || true"
  fi

  # Remove state dirs
  run "sudo rm -rf /var/lib/docker"
  run "sudo rm -rf /var/lib/containerd"

  # Start services back
  if command -v systemctl >/dev/null 2>&1; then
    run "sudo systemctl start containerd || true"
    run "sudo systemctl start docker || true"
  else
    run "sudo service containerd start || true"
    run "sudo service docker start || true"
  fi

  # Recreate default networks if needed (Docker does this automatically on start)
  run "docker network ls || true"
fi

header "Docker Disk Usage AFTER"
run "docker system df -v || true"

echo
echo "Done."
