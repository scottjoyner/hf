\
    #!/usr/bin/env bash
    set -euo pipefail

    # Defaults (override via flags or env)
    NS="${NS:-models-pipeline}"
    ENV_FILE="${ENV_FILE:-.env}"
    PIPELINE_IMAGE="${PIPELINE_IMAGE:-models-pipeline:latest}"
    REGISTRY_IMAGE="${REGISTRY_IMAGE:-models-registry:latest}"

    APPLY=false
    RUN_ONCE=false
    SHOW_URLS=false

    print_usage() {
      cat <<'USAGE'
    Usage: ./run_os_pipeline.sh [options]

    Options:
      --namespace <ns>         Target OpenShift project/namespace (default: models-pipeline)
      --env-file <file>        Path to .env file (default: .env)
      --pipeline-image <ref>   Full ref for models-pipeline image (default: models-pipeline:latest)
      --registry-image <ref>   Full ref for models-registry image (default: models-registry:latest)
      --apply                  Apply core resources (Namespace, Secrets/Config, PVCs, MinIO, Registry, DB Web, Cron)
      --run-once               Run one-time pipeline: scraper → downloader → metadata → sync
      --show-urls              After apply, print external Routes
      --help                   Show this help
    USAGE
    }

    need() { command -v "$1" >/dev/null 2>&1 || { echo "❌ Missing dependency: $1"; exit 1; }; }

    parse_flags() {
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --namespace) NS="${2:?}"; shift 2 ;;
          --env-file) ENV_FILE="${2:?}"; shift 2 ;;
          --pipeline-image) PIPELINE_IMAGE="${2:?}"; shift 2 ;;
          --registry-image) REGISTRY_IMAGE="${2:?}"; shift 2 ;;
          --apply) APPLY=true; shift ;;
          --run-once) RUN_ONCE=true; shift ;;
          --show-urls) SHOW_URLS=true; shift ;;
          --help) print_usage; exit 0 ;;
          *) echo "❌ Unknown option: $1"; print_usage; exit 1 ;;
        esac
      done
    }

    ensure_login() {
      need oc
      oc whoami >/dev/null 2>&1 || { echo "❌ Not logged in to OpenShift (run: oc login ...)"; exit 1; }
    }

    ensure_env() {
      [[ -f "$ENV_FILE" ]] || { echo "❌ .env not found at $ENV_FILE"; exit 1; }
      # shellcheck disable=SC1090
      set -a; source "$ENV_FILE"; set +a
      # Validate required keys
      for k in MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_BUCKET REGISTRY_ADMIN_TOKEN; do
        [[ -n "${!k:-}" ]] || { echo "❌ Missing $k in $ENV_FILE"; exit 1; }
      done
      export MINIO_SECURE="${MINIO_SECURE:-false}"
    }

    ensure_project() {
      if ! oc get namespace "$NS" >/dev/null 2>&1; then
        echo "🆕 Creating project: $NS"
        oc new-project "$NS" >/dev/null
      else
        echo "ℹ️ Using project: $NS"
      fi
    }

    render_apply() {
      export NS PIPELINE_IMAGE REGISTRY_IMAGE \
             MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_BUCKET MINIO_SECURE REGISTRY_ADMIN_TOKEN

      for f in 00-namespace.yaml 01-config-and-secrets.yaml 10-pvc.yaml 20-minio.yaml 30-registry.yaml 35-db-web.yaml 50-cron-manifest-exporter.yaml; do
        echo "📄 Applying k8s/$f"
        envsubst < "k8s/$f" | oc apply -f -
      done

      echo "⏳ Waiting for core deployments to become Ready..."
      oc -n "$NS" rollout status deploy/minio --timeout=180s
      oc -n "$NS" rollout status deploy/models-registry --timeout=180s
      oc -n "$NS" rollout status deploy/db-web --timeout=180s
    }

    print_routes() {
      echo "🌐 Routes in $NS:"
      oc -n "$NS" get route
    }

    # Create a Job from file with generateName; wait for completion; stream logs.
    run_job() {
      local yaml="$1"
      local jobname
      jobname="$(envsubst < "$yaml" | oc -n "$NS" create -f - -o jsonpath='{.metadata.name}')"
      echo "▶️ Created Job: $jobname"
      echo "⏳ Waiting for completion..."
      oc -n "$NS" wait --for=condition=complete "job/$jobname" --timeout=3600s || {
        echo "❌ Job $jobname did not complete successfully. Showing pods:"
        oc -n "$NS" get pods -l job-name="$jobname" -o wide
        echo "Logs (first pod):"
        pod="$(oc -n "$NS" get pods -l job-name="$jobname" -o jsonpath='{.items[0].metadata.name}')"
        oc -n "$NS" logs "$pod" --all-containers
        exit 2
      }
      # Show logs
      pod="$(oc -n "$NS" get pods -l job-name="$jobname" -o jsonpath='{.items[0].metadata.name}')"
      echo "📄 Logs for $jobname / $pod:"
      oc -n "$NS" logs "$pod" --all-containers
    }

    main() {
      parse_flags "$@"
      ensure_login
      ensure_env
      ensure_project

      pushd "$(dirname "$0")" >/dev/null

      if [[ "$APPLY" == true ]]; then
        render_apply
        [[ "$SHOW_URLS" == true ]] && print_routes
      fi

      if [[ "$RUN_ONCE" == true ]]; then
        echo "🔁 Running one-time pipeline: scraper → downloader → metadata → sync"
        run_job "k8s/40-job-scraper.yaml"
        run_job "k8s/41-job-downloader.yaml"
        run_job "k8s/42-job-metadata.yaml"
        run_job "k8s/43-job-sync.yaml"
        echo "✅ Pipeline completed."
      fi

      popd >/dev/null
    }

    main "$@"
