# OpenShift deployment for your models pipeline

This bundle converts your `docker-compose.yml` stack into OpenShift-native resources using best practices:
- Arbitrary, non-root UIDs (OpenShift restricted SCC) — images/volumes are group-writable.
- Secrets/ConfigMaps for configuration.
- PVCs for persistent data (RWX recommended).
- Deployments + Services + Routes for web endpoints.
- Jobs for one-shot tasks (scraper/downloader/metadata/sync) and a CronJob for the manifest exporter.
- A helper script **run_os_pipeline.sh** to set up the project and run the pipeline sequence on OpenShift.

> **Prereqs**
> - `oc` CLI authenticated to your cluster.
> - A default StorageClass that supports **ReadWriteMany** (RWX) for shared volumes. If RWX is not available, see the notes inside `k8s/pvc.yaml` and adjust access modes or redesign to use object storage for sharing state.
> - Push your images to a registry accessible by the cluster (e.g., `quay.io/<you>/models-pipeline:<tag>` and `quay.io/<you>/models-registry:<tag>`).

## Quick start

```bash
# 1) Push images to a registry your cluster can pull from
export PIPELINE_IMAGE="quay.io/YOU/models-pipeline:1.0.0"
export REGISTRY_IMAGE="quay.io/YOU/models-registry:1.0.0"

# 2) Confirm your .env has MINIO_ROOT_USER/MINIO_ROOT_PASSWORD/MINIO_BUCKET/REGISTRY_ADMIN_TOKEN
cp /path/to/your/.env .env

# 3) Create the project, apply resources, and run the one-time pipeline
bash run_os_pipeline.sh --namespace models-pipeline --apply --run-once
# Add --show-urls to print the public Routes after rollout
```

### What gets created

- **Namespace/Project** (if it doesn’t exist)
- **Secrets/ConfigMap** from your `.env`
- **PVCs**: `db-pvc`, `hf-models-pvc`, `data-pvc`, `cache-pvc`, `minio-data-pvc`
- **MinIO**: Deployment, Service, Routes (API + Console)
- **Registry** (FastAPI): Deployment, Service, Route
- **DB Web**: Deployment, Service, Route
- **Jobs**: `scraper`, `downloader`, `metadata`, `sync` (created with `generateName:` so each run is unique)
- **CronJob**: `manifest-exporter` runs daily

### Notes

- Internal services use `minio:9000` for MINIO_ENDPOINT. External access is via OpenShift Routes.
- Health checks: MinIO uses HTTP health endpoints; other services use TCP socket probes to avoid guessing paths.
- Security: Deployments/Jobs drop Linux capabilities, disable privilege escalation, and avoid a fixed `runAsUser`.
- If your cluster lacks RWX storage:
  - Change `accessModes` to `ReadWriteOnce` and **do not** run multiple replicas that write the same volume.
  - Or replace shared PV usage with your MinIO/S3 bucket in the app configuration.

