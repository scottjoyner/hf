<#  bootstrap-openshift.ps1
    Deploys your MinIO + models pipeline to OpenShift Local (CRC) on Windows.

    What it does:
      1) Ensures oc is available (loads via `crc oc-env` if needed)
      2) Logs into CRC using kubeadmin (auto-reads password from `crc console --credentials`)
      3) Creates/uses a project (default: models)
      4) Creates Secret `env-all` from .env
      5) Writes three manifests:
           - 00-storage.yaml
           - 10-minio.yaml  (then patches to 10-minio.patched.yaml with your MINIO creds)
           - 20-app.yaml    (then patches to 20-app.patched.yaml with your namespace)
      6) Applies manifests
      7) Creates two BuildConfigs and builds from your local repo dir
      8) Deploys workloads, waits for rollout, prints Routes
      9) (Optional) --RunOnce triggers scraper, downloader, metadata jobs sequentially

    Usage examples:
      .\bootstrap-openshift.ps1
      .\bootstrap-openshift.ps1 -Namespace models -EnvPath .\.env -RunOnce
#>

param(
  [string]$Namespace = "models",
  [string]$EnvPath = ".\.env",
  [switch]$RunOnce
)

$ErrorActionPreference = "Stop"

function Write-Section($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Exec([string]$cmd) {
  Write-Host "> $cmd" -ForegroundColor DarkGray
  $LASTEXITCODE = 0
  iex $cmd
  if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE): $cmd" }
}

# 0) Preflight: oc presence (load from CRC if needed)
Write-Section "Checking 'oc' CLI"
if (-not (Get-Command oc -ErrorAction SilentlyContinue)) {
  if (Get-Command crc -ErrorAction SilentlyContinue) {
    Write-Host "Loading oc from CRC…" -ForegroundColor Yellow
    crc oc-env | Invoke-Expression
  }
}
if (-not (Get-Command oc -ErrorAction SilentlyContinue)) {
  throw "'oc' not found. Install the OpenShift CLI or ensure CRC is installed."
}

# 0a) CRC login (kubeadmin)
Write-Section "Logging into CRC (kubeadmin)"
$api = "https://api.crc.testing:6443"
try {
  # If we're already kubeadmin, skip
  $user = (oc whoami 2>$null)
  if ($user -ne "kubeadmin") {
    $creds     = crc console --credentials
    $adminLine = ($creds | Select-String -Pattern 'To login as an admin').Line
    if (-not $adminLine) { throw "Could not find admin line in crc console --credentials output." }
    $pwd = [regex]::Match($adminLine, '-p\s+(\S+)').Groups[1].Value
    if (-not $pwd) { throw "Could not extract kubeadmin password." }
    Exec "oc login -u kubeadmin -p $pwd $api"
  } else {
    Write-Host "Already logged in as kubeadmin." -ForegroundColor Yellow
  }
} catch {
  Write-Host "Warning: kubeadmin auto-login failed: $($_.Exception.Message)" -ForegroundColor Yellow
  Write-Host "Continuing with current context: $(oc whoami 2>$null)" -ForegroundColor Yellow
}

# 0b) Ensure project
Write-Section "Ensuring project: $Namespace"
# Try to switch first; if it fails, try to create; if create fails because it exists, switch again.
$switched = $false
try { oc project $Namespace | Out-Null; $switched = $true } catch {}
if (-not $switched) {
  try {
    oc new-project $Namespace | Out-Null
  } catch {
    # Might already exist or RBAC may block create; try switch again
    oc project $Namespace | Out-Null
  }
}


# 0c) Ensure .env exists and create/update secret
Write-Section "Loading .env → Secret env-all"
if (-not (Test-Path $EnvPath)) { throw ".env not found at: $EnvPath" }

# Use --ignore-not-found and test on output, never let NotFound throw
$exists = oc get secret env-all -n $Namespace --ignore-not-found -o name 2>$null
if ($exists) {
  Exec "oc delete secret env-all -n $Namespace"
}
Exec "oc create secret generic env-all --from-env-file=$EnvPath -n $Namespace"

# Helper to read a key from env-all secret
function Get-EnvSecretValue([string]$key) {
  $b64 = oc get secret env-all -o jsonpath="{.data.$key}" -n $Namespace 2>$null
  if (-not $b64) { return $null }
  [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64))
}

$MINIO_ROOT_USER     = Get-EnvSecretValue "MINIO_ROOT_USER"
$MINIO_ROOT_PASSWORD = Get-EnvSecretValue "MINIO_ROOT_PASSWORD"
$MINIO_BUCKET        = (Get-EnvSecretValue "MINIO_BUCKET"); if (-not $MINIO_BUCKET) { $MINIO_BUCKET = "models" }

if (-not $MINIO_ROOT_USER -or -not $MINIO_ROOT_PASSWORD) {
  throw "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD must be present in $EnvPath"
}

# 1) Write 00-storage.yaml
Write-Section "Writing 00-storage.yaml"
@'
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: minio-data }
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 50Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: hf-models }
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 50Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: cache }
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 20Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: db }
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 10Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data }
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 20Gi } }
'@ | Set-Content -Path .\00-storage.yaml -Encoding UTF8

Exec "oc apply -f .\00-storage.yaml -n $Namespace"

# 2) Write 10-minio.yaml (with placeholders) → patch with real secrets → apply
Write-Section "Writing 10-minio.yaml (and patching secrets)"
@'
apiVersion: v1
kind: Secret
metadata: { name: minio-credentials }
type: Opaque
stringData:
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: minio }
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      securityContext: { fsGroup: 1000 }
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        args: ["server","/data","--console-address",":9001"]
        envFrom:
        - secretRef: { name: minio-credentials }
        ports:
        - { containerPort: 9000, name: s3 }
        - { containerPort: 9001, name: console }
        volumeMounts:
        - { name: data, mountPath: /data }
        readinessProbe:
          httpGet: { path: /minio/health/ready, port: 9000 }
          initialDelaySeconds: 10
        livenessProbe:
          httpGet: { path: /minio/health/live, port: 9000 }
          initialDelaySeconds: 20
      volumes:
      - name: data
        persistentVolumeClaim: { claimName: minio-data }
---
apiVersion: v1
kind: Service
metadata: { name: minio }
spec:
  selector: { app: minio }
  ports:
  - { name: s3, port: 9000, targetPort: s3 }
  - { name: console, port: 9001, targetPort: console }
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: { name: minio-s3 }
spec:
  to: { kind: Service, name: minio }
  port: { targetPort: s3 }
  tls: { termination: edge }
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: { name: minio-console }
spec:
  to: { kind: Service, name: minio }
  port: { targetPort: console }
  tls: { termination: edge }
---
apiVersion: batch/v1
kind: Job
metadata: { name: minio-mc-setup }
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: quay.io/minio/mc:latest
        env:
        - name: MINIO_ROOT_USER
          valueFrom: { secretKeyRef: { name: minio-credentials, key: MINIO_ROOT_USER } }
        - name: MINIO_ROOT_PASSWORD
          valueFrom: { secretKeyRef: { name: minio-credentials, key: MINIO_ROOT_PASSWORD } }
        - name: MINIO_BUCKET
          value: ${MINIO_BUCKET}
        command: ["/bin/sh","-lc"]
        args:
        - |
          mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
          mc mb -p "local/${MINIO_BUCKET}" || true
          mc anonymous set download "local/${MINIO_BUCKET}" || true
          echo "MinIO bucket ready: ${MINIO_BUCKET}"
'@ | Set-Content -Path .\10-minio.yaml -Encoding UTF8

(Get-Content .\10-minio.yaml) `
  -replace '\$\{MINIO_ROOT_USER\}',$MINIO_ROOT_USER `
  -replace '\$\{MINIO_ROOT_PASSWORD\}',$MINIO_ROOT_PASSWORD `
  -replace '\$\{MINIO_BUCKET\}',$MINIO_BUCKET `
  | Set-Content .\10-minio.patched.yaml -Encoding UTF8

Exec "oc apply -f .\10-minio.patched.yaml -n $Namespace"
Exec "oc rollout status deploy/minio -n $Namespace"

# 3) Write 20-app.yaml with ${NAMESPACE} placeholder → patch → apply
Write-Section "Writing 20-app.yaml (and patching namespace)"
@'
# Registry (8081)
apiVersion: apps/v1
kind: Deployment
metadata: { name: registry }
spec:
  replicas: 1
  selector: { matchLabels: { app: registry } }
  template:
    metadata: { labels: { app: registry } }
    spec:
      securityContext: { fsGroup: 1000 }
      containers:
      - name: registry
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-registry:latest
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: DB_PATH, value: "/app/db/models.db" }
        ports: [ { containerPort: 8081, name: http } ]
        volumeMounts:
        - { name: db, mountPath: /app/db }
        - { name: hf, mountPath: /app/hf_models }
      volumes:
      - { name: db, persistentVolumeClaim: { claimName: db } }
      - { name: hf, persistentVolumeClaim: { claimName: hf-models } }
---
apiVersion: v1
kind: Service
metadata: { name: registry }
spec:
  selector: { app: registry }
  ports: [ { port: 8081, targetPort: http } ]
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: { name: registry }
spec:
  to: { kind: Service, name: registry }
  port: { targetPort: 8081 }
  tls: { termination: edge }

# db-web (8080)
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: db-web }
spec:
  replicas: 1
  selector: { matchLabels: { app: db-web } }
  template:
    metadata: { labels: { app: db-web } }
    spec:
      securityContext: { fsGroup: 1000 }
      containers:
      - name: dbweb
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        command: ["python","-m","scripts.worker","db-web"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: DB_PATH, value: "/app/db/models.db" }
        ports: [ { containerPort: 8080, name: http } ]
        volumeMounts:
        - { name: db, mountPath: /app/db }
      volumes:
      - { name: db, persistentVolumeClaim: { claimName: db } }
---
apiVersion: v1
kind: Service
metadata: { name: db-web }
spec:
  selector: { app: db-web }
  ports: [ { port: 8080, targetPort: http } ]
---
apiVersion: route.openshift.io/v1
kind: Route
metadata: { name: db-web }
spec:
  to: { kind: Service, name: db-web }
  port: { targetPort: 8080 }
  tls: { termination: edge }

# pipeline (long-running "all")
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: pipeline }
spec:
  replicas: 1
  selector: { matchLabels: { app: pipeline } }
  template:
    metadata: { labels: { app: pipeline } }
    spec:
      securityContext: { fsGroup: 1000 }
      containers:
      - name: pipeline
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        command: ["python","-m","scripts.worker","all"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: DB_PATH, value: "/app/db/models.db" }
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: hf,    mountPath: /app/hf_models }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: hf,    persistentVolumeClaim: { claimName: hf-models } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }

# On-demand Jobs
---
apiVersion: batch/v1
kind: Job
metadata: { name: run-scraper }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { fsGroup: 1000 }
      containers:
      - name: scraper
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        command: ["python","-m","scripts.worker","scrape"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }
---
apiVersion: batch/v1
kind: Job
metadata: { name: run-downloader }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { fsGroup: 1000 }
      containers:
      - name: downloader
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        command: ["python","-m","scripts.worker","download"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: hf,    mountPath: /app/hf_models }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: hf,    persistentVolumeClaim: { claimName: hf-models } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }
---
apiVersion: batch/v1
kind: Job
metadata: { name: run-metadata }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { fsGroup: 1000 }
      containers:
      - name: metadata
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        command: ["python","-m","scripts.worker","metadata"]
        envFrom: [ { secretRef: { name: env-all } } ]
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }
'@ | Set-Content -Path .\20-app.yaml -Encoding UTF8

(Get-Content .\20-app.yaml) -replace '\$\{NAMESPACE\}', $Namespace `
  | Set-Content .\20-app.patched.yaml -Encoding UTF8

# 4) Apply MinIO + App manifests
Exec "oc apply -f .\10-minio.patched.yaml -n $Namespace"
Exec "oc apply -f .\20-app.patched.yaml -n $Namespace"

# 5) In-cluster builds (Buildah): models-pipeline + models-registry
Write-Section "Creating BuildConfigs (if needed) and building images in-cluster"
if (-not (oc get bc models-pipeline -n $Namespace 2>$null)) {
  Exec "oc new-build --name models-pipeline --binary --strategy=docker -n $Namespace"
}
Exec "oc start-build models-pipeline --from-dir . --follow -n $Namespace"

if (-not (oc get bc models-registry -n $Namespace 2>$null)) {
  Exec "oc new-build --name models-registry --binary --strategy=docker -n $Namespace"
  Exec "oc patch bc/models-registry -p '{""spec"":{""strategy"":{""dockerStrategy"":{""dockerfilePath"":""Dockerfile.registry""}}}}' -n $Namespace"
}
Exec "oc start-build models-registry --from-dir . --follow -n $Namespace"

# 6) Rollout and show routes
Write-Section "Waiting for deployments"
foreach ($d in @("minio","registry","pipeline","db-web")) {
  Exec "oc rollout status deploy/$d -n $Namespace"
}

Write-Section "Routes"
try {
  $minioS3  = oc get route minio-s3  -o jsonpath='https://{.spec.host}{"`n"}' -n $Namespace
  $minioCon = oc get route minio-console -o jsonpath='https://{.spec.host}{"`n"}' -n $Namespace
  $regURL   = oc get route registry -o jsonpath='http://{.spec.host}{"`n"}' -n $Namespace
  $dbURL    = oc get route db-web   -o jsonpath='http://{.spec.host}{"`n"}' -n $Namespace
  Write-Host ("MinIO S3:      {0}" -f $minioS3)
  Write-Host ("MinIO Console:  {0}" -f $minioCon)
  Write-Host ("Registry UI:    {0}" -f $regURL)
  Write-Host ("DB-Web UI:      {0}" -f $dbURL)
} catch { Write-Host "Routes not ready yet." -ForegroundColor Yellow }

# 7) Optional one-time pipeline (scraper→downloader→metadata)
if ($RunOnce) {
  Write-Section "RunOnce: scraper → downloader → metadata"
  Exec "oc create job run-scraper-$(Get-Random) --from=job/run-scraper -n $Namespace --wait"
  Exec "oc create job run-downloader-$(Get-Random) --from=job/run-downloader -n $Namespace --wait"
  Exec "oc create job run-metadata-$(Get-Random) --from=job/run-metadata -n $Namespace --wait"
  Write-Host "RunOnce complete."
}

Write-Section "Done"
