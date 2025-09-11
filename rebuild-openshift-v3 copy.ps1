param(
  [string]$Namespace = "models",
  [string]$EnvPath = ".\.env",
  [string]$ModelsCsvPath = ".\models.csv",
  [switch]$Nuke,
  [switch]$PurgeVolumes,
  [switch]$RunOnce
)

Set-StrictMode -Version Latest
$global:PSNativeCommandUseErrorActionPreference = $false
$ProgressPreference = 'SilentlyContinue'
$ErrorActionPreference = "Stop"

function Section($t){ Write-Host "`n=== $t ===" -ForegroundColor Cyan }
function Info($t){ Write-Host $t -ForegroundColor Gray }
function Warn($t){ Write-Host $t -ForegroundColor Yellow }
function Err($t){ Write-Host $t -ForegroundColor Red }
function Exec([string]$c){
  Write-Host "> $c" -ForegroundColor DarkGray
  $global:LASTEXITCODE=0; iex $c
  if($LASTEXITCODE -ne 0){ throw "Failed: $c" }
}

# -------- Helpers ----------
function Ensure-Oc {
  Section "Checking 'oc' CLI"
  if (-not (Get-Command oc -ErrorAction SilentlyContinue)) {
    if (Get-Command crc -ErrorAction SilentlyContinue) { & crc oc-env | Invoke-Expression }
  }
  if (-not (Get-Command oc -ErrorAction SilentlyContinue)) { throw "'oc' not found in PATH" }
  if (-not $Env:KUBECONFIG -or -not (Test-Path $Env:KUBECONFIG)) {
    $kc = Join-Path $Env:USERPROFILE ".crc\machines\crc\kubeconfig"
    if (Test-Path $kc) { $Env:KUBECONFIG = $kc }
  }
  Info "oc: $(oc version --client=true --short 2>$null)"
}

function Login-Kubeadmin {
  Section "Logging into CRC (kubeadmin)"
  $api = "https://api.crc.testing:6443"
  try {
    $me = (oc whoami 2>$null)
    if ($me -ne "kubeadmin") {
      $creds = crc console --credentials
      $line  = ($creds | Select-String -Pattern 'To login as an admin').Line
      $pwd   = [regex]::Match($line, '-p\s+(\S+)').Groups[1].Value
      if (-not $pwd) { throw "Could not extract kubeadmin password" }
      Exec "oc login -u kubeadmin -p $pwd $api --insecure-skip-tls-verify=true"
    } else {
      Info "Already logged in as kubeadmin."
    }
  } catch { Warn "login check: $($_.Exception.Message)" }
}

function Ensure-Namespace {
  Section "Preparing namespace: $Namespace"
  if ($Nuke) {
    Warn "Nuke requested -> deleting namespace..."
    Exec "oc delete ns $Namespace --ignore-not-found=true"
    Info "Waiting for namespace deletion..."
    $deadline = (Get-Date).AddMinutes(3)
    while ((Get-Date) -lt $deadline) {
      $exists = oc get ns $Namespace --ignore-not-found -o jsonpath='{.metadata.name}' 2>$null
      if (-not $exists) { break }
      Start-Sleep -Seconds 3
    }
    Exec "oc new-project $Namespace"
  } else {
    $ok = $true; try { oc project $Namespace | Out-Null } catch { $ok = $false }
    if (-not $ok) { Exec "oc new-project $Namespace" }
  }
}

function Get-EnvSecretValue([string]$key){
  $b64 = oc get secret env-all -o jsonpath="{.data.$key}" -n $Namespace 2>$null
  if (-not $b64) { return $null }
  [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64))
}

function Await-Deployment([string]$name){
  Exec "oc rollout status deploy/$name -n $Namespace --timeout=240s"
}

function Await-Route([string]$route,[string]$scheme="https",[string]$path="/"){
  $host = oc get route $route -n $Namespace -o jsonpath='{.spec.host}' 2>$null
  if(-not $host){ return }
  $url = "$scheme://$host$path"
  Info "Waiting for route $route -> $url"
  $deadline = (Get-Date).AddMinutes(3)
  while((Get-Date) -lt $deadline){
    try{
      $r = Invoke-WebRequest -Uri $url -Method Head -UseBasicParsing -TimeoutSec 5
      if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500){ break }
    }catch{}
    Start-Sleep 3
  }
}

function Oc-ExitCode([string]$cmd){
  Write-Host "> $cmd" -ForegroundColor DarkGray
  $old = $ErrorActionPreference
  try {
    $ErrorActionPreference = 'Continue'
    $global:LASTEXITCODE = 0
    & cmd /c $cmd > $null 2>&1
    return $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $old
  }
}

function Start-And-Wait-Build([string]$name){
  Section "Start build: $name"
  Exec "oc start-build $name --from-dir . --follow -n $Namespace"
  $b = (oc get builds -l build=$name -n $Namespace --sort-by=.metadata.creationTimestamp `
         -o jsonpath='{.items[-1:].metadata.name}')
  if (-not $b) { throw "No build found for BuildConfig '$name' after start-build." }
  $deadline = (Get-Date).AddMinutes(30)
  do {
    $phase = oc get build/$b -n $Namespace -o jsonpath='{.status.phase}'
    if ($phase -in @('Complete','Failed','Error','Cancelled')) { break }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $deadline)
  if ($phase -ne 'Complete') {
    Err "Build $b not complete (phase=$phase). Tail logs:"
    oc logs build/$b -n $Namespace --tail=200
    throw "Build failed: $b"
  }
}

function Wait-ImageStreamTag([string]$name,[string]$tag="latest",[int]$timeoutSec=180){
  Section "Waiting for ImageStreamTag $name:$tag"
  $deadline = (Get-Date).AddSeconds($timeoutSec)
  while((Get-Date) -lt $deadline){
    if ((Oc-ExitCode "oc get istag $name:$tag -n $Namespace") -eq 0){ return }
    Start-Sleep 3
  }
  throw "ImageStreamTag $name:$tag not available in $timeoutSec seconds"
}

function Set-Build-DockerfilePath([string]$bcName, [string]$dockerfilePath){
  $patchObj = @{ spec = @{ strategy = @{ dockerStrategy = @{ dockerfilePath = $dockerfilePath } } } }
  $patchJson = $patchObj | ConvertTo-Json -Depth 6
  $tmp = [System.IO.Path]::GetTempFileName()
  Set-Content -Path $tmp -Value $patchJson -Encoding UTF8
  Write-Host "> oc patch bc/$bcName --type=merge --patch-file $tmp -n $Namespace" -ForegroundColor DarkGray
  $global:LASTEXITCODE = 0
  oc patch "bc/$bcName" --type=merge --patch-file "$tmp" -n $Namespace | Out-Null
  Remove-Item $tmp -Force -ErrorAction SilentlyContinue
  if($LASTEXITCODE -ne 0){ throw "Failed to patch bc/$bcName dockerfilePath" }
}

function Scale-Down-Deployments([string[]]$names){
  try { $obj = oc -n $Namespace get deploy -o json 2>$null | ConvertFrom-Json } catch { return }
  if (-not $obj -or -not $obj.items) { return }
  $existing = @($obj.items | ForEach-Object { $_.metadata.name })
  foreach ($d in ($names | Where-Object { $existing -contains $_ })) {
    Write-Host "> oc -n $Namespace scale deploy/$d --replicas=0" -ForegroundColor DarkGray
    oc -n $Namespace scale deploy/$d --replicas=0 2>$null | Out-Null
  }
}

function Purge-InNamespace {
  Section "Purging resources in namespace: $Namespace"
  Scale-Down-Deployments @("minio","registry","db-web","pipeline")
  Exec "oc delete deploy,job,cronjob,svc,route,ing,cm --all --ignore-not-found -n $Namespace"
  Exec "oc delete secret env-all minio-credentials --ignore-not-found -n $Namespace"
  Exec "oc delete bc,build,is,imagestreamtag --all --ignore-not-found -n $Namespace"
  if ($PurgeVolumes) {
    Warn "Deleting PVCs per -PurgeVolumes"
    Exec "oc delete pvc --all --ignore-not-found -n $Namespace"
    try {
      $pvs = oc get pv -o json 2>$null | ConvertFrom-Json
      $toDel = @()
      foreach ($pv in $pvs.items) {
        if ($pv.spec.claimRef -and $pv.spec.claimRef.namespace -eq $Namespace) { $toDel += $pv.metadata.name }
      }
      if ($toDel.Count -gt 0) {
        Write-Host "> oc delete pv $($toDel -join ' ')" -ForegroundColor DarkGray
        oc delete pv $toDel | Out-Null
      }
    } catch {}
  }
  Info "Waiting for pods to terminate..."
  $deadline = (Get-Date).AddMinutes(3)
  while ((Get-Date) -lt $deadline) {
    $podsOut = (oc get pods -n $Namespace --no-headers 2>$null)
    if (-not $podsOut -or $podsOut -match "No resources found") { break }
    Start-Sleep -Seconds 3
  }
  if ($PurgeVolumes) {
    Section "Recreating PVCs from 00-storage.yaml"
    Exec "oc apply -f .\00-storage.yaml -n $Namespace"
  }
}

function Ensure-MinioSecret {
  Section "Creating secret minio-credentials (idempotent)"
  if (-not $script:MINIO_ROOT_USER -or -not $script:MINIO_ROOT_PASSWORD) {
    $script:MINIO_ROOT_USER     = Get-EnvSecretValue "MINIO_ROOT_USER"
    $script:MINIO_ROOT_PASSWORD = Get-EnvSecretValue "MINIO_ROOT_PASSWORD"
  }
  if (-not $script:MINIO_ROOT_USER -or -not $script:MINIO_ROOT_PASSWORD) {
    throw "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD missing in env-all"
  }
  oc -n $Namespace delete secret minio-credentials --ignore-not-found | Out-Null
  $null = oc -n $Namespace create secret generic minio-credentials `
    --from-literal=MINIO_ROOT_USER="$script:MINIO_ROOT_USER" `
    --from-literal=MINIO_ROOT_PASSWORD="$script:MINIO_ROOT_PASSWORD"
}

function Wait-Job([string]$name,[int]$timeoutSec=3600){
  $exit = Oc-ExitCode "oc wait --for=condition=complete job/$name -n $Namespace --timeout=${timeoutSec}s"
  if($exit -ne 0){
    Err "Job $name did not complete. Showing logs:"
    $pods = (oc get pods -n $Namespace -l job-name=$name -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')
    foreach($p in $pods.Split(" ",[System.StringSplitOptions]::RemoveEmptyEntries)){
      Write-Host "---- logs for pod $p ----" -ForegroundColor Yellow
      oc logs -n $Namespace $p --all-containers --tail=200
    }
    throw "Job $name failed or timed out."
  }
}

function RunPipelineSequential {
  Section "RunOnce: scraper -> downloader -> metadata"
  $scr = "run-scraper-$(Get-Random)";   Exec "oc create job $scr --from=job/run-scraper -n $Namespace"
  Wait-Job $scr 1800

  $dnl = "run-downloader-$(Get-Random)"; Exec "oc create job $dnl --from=job/run-downloader -n $Namespace"
  Wait-Job $dnl 7200

  $md  = "run-metadata-$(Get-Random)";  Exec "oc create job $md --from=job/run-metadata -n $Namespace"
  Wait-Job $md 1800

  Info "RunOnce sequence complete."
}

# ---------------- Start ----------------
Ensure-Oc
Login-Kubeadmin
Ensure-Namespace
if (-not $Nuke) { Purge-InNamespace }

Section "Loading .env -> Secret env-all"
if (-not (Test-Path $EnvPath)) { throw ".env not found at: $EnvPath" }
Exec "oc delete secret env-all -n $Namespace --ignore-not-found"
Exec "oc create secret generic env-all --from-env-file=$EnvPath -n $Namespace"

$MINIO_ROOT_USER     = Get-EnvSecretValue "MINIO_ROOT_USER"
$MINIO_ROOT_PASSWORD = Get-EnvSecretValue "MINIO_ROOT_PASSWORD"
$MINIO_BUCKET        = (Get-EnvSecretValue "MINIO_BUCKET"); if (-not $MINIO_BUCKET){ $MINIO_BUCKET="models" }
if (-not $MINIO_ROOT_USER -or -not $MINIO_ROOT_PASSWORD){ throw "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD missing in .env" }

# Always (re)create storage first
Section "Writing 00-storage.yaml"
@'
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: minio-data }
spec:
  accessModes: ["ReadWriteMany"]
  resources: { requests: { storage: 50Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: hf-models }
spec:
  accessModes: ["ReadWriteMany"]
  resources: { requests: { storage: 50Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: cache }
spec:
  accessModes: ["ReadWriteMany"]
  resources: { requests: { storage: 5Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: db }
spec:
  accessModes: ["ReadWriteMany"]
  resources: { requests: { storage: 5Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data }
spec:
  accessModes: ["ReadWriteMany"]
  resources: { requests: { storage: 10Gi } }
'@ | Set-Content -Path .\00-storage.yaml -Encoding UTF8
Exec "oc apply -f .\00-storage.yaml -n $Namespace"

# MinIO infra early (so downloader can push to S3 if needed)
Section "Writing 10-minio.yaml"
@'
apiVersion: apps/v1
kind: Deployment
metadata: { name: minio }
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      securityContext:
        seccompProfile: { type: RuntimeDefault }
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        imagePullPolicy: IfNotPresent
        args: ["server","/data","--console-address",":9001"]
        envFrom: [ { secretRef: { name: minio-credentials } } ]
        ports:
        - { containerPort: 9000, name: s3 }
        - { containerPort: 9001, name: console }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 100m, memory: 256Mi }
          limits:   { cpu: 500m, memory: 512Mi }
        volumeMounts:
        - { name: data, mountPath: /data }
        readinessProbe:
          httpGet: { path: /minio/health/ready, port: 9000 }
          initialDelaySeconds: 10
          periodSeconds: 10
          failureThreshold: 6
        livenessProbe:
          httpGet: { path: /minio/health/live, port: 9000 }
          initialDelaySeconds: 20
          periodSeconds: 20
          failureThreshold: 6
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
  backoffLimit: 6
  template:
    spec:
      restartPolicy: OnFailure
      securityContext:
        seccompProfile: { type: RuntimeDefault }
      initContainers:
      - name: wait-minio
        image: quay.io/minio/mc:latest
        command: ["/bin/sh","-lc"]
        args:
        - |
          i=1
          while [ "$i" -le 60 ]; do
            if wget -qO- http://minio:9000/minio/health/ready >/dev/null 2>&1; then
              exit 0
            fi
            if (exec 3<>/dev/tcp/minio/9000) 2>/dev/null; then
              exec 3>&-
              exit 0
            fi
            echo "waiting for minio:9000 ($i/60)"; i=$((i+1)); sleep 3
          done
          echo "minio not ready"; exit 1
      containers:
      - name: mc
        image: quay.io/minio/mc:latest
        imagePullPolicy: IfNotPresent
        env:
        - name: MINIO_ROOT_USER
          valueFrom: { secretKeyRef: { name: minio-credentials, key: MINIO_ROOT_USER } }
        - name: MINIO_ROOT_PASSWORD
          valueFrom: { secretKeyRef: { name: minio-credentials, key: MINIO_ROOT_PASSWORD } }
        - name: MINIO_BUCKET
          valueFrom: { secretKeyRef: { name: env-all, key: MINIO_BUCKET } }
        - name: HOME
          value: /tmp
        - name: MC_CONFIG_DIR
          value: /tmp/mc
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 25m, memory: 64Mi }
          limits:   { cpu: 200m, memory: 256Mi }
        command: ["/bin/sh","-lc"]
        args:
        - |
          set -euo pipefail
          mkdir -p "$MC_CONFIG_DIR"
          mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
          (mc ls "local/${MINIO_BUCKET}" || mc mb -p "local/${MINIO_BUCKET}")
          mc anonymous set download "local/${MINIO_BUCKET}" || true
          echo "MinIO bucket ready: ${MINIO_BUCKET}"
'@ | Set-Content -Path .\10-minio.yaml -Encoding UTF8

Ensure-MinioSecret
Exec "oc apply -f .\10-minio.yaml -n $Namespace"
Await-Deployment "minio"
Await-Route "minio-console" "https" "/minio/health/ready"

# BuildConfigs BEFORE app deploys (avoid image races)
Section "Creating BuildConfigs (binary builds)"
if ( (Oc-ExitCode "oc get bc models-pipeline -n $Namespace") -ne 0 ) {
  Exec "oc new-build --name models-pipeline --binary --strategy=docker -n $Namespace"
}
if ( (Oc-ExitCode "oc get bc models-registry -n $Namespace") -ne 0 ) {
  Exec "oc new-build --name models-registry --binary --strategy=docker -n $Namespace"
}
# Optional: point BCs to specific Dockerfiles (registry has custom one)
try { Set-Build-DockerfilePath -bcName "models-registry" -dockerfilePath "Dockerfile.registry" } catch {}
# (models-pipeline uses default Dockerfile; patch here if needed)

# Start builds and wait for imagestream tags
Start-And-Wait-Build "models-pipeline"
Start-And-Wait-Build "models-registry"
Wait-ImageStreamTag "models-pipeline" "latest" 300
Wait-ImageStreamTag "models-registry" "latest" 300

# App manifests (note: no ConfigMap mount for models.csv; we seed PVC instead)
Section "Writing 20-app.yaml (workloads; read models.csv from /app/data)"
@"
apiVersion: apps/v1
kind: Deployment
metadata: { name: registry }
spec:
  replicas: 1
  selector: { matchLabels: { app: registry } }
  template:
    metadata: { labels: { app: registry } }
    spec:
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      initContainers:
      - name: fix-perms
        image: registry.access.redhat.com/ubi9/ubi-minimal
        command: ["/bin/sh","-lc"]
        args: [ "set -eu; mkdir -p /app/db /app/hf_models; chmod -R g+rwX /app || true" ]
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        volumeMounts:
        - { name: db, mountPath: /app/db }
        - { name: hf, mountPath: /app/hf_models }
      containers:
      - name: registry
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-registry:latest
        imagePullPolicy: IfNotPresent
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: DB_PATH, value: "/app/db/models.db" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 50m, memory: 128Mi }
          limits:   { cpu: 500m, memory: 512Mi }
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
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      initContainers:
      - name: fix-perms
        image: registry.access.redhat.com/ubi9/ubi-minimal
        command: ["/bin/sh","-lc"]
        args: [ "set -eu; mkdir -p /app/db; chmod -R g+rwX /app || true" ]
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        volumeMounts:
        - { name: db, mountPath: /app/db }
        - { name: cache, mountPath: /app/cache }
        - { name: hf,    mountPath: /app/hf_models }
      containers:
      - name: dbweb
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        imagePullPolicy: IfNotPresent
        command: ["python","-m","scripts.worker","db-web"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: DB_PATH, value: "/app/db/models.db" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 25m, memory: 64Mi }
        ports: [ { containerPort: 8080, name: http } ]
        volumeMounts:
        - { name: db, mountPath: /app/db }
        - { name: cache, mountPath: /app/cache }
      volumes:
      - { name: db, persistentVolumeClaim: { claimName: db } }
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: hf,    persistentVolumeClaim: { claimName: hf-models } }
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
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      containers:
      - name: pipeline
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        imagePullPolicy: IfNotPresent
        command: ["python","-m","scripts.worker","all"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: DB_PATH, value: "/app/db/models.db" }
        - { name: MODELS_CSV, value: "/app/data/models.csv" }
        - { name: HOME, value: "/tmp" }
        - { name: XDG_CACHE_HOME, value: "/app/cache/xdg" }
        - { name: HF_HOME, value: "/app/cache/hf" }
        - { name: HUGGINGFACE_HUB_CACHE, value: "/app/cache/huggingface" }
        - { name: TRANSFORMERS_CACHE, value: "/app/cache/transformers" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 100m, memory: 512Mi }
          limits:   { cpu: 2000m, memory: 4Gi }
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
# Template jobs (we'll clone-from for RunOnce)
apiVersion: batch/v1
kind: Job
metadata: { name: run-scraper }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      containers:
      - name: scraper
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        imagePullPolicy: IfNotPresent
        command: ["python","-m","scripts.worker","scrape"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: MODELS_CSV, value: "/app/data/models.csv" }
        - { name: HOME, value: "/tmp" }
        - { name: XDG_CACHE_HOME, value: "/app/cache/xdg" }
        - { name: HF_HOME, value: "/app/cache/hf" }
        - { name: HUGGINGFACE_HUB_CACHE, value: "/app/cache/huggingface" }
        - { name: TRANSFORMERS_CACHE, value: "/app/cache/transformers" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 25m, memory: 128Mi }
          limits:   { cpu: 500m, memory: 512Mi }
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
        - { name: hf,    mountPath: /app/hf_models }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }
      - { name: hf,    persistentVolumeClaim: { claimName: hf-models } }
---
apiVersion: batch/v1
kind: Job
metadata: { name: run-downloader }
spec:
  template:
    spec:
      restartPolicy: OnFailure
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      containers:
      - name: downloader
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        imagePullPolicy: IfNotPresent
        command: ["python","-m","scripts.worker","download"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MINIO_ENDPOINT, value: "minio:9000" }
        - { name: MINIO_SECURE, value: "false" }
        - { name: MODELS_CSV, value: "/app/data/models.csv" }
        - { name: HOME, value: "/tmp" }
        - { name: XDG_CACHE_HOME, value: "/app/cache/xdg" }
        - { name: HF_HOME, value: "/app/cache/hf" }
        - { name: HUGGINGFACE_HUB_CACHE, value: "/app/cache/huggingface" }
        - { name: TRANSFORMERS_CACHE, value: "/app/cache/transformers" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 50m, memory: 256Mi }
          limits:   { cpu: 2000m, memory: 4Gi }
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
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      containers:
      - name: metadata
        image: image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/models-pipeline:latest
        imagePullPolicy: IfNotPresent
        command: ["python","-m","scripts.worker","metadata"]
        envFrom: [ { secretRef: { name: env-all } } ]
        env:
        - { name: MODELS_CSV, value: "/app/data/models.csv" }
        - { name: HOME, value: "/tmp" }
        - { name: XDG_CACHE_HOME, value: "/app/cache/xdg" }
        - { name: HF_HOME, value: "/app/cache/hf" }
        - { name: HUGGINGFACE_HUB_CACHE, value: "/app/cache/huggingface" }
        - { name: TRANSFORMERS_CACHE, value: "/app/cache/transformers" }
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        resources:
          requests: { cpu: 25m, memory: 128Mi }
          limits:   { cpu: 1000m, memory: 1Gi }
        volumeMounts:
        - { name: cache, mountPath: /app/cache }
        - { name: db,    mountPath: /app/db }
        - { name: data,  mountPath: /app/data }
      volumes:
      - { name: cache, persistentVolumeClaim: { claimName: cache } }
      - { name: db,    persistentVolumeClaim: { claimName: db } }
      - { name: data,  persistentVolumeClaim: { claimName: data } }
"@ | Set-Content -Path .\20-app.yaml -Encoding UTF8

(Get-Content .\20-app.yaml) -replace '\$\{NAMESPACE\}', $Namespace `
  | Set-Content .\20-app.patched.yaml -Encoding UTF8

# Apply infra and workloads (after builds are available)
Exec "oc apply -f .\20-app.patched.yaml -n $Namespace"

# ---- Seed models.csv into the shared /app/data PVC (single source of truth) ----
if (Test-Path $ModelsCsvPath) {
  Section "Seeding /app/data/models.csv into PVC 'data'"
  # Create a configmap with file contents
  oc -n $Namespace delete configmap models-csv --ignore-not-found | Out-Null
  (oc -n $Namespace create configmap models-csv --from-file=models.csv="$ModelsCsvPath" -o yaml --dry-run=client) | oc apply -f - | Out-Null

  # One-shot job to copy CM -> PVC (because CM mounts are read-only)
  @"
apiVersion: batch/v1
kind: Job
metadata: { name: seed-models-csv }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext: { seccompProfile: { type: RuntimeDefault } }
      volumes:
      - name: data
        persistentVolumeClaim: { claimName: data }
      - name: cm
        configMap:
          name: models-csv
          items: [{ key: models.csv, path: models.csv }]
      containers:
      - name: seeder
        image: registry.access.redhat.com/ubi9/ubi-minimal
        command: ["/bin/sh","-lc"]
        args:
        - |
          set -euo pipefail
          mkdir -p /app/data
          cp -f /cm/models.csv /app/data/models.csv
          ls -l /app/data/models.csv
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          readOnlyRootFilesystem: false
          capabilities: { drop: ["ALL"] }
        volumeMounts:
        - { name: data, mountPath: /app/data }
        - { name: cm,   mountPath: /cm, readOnly: true }
"@ | Set-Content .\99-seed-models.yaml -Encoding UTF8

  # Recreate job if it already exists
  oc -n $Namespace delete job seed-models-csv --ignore-not-found | Out-Null
  Exec "oc apply -f .\99-seed-models.yaml -n $Namespace"
  Wait-Job "seed-models-csv" 120
} else {
  Warn "No local models.csv at $ModelsCsvPath. Workloads will expect /app/data/models.csv in PVC 'data'."
}

# Roll out and wait on app deployments
foreach ($d in @("registry","db-web","pipeline")) {
  Exec "oc rollout restart deploy/$d -n $Namespace"
  Await-Deployment $d
}

# Routes (print & light wait)
Section "Routes"
try{
  $minioS3  = oc get route minio-s3  -o jsonpath='https://{.spec.host}{"`n"}' -n $Namespace
  $minioCon = oc get route minio-console -o jsonpath='https://{.spec.host}{"`n"}' -n $Namespace
  $regURL   = oc get route registry -o jsonpath='http://{.spec.host}{"`n"}' -n $Namespace
  $dbURL    = oc get route db-web   -o jsonpath='http://{.spec.host}{"`n"}' -n $Namespace
  Write-Host ("MinIO S3:      {0}" -f $minioS3)
  Write-Host ("MinIO Console:  {0}" -f $minioCon)
  Write-Host ("Registry UI:    {0}" -f $regURL)
  Write-Host ("DB-Web UI:      {0}" -f $dbURL)
  Await-Route "db-web" "http" "/"
  Await-Route "registry" "http" "/"
}catch{ Warn "Routes not ready yet." }

# Ensure MinIO bucket each run
Section "Ensuring MinIO bucket exists (mc setup)"
oc -n $Namespace delete job minio-mc-setup --ignore-not-found | Out-Null
Exec "oc apply -f .\10-minio.yaml -n $Namespace"
Wait-Job "minio-mc-setup" 180

# Optional sequential pipeline
if ($RunOnce) {
  RunPipelineSequential
}

Section "Done"
