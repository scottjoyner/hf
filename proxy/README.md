# Internal Model Mirror (Compliant, Air‑gap Friendly)

> **Purpose**: Let containers pull ML models *without* direct internet access by mirroring approved Hugging Face assets into an internal S3/MinIO bucket. This respects firewall policy and supports review, provenance, and repeatability.

## Why this design?
- **Compliant**: No firewall evasion. Internet access is centralized to a reviewed sync job.
- **Reliable & fast**: Containers pull from local object storage (MinIO/S3), not from the public internet.
- **Auditable**: Every artifact is recorded in a manifest (repo, revision, SHA256, size, license).
- **Secure by default**: License allow‑list, integrity hashing, optional malware scan and provenance signing hooks.

## Architecture
1. **Sync Job (outside‑egress host/runner)**: Pulls approved HF repos using a token, writes a CSV manifest, and uploads files + manifest to MinIO/S3.
2. **Internal Registry (MinIO/S3)**: Read‑only bucket for workloads.
3. **Workloads**: Fetch models from `s3://<bucket>/<prefix>/...` or `http(s)://minio:9000/<bucket>/<prefix>/...` instead of huggingface.co.

Optional:
- **Approved Egress Proxy**: If your org has one, you can set `HTTP_PROXY`/`HTTPS_PROXY` in the sync job and/or workloads. Do **not** create unapproved tunnels.

## Quick Start

### 0) Prereqs
- Python 3.10+ on the sync host.
- Access to Hugging Face with a token that is allowed to download the desired repos.
- Access to your internal MinIO/S3 with credentials.
- (Optional) Corporate approved egress proxy URL.

### 1) Configure `.env`
Copy and edit `.env.example` to `.env`:

```bash
cp .env.example .env
# edit .env to include: HF_TOKEN, HF_REPOS, MINIO_* creds, LICENSE_ALLOWLIST, etc.
```

### 2) Install and run the sync job
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r mirror/requirements.txt
python mirror/mirror_hf_to_minio.py --dry-run   # sanity check
python mirror/mirror_hf_to_minio.py             # actual sync
```

Outputs:
- `data/manifest.csv` — catalog of mirrored artifacts.
- Files uploaded to `s3://$MINIO_BUCKET/$MINIO_PREFIX/<repo_id>/...`

### 3) Point workloads to the internal mirror
- **Env-based**:
  - `MODEL_BASE_URL=http://minio:9000`
  - `MODEL_BUCKET=models`
  - `MODEL_PREFIX=mirrors/hf`
- Or use S3 URLs and SDKs (boto3/minio). See `k8s/example-workload.yaml`.

### 4) Scheduled syncs
Pick one:
- **GitHub Actions** (runner with internet): `.github/workflows/mirror.yml`
- **systemd timer** on a Linux host: `scripts/systemd/mirror-models.service` + `.timer`

## Security & Compliance

- **License allow‑list**: Only mirror repos with licenses in `LICENSE_ALLOWLIST`.
- **Integrity**: SHA256 recorded for each object; uploaded with ETag/metadata.
- **Provenance (optional)**: Hook in SBOM generation and `cosign attest` in `mirror_hf_to_minio.py` (search for `# TODO: SBOM/provenance`). 
- **Access control**: Bucket is read‑only for workloads; write access restricted to CI/service identity.
- **Secrets**: Use environment variables/secret stores; never commit tokens.
- **Approvals**: Ensure model list and licenses are approved by Security/Legal.

## Kubernetes Example (Workload)
See `k8s/example-workload.yaml` for pointing an app at MinIO and **(if available) an approved corporate proxy**.

> **Important**: This bundle does **not** bypass firewalls or auth. It implements a policy‑friendly mirror so your workloads never need to reach `huggingface.co` directly.
