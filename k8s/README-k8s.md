# Kubernetes notes
- Apply MinIO stack first:
  kubectl apply -f k8s/minio.yaml
- Create secrets for HF and AWS:
  kubectl create secret generic hf-token -n models-pipeline --from-literal=token=YOUR_HF_TOKEN
  kubectl create secret generic aws-s3-creds -n models-pipeline --from-literal=access_key=AKIA... --from-literal=secret_key=...
- Build & push the image referenced in `cronjob.yaml` (ghcr.io/yourorg/models-pipeline:latest), then:
  kubectl apply -f k8s/cronjob.yaml
