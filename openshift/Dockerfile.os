# OpenShift-friendly Python image
FROM python:3.11-slim

WORKDIR /app

# Minimal OS deps; install mc client
RUN apt-get update && apt-get install -y --no-install-recommends         curl ca-certificates gnupg &&         curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc &&         chmod +x /usr/local/bin/mc &&         pip install --no-cache-dir awscli &&         rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1         PYTHONDONTWRITEBYTECODE=1         PIP_DISABLE_PIP_VERSION_CHECK=1         PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && rm -rf /root/.cache

COPY scripts/ ./scripts/
COPY data/ ./data/

# Make image work with arbitrary UIDs (OpenShift restricted SCC)
RUN mkdir -p /app && chgrp -R 0 /app && chmod -R g=u /app

# No fixed USER here; let OpenShift assign an arbitrary UID.
ENTRYPOINT ["python", "-m", "scripts.worker"]
CMD ["help"]
