# syntax=docker/dockerfile:1.6
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    APP_HOME=/app

WORKDIR ${APP_HOME}

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# App code
COPY scripts/ ./scripts/
COPY scripts/webapp ./scripts/webapp
COPY data/models.csv ./data/models.csv
COPY scripts/entrypoint.sh ./entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Use non-root
RUN mkdir -p /app/db && useradd -m -u 1000 appuser
USER appuser

ENV PATH="${APP_HOME}/scripts:${PATH}"

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["worker", "help"]
