# Use official slim Python base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install OS dependencies (curl, mc, optional awscli)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gnupg && \
    curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc && \
    # Optional: AWS CLI support (comment out if not needed)
    pip install --no-cache-dir awscli && \
    rm -rf /var/lib/apt/lists/*

# Python environment tweaks
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

# Copy Python requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

# Copy application code (scripts and optional data)
COPY scripts/ ./scripts/
COPY data/ ./data/

# --- grab mc binary from official image ---
FROM quay.io/minio/mc:latest AS mcbin

# --- final runtime image ---
FROM base
# place mc on PATH
COPY --from=mcbin /usr/bin/mc /usr/local/bin/mc

# good defaults so mc can write its config without PVCs
ENV HOME=/tmp MC_CONFIG_DIR=/tmp/mc
# Ensure scripts is a package
RUN test -f scripts/__init__.py || touch scripts/__init__.py


# Entry and default command
ENTRYPOINT ["python", "-m", "scripts.worker"]
CMD ["help"]

EXPOSE 8081
EXPOSE 8080
EXPOSE 9000
EXPOSE 9001



