# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# OS deps (curl for healthchecks / mc bootstrap logs)
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App files
COPY scripts/ ./scripts/
# (optional) bring in defaults; they’ll be bind-mounted at runtime anyway
COPY data/ ./data/

# Make sure scripts is a package
RUN test -f scripts/__init__.py || touch scripts/__init__.py

# Default entrypoint: run the worker CLI module
ENTRYPOINT ["python","-m","scripts.worker"]
# Default command if none given
CMD ["help"]
