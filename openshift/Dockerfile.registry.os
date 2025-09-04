FROM python:3.11-slim
WORKDIR /app

# Bring codebase
COPY . /app

# Install API deps
COPY registry_app/requirements.txt /app/registry_app/requirements.txt
RUN pip install --no-cache-dir -r /app/registry_app/requirements.txt

# OpenShift arbitrary UID friendly
RUN chgrp -R 0 /app && chmod -R g=u /app

EXPOSE 8081
ENV DB_PATH=/app/db/models.db         UVICORN_HOST=0.0.0.0         UVICORN_PORT=8081

CMD ["uvicorn", "registry_app.main:app", "--host", "0.0.0.0", "--port", "8081"]
