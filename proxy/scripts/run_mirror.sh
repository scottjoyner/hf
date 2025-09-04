#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
source .venv/bin/activate
pip install -r mirror/requirements.txt
python mirror/mirror_hf_to_minio.py "$@"
