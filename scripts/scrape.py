#!/usr/bin/env python3
"""
scrape.py — Enrich models CSV with Hugging Face metadata (cached) and optionally
seed a SQLite DB.

- Reads an input CSV with at least: model_name, url (optionally updated_url)
- Normalizes any HF URLs to canonical form: https://huggingface.co/<org>/<repo> (or /<repo>)
- Fetches /api/models/<repo> (with caching) to add: model_description, params, model_size
- Writes the enriched CSV to the requested output path (default: data/models_enriched.csv)
- Optionally seeds/updates a SQLite DB if DB_PATH is set (via scripts/models_db.py)

Usage:
  python scripts/scrape.py --input data/models.csv --output data/models_enriched.csv
"""

import os
import json
import time
from pathlib import Path
from typing import Tuple
from pathlib import Path
import pandas as pd
import requests
from tqdm import tqdm

# Try to use shared normalizer; fall back to local helpers if unavailable
try:
    from scripts.hf_normalize import (
        is_hf_url,
        canonicalize_hf_url,
        canonical_repo_id_from_url,
        repo_id_from_any,
    )
except Exception:
    from urllib.parse import urlparse

    HUGGINGFACE = "huggingface.co"

    def is_hf_url(url: str) -> bool:
        if not isinstance(url, str) or not url:
            return False
        try:
            u = urlparse(url)
            return u.scheme in ("http", "https") and u.netloc == HUGGINGFACE
        except Exception:
            return False

    def canonical_repo_id_from_url(url: str):
        if not is_hf_url(url):
            return None
        u = urlparse(url)
        parts = [p for p in u.path.strip("/").split("/") if p]
        if not parts:
            return None
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
        return parts[0]

    def canonicalize_hf_url(url: str) -> str:
        rid = canonical_repo_id_from_url(url)
        return f"https://{HUGGINGFACE}/{rid}" if rid else ""

    def repo_id_from_any(url: str, fallback_name: str = ""):
        rid = canonical_repo_id_from_url(url) if is_hf_url(url) else None
        if rid:
            return rid
        if isinstance(fallback_name, str) and "/" in fallback_name.strip("/"):
            return fallback_name.strip("/")
        if isinstance(fallback_name, str) and fallback_name and "/" not in fallback_name:
            return fallback_name.strip()
        return None

# --------------------------
# Configuration
# --------------------------
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN", "").strip()
HEADERS = {"User-Agent": "models-scraper/1.0"}
if HUGGINGFACE_TOKEN:
    HEADERS["Authorization"] = f"Bearer {HUGGINGFACE_TOKEN}"

# --------------------------
# Helpers
# --------------------------
def _cache_path_for(repo_id: str) -> Path:
    return CACHE_DIR / f"{repo_id.replace('/', '__')}.json"

def fetch_model_info(model_id: str) -> Tuple[str, str, str]:
    """
    Return (model_description, params, size) from cached HF model JSON.
    Cache misses call HF API and store JSON. Returns empty strings on failure.
    """
    cpath = _cache_path_for(model_id)
    if cpath.exists():
        try:
            data = json.loads(cpath.read_text())
        except Exception:
            data = None
    else:
        data = None

    if data is None:
        api_url = f"https://huggingface.co/api/models/{model_id}"
        try:
            r = requests.get(api_url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            cpath.write_text(json.dumps(data, indent=2))
            time.sleep(0.25)  # be nice to the API
        except requests.HTTPError as e:
            print(f"[WARN] HF API error for {model_id}: {e}")
            return "", "", ""
        except Exception as e:
            print(f"[WARN] Failed to fetch {model_id}: {e}")
            return "", "", ""

    card = data.get("cardData") or {}
    model_description = card.get("summary") or data.get("pipeline_tag") or ""
    params = card.get("params") or ""
    size = card.get("model_size") or ""
    return str(model_description or ""), str(params or ""), str(size or "")

def enrich_csv(input_csv: str = "data/models.csv", output_csv: str = "data/models_enriched.csv") -> None:
    df = pd.read_csv(input_csv)

    # Build/refresh 'updated_url' as canonical HF URLs where possible
    updated_urls = []
    for _, row in df.iterrows():
        raw = row.get("updated_url") if isinstance(row.get("updated_url"), str) else row.get("url")
        updated_urls.append(canonicalize_hf_url(raw) if isinstance(raw, str) else "")
    df["updated_url"] = updated_urls

    new_descriptions, new_params, new_sizes = [], [], []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Enriching"):
        url = (row.get("updated_url") or row.get("url") or "").strip()
        model_name = (row.get("model_name") or "").strip()

        # Resolve repo id from URL (preferred) or fallback from name
        if is_hf_url(url):
            rid = canonical_repo_id_from_url(url) or ""
        else:
            rid = repo_id_from_any(url, model_name) or ""

        if not rid:
            new_descriptions.append(""); new_params.append(""); new_sizes.append("")
            continue

        desc, params, size = fetch_model_info(rid)
        new_descriptions.append(desc); new_params.append(params); new_sizes.append(size)

    df["model_description"] = new_descriptions
    df["params"] = new_params
    df["model_size"] = new_sizes

    out_dir = Path(output_csv).parent
    if str(out_dir) in ("", ".", None):   # handle bare filename like "models_enriched.csv"
        output_csv = str(Path("data") / Path(output_csv).name)
        out_dir = Path(output_csv).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Enriched data saved to {output_csv}")

    # Optionally seed SQLite DB with minimal rows
    _maybe_seed_db(output_csv)

# --------------------------
# Optional DB seeding
# --------------------------
def _maybe_seed_db(enriched_csv_path: str) -> None:
    db_path = os.getenv("DB_PATH", "/app/db/models.db")
    if not db_path:
        return
    try:
        from scripts.models_db import init_db, upsert_model
    except Exception:
        # DB module not present; silently skip
        return

    try:
        init_db(db_path)
        df = pd.read_csv(enriched_csv_path)
        for _, row in df.iterrows():
            url = (row.get("updated_url") or row.get("url") or "").strip()
            if not is_hf_url(url):
                continue
            rid = canonical_repo_id_from_url(url)
            if not rid:
                continue
            upsert_model(db_path, rid, {
                "canonical_url": f"https://huggingface.co/{rid}",
                "model_name": rid.split("/")[-1],
                "model_description": row.get("model_description") or "",
                "params_raw": row.get("params") or "",
                "model_size_raw": row.get("model_size") or "",
            })
        print(f"[DB] Seeded/updated models table at {db_path}")
    except Exception as e:
        print(f"[DB] seed skipped: {e}")

# --------------------------
# CLI
# --------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Enrich models CSV with HF metadata (cached).")
    ap.add_argument("--input", default="data/models.csv", help="Path to input CSV")
    ap.add_argument("--output", default="data/models_enriched.csv", help="Path to output CSV")
    args = ap.parse_args()

    enrich_csv(args.input, args.output)
