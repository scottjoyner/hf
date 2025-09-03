#!/usr/bin/env python3
# scripts/hf_normalize.py
from urllib.parse import urlparse

_HF_HOST = "huggingface.co"

def is_hf_url(url: str) -> bool:
    if not isinstance(url, str) or not url:
        return False
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https") and u.netloc == _HF_HOST
    except Exception:
        return False

def canonical_repo_id_from_url(url: str):
    """
    Return '<org>/<repo>' or 'repo' from any HF URL, stripping extras like
    /tree/<rev>, /resolve/<rev>, query, fragment.
    """
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
    return f"https://{_HF_HOST}/{rid}" if rid else ""

def repo_id_from_any(url: str, fallback_name: str = ""):
    """
    Best-effort resolver:
      1) Canonicalize HF URL if possible.
      2) Else if fallback_name contains 'org/repo', use it; else single-tenant 'repo'.
    """
    rid = canonical_repo_id_from_url(url) if is_hf_url(url) else None
    if rid:
        return rid
    if isinstance(fallback_name, str) and "/" in fallback_name.strip("/"):
        return fallback_name.strip("/")
    if isinstance(fallback_name, str) and fallback_name and "/" not in fallback_name:
        return fallback_name.strip()
    return None
