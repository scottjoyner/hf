#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

API = os.environ.get("MODEL_REGISTRY_URL", "http://localhost:8081")
API_KEY = os.environ.get("MODEL_REGISTRY_API_KEY", "")

def _h():
    return {"x-api-key": API_KEY} if API_KEY else {}

def cmd_list(args):
    params = {}
    if args.q: params["q"] = args.q
    if args.updated_since: params["updated_since"] = args.updated_since
    params["limit"] = args.limit
    r = requests.get(urljoin(API, "/v1/models"), params=params, headers=_h(), timeout=30)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

def cmd_manifest(args):
    params = {"presign": "1" if args.presign else "0", "expires": args.expires}
    r = requests.get(urljoin(API, f"/v1/manifest/{args.repo_id}"), params=params, headers=_h(), timeout=60)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

def cmd_pull(args):
    # Grab manifest with presigned URLs, download all files into a local directory
    params = {"presign": "1", "expires": args.expires}
    r = requests.get(urljoin(API, f"/v1/manifest/{args.repo_id}"), params=params, headers=_h(), timeout=60)
    r.raise_for_status()
    mani = r.json()
    out = Path(args.out or mani["repo_id"].replace("/", "__"))
    out.mkdir(parents=True, exist_ok=True)

    import hashlib
    def sha256_of(path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""):
                h.update(chunk)
        return h.hexdigest()

    ok = 0
    for f in mani["files"]:
        name = f["rfilename"]
        url = f.get("presigned_url")
        if not url:
            print(f"skip (no URL): {name}", file=sys.stderr)
            continue
        dest = out / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            with dest.open("wb") as g:
                for chunk in resp.iter_content(chunk_size=1<<20):
                    if chunk:
                        g.write(chunk)
        if f.get("sha256"):
            calc = sha256_of(dest)
            if calc != f["sha256"]:
                print(f"hash mismatch: {name}", file=sys.stderr)
                continue
        ok += 1
        print(f"downloaded: {name}")
    print(f"done: {ok}/{len(mani['files'])} files -> {out}")

def cmd_changes(args):
    since = args.since or int(time.time()) - 86400
    r = requests.get(urljoin(API, "/v1/changes"), params={"since": since, "limit": args.limit}, headers=_h(), timeout=30)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

def main():
    ap = argparse.ArgumentParser(prog="modelctl", description="Client for Model Registry API")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list", help="List/search models")
    p.add_argument("--q")
    p.add_argument("--updated-since", type=int)
    p.add_argument("--limit", type=int, default=100)
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("manifest", help="Get manifest for a model")
    p.add_argument("repo_id")
    p.add_argument("--presign", action="store_true")
    p.add_argument("--expires", type=int, default=3600)
    p.set_defaults(func=cmd_manifest)

    p = sub.add_parser("pull", help="Download all files for a model")
    p.add_argument("repo_id")
    p.add_argument("--out")
    p.add_argument("--expires", type=int, default=3600)
    p.set_defaults(func=cmd_pull)

    p = sub.add_parser("changes", help="Change feed since timestamp")
    p.add_argument("--since", type=int)
    p.add_argument("--limit", type=int, default=500)
    p.set_defaults(func=cmd_changes)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
