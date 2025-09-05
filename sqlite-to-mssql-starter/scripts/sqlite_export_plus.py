
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export ALL (or selected) SQLite tables to CSV + manifest (row counts + sha256).
Usage:
  python sqlite_export_plus.py --sqlite /path/to/app.db --out ../import [--tables T1 T2 ...]
"""
import argparse, csv, os, sqlite3, sys, json, hashlib
from pathlib import Path

def list_tables(conn):
  cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1")
  return [r[0] for r in cur.fetchall()]

def sha256_file(path):
  h = hashlib.sha256()
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(1024*1024), b""):
      h.update(chunk)
  return h.hexdigest()

def export_table(conn, table, out_dir):
  out_path = Path(out_dir) / f"{table}.csv"
  cur = conn.execute(f"SELECT * FROM {table}")
  cols = [d[0] for d in cur.description]

  rows = 0
  with open(out_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for row in cur:
      clean = []
      for v in row:
        if isinstance(v, (bytes, bytearray)):
          clean.append(v.hex())
        else:
          clean.append(v)
      w.writerow(clean)
      rows += 1

  return {"table": table, "rows": rows, "csv": str(out_path), "sha256": sha256_file(out_path)}

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--sqlite", required=True)
  ap.add_argument("--out", required=True)
  ap.add_argument("--tables", nargs="*")
  args = ap.parse_args()

  os.makedirs(args.out, exist_ok=True)
  conn = sqlite3.connect(args.sqlite)
  conn.row_factory = sqlite3.Row
  manifest = {"sqlite": args.sqlite, "tables": []}
  try:
    tables = args.tables or list_tables(conn)
    if not tables:
      print("No tables found.", file=sys.stderr)
      sys.exit(2)
    for t in tables:
      info = export_table(conn, t, args.out)
      manifest["tables"].append(info)
    with open(Path(args.out) / "manifest.json", "w", encoding="utf-8") as mf:
      json.dump(manifest, mf, indent=2)
    print(f"✔ Wrote manifest with {len(manifest['tables'])} tables -> {Path(args.out) / 'manifest.json'}")
  finally:
    conn.close()

if __name__ == "__main__":
  main()
