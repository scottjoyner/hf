
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export SQLite tables to CSV (UTF-8 with headers) suitable for SQL Server BULK INSERT.
Usage:
  python sqlite_export.py --sqlite /path/to/app.db --out ../import --tables UserProfile LogEvent
If --tables is omitted, exports all non-internal tables.
"""
import argparse, csv, os, sqlite3, sys
from pathlib import Path

def list_tables(conn):
  cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1")
  return [r[0] for r in cur.fetchall()]

def export_table(conn, table, out_dir):
  out_path = Path(out_dir) / f"{table}.csv"
  cur = conn.execute(f"SELECT * FROM {table}")
  cols = [d[0] for d in cur.description]

  with open(out_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    for row in cur:
      clean = []
      for v in row:
        if isinstance(v, (bytes, bytearray)):
          clean.append(v.hex())
        else:
          clean.append(v)
      writer.writerow(clean)
  print(f"✔ Exported {table} -> {out_path}")

def main():
  ap = argparse.ArgumentParser()
  ap.add_argument("--sqlite", required=True, help="Path to SQLite .db")
  ap.add_argument("--out", required=True, help="Output folder for CSVs")
  ap.add_argument("--tables", nargs="*", help="Tables to export (default: all user tables)")
  args = ap.parse_args()

  os.makedirs(args.out, exist_ok=True)
  conn = sqlite3.connect(args.sqlite)
  try:
    tables = args.tables or list_tables(conn)
    if not tables:
      print("No tables found.", file=sys.stderr)
      sys.exit(2)
    for t in tables:
      export_table(conn, t, args.out)
  finally:
    conn.close()

if __name__ == "__main__":
  main()
