# src/pipeline.py
# Purpose: Orchestrate detail fetching and parsing using the existing modules.
# Updated for new schema: source_id (not platform_id), JSON arrays instead of JSONL, include batch_id/crawl_method.

from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import List, Optional
from collections import defaultdict

from src.fetch import fetch_detail_pages
from src.parse_detail import parse_all_details, to_adapted_rows
from src.settings import now_utc_iso
from src.settings import PROJECT_ROOT

BATCHES_ROOT = PROJECT_ROOT / "data" / "batches"

def latest_batch() -> Path:
    if not BATCHES_ROOT.exists():
        raise RuntimeError("No batches folder found. Run: python -m src.batch")
    candidates = [p for p in BATCHES_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("No batch directories found. Run: python -m src.batch")
    return max(candidates, key=lambda p: p.stat().st_mtime)

def load_listing_urls(batch_dir: Path) -> List[str]:
    lu_path = batch_dir / "structured" / "listing_urls.json"
    if not lu_path.exists():
        raise FileNotFoundError(
            "structured/listing_urls.json not found. "
            "Run: python -m src.fetch  then  python -m src.extract_search"
        )
    payload = json.loads(lu_path.read_text(encoding="utf-8"))
    urls = payload.get("urls", [])
    out: List[str] = []
    for row in urls:
        if isinstance(row, dict):
            url = row.get("source_url")
        else:
            url = str(row)
        if url:
            out.append(url)
    if not out:
        raise RuntimeError("No detail URLs inside listing_urls.json")
    return out

def next_detail_index(raw_dir: Path) -> int:
    """Return the next index for detail files (start at 1001)."""
    existing = sorted(raw_dir.glob("1???_raw.html"))
    if not existing:
        return 1001
    last = max(int(p.name[:4]) for p in existing)
    return last + 1

def fetch_details(n: int, batch_id: Optional[str] = None) -> None:
    batch_dir = latest_batch() if batch_id is None else (BATCHES_ROOT / batch_id)
    raw_dir = batch_dir / "raw"
    urls = load_listing_urls(batch_dir)

    start_idx = next_detail_index(raw_dir)
    subset = urls[:n]
    print(f"Batch: {batch_dir.name}")
    print(f"Fetching {len(subset)} details starting at idx {start_idx} ...")
    fetch_detail_pages(subset, batch_id=batch_dir.name, start_idx=start_idx)
    print("✅ fetch-details done at", now_utc_iso())

def parse_details(limit: int, batch_id: Optional[str] = None, mode: str = "raw") -> None:
    batch_dir = latest_batch() if batch_id is None else (BATCHES_ROOT / batch_id)
    print(f"Batch: {batch_dir.name}")

    if mode == "raw":
        parse_all_details(batch_id=batch_dir.name, limit=limit)
    else:
        struct = batch_dir / "structured"
        buckets = defaultdict(list)
        # مرّ على كل ملفات 1***.json التي يولدها parse_detail
        for p in sorted(struct.glob("1???*.json")):
            rec = json.loads(p.read_text(encoding="utf-8"))
            rows = to_adapted_rows(rec)
            for tbl, arr in rows.items():
                buckets[tbl].extend(arr)
        # اكتب JSON (array) بدل JSONL
        for tbl, arr in buckets.items():
            out = struct / f"{tbl}.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(arr, f, ensure_ascii=False, indent=2)
        print("✅ wrote adapted JSON files in", struct)

    print("✅ parse-details done at", now_utc_iso())

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("fetch-details", help="Fetch N detail pages into the latest batch")
    s1.add_argument("--n", type=int, default=10)

    s2 = sub.add_parser("parse-details", help="Parse up to LIMIT detail pages in the latest batch")
    s2.add_argument("--limit", type=int, default=10)
    s2.add_argument("--mode", choices=["raw", "adapted"], default="raw")

    s3 = sub.add_parser("run", help="Fetch N detail pages then parse them")
    s3.add_argument("--n", type=int, default=10)

    args = ap.parse_args()
    if args.cmd == "fetch-details":
        fetch_details(args.n)
    elif args.cmd == "parse-details":
        parse_details(args.limit, mode=args.mode)
    elif args.cmd == "run":
        fetch_details(args.n)
        parse_details(args.n)
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
