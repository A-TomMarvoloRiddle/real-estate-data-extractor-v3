# src/pipeline.py
# Orchestrate: fetch → parse (per-page) → aggregate (adapted tables) → cleaning/QA

from __future__ import annotations
import argparse
import re
from pathlib import Path
from typing import Dict, Optional
from collections import defaultdict

from src.fetch import fetch_detail_pages
from src.parse_detail import parse_all_details, to_adapted_rows
from src.settings import make_batch_dirs, to_float, to_int, s_trim, latest_batch_dir, read_json, write_json

NUM_RE = re.compile(r"[^\d\.]+")

# ---------------- helpers ----------------

def ensure_dirs(batch_id: Optional[str]) -> Dict[str, Path]:
    if batch_id:
        return make_batch_dirs(batch_id)
    latest = latest_batch_dir()
    return {"base": latest, "raw": latest / "raw", "structured": latest / "structured", "qa": latest / "qa"}

# ---------------- core steps ----------------
def fetch_details(n: int, batch_id: Optional[str] = None):
    dirs = ensure_dirs(batch_id)
    raw_dir = dirs["raw"]
    urls = read_json(dirs["base"]/ "structured"/ "listing_urls.json", {}).get("urls", [])
    urls = [r["source_url"] if isinstance(r, dict) else str(r) for r in urls]
    if not urls: 
        raise RuntimeError("No detail URLs in listing_urls.json")
    start_idx = max([int(p.name[:4]) for p in raw_dir.glob("1???_raw.html")] or [1000]) + 1
    subset = urls[:n]
    print(f"Batch {dirs['base'].name}: fetching {len(subset)} details …")
    fetch_detail_pages(subset, batch_id=dirs["base"].name, start_idx=start_idx)

def parse_details(limit: int, batch_id: Optional[str] = None, mode: str = "raw"):
    dirs = ensure_dirs(batch_id)
    struct = dirs["structured"]

    if mode == "raw":
        parse_all_details(batch_id=dirs["base"].name, limit=limit)
        print("✅ parse-details (raw) done")
        return

    # adapted: aggregate rows
    buckets = defaultdict(list)
    detail_files = sorted(struct.glob("1???*.json"))[:limit]
    if not detail_files: 
        raise FileNotFoundError("No detail JSON files. Run parse-details --mode raw first.")

    for f in detail_files:
        rec = read_json(f, {})
        rows = to_adapted_rows(rec)
        for tbl, arr in rows.items():
            if not arr: 
                continue
            buckets[tbl].extend(arr)

    # cleaning
    listings = buckets.get("listings", [])
    properties = buckets.get("properties", [])
    media = buckets.get("media", [])
    agents = buckets.get("agents", [])
    price_history = buckets.get("price_history", [])
    engagement = buckets.get("engagement", [])
    locations = buckets.get("locations", [])
    financials = buckets.get("financials", [])
    community_attributes = buckets.get("community_attributes", [])
    similar_properties = buckets.get("similar_properties", [])

    # normalize strings & numbers
    for li in listings:
        li["title"] = s_trim(li.get("title"))
        if li.get("title") and li["title"].lower() in ("about this home", "about this house"):
            li["title"] = None
        li["description"] = s_trim(li.get("description"))
        li["list_price"] = to_int(li.get("list_price"))

    for p in properties:
        p["street_address"]   = s_trim(p.get("street_address"))
        p["unit_number"]      = s_trim(p.get("unit_number"))
        p["city"]             = s_trim(p.get("city"))
        p["state"]            = s_trim(p.get("state"))
        p["postal_code"]      = s_trim(p.get("postal_code"))
        p["beds"]             = to_float(p.get("beds"))
        p["baths"]            = to_float(p.get("baths"))
        p["interior_area_sqft"]= to_int(p.get("interior_area_sqft"))
        p["lot_size_sqft"]    = to_int(p.get("lot_size_sqft"))
        p["year_built"]       = to_int(p.get("year_built"))

    # recompute price_per_sqft from properties map
    area_by_prop = {p["property_id"]: p.get("interior_area_sqft") for p in properties}
    for lis in listings:
        lp, a = lis.get("list_price"), area_by_prop.get(lis["property_id"])
        lis["price_per_sqft"] = (float(lp)/float(a)) if (lp and a) else None

    # media: dedup per listing + cap per listing (e.g., 20)
    per_listing = defaultdict(list)
    for m in media:
        lid = m.get("listing_id")
        u = s_trim(m.get("media_url"))
        if not lid or not u: 
            continue
        per_listing[(lid, u)].append(m)
    deduped_media = []
    seen_per_listing = defaultdict(set)
    for m in media:
        lid = m.get("listing_id")
        u = s_trim(m.get("media_url"))
        if not lid or not u: 
            continue
        if u in seen_per_listing[lid]: 
            continue
        if len(seen_per_listing[lid]) >= 20:
            continue
        seen_per_listing[lid].add(u)
        deduped_media.append(m)
    media = deduped_media

    # write outputs (ALL tables, no global caps)
    write_json(struct/ "listings.json", listings)
    write_json(struct/ "properties.json", properties)
    write_json(struct/ "media.json", media)
    write_json(struct/ "agents.json", agents)
    write_json(struct/ "price_history.json", price_history)
    write_json(struct/ "financials.json", financials)
    write_json(struct/ "engagement.json", engagement)
    write_json(struct/ "community_attributes.json", community_attributes)
    write_json(struct/ "similar_properties.json", similar_properties)
    write_json(struct/ "locations.json", locations)

    print("✅ wrote adapted JSON files in", struct)

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s1 = sub.add_parser("fetch-details")
    s1.add_argument("--n", type=int, default=10)
    s2 = sub.add_parser("parse-details")
    s2.add_argument("--limit", type=int, default=50)
    s2.add_argument("--mode", choices=["raw","adapted"], default="raw")
    s3 = sub.add_parser("run")
    s3.add_argument("--n", type=int, default=20)
    s3.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    if args.cmd=="fetch-details": 
        fetch_details(args.n)
    elif args.cmd=="parse-details": 
        parse_details(args.limit, mode=args.mode)
    elif args.cmd=="run":
        fetch_details(args.n)
        parse_all_details(limit=args.limit)
        parse_details(limit=args.limit, mode="adapted")

if __name__=="__main__": 
    main()
