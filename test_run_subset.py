#!/usr/bin/env python3
"""
Run a small test batch with N detail URLs.
Writes JSON tables under data/test_runs/<batch_id> for inspection.
"""
import os, sys, json
from pathlib import Path
import argparse

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.firecrawl_client import Firecrawl
from src.settings import load_config, TEST_DATA_ROOT, ensure_dir, get_api_key, new_batch_id
from src.steps.search_links import collect_search_links_light
from src.steps.fetch_details import fetch_and_parse_details
from src.schemas import PropertiesFile, ListingsFile, MediaFile, PriceHistoryFile
from src.utils import write_json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--n", type=int, default=3, help="number of listings to fetch")
    args = ap.parse_args()

    api_key = get_api_key()
    if not api_key:
        print("Missing FIRECRAWL_API_KEY env var")
        return

    cfg = load_config()
    run_cfg = cfg.get("run", {}) or {}
    sleep_lo, sleep_hi = run_cfg.get("sleep_range_sec", [1.2, 2.8])
    timeout = run_cfg.get("request_timeout_sec", 30)
    fc = Firecrawl(api_key=api_key, timeout=timeout, sleep_range=(sleep_lo, sleep_hi))

    batch_id = new_batch_id(prefix="test-")
    outdir = TEST_DATA_ROOT / batch_id
    ensure_dir(outdir)
    ensure_dir(outdir / "raw")

    # get first ZIP
    areas = cfg.get("areas", [])
    if not areas:
        print("No areas in config")
        return
    area = areas[0]
    zips = area.get("zips", [])[:1]
    tmpl = cfg.get("seeds", {}).get("zillow", {}).get("zip_search")
    if not tmpl:
        print("No zillow template in config")
        return
    urls = collect_search_links_light(fc, "zillow", tmpl, zips, (sleep_lo, sleep_hi), per_zip_limit=args.n)
    urls = urls[: args.n]
    print(f"Collected {len(urls)} urls for test")

    props, listings, media, ph = fetch_and_parse_details(
        fc, urls, outdir, crawl_method="firecrawl_v1_test",
        sleep_range=(sleep_lo, sleep_hi), start_listing_id=9990
    )

    props_list = list(props.values())
    write_json(outdir / "properties.json", PropertiesFile(props_list).root)
    write_json(outdir / "listings.json", ListingsFile(listings).root)
    write_json(outdir / "media.json", MediaFile(media).root)
    write_json(outdir / "price_history.json", PriceHistoryFile(ph).root)
    write_json(outdir / "summary.json", {
        "properties": len(props_list),
        "listings": len(listings),
        "media": len(media),
        "price_history": len(ph),
        "urls_tested": urls,
    })

    print("Test run written to", outdir)

if __name__ == "__main__":
    main()
