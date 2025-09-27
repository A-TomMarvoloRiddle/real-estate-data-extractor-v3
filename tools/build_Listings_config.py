#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build listings_config.json directly from CSVs in config/raw/.
"""

import csv, json, os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# paths to Dipayan's CSVs
REGIONS_CSV = os.path.join(BASE_DIR, "config", "raw", "top_regions_with_listing_count_and_median_price.csv")
ZIPS_CSV    = os.path.join(BASE_DIR, "config", "raw", "top_zipcodes_per_city.csv")
OUT_JSON    = os.path.join(BASE_DIR, "config", "listings_config.json")

def to_zip5(z):
    s = str(z).strip()
    s = "".join(ch for ch in s if ch.isdigit())
    return s.zfill(5)[:5] if s else ""

def build_areas(zips_csv):
    areas = defaultdict(list)
    with open(zips_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            city  = (r.get("CityName") or "").strip()
            state = (r.get("StateName") or "").strip().upper()
            zip5  = to_zip5(r.get("ZipCode") or "")
            if city and state and zip5:
                areas[(city, state)].append(zip5)
    # convert to list of dicts
    result = []
    for (city, state), zlist in areas.items():
        result.append({"city": city, "state": state, "zips": sorted(set(zlist))})
    return result

def build_city_stats(regions_csv):
    stats = []
    with open(regions_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            city  = (r.get("CityName") or "").strip()
            state = (r.get("StateName") or "").strip().upper()
            median = r.get("MedianPrice")
            newlst = r.get("NewListings")
            if city and state:
                stats.append({
                    "city": city,
                    "state": state,
                    "median_price": float(str(median).replace(",", "").replace("$", "")) if median else None,
                    "new_listings": float(str(newlst).replace(",", "")) if newlst else None
                })
    return stats

def main():
    areas = build_areas(ZIPS_CSV)
    city_stats = build_city_stats(REGIONS_CSV)

    cfg = {
        "run": {
            "request_timeout_sec": 30,
            "sleep_range_sec": [1.2, 2.8],
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        },
        "crawl_method": "firecrawl_v1",
        "seeds": {
            "zillow": { "zip_search": "https://www.zillow.com/homes/{ZIP}_rb/" },
            "redfin": { "zip_search": "https://www.redfin.com/zip/{ZIP}" }
        },
        "areas": areas,
        "city_stats": city_stats
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote {OUT_JSON}")
    print(f" - cities: {len(areas)}")
    print(f" - total ZIPs: {sum(len(a['zips']) for a in areas)}")

if __name__ == "__main__":
    main()