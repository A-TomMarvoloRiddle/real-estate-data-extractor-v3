# src/batch.py
# Purpose: Initialize a new batch with ID, folders, and seed search pages.

import json
from src.settings import CFG, make_batch_dirs, today_ymd, now_utc_iso

def init_batch() -> str:
    """
    Create new batch folders and seed search pages file.
    Returns: BATCH_ID
    """
    # ---- Derive ZIP list from areas ----
    zip_codes = []
    for area in CFG.get("areas", []):
        for z in area.get("zips", []):
            zip_codes.append({
                "city": area["city"],
                "state": area["state"],
                "zip": z
            })

    # ---- Build search pages (per platform per ZIP) ----
    search_pages = []
    for z in zip_codes:
        zip_code = z["zip"]
        city = z["city"]
        state = z["state"]

        if "redfin" in CFG["seeds"]:
            search_pages.append({
                "source_id": "redfin",
                "zip": zip_code,
                "city": city,
                "state": state,
                "crawl_method": CFG.get("crawl_method", "firecrawl_v1"),
                "url": CFG["seeds"]["redfin"]["zip_search"].format(ZIP=zip_code)
            })

        if "zillow" in CFG["seeds"]:
            search_pages.append({
                "source_id": "zillow",
                "zip": zip_code,
                "city": city,
                "state": state,
                "crawl_method": CFG.get("crawl_method", "firecrawl_v1"),
                "url": CFG["seeds"]["zillow"]["zip_search"].format(ZIP=zip_code)
            })

    # ---- Optional hardcoded detail URLs ----
    detail_pages = [
        {"source_id": "unknown", "url": u,
         "crawl_method": CFG.get("crawl_method", "firecrawl_v1")}
        for u in CFG["seeds"].get("detail_urls", [])
    ]

    # ---- Create batch_id and dirs ----
    TODAY = today_ymd()
    BATCH_ID = f"{TODAY}_zips{len(zip_codes)}"
    dirs = make_batch_dirs(BATCH_ID)

    # ---- Persist seeds ----
    seeds_path = dirs["structured"] / "seed_search_pages.json"
    seeds_obj = {
        "batch_id": BATCH_ID,
        "generated_at": now_utc_iso(),
        "counts": {
            "zip_total": len(zip_codes),
            "search_pages_total": len(search_pages),
            "detail_pages_total": len(detail_pages)
        },
        "search_pages": search_pages,
        "detail_pages": detail_pages
    }
    seeds_path.write_text(json.dumps(seeds_obj, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"✅ Batch {BATCH_ID} ready at {dirs['base'].resolve()}")
    print(f"Seeds file: {seeds_path}")
    return BATCH_ID

if __name__ == "__main__":
    init_batch()
