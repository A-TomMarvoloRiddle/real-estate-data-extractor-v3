# pipeline.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

from .settings import DATA_DIR, load_listings_config, now_utc_iso
from .utils import write_json, ensure_dir, jitter_sleep
from .firecrawl_client import Firecrawl
from .steps.search_links import collect_detail_urls_for_config
from .steps.fetch_details import fetch_and_parse_details

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _new_batch_root() -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    root = Path(DATA_DIR) / "batches" / f"batch-{ts}"
    root.mkdir(parents=True, exist_ok=True)
    (root / "raw").mkdir(parents=True, exist_ok=True)
    return root


def _write_tables(
    batch_root: Path,
    properties: Dict[str, dict],
    listings: List[dict],
    media_rows: List[dict],
    price_history: List[dict],
) -> None:
    # properties.json as an array of objects
    props_arr = list(properties.values())
    write_json(batch_root / "properties.json", props_arr)
    write_json(batch_root / "listings.json", listings)
    write_json(batch_root / "media.json", media_rows)
    write_json(batch_root / "price_history.json", price_history)


def _maybe_read_list(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _maybe_read_rejects_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(obj, dict) and "count" in obj:
            return int(obj.get("count") or 0)
        if isinstance(obj, list):
            return len(obj)
    except Exception:
        pass
    return 0


def _write_summary(
    batch_root: Path,
    properties: Dict[str, dict],
    listings: List[dict],
    media_rows: List[dict],
    price_history: List[dict],
) -> None:
    # counts from in-memory
    props_count = len(properties)
    listings_count = len(listings)
    media_count = len(media_rows)
    price_hist_count = len(price_history)

    # optional tables (written by fetch_details.py if present)
    agents = _maybe_read_list(batch_root / "agents.json")
    monthly = _maybe_read_list(batch_root / "monthly_costs.json")

    # qa rejects (object with count or list)
    rejects_count = _maybe_read_rejects_count(batch_root / "qa_rejects.json")

    summary = {
        "properties": props_count,
        "listings": listings_count,
        "media": media_count,
        "price_history": price_hist_count,
        # NEW: extended counts per Debayan schema extras
        "agents": len(agents),
        "monthly_costs": len(monthly),
        "qa_rejects": rejects_count,
        "generated_at": now_utc_iso(),
        "files": {
            "properties": "properties.json",
            "listings": "listings.json",
            "media": "media.json",
            "price_history": "price_history.json",
            "agents": "agents.json" if agents else None,
            "monthly_costs": "monthly_costs.json" if monthly else None,
            "qa_rejects": "qa_rejects.json" if rejects_count else None,
        },
    }
    write_json(batch_root / "summary.json", summary)


def run_pipeline() -> Path:
    # 1) Load config
    cfg = load_listings_config()
    sleep_range: Tuple[float, float] = tuple(cfg.get("run", {}).get("sleep_range_sec", [1.2, 2.8]))  # type: ignore
    crawl_method = cfg.get("crawl_method") or "firecrawl_v1"

    # 2) Prepare batch directory
    batch_root = _new_batch_root()
    log.info(f"[pipeline] batch dir: {batch_root}")

    # 3) Collect detail URLs (search step)
    fc = Firecrawl()
    detail_urls = collect_detail_urls_for_config(fc, cfg, batch_root)
    write_json(batch_root / "detail_urls.json", detail_urls)
    log.info(f"[pipeline] collected {len(detail_urls)} detail urls")

    # 4) Fetch + parse detail pages
    properties, listings, media_rows, price_history = fetch_and_parse_details(
        fc=fc,
        detail_urls=detail_urls,
        batch_root=batch_root,
        crawl_method=crawl_method,
        sleep_range=sleep_range,  # jitter already inside
        start_listing_id=1000,
    )

    # 5) Write tables
    _write_tables(batch_root, properties, listings, media_rows, price_history)

    # 6) Summary (now includes agents & monthly_costs if present)
    _write_summary(batch_root, properties, listings, media_rows, price_history)

    log.info(f"[pipeline] done. batch={batch_root.name}")
    return batch_root


if __name__ == "__main__":
    run_pipeline()
