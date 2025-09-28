# steps/search_links.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import re

from ..firecrawl_client import Firecrawl
from ..utils import jitter_sleep, write_json

log = logging.getLogger(__name__)

# ---------------------------
# Regex patterns for detail pages
# ---------------------------

# Zillow detail URL examples:
# https://www.zillow.com/homedetails/113-E-19th-St-New-York-NY-10003/31506434_zpid/
ZILLOW_DETAIL_RE = re.compile(
    r"https?://www\.zillow\.com/homedetails/[^\s)\"']+?_zpid/?",
    re.I,
)

# Redfin detail URL examples:
# https://www.redfin.com/NY/New-York/135-E-19th-St-10003/home/214073302
REDFIN_DETAIL_RE = re.compile(
    r"https?://www\.redfin\.com/[^\s)\"']*?/home/\d+[^\s)\"']*",
    re.I,
)

# Also pick detail links appearing in markdown link targets: (...), avoid images
MD_LINK_RE = re.compile(r"(?<!\!)\[[^\]]*\]\((https?://[^\s)]+)\)")

# Some pages include trailing ')' from markdown or trailing punctuation
def _clean_url(u: str) -> str:
    return u.strip().strip(") ").rstrip(").,")

def _extract_links_from_html_and_md(html: str, md: str) -> Set[str]:
    urls: Set[str] = set()

    # From raw HTML/text
    for m in ZILLOW_DETAIL_RE.finditer(html or ""):
        urls.add(_clean_url(m.group(0)))
    for m in REDFIN_DETAIL_RE.finditer(html or ""):
        urls.add(_clean_url(m.group(0)))

    # From Markdown links
    for m in MD_LINK_RE.finditer(md or ""):
        target = _clean_url(m.group(1))
        if ZILLOW_DETAIL_RE.search(target) or REDFIN_DETAIL_RE.search(target):
            urls.add(target)

    return urls

def _scrape(fc: Firecrawl, url: str) -> tuple[str, str]:
    """
    Firecrawl v1 simple scrape; returns (html, markdown)
    """
    res = fc.scrape(url)
    if isinstance(res, dict):
        html = (res.get("html") or res.get("raw_html") or "") or ""
        md = res.get("markdown") or ""
        return html, md
    return "", ""

def _collect_for_zip(
    fc: Firecrawl,
    zip_code: str,
    seeds: Dict[str, Dict[str, str]],
    sleep_range: Tuple[float, float],
    per_zip_limit: Optional[int],
) -> Set[str]:
    urls: Set[str] = set()

    # Zillow zip search
    z_seed = ((seeds.get("zillow") or {}).get("zip_search") or "").strip()
    if z_seed:
        z_url = z_seed.format(ZIP=zip_code)
        log.info(f"[search-zip] Zillow {zip_code} => {z_url}")
        try:
            html, md = _scrape(fc, z_url)
            urls |= _extract_links_from_html_and_md(html, md)
        except Exception as e:
            log.warning(f"[search-zip] Zillow zip={zip_code} scrape failed: {e}")
        jitter_sleep(*sleep_range)

    # Redfin zip search
    r_seed = ((seeds.get("redfin") or {}).get("zip_search") or "").strip()
    if r_seed:
        r_url = r_seed.format(ZIP=zip_code)
        log.info(f"[search-zip] Redfin {zip_code} => {r_url}")
        try:
            html, md = _scrape(fc, r_url)
            urls |= _extract_links_from_html_and_md(html, md)
        except Exception as e:
            log.warning(f"[search-zip] Redfin zip={zip_code} scrape failed: {e}")
        jitter_sleep(*sleep_range)

    # Per-zip cap
    if per_zip_limit and per_zip_limit > 0 and len(urls) > per_zip_limit:
        urls = set(list(urls)[:per_zip_limit])

    return urls

def collect_detail_urls_for_config(
    fc: Firecrawl,
    cfg: Dict,
    batch_root: Path,
) -> List[str]:
    """
    Collect listing detail URLs for all zips in config.
    Writes a small diagnostic file 'search_links.json' with counts per city/zip.
    Returns a deduped flat list of URLs.
    """
    run_cfg = cfg.get("run", {}) or {}
    sleep_range = tuple(run_cfg.get("sleep_range_sec", [1.2, 2.8]))  # type: ignore
    per_zip_limit = run_cfg.get("per_zip_limit")
    seeds = cfg.get("seeds", {}) or {}

    stats = {
        "areas": [],
        "total_urls": 0,
    }

    all_urls: Set[str] = set()

    for area in (cfg.get("areas") or []):
        city = (area.get("city") or "").strip()
        state = (area.get("state") or "").strip()
        zips = area.get("zips") or []
        area_entry = {"city": city, "state": state, "zips": []}

        for z in zips:
            z = str(z).strip()
            urls = _collect_for_zip(fc, z, seeds, sleep_range, per_zip_limit)
            area_entry["zips"].append({"zip": z, "count": len(urls)})
            all_urls |= urls

        stats["areas"].append(area_entry)

    stats["total_urls"] = len(all_urls)

    # Write diagnostic counts file
    try:
        write_json(batch_root / "search_links.json", stats)
        log.info(f"[search] total collected urls={stats['total_urls']}")
    except Exception as e:
        log.warning(f"[search] failed to write search_links.json: {e}")

    # Return deduped list
    return sorted(all_urls)
