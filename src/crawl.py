# crawl.py
# Unified crawling utilities: init batch, fetch search pages, extract listing URLs, fetch detail pages.
from __future__ import annotations

import json
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import requests

# ==== project settings (single source of truth) ====
from settings import (
    CFG,
    PROJECT_ROOT,
    REQUEST_TIMEOUT_SEC,
    SLEEP_RANGE_SEC,
    default_headers,
    make_batch_dirs,
    now_utc_iso,
)

# ============================ paths & helpers ============================

def _batches_root() -> Path:
    return PROJECT_ROOT / "data" / "batches"

def _latest_batch_dir() -> Optional[Path]:
    root = _batches_root()
    if not root.exists():
        return None
    dirs = [p for p in root.iterdir() if p.is_dir()]
    return max(dirs, key=lambda p: p.stat().st_mtime) if dirs else None

def _slugify_area(area: dict) -> str:
    """
    Create a safe slug for search paths from area info inside CFG.
    Prefers explicit 'slug'; fallback: 'city-statecode' or 'city-state' lowercased and dashed.
    """
    if not isinstance(area, dict):
        return "unknown"
    if "slug" in area and area["slug"]:
        return str(area["slug"]).strip("/").replace(" ", "-")
    city = str(area.get("city", "")).strip().replace(" ", "-")
    stc = str(area.get("state_code") or area.get("state") or "").strip().replace(" ", "-")
    slug = "-".join([s for s in [city, stc] if s]).lower()
    return slug or "unknown"

def _read_or_empty_json(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _unique(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

# ============================ Batch init ============================

def init_batch() -> Path:
    """
    Create a new batch folder under data/batches/<batch_id> with standard subfolders.
    Also materializes seed_search_pages.json using CFG['seeds'] and CFG['areas'] if available.
    """
    batch_id = now_utc_iso().replace(":", "").replace("-", "").replace("T", "")[:14]
    batch_dir = _batches_root() / batch_id
    paths = make_batch_dirs(batch_dir)  # uses settings.py to create raw/structured/qa/logs etc.
    (batch_dir / "meta.json").write_text(json.dumps({"batch_id": batch_id, "created_at": now_utc_iso()}, indent=2), encoding="utf-8")

    # Build seed search pages (best-effort; relies on CFG structure but tolerates partial data)
    seeds_cfg = CFG.get("seeds", {}) or {}
    areas_cfg = CFG.get("areas", []) or []
    seed_urls: List[str] = []

    def add_url(u: str):
        if isinstance(u, str) and u.startswith("http"):
            seed_urls.append(u)

    # If CFG already provides explicit seed urls, honor them
    explicit = seeds_cfg.get("explicit_urls") or []
    for u in explicit:
        add_url(u)

    # Heuristics for Zillow/Redfin based on areas
    # Accept custom templates if provided (e.g., "{base}/<slug>/")
    z = seeds_cfg.get("zillow", {}) or {}
    r = seeds_cfg.get("redfin", {}) or {}
    z_base = z.get("base", "https://www.zillow.com")
    r_base = r.get("base", "https://www.redfin.com")

    # Optional template strings; fallback to simple "/<slug>/" format
    z_tpl = z.get("template") or "{base}/{slug}/"
    r_tpl = r.get("template") or "{base}/{slug}"

    for area in areas_cfg:
        slug = _slugify_area(area)
        # Allow per-area override
        if "zillow_url" in area:
            add_url(str(area["zillow_url"]))
        else:
            add_url(z_tpl.format(base=z_base.rstrip("/"), slug=slug.strip("/")))

        if "redfin_url" in area:
            add_url(str(area["redfin_url"]))
        else:
            add_url(r_tpl.format(base=r_base.rstrip("/"), slug=slug.strip("/")))

    # Deduplicate and persist
    seed_urls = _unique([u.rstrip("/") for u in seed_urls])
    seeds_path = batch_dir / "seed_search_pages.json"
    seeds_path.write_text(json.dumps({"seeds": seed_urls}, indent=2), encoding="utf-8")

    print(f"[init-batch] Created {batch_dir} with {len(seed_urls)} seed search URLs")
    return batch_dir

# ============================ HTTP helpers ============================

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(default_headers())
    return s

def _save_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="ignore")

def _sleep():
    low, high = SLEEP_RANGE_SEC
    time.sleep(random.uniform(low, high))

# ============================ Fetch search pages ============================

def _candidate_search_seeds(batch_dir: Path) -> List[str]:
    seeds = []
    # Prefer batch-local seeds
    batch_seeds = _read_or_empty_json(batch_dir / "seed_search_pages.json")
    if "seeds" in batch_seeds and isinstance(batch_seeds["seeds"], list):
        seeds.extend([str(u) for u in batch_seeds["seeds"] if isinstance(u, str)])

    # Fallback to CFG (in case batch was created manually)
    if not seeds:
        seeds_cfg = CFG.get("seeds", {}) or {}
        explicit = seeds_cfg.get("explicit_urls") or []
        seeds.extend([u for u in explicit if isinstance(u, str)])
    return _unique(seeds)

def fetch_search_pages(limit: Optional[int] = None, overwrite: bool = False) -> Tuple[Path, int]:
    """
    Fetch seed search pages and persist HTML under raw/search/*.html
    """
    batch_dir = _latest_batch_dir() or init_batch()
    raw_dir = (batch_dir / "raw" / "search")
    raw_dir.mkdir(parents=True, exist_ok=True)

    seeds = _candidate_search_seeds(batch_dir)
    if limit:
        seeds = seeds[:limit]

    s = _session()
    saved = 0
    for idx, url in enumerate(seeds, start=1):
        out = raw_dir / f"{idx:04d}_search.html"
        if out.exists() and not overwrite:
            print(f"[fetch-search] Skip existing: {out.name}")
            continue

        try:
            r = s.get(url, timeout=REQUEST_TIMEOUT_SEC)
            r.raise_for_status()
            _save_text(out, r.text)
            saved += 1
            print(f"[fetch-search] OK {idx}/{len(seeds)} → {url}")
        except requests.RequestException as e:
            print(f"[fetch-search] ERROR {idx}/{len(seeds)} → {url} :: {e}")
        _sleep()

    return batch_dir, saved

# ============================ Extract listing URLs from search HTML ============================

NEXT_DATA_RE = re.compile(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL | re.IGNORECASE)
URL_RE = re.compile(r'https?://[a-z0-9\.-]+/(?:homedetails|home|house|apartment|condo)[^"\s<>]*', re.IGNORECASE)

def _extract_from_next_data(html: str) -> List[str]:
    """
    Generic extractor for frameworks embedding JSON state (Next.js etc.).
    Tries to parse any property-like URLs from the JSON dump if present.
    """
    m = NEXT_DATA_RE.search(html or "")
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
    except Exception:
        return []
    urls: List[str] = []

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
        elif isinstance(obj, str):
            if URL_RE.match(obj):
                urls.append(obj)

    walk(data)
    return urls

def _extract_urls_from_html(html: str) -> List[str]:
    if not html:
        return []
    urls = []

    # 1) Next.js JSON state (often richest)
    urls.extend(_extract_from_next_data(html))

    # 2) Regex fallback over anchors / inline JSON
    urls.extend(URL_RE.findall(html))

    # 3) Normalize, filter for Zillow/Redfin/major portals, deduplicate
    cleaned = []
    for u in urls:
        u = u.split("&")[0].rstrip("/\"'")
        if any(host in u for host in ["zillow.com", "redfin.com", "realtor.com"]):
            cleaned.append(u)
    return _unique(cleaned)

def extract_listing_urls() -> Tuple[Path, int]:
    """
    Read all raw/search/*.html, extract listing URLs, and write unique list to detail_urls.json
    """
    batch_dir = _latest_batch_dir()
    if not batch_dir:
        raise FileNotFoundError("No batch found. Run init_batch or fetch_search_pages first.")
    search_dir = batch_dir / "raw" / "search"
    if not search_dir.exists():
        raise FileNotFoundError(f"Search folder not found: {search_dir}")

    urls: List[str] = []
    files = sorted(search_dir.glob("*.html"))
    for fp in files:
        try:
            html = fp.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            html = ""
        urls.extend(_extract_urls_from_html(html))

    urls = _unique(urls)
    out = batch_dir / "detail_urls.json"
    out.write_text(json.dumps({"urls": urls, "count": len(urls)}, indent=2), encoding="utf-8")
    print(f"[extract-urls] Found {len(urls)} URLs from {len(files)} search pages")
    return batch_dir, len(urls)

# ============================ Fetch detail pages ============================

@dataclass
class DetailFetchResult:
    total: int
    saved: int
    skipped: int
    errors: int

def _iter_detail_urls(batch_dir: Path) -> List[str]:
    detail_file = batch_dir / "detail_urls.json"
    data = _read_or_empty_json(detail_file)
    urls = data.get("urls") or []
    return [u for u in urls if isinstance(u, str)]

def fetch_detail_pages(n: Optional[int] = None, overwrite: bool = False) -> Tuple[Path, DetailFetchResult]:
    """
    Fetch detail pages listed in detail_urls.json and save under raw/detail/ as 1001_raw.html, 1002_raw.html, ...
    """
    batch_dir = _latest_batch_dir()
    if not batch_dir:
        raise FileNotFoundError("No batch found. Run init_batch + fetch_search + extract-urls first.")
    raw_detail = batch_dir / "raw" / "detail"
    raw_detail.mkdir(parents=True, exist_ok=True)

    urls = _iter_detail_urls(batch_dir)
    if not urls:
        raise FileNotFoundError("detail_urls.json is missing or has no URLs. Run extract_listing_urls first.")
    if n:
        urls = urls[:n]

    s = _session()
    saved = skipped = errors = 0
    for i, url in enumerate(urls, start=1):
        out = raw_detail / f"{1000 + i:04d}_raw.html"
        if out.exists() and not overwrite:
            skipped += 1
            continue
        try:
            r = s.get(url, timeout=REQUEST_TIMEOUT_SEC)
            r.raise_for_status()
            _save_text(out, r.text)
            saved += 1
            if i % 10 == 0:
                print(f"[fetch-details] {i}/{len(urls)} saved so far…")
        except requests.RequestException as e:
            errors += 1
            print(f"[fetch-details] ERROR {i}/{len(urls)} → {url} :: {e}")
        _sleep()

    print(f"[fetch-details] done. total={len(urls)} saved={saved} skipped={skipped} errors={errors}")
    return batch_dir, DetailFetchResult(total=len(urls), saved=saved, skipped=skipped, errors=errors)

# ============================ Convenience combined runs ============================

def run_full_search(limit: Optional[int] = None, overwrite: bool = False) -> Path:
    """
    Convenience: init (if needed) + fetch search pages + extract URLs.
    """
    if not _latest_batch_dir():
        init_batch()
    batch_dir, _ = fetch_search_pages(limit=limit, overwrite=overwrite)
    extract_listing_urls()
    return batch_dir


if __name__ == "__main__":
    # This file is meant to be imported by pipeline.py; direct run won't expose CLI.
    bd = _latest_batch_dir() or init_batch()
    print(f"Latest batch: {bd}")
