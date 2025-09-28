# src/fetch.py
# Purpose: Fetch search/detail pages and persist raw HTML + minimal metadata to the batch folders.
from __future__ import annotations

import json
import random
import time
import requests
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from src.settings import (
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

def _find_latest_batch_id() -> Optional[str]:
    root = _batches_root()
    if not root.exists():
        return None
    dirs = [p for p in root.iterdir() if p.is_dir()]
    if not dirs:
        return None
    latest = max(dirs, key=lambda p: p.stat().st_mtime)
    return latest.name

def _resolve_dirs(batch_id: Optional[str]) -> Dict[str, Path]:
    if batch_id is None:
        batch_id = _find_latest_batch_id()
        if not batch_id:
            raise RuntimeError("No batches found. Run src/batch.py to create one.")
    return make_batch_dirs(batch_id)

def _seeds_path(struct_dir: Path) -> Path:
    return struct_dir / "seed_search_pages.json"

def _detect_source_id(row: Dict[str, str]) -> str:
    """Return 'zillow' | 'redfin' | 'unknown' based on explicit source_id or URL."""
    p = (row.get("source_id") or "").lower()
    if p in ("zillow", "redfin"):
        return p
    u = row.get("url", "")
    if "zillow.com" in u:
        return "zillow"
    if "redfin.com" in u:
        return "redfin"
    return "unknown"

def _balanced_mix(rows: List[Dict[str, str]], limit: int) -> List[Dict[str, str]]:
    z = [r for r in rows if _detect_source_id(r) == "zillow"]
    r = [r for r in rows if _detect_source_id(r) == "redfin"]
    o = [r for r in rows if _detect_source_id(r) == "unknown"]

    random.shuffle(z)
    random.shuffle(r)
    random.shuffle(o)

    take_z = min(max(limit // 2, 1), len(z))
    take_r = min(limit - take_z, len(r))

    mixed: List[Dict[str, str]] = []
    for i in range(max(take_z, take_r)):
        if i < take_z:
            mixed.append(z[i])
        if len(mixed) >= limit: 
            break
        if i < take_r: 
            mixed.append(r[i])
        if len(mixed) >= limit: 
            break

    remaining = limit - len(mixed)
    if remaining > 0:
        pool = o + z[take_z:] + r[take_r:]
        mixed.extend(pool[:remaining])

    return mixed[:limit]

# ============================ data classes ============================

@dataclass
class FetchResult:
    status: int
    final_url: str
    html_file: str
    meta_file: str
    resp_file: str

# ============================ core fetching ============================

def _infer_source_id(url: str) -> str:
    host = urlparse(url).hostname or ""
    if "zillow.com" in host:
        return "zillow"
    if "redfin.com" in host:
        return "redfin"
    return "unknown"

def _should_retry(status: int) -> bool:
    return status in (429,) or (500 <= status <= 599)

def fetch_and_save(
    idx: int,
    url: str,
    raw_dir: Path,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT_SEC,
    max_retries: int = 2,
    seed_kind: str = "search_or_detail",
    batch_id: Optional[str] = None,
) -> FetchResult:
    raw_dir.mkdir(parents=True, exist_ok=True)
    headers = headers or default_headers()

    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= max_retries:
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            status = r.status_code

            html_path = raw_dir / f"{idx:04d}_raw.html"
            meta_path = raw_dir / f"{idx:04d}_meta.json"
            resp_path = raw_dir / f"{idx:04d}_response.json"

            html_path.write_text(r.text or "", encoding="utf-8", errors="ignore")

            resp = {"status": status, "final_url": r.url, "headers": dict(r.headers)}
            resp_path.write_text(json.dumps(resp, indent=2), encoding="utf-8")

            source_id = _infer_source_id(r.url or url)
            meta = {
                "batch_id": batch_id,
                "requested_url": url,
                "final_url": r.url,
                "status": status,
                "scraped_timestamp": now_utc_iso(),
                "source_id": source_id,
                "crawl_method": CFG.get("crawl_method", "firecrawl_v1"),
                "seed_kind": seed_kind,
                "idx": idx,
            }
            meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

            return FetchResult(status, r.url, str(html_path), str(meta_path), str(resp_path))

        except Exception as e:
            last_exc = e
            status = 0

        if attempt < max_retries and (status == 0 or _should_retry(status)):
            backoff = 1.5 ** attempt + random.uniform(0.0, 0.5)
            time.sleep(backoff)
            attempt += 1
            continue
        break

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch {url}")

def polite_sleep():
    lo, hi = SLEEP_RANGE_SEC
    time.sleep(random.uniform(lo, hi))

# ============================ public entrypoints ============================

def fetch_first_search_page(batch_id: Optional[str] = None) -> FetchResult:
    dirs = _resolve_dirs(batch_id)
    struct_dir, raw_dir = dirs["structured"], dirs["raw"]

    seeds = _seeds_path(struct_dir)
    if not seeds.exists():
        raise FileNotFoundError(f"Seeds file not found at {seeds}. Run src/batch.py first.")

    payload = json.loads(seeds.read_text(encoding="utf-8"))
    search_pages: List[Dict[str, str]] = payload.get("search_pages", [])
    if not search_pages:
        raise RuntimeError("No search pages in seeds. Check your config areas/zips.")

    first = next((r for r in search_pages if "zillow.com" in r.get("url", "")), None) or search_pages[0]
    url = first["url"]

    res = fetch_and_save(1, url, raw_dir, seed_kind="search", batch_id=payload.get("batch_id"))
    return res

def fetch_search_pages(batch_id: Optional[str] = None, limit: int = 10) -> List[FetchResult]:
    dirs = _resolve_dirs(batch_id)
    struct_dir, raw_dir = dirs["structured"], dirs["raw"]

    seeds = _seeds_path(struct_dir)
    if not seeds.exists():
        raise FileNotFoundError(f"Seeds file not found at {seeds}. Run src/batch.py first.")

    payload = json.loads(seeds.read_text(encoding="utf-8"))
    search_pages: List[Dict[str, str]] = payload.get("search_pages", [])
    if not search_pages:
        raise RuntimeError("No search pages in seeds. Check your config areas/zips.")

    mixed = _balanced_mix(search_pages, limit)

    results: List[FetchResult] = []
    for i, row in enumerate(mixed, start=1):
        url = row["url"]
        try:
            res = fetch_and_save(i, url, raw_dir, seed_kind="search", batch_id=payload.get("batch_id"))
            results.append(res)
            print(f"[{i}/{limit}] {res.status} -> {url}")
        except Exception as e:
            print(f"[{i}/{limit}] ERROR {type(e).__name__}: {e}")
        polite_sleep()
    return results

def fetch_detail_pages(urls: List[str], batch_id: Optional[str] = None, start_idx: int = 1001) -> List[FetchResult]:
    dirs = _resolve_dirs(batch_id)
    raw_dir = dirs["raw"]

    results: List[FetchResult] = []
    for i, url in enumerate(urls, start=0):
        idx = start_idx + i
        try:
            res = fetch_and_save(idx, url, raw_dir, seed_kind="detail", batch_id=batch_id)
            results.append(res)
            print(f"[{i+1}] {res.status} -> {url}")
        except Exception as e:
            print(f"[{i+1}] ERROR {type(e).__name__}: {e}")
        polite_sleep()
    return results

# ============================ CLI ============================

if __name__ == "__main__":
    out = fetch_first_search_page()
    print("✅ Saved:", out)
