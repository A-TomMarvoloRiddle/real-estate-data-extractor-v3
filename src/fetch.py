# src/fetch.py
# Purpose: Fetch search/detail pages and persist raw HTML + minimal metadata to the batch folders.
from __future__ import annotations
import os
import json
import random
import time
import requests
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
from dotenv import load_dotenv
from src.settings import (
    CFG,
    PROJECT_ROOT,
    REQUEST_TIMEOUT_SEC,
    SLEEP_RANGE_SEC,
    default_headers,
    make_batch_dirs,
    now_utc_iso,
    latest_batch_dir
)

load_dotenv()
FIRECRAWL_API = "https://api.firecrawl.dev"
FIRECRAWL_KEY = os.getenv("FIRECRAWL_API_KEY", "fc-304a1e6ceefb4b18ae9f8073b9e15059")

UA_POOL = [
    CFG["run"]["user_agent"],
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

def redfin_headers() -> Dict[str, str]:
    ua = random.choice(UA_POOL)
    base = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    # Referer مفيد لتقليل 403
    base["Referer"] = "https://www.redfin.com/"
    return base

def choose_headers_for(url: str) -> Dict[str, str]:
    if "redfin.com" in url:
        h = redfin_headers()
        # دمج أي هيدر افتراضي عندك
        h.update({k:v for k,v in default_headers().items() if k not in h})
        return h
    return default_headers()

def fetch_via_firecrawl(url: str, timeout: int) -> Optional[str]:
    """fetch HTML via Firecrawl API if API key is set and crawl_method is 'firecrawl_v1'."""
    if not FIRECRAWL_KEY or CFG.get("crawl_method") != "firecrawl_v1":
        return None
    try:
        r = requests.post(
            f"{FIRECRAWL_API}/v1/scrape",
            headers={"Authorization": f"Bearer {FIRECRAWL_KEY}", "Content-Type": "application/json"},
            json={"url": url, "formats": ["html"]},
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        html = data.get("html") or data.get("data", {}).get("html") or ""
        return html if isinstance(html, str) and html.strip() else None
    except Exception:
        return None


# ============================ paths & helpers ============================

def _batches_root() -> Path:
    return PROJECT_ROOT / "data" / "batches"

def _find_latest_batch_id() -> Optional[str]:
    root = _batches_root()
    if not root.exists():
        return None
    latest = latest_batch_dir()
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

    headers = headers or choose_headers_for(url)

    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= max_retries:
        html_text = None
        final_url = url
        status = 0
        try:
            #try Firecrawl if configured
            html_text = fetch_via_firecrawl(url, timeout=timeout)

            # fallback to requests if Firecrawl not used or failed
            if not html_text:
                r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
                status = r.status_code
                final_url = r.url
                html_text = r.text or ""

            html_path = raw_dir / f"{idx:04d}_raw.html"
            meta_path = raw_dir / f"{idx:04d}_meta.json"
            resp_path = raw_dir / f"{idx:04d}_response.json"

            #save HTML with utf-8 and ignore errors
            html_path.write_text(html_text, encoding="utf-8", errors="ignore")

            resp = {
                "status": status or (200 if html_text else 0),
                "final_url": final_url,
                "headers": dict(r.headers) if 'r' in locals() else {},
            }
            resp_path.write_text(json.dumps(resp, indent=2), encoding="utf-8")

            source_id = _infer_source_id(final_url or url)
            meta = {
                "batch_id": batch_id,
                "requested_url": url,
                "final_url": final_url,
                "status": resp["status"],
                "scraped_timestamp": now_utc_iso(),
                "source_id": source_id,
                "crawl_method": CFG.get("crawl_method", "requests"),
                "seed_kind": seed_kind,
                "idx": idx,
            }
            meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

            return FetchResult(resp["status"], final_url, str(html_path), str(meta_path), str(resp_path))

        except Exception as e:
            last_exc = e
            status = 0

        if attempt < max_retries and (status == 0 or _should_retry(status)):
            backoff = 1.5 ** attempt + random.uniform(0.0, 0.5)
            time.sleep(backoff)
            headers = choose_headers_for(url)
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

def fetch_search_pages(batch_id: Optional[str] = None, limit: int = 999999) -> List[FetchResult]:
    dirs = _resolve_dirs(batch_id)
    struct_dir, raw_dir = dirs["structured"], dirs["raw"]

    seeds = _seeds_path(struct_dir)
    if not seeds.exists():
        raise FileNotFoundError(f"Seeds file not found at {seeds}. Run src/batch.py first.")

    payload = json.loads(seeds.read_text(encoding="utf-8"))
    search_pages: List[Dict[str, str]] = payload.get("search_pages", [])
    if not search_pages:
        raise RuntimeError("No search pages in seeds. Check your config areas/zips.")

    # ❗️بدون balanced_mix — نجيب الكل حسب ما جاء بالملف
    rows = search_pages[: min(limit, len(search_pages))]

    results: List[FetchResult] = []
    for i, row in enumerate(rows, start=1):
        url = row["url"]
        try:
            res = fetch_and_save(i, url, raw_dir, seed_kind="search", batch_id=payload.get("batch_id"))
            results.append(res)
            print(f"[{i}/{len(rows)}] {res.status} -> {url}")
        except Exception as e:
            print(f"[{i}/{len(rows)}] ERROR {type(e).__name__}: {e}")
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
    # out = fetch_first_search_page()
    out = fetch_search_pages(limit=99999)
    print("✅ Saved:", out)