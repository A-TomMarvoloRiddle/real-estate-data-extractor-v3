# src/extract_search.py
# Robust search extractor for Zillow & Redfin search pages (ZIP/city).
# - Parses Zillow __NEXT_DATA__ JSON for listResults/mapResults.
# - Parses Redfin preloaded state when available; falls back to anchor patterns.
# - Detects CAPTCHA/404 and reports clearly in summary.
# - Writes structured/listing_urls.json with {"urls":[{"source_id","source_url"}], "by_source":{...}}
# - Writes search_extraction_summary.json with counters.

from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict
from src.settings import PROJECT_ROOT
from src.pipeline import latest_batch_dir

BATCHES_ROOT = PROJECT_ROOT / "data" / "batches"

# -------- Helpers -------- 

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def looks_like_zillow(html: str) -> bool:
    return "Zillow" in html or "zillow.com" in html or '"og:site_name" content="Zillow' in html

def looks_like_redfin(html: str) -> bool:
    return "Redfin" in html or "redfin.com" in html

def is_perimeterx_captcha(html: str) -> bool:
    return "px-captcha" in html or "Access to this page has been denied" in html or "PerimeterX" in html

def is_redfin_not_found(html: str) -> bool:
    # Redfin 404 UX (“Oops… lost that one.”)
    return 'NotFoundPage route-NotFoundPage' in html or "Oops… lost that one." in html

def strip_ws(s: Optional[str]) -> Optional[str]:
    if s is None: 
        return None
    s2 = s.strip()
    return s2 if s2 else None

def to_abs(url: str, base: str) -> str:
    if url.startswith("http"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        # Pick domain from base
        if "zillow.com" in base:
            return "https://www.zillow.com" + url
        if "redfin.com" in base:
            return "https://www.redfin.com" + url
    return url

# -------- Zillow parsing -------- 

NEXT_DATA_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(?P<json>[\s\S]+?)</script>',
    re.IGNORECASE
)

def parse_zillow_listings_from_next_data(html: str) -> List[str]:
    """
    Returns absolute detail URLs from Zillow __NEXT_DATA__ JSON.
    """
    m = NEXT_DATA_RE.search(html)
    if not m:
        return []

    try:
        data = json.loads(m.group("json"))
    except Exception:
        return []

    # Known path:
    # data["props"]["pageProps"]["searchPageState"]["cat1"]["searchResults"]["listResults"]
    # or mapResults
    def dig(obj: Any, path: List[str]) -> Any:
        cur = obj
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                return None
            cur = cur[k]
        return cur

    paths = [
        ["props","pageProps","searchPageState","cat1","searchResults","listResults"],
        ["props","pageProps","searchPageState","cat1","searchResults","mapResults"],
    ]

    urls: List[str] = []
    for path in paths:
        arr = dig(data, path)
        if isinstance(arr, list):
            for item in arr:
                # common fields
                detail_url = item.get("detailUrl") or item.get("hdpUrl")
                if detail_url:
                    urls.append(to_abs(detail_url, "https://www.zillow.com"))
                else:
                    # fallback by zpid if present (construct)
                    zpid = item.get("zpid")
                    if zpid:
                        urls.append(f"https://www.zillow.com/homedetails/{zpid}_zpid")
    # Dedup preserve order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# -------- Redfin parsing -------- 

STATE_PATTERNS = [
    re.compile(r'window\.__REDUX_STATE__\s*=\s*(\{[\s\S]+?\});', re.IGNORECASE),
    re.compile(r'window\.__BOOTSTRAP_STATE__\s*=\s*(\{[\s\S]+?\});', re.IGNORECASE),
    re.compile(r'<script[^>]+id="__REDUX_STATE__"[^>]*>([\s\S]+?)</script>', re.IGNORECASE),
]

#any href with /home/{id}
HREF_ANY_HOME_RE = re.compile(r'href="(?P<href>[^"]*/home/\d+[^"]*?)"', re.IGNORECASE)

def parse_redfin_listings(html: str) -> List[str]:
    urls: List[str] = []

    # 1) جرّب JSON state
    for pat in STATE_PATTERNS:
        m = pat.search(html)
        if not m:
            continue
        blob = m.group(1).strip()
        if blob.endswith(";"):
            blob = blob[:-1]
        try:
            state = json.loads(blob)
        except Exception:
            continue

        def collect_urls_from_obj(o: Any) -> None:
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, str) and "/home/" in v:
                        urls.append(to_abs(v, "https://www.redfin.com"))
                    else:
                        collect_urls_from_obj(v)
            elif isinstance(o, list):
                for it in o:
                    collect_urls_from_obj(it)

        collect_urls_from_obj(state)

    # 2) فولباك: regex عام لكل href فيه /home/{id}
    if not urls:
        for m in HREF_ANY_HOME_RE.finditer(html):
            urls.append(to_abs(m.group("href"), "https://www.redfin.com"))

    # Dedup
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# -------- Orchestrate over batch/raw

def detect_source_from_html(html: str, fallback_seed_url: Optional[str]) -> str:
    if looks_like_zillow(html) or (fallback_seed_url and "zillow.com" in fallback_seed_url):
        return "zillow"
    if looks_like_redfin(html) or (fallback_seed_url and "redfin.com" in fallback_seed_url):
        return "redfin"
    return "unknown"

def load_seed_map(batch_dir: Path) -> Dict[str, str]:
    # optional: map 0007 -> original seed url for better source detection
    seed_path = batch_dir / "seed_search_pages.json"
    if not seed_path.exists():
        return {}
    try:
        data = json.loads(seed_path.read_text(encoding="utf-8"))
        # expect {"pages":[{"idx":7,"seed_url":"https://..."}, ...]}
        by_idx = {}
        for row in data.get("pages", []):
            idx = row.get("idx")
            url = row.get("seed_url")
            if isinstance(idx, int) and url:
                by_idx[str(idx).zfill(4)] = url
        return by_idx
    except Exception:
        return {}

def main():
    batch_dir = latest_batch_dir()
    raw_dir = batch_dir / "raw"
    struct_dir = batch_dir / "structured"
    struct_dir.mkdir(parents=True, exist_ok=True)

    seed_by_idx = load_seed_map(batch_dir)

    listing_urls: List[Dict[str, str]] = []
    counters = defaultdict(int)
    per_source = defaultdict(int)
    pages_meta: List[Dict[str, Any]] = []

    # iterate raw 000*_raw.html (search pages)
    for p in sorted(raw_dir.glob("0???_raw.html")):
        idx4 = p.name[:4]
        html = read_text(p)
        seed_url = seed_by_idx.get(idx4)

        # classify
        if is_perimeterx_captcha(html):
            counters["blocked"] += 1
            pages_meta.append({"idx": idx4, "status": "blocked", "seed_url": seed_url})
            continue
        if is_redfin_not_found(html):
            counters["not_found"] += 1
            pages_meta.append({"idx": idx4, "status": "not_found", "seed_url": seed_url})
            continue

        src = detect_source_from_html(html, seed_url)
        if src == "zillow":
            urls = parse_zillow_listings_from_next_data(html)
        elif src == "redfin":
            urls = parse_redfin_listings(html)
        else:
            urls = []

        if urls:
            counters["pages_ok"] += 1
            per_source[src] += 1
            for u in urls:
                listing_urls.append({"source_id": src, "source_url": u})
        else:
            # page loaded but no data parsed
            counters["pages_empty"] += 1
            pages_meta.append({"idx": idx4, "status": "empty", "seed_url": seed_url})

    # deduplicate by URL keeping first source_id
    seen = set()
    deduped: List[Dict[str,str]] = []
    for row in listing_urls:
        u = row["source_url"]
        if u in seen:
            continue
        seen.add(u)
        deduped.append(row)

    # write listing_urls.json in a shape pipeline.load_listing_urls understands
    out_payload = {
        "urls": deduped,
        "by_source": {
            k: sum(1 for r in deduped if r["source_id"] == k)
            for k in sorted(set(r["source_id"] for r in deduped))
        }
    }
    (struct_dir / "listing_urls.json").write_text(
        json.dumps(out_payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # summary
    summary = {
        "total_urls": len(deduped),
        "pages_ok": counters["pages_ok"],
        "pages_empty": counters["pages_empty"],
        "blocked": counters["blocked"],
        "not_found": counters["not_found"],
        "by_source_pages": dict(per_source),
        "pages_meta": pages_meta
    }
    (batch_dir / "search_extraction_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"listing_urls.json: {len(deduped)} (by_source={out_payload['by_source']}), "
          f"pages ok={counters['pages_ok']}, empty={counters['pages_empty']}, "
          f"blocked={counters['blocked']}, not_found={counters['not_found']}")

if __name__ == "__main__":
    main()
