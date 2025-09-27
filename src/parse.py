# parse.py
# Unified parsing utilities:
#  - parse HTML detail pages saved under data/batches/<id>/raw/detail/*.html
#  - adapt parsed records into per-table JSONL under structured/
#  - optional Firecrawl extraction into the same per-table outputs

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# ==== project settings (single source of truth) ====
from settings import (
    CFG,
    PROJECT_ROOT,
    REQUEST_TIMEOUT_SEC,
    SLEEP_RANGE_SEC,
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

def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

def _read_json(path: Path, default=None):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default

def _write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def _write_jsonl(path: Path, rows: Iterable[Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def _unique(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _structured_dir(batch_dir: Path) -> Path:
    return batch_dir / "structured"

def _parsed_dir(batch_dir: Path) -> Path:
    return _structured_dir(batch_dir) / "parsed"

def _raw_detail_dir(batch_dir: Path) -> Path:
    return batch_dir / "raw" / "detail"

# ============================ lightweight HTML parsing ============================

# We rely on robust regex + JSON-LD sniffing to avoid extra dependencies.
JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
META_RE = re.compile(
    r'<meta\s+(?:property|name)=["\']([^"\']+)["\']\s+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
TITLE_RE = re.compile(r'<title[^>]*>(.*?)</title>', re.IGNORECASE | re.DOTALL)
OG_URL_KEYS = {"og:url", "twitter:url"}
OG_IMG_KEYS = {"og:image", "twitter:image"}
PRICE_HINT_RE = re.compile(r'["\']price["\']\s*:\s*["\']?([\d,\.]+)', re.IGNORECASE)
BEDS_HINT_RE = re.compile(r'["\'](beds|bedrooms)["\']\s*:\s*["\']?(\d+)', re.IGNORECASE)
BATHS_HINT_RE = re.compile(r'["\'](baths|bathrooms)["\']\s*:\s*["\']?([\d\.]+)', re.IGNORECASE)
AREA_HINT_RE = re.compile(r'["\'](area|floorSize|livingArea|sqft)["\']\s*:\s*["\']?([\d,\.]+)', re.IGNORECASE)
ADDRESS_HINT_RE = re.compile(r'["\'](streetAddress|address|fullAddress)["\']\s*:\s*["\']([^"\']+)', re.IGNORECASE)
ZIP_HINT_RE = re.compile(r'\b(\d{5})(?:-\d{4})?\b')
LAT_RE = re.compile(r'"latitude"\s*:\s*([\-0-9\.]+)')
LON_RE = re.compile(r'"longitude"\s*:\s*([\-0-9\.]+)')

def _parse_json_ld_blocks(html: str) -> List[Dict]:
    blocks = []
    for m in JSON_LD_RE.finditer(html or ""):
        raw = (m.group(1) or "").strip()
        # Handle multiple JSON objects or arrays
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                blocks.extend([x for x in data if isinstance(x, dict)])
            elif isinstance(data, dict):
                blocks.append(data)
        except Exception:
            # Sometimes there are stray "</script>" or invalid JSON—best effort cleanup:
            cleaned = raw.replace("\n", " ").replace("\r", " ").strip()
            cleaned = cleaned.split("</script>")[0]
            try:
                data = json.loads(cleaned)
                if isinstance(data, list):
                    blocks.extend([x for x in data if isinstance(x, dict)])
                elif isinstance(data, dict):
                    blocks.append(data)
            except Exception:
                continue
    return blocks

def _parse_meta_tags(html: str) -> Dict[str, str]:
    metas = {}
    for m in META_RE.finditer(html or ""):
        k, v = m.group(1).strip(), m.group(2).strip()
        metas[k] = v
    return metas

def _guess_source(url: str) -> str:
    if "zillow.com" in (url or ""):
        return "zillow"
    if "redfin.com" in (url or ""):
        return "redfin"
    if "realtor.com" in (url or ""):
        return "realtor"
    return "unknown"

def _coerce_num(x: Optional[str]) -> Optional[float]:
    if not x:
        return None
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def _split_city_state_zip(addr: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not addr:
        return None, None, None
    # naive split by comma, then try to detect zip
    parts = [p.strip() for p in addr.split(",")]
    city = state = zipc = None
    if len(parts) >= 2:
        city = parts[-2]
        tail = parts[-1]
        # state can be two-letter or word; zip as 5 digits
        m = ZIP_HINT_RE.search(tail)
        if m:
            zipc = m.group(1)
            state = tail.replace(zipc, "").strip().strip(",")
        else:
            state = tail
    return city or None, state or None, zipc or None

def parse_one_detail(html: str, url_hint: Optional[str] = None) -> Dict:
    """
    Best-effort parser extracting common fields from listing detail HTML.
    Returns a flat dict with lightweight normalized fields.
    """
    metas = _parse_meta_tags(html)
    title = None
    tm = TITLE_RE.search(html)
    if tm:
        title = re.sub(r'\s+', ' ', tm.group(1)).strip()

    # Prefer canonical URL from meta og:url / twitter:url
    url = None
    for k in OG_URL_KEYS:
        if metas.get(k):
            url = metas[k]
            break
    url = url or url_hint

    # Try JSON-LD for rich data
    jsonld = _parse_json_ld_blocks(html)
    data = {
        "url": url,
        "title": title,
        "source": _guess_source(url or ""),
        "address": None,
        "price": None,
        "beds": None,
        "baths": None,
        "area_sqft": None,
        "images": [],           # list[str]
        "description": None,
        "agent_name": None,
        "agent_phone": None,
        "lat": None,
        "lon": None,
        "city": None,
        "state": None,
        "zip": None,
        "raw_meta": {},
        "raw_jsonld": jsonld,
    }

    # Pull common fields from JSON-LD (Offer, Place, RealEstateListing variants)
    def _dig(obj: Dict, keys: List[str]) -> Optional[str]:
        for k in keys:
            v = obj.get(k)
            if isinstance(v, (str, int, float)):
                return str(v)
            if isinstance(v, dict):
                # common nesting
                for kk in ["value", "name", "text", "price"]:
                    if kk in v and isinstance(v[kk], (str, int, float)):
                        return str(v[kk])
        return None

    # Collect images from JSON-LD (image: str | [str] | {url:...} | [{url:...}])
    def _collect_images_from(obj) -> List[str]:
        out = []
        img = obj.get("image")
        if isinstance(img, str):
            out.append(img)
        elif isinstance(img, list):
            for it in img:
                if isinstance(it, str):
                    out.append(it)
                elif isinstance(it, dict):
                    u = it.get("url")
                    if isinstance(u, str):
                        out.append(u)
        elif isinstance(img, dict):
            u = img.get("url")
            if isinstance(u, str):
                out.append(u)
        return out

    # Merge info from JSON-LD blocks (keep first non-empty)
    for block in jsonld:
        if not data["address"]:
            # Address can be string or dict (PostalAddress)
            addr = block.get("address")
            if isinstance(addr, str):
                data["address"] = addr
            elif isinstance(addr, dict):
                # Combine street + locality + region + postalCode
                parts = [
                    addr.get("streetAddress") or "",
                    addr.get("addressLocality") or "",
                    addr.get("addressRegion") or "",
                    addr.get("postalCode") or "",
                ]
                cand = ", ".join([p for p in parts if p]).strip(", ").strip()
                data["address"] = cand or None

        if data["price"] is None:
            v = _dig(block, ["price", "priceSpecification", "offers"])
            if v:
                data["price"] = _coerce_num(v)

        if data["beds"] is None:
            v = _dig(block, ["numberOfRooms", "bedroomCount", "bedrooms"])
            if v:
                data["beds"] = _coerce_num(v)

        if data["baths"] is None:
            v = _dig(block, ["numberOfBathroomsTotal", "bathroomCount", "bathrooms"])
            if v:
                data["baths"] = _coerce_num(v)

        if data["area_sqft"] is None:
            # Often appears as floorSize -> { value, unitText }
            fs = block.get("floorSize")
            if isinstance(fs, dict):
                v = fs.get("value") or fs.get("valueReference")
                if v is None and isinstance(fs.get("value"), dict):
                    v = fs["value"].get("value")
                data["area_sqft"] = _coerce_num(v)
            else:
                v = _dig(block, ["floorSize", "area", "livingArea"])
                if v:
                    data["area_sqft"] = _coerce_num(v)

        if not data["images"]:
            data["images"] = _collect_images_from(block)

        if data["description"] is None:
            v = _dig(block, ["description"])
            if v:
                data["description"] = v

        if data["agent_name"] is None:
            ag = block.get("seller") or block.get("agent")
            if isinstance(ag, dict):
                nm = ag.get("name")
                if isinstance(nm, str):
                    data["agent_name"] = nm

        if data["lat"] is None or data["lon"] is None:
            geo = block.get("geo")
            if isinstance(geo, dict):
                lat = _coerce_num(geo.get("latitude"))
                lon = _coerce_num(geo.get("longitude"))
                data["lat"] = data["lat"] or lat
                data["lon"] = data["lon"] or lon

    # Meta tags (og:image etc.)
    if not data["images"]:
        for k in OG_IMG_KEYS:
            if metas.get(k):
                data["images"].append(metas[k])

    # Generic regex hints across page JSON blobs
    if data["price"] is None:
        m = PRICE_HINT_RE.search(html)
        if m:
            data["price"] = _coerce_num(m.group(1))

    if data["beds"] is None:
        m = BEDS_HINT_RE.search(html)
        if m:
            data["beds"] = _coerce_num(m.group(2))

    if data["baths"] is None:
        m = BATHS_HINT_RE.search(html)
        if m:
            data["baths"] = _coerce_num(m.group(2))

    if data["area_sqft"] is None:
        m = AREA_HINT_RE.search(html)
        if m:
            data["area_sqft"] = _coerce_num(m.group(2))

    if not data["address"]:
        m = ADDRESS_HINT_RE.search(html)
        if m:
            data["address"] = m.group(2).strip()

    if data["lat"] is None:
        m = LAT_RE.search(html)
        if m:
            data["lat"] = _coerce_num(m.group(1))
    if data["lon"] is None:
        m = LON_RE.search(html)
        if m:
            data["lon"] = _coerce_num(m.group(1))

    # Derive city/state/zip from address if possible
    if data["address"]:
        city, state, zipc = _split_city_state_zip(data["address"])
        data["city"] = city
        data["state"] = state
        data["zip"] = zipc

    data["raw_meta"] = metas
    return data

# ============================ parse all & adapt ============================

@dataclass
class ParseSummary:
    total_files: int
    parsed_ok: int
    saved_individual: int

def _iter_raw_detail_files(batch_dir: Path) -> List[Path]:
    folder = _raw_detail_dir(batch_dir)
    if not folder.exists():
        return []
    return sorted(folder.glob("*_raw.html"))

def parse_all_details(limit: Optional[int] = None, save_individual: bool = True) -> Tuple[Path, ParseSummary, List[Dict]]:
    """
    Iterate raw/detail/*.html, parse each, write parsed individual JSON (optional),
    and return list of parsed dicts.
    """
    batch_dir = _latest_batch_dir()
    if not batch_dir:
        raise FileNotFoundError("No batch found. Run crawl first.")
    files = _iter_raw_detail_files(batch_dir)
    if limit:
        files = files[:limit]
    out_dir = _parsed_dir(batch_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parsed: List[Dict] = []
    saved_individual = 0
    for fp in files:
        html = _read_text(fp)
        # try infer URL from a neighbour mapping if exists later; for now, None
        rec = parse_one_detail(html, url_hint=None)
        parsed.append(rec)

        if save_individual:
            out = out_dir / fp.name.replace("_raw.html", "_parsed.json")
            _write_json(out, rec)
            saved_individual += 1

    # Also write an aggregate JSON (useful for QA)
    agg_path = out_dir / "details_parsed.json"
    _write_json(agg_path, {"count": len(parsed), "items": parsed})

    return batch_dir, ParseSummary(total_files=len(files), parsed_ok=len(parsed), saved_individual=saved_individual), parsed

# ---------- ADAPTED rows (tables) ----------

def _norm_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _infer_property_id(url: Optional[str], idx: int) -> str:
    # Stable local id for joining across tables
    if url:
        # e.g., extract zpid if present, else hash-like tail
        m = re.search(r'/(?:homedetails|home)/[^/]+/(\d+)_zpid', url)
        if m:
            return f"zpid:{m.group(1)}"
        m2 = re.search(r'/home/(\d+)', url)
        if m2:
            return f"rfid:{m2.group(1)}"
        # fallback:
        tail = re.sub(r'[^a-zA-Z0-9]', '', url)[-14:]
        return f"url:{tail}"
    return f"loc:{1000+idx}"

def to_adapted_rows(parsed: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Map parsed detail dicts into per-table row lists.
    Tables:
      - listings, properties, media, agents, price_history, locations, engagement, similar_properties
    """
    listings, properties, media, agents, price_history, locations, engagement, similar_properties = ([] for _ in range(8))

    now = now_utc_iso()
    for i, rec in enumerate(parsed):
        pid = _infer_property_id(rec.get("url"), i)
        src = rec.get("source") or "unknown"
        listings.append({
            "property_id": pid,
            "url": rec.get("url"),
            "source": src,
            "title": rec.get("title"),
            "address": rec.get("address"),
            "price": _norm_float(rec.get("price")),
            "beds": _norm_float(rec.get("beds")),
            "baths": _norm_float(rec.get("baths")),
            "area_sqft": _norm_float(rec.get("area_sqft")),
            "status": None,
            "listed_at": None,
            "updated_at": now,
        })

        properties.append({
            "property_id": pid,
            "type": None,
            "year_built": None,
            "lot_size_sqft": None,
            "parking": None,
            "heating": None,
            "cooling": None,
        })

        # Media rows from images
        for img in rec.get("images") or []:
            media.append({
                "property_id": pid,
                "media_type": "image",
                "url": img,
                "caption": None,
                "position": None,
            })

        # Agent (if any)
        if rec.get("agent_name") or rec.get("agent_phone"):
            agents.append({
                "property_id": pid,
                "name": rec.get("agent_name"),
                "phone": rec.get("agent_phone"),
                "brokerage": None,
            })

        # Price history (unknown at this stage; keep empty)
        # Keep structure in case future enrichment fills it
        # price_history.append({...})

        # Location row
        locations.append({
            "property_id": pid,
            "lat": _norm_float(rec.get("lat")),
            "lon": _norm_float(rec.get("lon")),
            "city": rec.get("city"),
            "state": rec.get("state"),
            "zip": rec.get("zip"),
        })

        # Engagement placeholder
        engagement.append({
            "property_id": pid,
            "views": None,
            "saves": None,
            "last_seen_at": now,
        })

        # Similar properties placeholder
        # similar_properties.append({...})

    return {
        "listings": listings,
        "properties": properties,
        "media": media,
        "agents": agents,
        "price_history": price_history,
        "locations": locations,
        "engagement": engagement,
        "similar_properties": similar_properties,
    }

def _write_tables(batch_dir: Path, tables: Dict[str, List[Dict]]):
    sdir = _structured_dir(batch_dir)
    # One JSONL per table
    for name, rows in tables.items():
        _write_jsonl(sdir / f"{name}.jsonl", rows)
    # Also a compact JSON for quick inspection
    _write_json(sdir / "tables_compact.json", {k: len(v) for k, v in tables.items()})

def parse_details_and_adapt(limit: Optional[int] = None) -> Tuple[Path, ParseSummary]:
    """
    One shot: parse all raw detail HTML, then adapt into per-table JSONL files.
    """
    batch_dir, summary, parsed = parse_all_details(limit=limit, save_individual=True)
    tables = to_adapted_rows(parsed)
    _write_tables(batch_dir, tables)
    return batch_dir, summary

# ============================ Firecrawl extraction (optional path) ============================

def _firecrawl_api_base() -> str:
    return CFG.get("firecrawl", {}).get("api_base", "https://api.firecrawl.dev")

def _firecrawl_api_key() -> Optional[str]:
    # Prefer ENV if set; else from CFG.firecrawl.api_key
    return os.getenv("FIRECRAWL_API_KEY") or CFG.get("firecrawl", {}).get("api_key")

def _detail_urls_from_batch(batch_dir: Path) -> List[str]:
    j = _read_json(batch_dir / "detail_urls.json", default={}) or {}
    urls = j.get("urls") or []
    return [u for u in urls if isinstance(u, str)]

def firecrawl_extract(limit: Optional[int] = None, delay_sec: Optional[float] = None) -> Tuple[Path, Dict[str, int]]:
    """
    Extract final tables directly via Firecrawl (when available).
    Writes the same per-table JSONL files in structured/.
    """
    batch_dir = _latest_batch_dir()
    if not batch_dir:
        raise FileNotFoundError("No batch found. Run crawl first.")
    urls = _detail_urls_from_batch(batch_dir)
    if not urls:
        raise FileNotFoundError("detail_urls.json missing or empty. Run extract-urls first.")
    if limit:
        urls = urls[:limit]

    api_base = _firecrawl_api_base().rstrip("/")
    api_key = _firecrawl_api_key()
    if not api_key:
        raise RuntimeError("Firecrawl API key is missing. Set FIRECRAWL_API_KEY env or CFG.firecrawl.api_key.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Accumulators for table rows
    acc = {
        "listings": [],
        "properties": [],
        "media": [],
        "agents": [],
        "price_history": [],
        "locations": [],
        "engagement": [],
        "similar_properties": [],
    }

    # Minimal schema expectation (we'll normalize defensively)
    def _ingest(payload: Dict, url: str, idx: int):
        pid = _infer_property_id(url, idx)

        # listings
        acc["listings"].append({
            "property_id": pid,
            "url": url,
            "source": _guess_source(url),
            "title": payload.get("title"),
            "address": payload.get("address"),
            "price": _norm_float(payload.get("price")),
            "beds": _norm_float(payload.get("beds")),
            "baths": _norm_float(payload.get("baths")),
            "area_sqft": _norm_float(payload.get("area_sqft")),
            "status": payload.get("status"),
            "listed_at": payload.get("listed_at"),
            "updated_at": now_utc_iso(),
        })

        # properties
        acc["properties"].append({
            "property_id": pid,
            "type": payload.get("property_type"),
            "year_built": payload.get("year_built"),
            "lot_size_sqft": _norm_float(payload.get("lot_size_sqft")),
            "parking": payload.get("parking"),
            "heating": payload.get("heating"),
            "cooling": payload.get("cooling"),
        })

        # media
        for img in payload.get("images") or []:
            acc["media"].append({
                "property_id": pid,
                "media_type": "image",
                "url": img,
                "caption": None,
                "position": None,
            })

        # agents
        ag = payload.get("agent") or {}
        if isinstance(ag, dict) and (ag.get("name") or ag.get("phone")):
            acc["agents"].append({
                "property_id": pid,
                "name": ag.get("name"),
                "phone": ag.get("phone"),
                "brokerage": ag.get("brokerage"),
            })

        # price history
        for ph in payload.get("price_history") or []:
            if isinstance(ph, dict):
                acc["price_history"].append({
                    "property_id": pid,
                    "date": ph.get("date"),
                    "event": ph.get("event"),
                    "price": _norm_float(ph.get("price")),
                })

        # location
        loc = payload.get("location") or {}
        acc["locations"].append({
            "property_id": pid,
            "lat": _norm_float(loc.get("lat")),
            "lon": _norm_float(loc.get("lon")),
            "city": loc.get("city"),
            "state": loc.get("state"),
            "zip": loc.get("zip"),
        })

        # engagement
        eng = payload.get("engagement") or {}
        acc["engagement"].append({
            "property_id": pid,
            "views": _norm_float(eng.get("views")),
            "saves": _norm_float(eng.get("saves")),
            "last_seen_at": now_utc_iso(),
        })

        # similar properties
        for su in payload.get("similar_properties") or []:
            if isinstance(su, str):
                acc["similar_properties"].append({"property_id": pid, "similar_url": su})

    # Actual calls
    session = requests.Session()
    count_ok = count_err = 0
    for i, url in enumerate(urls, start=1):
        body = {
            "url": url,
            # If you have a server-side schema with Firecrawl, you can pass it here.
            # Using default extraction (model-dependent) for now.
        }
        try:
            r = session.post(f"{api_base}/v1/scrape", headers=headers, json=body, timeout=REQUEST_TIMEOUT_SEC)
            r.raise_for_status()
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            # Accept multiple possible keys; normalize to a "payload"
            payload = (
                data.get("json") or data.get("data") or data.get("result") or data
            )
            if not isinstance(payload, dict):
                payload = {}
            _ingest(payload, url, i)
            count_ok += 1
            if delay_sec:
                time.sleep(delay_sec)
        except requests.RequestException as e:
            count_err += 1
            print(f"[firecrawl] ERROR {i}/{len(urls)} → {url} :: {e}")

    # Write per-table files
    _write_tables(batch_dir, acc)
    return batch_dir, {"ok": count_ok, "error": count_err, "total": len(urls)}

# ============================ module smoke test ============================

if __name__ == "__main__":
    bd = _latest_batch_dir()
    if not bd:
        raise SystemExit("No batch found. Run crawl flow first.")
    print(f"[parse] latest batch = {bd}")
    # Quick local test: parse few files and adapt
    _, summary = parse_details_and_adapt(limit=5)
    print(f"[parse] summary: {summary}")
