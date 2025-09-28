# src/parse_detail.py
# Purpose: Convert saved detail pages (1001_raw.html, 1002_...)
# into structured JSON using multi-strategy parsing (Redfin/Zillow/schema.org/regex).
# Updated for new schema: source_id (not platform_id), add crawl_method, engagement, similar, financials, etc.

from __future__ import annotations
import json
import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from src.settings import make_batch_dirs, now_utc_iso, CFG
from src.settings import PROJECT_ROOT

# ---------------------------- helpers ----------------------------
def _latest_batch() -> str:
    root = PROJECT_ROOT / "data" / "batches"
    latest = max((p for p in root.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, default=None)
    if latest is None:
        raise RuntimeError("No batches found. Run src/batch.py first.")
    return latest.name

def _resolve_dirs(batch_id: Optional[str]) -> Dict[str, Path]:
    return make_batch_dirs(batch_id) if batch_id else make_batch_dirs(_latest_batch())

def safe_float(x) -> Optional[float]:
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = re.sub(r"[^\d\.]", "", str(x))
    try: return float(s) if s else None
    except: return None

def to_int(x) -> Optional[int]:
    v = safe_float(x)
    return int(v) if v is not None else None

def _read_html_meta(raw_dir: Path, idx: int) -> tuple[str, Dict[str, Any]]:
    html = (raw_dir / f"{idx:04d}_raw.html").read_text(encoding="utf-8", errors="ignore")
    meta = json.loads((raw_dir / f"{idx:04d}_meta.json").read_text(encoding="utf-8"))
    return html, meta

# ------------------------- site parsers --------------------------
# (نفس المنطق القديم مع تحديث platform_id → source_id)
def parse_redfin(soup: BeautifulSoup, html_text: str) -> Dict[str, Any]:
    out = {
        "source_id": "redfin",
        "external_property_id": None,
        "address": {"street": None, "unit": None, "city": None, "state": None, "postal_code": None},
        "list_price": None,
        "beds": None,
        "baths": None,
        "interior_area_sqft": None,
        "year_built": None,
        "photos": [],
    }
    nxt = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if nxt and nxt.string:
        try:
            data = json.loads(nxt.string)
        except: data = {}
        def walk(n):
            if isinstance(n, dict):
                if "propertyId" in n and str(n["propertyId"]).isdigit():
                    out["external_property_id"] = str(n["propertyId"])
                if "streetLine" in n or "city" in n:
                    out["address"].update({
                        "street": n.get("streetLine") or out["address"]["street"],
                        "unit": n.get("unitNumber") or n.get("unit") or out["address"]["unit"],
                        "city": n.get("city") or out["address"]["city"],
                        "state": n.get("state") or n.get("stateCode") or out["address"]["state"],
                        "postal_code": str(n.get("zip") or n.get("postalCode") or out["address"]["postal_code"] or "").strip() or None,
                    })
                out["list_price"] = out["list_price"] or safe_float(n.get("price") or n.get("listPrice"))
                out["beds"]  = out["beds"]  or safe_float(n.get("beds"))
                out["baths"] = out["baths"] or safe_float(n.get("baths") or n.get("bathsTotal"))
                if out["interior_area_sqft"] is None:
                    for kk in ("squareFeet","sqFt","livingArea"):
                        if kk in n: out["interior_area_sqft"] = to_int(n[kk]); break
                if "yearBuilt" in n and str(n["yearBuilt"]).isdigit():
                    out["year_built"] = int(n["yearBuilt"])
                if "photos" in n and isinstance(n["photos"], list):
                    for p in n["photos"]:
                        u = p.get("url") if isinstance(p, dict) else None
                        if u and u not in out["photos"]: out["photos"].append(u)
                for v in n.values(): walk(v)
            elif isinstance(n, list):
                for v in n: walk(v)
        walk(data)
    return out

def parse_zillow(soup: BeautifulSoup, html_text: str) -> Dict[str, Any]:
    out = {
        "source_id": "zillow",
        "external_property_id": None,
        "address": {"street": None, "unit": None, "city": None, "state": None, "postal_code": None},
        "list_price": None, "beds": None, "baths": None, "interior_area_sqft": None, "year_built": None, "photos": []
    }
    for sc in soup.find_all("script", attrs={"data-zrr-shared-data-key": True}):
        txt = (sc.string or "").replace("<!--","").replace("-->","").strip()
        if not txt: continue
        try: data = json.loads(txt)
        except: continue
        def walk(n):
            if isinstance(n, dict):
                if "zpid" in n and str(n["zpid"]).isdigit(): out["external_property_id"] = str(n["zpid"])
                if "streetAddress" in n or "city" in n:
                    out["address"].update({
                        "street": n.get("streetAddress") or out["address"]["street"],
                        "unit": n.get("unitNumber") or n.get("unit") or out["address"]["unit"],
                        "city": n.get("city") or out["address"]["city"],
                        "state": n.get("state") or out["address"]["state"],
                        "postal_code": n.get("zipcode") or n.get("postalCode") or out["address"]["postal_code"],
                    })
                out["list_price"] = out["list_price"] or safe_float(n.get("price") or n.get("listPrice"))
                out["beds"]  = out["beds"]  or safe_float(n.get("bedrooms") or n.get("beds"))
                out["baths"] = out["baths"] or safe_float(n.get("bathrooms") or n.get("baths"))
                if out["interior_area_sqft"] is None:
                    for kk in ("livingArea","finishedSqFt"):
                        if kk in n: out["interior_area_sqft"] = to_int(n[kk]); break
                if "yearBuilt" in n and str(n["yearBuilt"]).isdigit():
                    out["year_built"] = int(n["yearBuilt"])
                for k in ("photos","photoGallery"):
                    if isinstance(n.get(k), list):
                        for p in n[k]:
                            if isinstance(p, dict):
                                u = p.get("url") or p.get("rawUrl")
                                if u and u not in out["photos"]: out["photos"].append(u)
                for v in n.values(): walk(v)
            elif isinstance(n, list):
                for v in n: walk(v)
        walk(data)
    return out

# --------------------------- core API ---------------------------
@dataclass
class ParsedRecord:
    idx: int
    data: Dict[str, Any]
    path: Path

def parse_one_detail(idx: int, batch_id: Optional[str] = None) -> ParsedRecord:
    dirs = _resolve_dirs(batch_id)
    raw_dir, struct_dir = dirs["raw"], dirs["structured"]
    html_text, meta = _read_html_meta(raw_dir, idx)
    source_url = (meta.get("final_url") or meta.get("requested_url") or "").lower()
    soup = BeautifulSoup(html_text, "html.parser")

    if "redfin.com" in source_url:
        rec = parse_redfin(soup, html_text); source_id = "redfin"
    elif "zillow.com" in source_url:
        rec = parse_zillow(soup, html_text); source_id = "zillow"
    else:
        rec = parse_redfin(soup, html_text) or parse_zillow(soup, html_text)
        source_id = rec.get("source_id","unknown")

    structured = {
        "listing_id": None,
        "source_id": source_id,
        "source_url": source_url,
        "external_property_id": rec["external_property_id"],
        "batch_id": dirs["base"].name,
        "crawl_method": CFG.get("crawl_method","firecrawl_v1"),
        "scraped_timestamp": now_utc_iso(),
        "address": rec["address"],
        "beds": rec["beds"], "baths": rec["baths"],
        "interior_area_sqft": to_int(rec["interior_area_sqft"]),
        "year_built": to_int(rec["year_built"]),
        "list_price": safe_float(rec["list_price"]),
        "photos": rec["photos"][:50],
        "listing_type": "sell",
        "status": None,
        "list_date": None,
        "days_on_market": None,
        "price_per_sqft": (safe_float(rec["list_price"]) / to_int(rec["interior_area_sqft"])) if safe_float(rec["list_price"]) and to_int(rec["interior_area_sqft"]) else None,
        "features": {},
        "engagement": {"views": None,"saves": None,"share_count": None},
        "similar_properties": [],
        "financials": {},
        "community_attributes": {}
    }
    out_path = struct_dir / f"{idx:04d}.json"
    out_path.write_text(json.dumps(structured, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"✅ Parsed {idx} -> {out_path}")
    return ParsedRecord(idx=idx, data=structured, path=out_path)

def parse_all_details(batch_id: Optional[str] = None, limit: int = 10, start_idx: int = 1001) -> List[ParsedRecord]:
    dirs = _resolve_dirs(batch_id); raw_dir = dirs["raw"]
    files = sorted(raw_dir.glob("1???_raw.html"))[:limit]
    if not files:
        raise FileNotFoundError("No detail raw files found (e.g., 1001_raw.html). Run fetch_detail_pages first.")
    results: List[ParsedRecord] = []
    for f in files:
        idx = int(f.name[:4])
        try: results.append(parse_one_detail(idx, batch_id=dirs["base"].name))
        except Exception as e: print(f"[{idx}] ERROR {type(e).__name__}: {e}")
    return results

# --- NEW: Adapted writer ---
def _stable_uuid(*parts: str) -> str:
    return hashlib.sha1("|".join([p for p in parts if p]).encode("utf-8")).hexdigest()[:32]

def to_adapted_rows(structured: dict) -> dict:
    listing_id = _stable_uuid(structured["source_id"], structured["external_property_id"] or structured["source_url"])
    property_id = listing_id
    # listings
    listing_row = {
        "listing_id": listing_id,
        "property_id": property_id,
        "batch_id": structured["batch_id"],
        "source_id": structured["source_id"],
        "source_url": structured["source_url"],
        "crawl_method": structured["crawl_method"],
        "scraped_timestamp": structured["scraped_timestamp"],
        "list_date": structured.get("list_date"),
        "days_on_market": structured.get("days_on_market"),
        "description": None,
        "listing_type": structured.get("listing_type"),
        "status": structured.get("status"),
        "title": None,
        "list_price": structured.get("list_price"),
        "price_per_sqft": structured.get("price_per_sqft"),
    }
    # properties
    a = structured["address"] or {}
    prop_row = {
        "property_id": property_id,
        "street_address": a.get("street"),
        "unit_number": a.get("unit"),
        "city": a.get("city"),
        "state": a.get("state"),
        "postal_code": a.get("postal_code"),
        "interior_area_sqft": structured.get("interior_area_sqft"),
        "year_built": structured.get("year_built"),
        "beds": structured.get("beds"),
        "baths": structured.get("baths"),
        "features": structured.get("features") or {},
        "created_at": structured["scraped_timestamp"],
        "updated_at": structured["scraped_timestamp"],
        "property_type": structured.get("property_type"),
        "property_subtype": structured.get("property_subtype"),
        "condition": structured.get("condition"),
    }
    # media
    media_rows = [
        {"listing_id": listing_id,"media_url": u,"caption": None,"display_order": i,"is_primary": (i==0),
         "created_at": structured["scraped_timestamp"],"media_type": "image"}
        for i,u in enumerate(structured.get("photos") or [])
    ]
    return {
        "listings":[listing_row],
        "properties":[prop_row],
        "media":media_rows,
        "agents":[],
        "price_history":[],
        "engagement":[structured.get("engagement")],
        "similar_properties":structured.get("similar_properties"),
        "financials":[structured.get("financials")],
        "community_attributes":[structured.get("community_attributes")]
    }

if __name__ == "__main__":
    parse_all_details(limit=10)
