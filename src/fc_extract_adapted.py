# src/fc_extract_adapted.py
# Purpose: Firecrawl-based extractor → JSON arrays per table (new schema)

from __future__ import annotations
import os, json, re, time, hashlib
from pathlib import Path
from typing import List, Optional, Dict, Any
from collections import defaultdict

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from firecrawl import FirecrawlApp

from src.settings import CFG, now_utc_iso, PROJECT_ROOT
from src.batch import init_batch
from src.fetch import fetch_search_pages
from src.extract_search import extract_listing_urls

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
load_dotenv()
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "").strip()
BATCHES_ROOT = PROJECT_ROOT / "data" / "batches"

def latest_batch_dir() -> Path:
    candidates = [p for p in BATCHES_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise RuntimeError("No batch folder found. Run batch first.")
    return max(candidates, key=lambda p: p.stat().st_mtime)

def ensure_batch_id(batch_id: Optional[str]) -> str:
    return batch_id or init_batch()

# ---------------------------------------------------------------------
# DB-like schemas
# ---------------------------------------------------------------------
class PropertyRow(BaseModel):
    property_id: str
    street_address: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    interior_area_sqft: Optional[int] = None
    lot_size_sqft: Optional[int] = None
    year_built: Optional[int] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    property_type: Optional[str] = None
    property_subtype: Optional[str] = None
    condition: Optional[str] = None
    features: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class ListingRow(BaseModel):
    listing_id: str
    property_id: str
    batch_id: str
    source_id: str
    source_url: str
    crawl_method: str
    scraped_timestamp: str
    list_date: Optional[str] = None
    days_on_market: Optional[int] = None
    description: Optional[str] = None
    listing_type: Optional[str] = None
    status: Optional[str] = None
    title: Optional[str] = None
    list_price: Optional[int] = None
    price_per_sqft: Optional[float] = None

class MediaRow(BaseModel):
    listing_id: str
    media_url: str
    caption: Optional[str] = None
    display_order: int = 0
    is_primary: bool = False
    created_at: Optional[str] = None
    media_type: Optional[str] = "image"

class AgentRow(BaseModel):
    listing_id: str
    agent_name: Optional[str] = None
    phone: Optional[str] = None
    brokerage: Optional[str] = None
    email: Optional[str] = None

class PriceHistoryRow(BaseModel):
    listing_id: str
    event_date: Optional[str] = None
    event_type: Optional[str] = None
    price: Optional[int] = None
    notes: Optional[str] = None

class LocationRow(BaseModel):
    location_id: str
    street_address: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class EngagementRow(BaseModel):
    listing_id: str
    views: Optional[int] = None
    saves: Optional[int] = None
    shares: Optional[int] = None

class SimilarRow(BaseModel):
    listing_id: str
    similar_url: str

class FinancialRow(BaseModel):
    listing_id: str
    hoa_fee: Optional[int] = None
    property_taxes_annual: Optional[int] = None

class CommunityRow(BaseModel):
    listing_id: str
    climate_risks: Optional[List[int]] = None
    amenities: Optional[List[str]] = None
    walk_score: Optional[int] = None

# ---------------------------------------------------------------------
# Extracted detail schema (Firecrawl output)
# ---------------------------------------------------------------------
class ExtractedAgent(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    brokerage: Optional[str] = None
    email: Optional[str] = None

class ExtractedPriceEvent(BaseModel):
    event_date: Optional[str] = None
    event_type: Optional[str] = None
    price: Optional[str] = None
    notes: Optional[str] = None

class ExtractedDetail(BaseModel):
    source_id: Optional[str] = None
    source_url: str
    external_property_id: Optional[str] = None
    scraped_timestamp: Optional[str] = None
    address: Dict[str, Any]
    list_price: Optional[Any] = None
    listing_type: Optional[str] = None
    status: Optional[str] = None
    list_date: Optional[Any] = None
    days_on_market: Optional[Any] = None
    beds: Optional[Any] = None
    baths: Optional[Any] = None
    interior_area_sqft: Optional[Any] = None
    lot_size_sqft: Optional[Any] = None
    year_built: Optional[Any] = None
    property_type: Optional[str] = None
    property_subtype: Optional[str] = None
    condition: Optional[str] = None
    description: Optional[str] = None
    features: Optional[Dict[str, Any]] = None
    images: Optional[List[str]] = None
    agents: Optional[List[ExtractedAgent]] = None
    price_history: Optional[List[ExtractedPriceEvent]] = None
    hoa_fee: Optional[Any] = None
    property_taxes_annual: Optional[Any] = None
    metrics_views: Optional[Any] = None
    metrics_saves: Optional[Any] = None
    metrics_shares: Optional[Any] = None
    similar_properties: Optional[List[str]] = None

class ExtractedDetailPage(BaseModel):
    details: ExtractedDetail

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def stable_uuid(*parts: str) -> str:
    return hashlib.sha1("|".join([p for p in parts if p]).encode("utf-8")).hexdigest()

def to_int(x) -> Optional[int]:
    if x is None: return None
    s = re.sub(r"[^\d\.]", "", str(x))
    if not s: return None
    try: return int(float(s))
    except: return None

def to_float(x) -> Optional[float]:
    if x is None: return None
    s = re.sub(r"[^\d\.]", "", str(x))
    if not s: return None
    try: return float(s)
    except: return None

def make_location_id(addr: Dict[str, Any]) -> str:
    key = "|".join([str(addr.get(k, "") or "") for k in ("street","unit","city","state","postal_code","latitude","longitude")])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def normalize_detail(d: ExtractedDetail, batch_id: str) -> Dict[str, List[Dict[str, Any]]]:
    sid = (d.source_id or "").lower().strip() or "unknown"
    ext = (d.external_property_id or "").strip()
    url = d.source_url.strip()
    listing_id = stable_uuid(sid, ext or url)
    property_id = listing_id

    list_price_i = to_int(d.list_price)
    sqft_i = to_int(d.interior_area_sqft)

    L = ListingRow(
        listing_id=listing_id, property_id=property_id, batch_id=batch_id,
        source_id=sid, source_url=url, crawl_method=CFG.get("crawl_method", "firecrawl_v1"),
        scraped_timestamp=d.scraped_timestamp or now_utc_iso(),
        list_date=d.list_date, days_on_market=to_int(d.days_on_market),
        description=d.description, listing_type=(d.listing_type or "sell"),
        status=d.status, title=None, list_price=list_price_i,
        price_per_sqft=(list_price_i/sqft_i) if list_price_i and sqft_i else None
    ).model_dump()

    addr = d.address or {}
    P = PropertyRow(
        property_id=property_id, street_address=addr.get("street"), unit_number=addr.get("unit"),
        city=addr.get("city"), state=addr.get("state"), postal_code=addr.get("postal_code"),
        latitude=addr.get("latitude"), longitude=addr.get("longitude"),
        interior_area_sqft=sqft_i, lot_size_sqft=to_int(d.lot_size_sqft),
        year_built=to_int(d.year_built), beds=to_float(d.beds), baths=to_float(d.baths),
        property_type=d.property_type, property_subtype=d.property_subtype, condition=d.condition,
        features=(d.features or {}), created_at=d.scraped_timestamp or now_utc_iso(),
        updated_at=d.scraped_timestamp or now_utc_iso(),
    ).model_dump()

    media_rows = [MediaRow(listing_id=listing_id, media_url=u, display_order=i, is_primary=(i==0),
                           created_at=d.scraped_timestamp or now_utc_iso()).model_dump()
                  for i,u in enumerate((d.images or [])[:50])]

    agent_rows = [AgentRow(listing_id=listing_id, agent_name=ag.name, phone=ag.phone,
                           brokerage=ag.brokerage, email=ag.email).model_dump()
                  for ag in (d.agents or [])]

    ph_rows = [PriceHistoryRow(listing_id=listing_id, event_date=ev.event_date,
                               event_type=ev.event_type, price=to_int(ev.price), notes=ev.notes).model_dump()
               for ev in (d.price_history or [])]

    location_id = make_location_id(addr)
    loc_row = LocationRow(location_id=location_id, street_address=addr.get("street"),
                          unit_number=addr.get("unit"), city=addr.get("city"),
                          state=addr.get("state"), postal_code=addr.get("postal_code"),
                          latitude=addr.get("latitude"), longitude=addr.get("longitude")).model_dump()

    eng_row = EngagementRow(listing_id=listing_id, views=to_int(d.metrics_views),
                            saves=to_int(d.metrics_saves), shares=to_int(d.metrics_shares)).model_dump()

    sim_rows = [SimilarRow(listing_id=listing_id, similar_url=su).model_dump()
                for su in (d.similar_properties or []) if su]

    fin_row = FinancialRow(listing_id=listing_id, hoa_fee=to_int(d.hoa_fee),
                           property_taxes_annual=to_int(d.property_taxes_annual)).model_dump()

    comm_row = CommunityRow(listing_id=listing_id, climate_risks=[], amenities=[], walk_score=None).model_dump()

    return {"listings":[L],"properties":[P],"media":media_rows,"agents":agent_rows,
            "price_history":ph_rows,"locations":[loc_row],"engagement":[eng_row],
            "similar_properties":sim_rows,"financials":[fin_row],"community_attributes":[comm_row]}

def dump_json(path: Path, rows: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------------------------------------------------------------------
# Firecrawl extract
# ---------------------------------------------------------------------
PROMPT = """
You are extracting a SINGLE property detail page into this JSON schema:
(details object with: source_id, source_url, external_property_id, scraped_timestamp,
address { street, unit, city, state, postal_code, latitude, longitude },
list_price, listing_type, status, list_date, days_on_market,
beds, baths, interior_area_sqft, lot_size_sqft, year_built,
property_type, property_subtype, condition,
description, features,
images[], agents[], price_history[],
hoa_fee, property_taxes_annual,
metrics_views, metrics_saves, metrics_shares,
similar_properties[])
""".strip()

def _to_dict_like(x):
    if x is None: return None
    md = getattr(x,"model_dump",None)
    if callable(md):
        try: return md()
        except: pass
    d = getattr(x,"dict",None)
    if callable(d):
        try: return d()
        except: pass
    if hasattr(x,"__dict__"):
        try: return dict(x.__dict__)
        except: pass
    if isinstance(x,(dict,list,str,int,float,bool)):
        return x
    return None

def _unwrap_details(result) -> Optional[dict]:
    if result is None: return None
    data_attr = getattr(result,"data",None)
    data = _to_dict_like(data_attr) if data_attr is not None else None
    if isinstance(result,dict):
        if result.get("error"): return None
        data = result.get("data") or result.get("results") or result.get("items") or data
    if isinstance(data,list) and data:
        first=_to_dict_like(data[0])
        if isinstance(first,dict) and "details" in first: return first["details"]
    if isinstance(data,dict) and "details" in data: return data["details"]
    return None

def extract_one(fc: FirecrawlApp,url:str)->Optional[ExtractedDetail]:
    try:
        r=fc.extract([url],prompt=PROMPT,schema=ExtractedDetailPage.model_json_schema())
        d=_unwrap_details(r)
        if d: return ExtractedDetail.model_validate(d)
    except Exception: pass
    try:
        r=fc.extract([url],prompt=PROMPT)
        d=_unwrap_details(r)
        if d: return ExtractedDetail.model_validate(d)
    except Exception: pass
    try:
        batch_dir=latest_batch_dir()
        debug=batch_dir/"structured"/f"failed_extract_{hashlib.md5(url.encode()).hexdigest()[:10]}.json"
        debug.write_text(json.dumps({"url":url,"raw":_to_dict_like(locals().get("r"))},ensure_ascii=False,indent=2))
    except: pass
    return None

# ---------------------------------------------------------------------
# URL prep
# ---------------------------------------------------------------------
def load_or_prepare_urls(batch_id:str,limit:int,seed_limit:int=4)->List[str]:
    struct_dir=BATCHES_ROOT/batch_id/"structured"
    urls_path=struct_dir/"listing_urls.json"
    if urls_path.exists():
        payload=json.loads(urls_path.read_text(encoding="utf-8"))
        url_rows=payload.get("urls") or []
        urls=[r["source_url"] if isinstance(r,dict) else str(r) for r in url_rows]
        return urls[:limit]
    fetch_search_pages(batch_id=batch_id,limit=seed_limit)
    extract_listing_urls(batch_id=batch_id,max_search_files=seed_limit)
    if urls_path.exists():
        payload=json.loads(urls_path.read_text(encoding="utf-8"))
        url_rows=payload.get("urls") or []
        urls=[r["source_url"] if isinstance(r,dict) else str(r) for r in url_rows]
        return urls[:limit]
    return []

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(batch_id: Optional[str]=None,limit:int=10,delay_sec:float=1.0,seed_limit:int=4,new_batch:bool=False):
    if not FIRECRAWL_API_KEY:
        raise RuntimeError("Set FIRECRAWL_API_KEY in .env")
    fc=FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    if new_batch or not batch_id:
        batch_id=ensure_batch_id(batch_id)
    batch_dir=BATCHES_ROOT/batch_id
    struct_dir=batch_dir/"structured"; struct_dir.mkdir(parents=True,exist_ok=True)
    urls=load_or_prepare_urls(batch_id=batch_id,limit=limit,seed_limit=seed_limit)
    if not urls: raise RuntimeError("No URLs to extract")
    print(f"Batch: {batch_id} | URLs: {len(urls)}")
    buckets:Dict[str,List[Dict[str,Any]]]=defaultdict(list)
    for i,url in enumerate(urls,1):
        print(f"[{i}/{len(urls)}] {url}")
        det=extract_one(fc,url)
        if not det:
            print("   → no details extracted"); time.sleep(delay_sec); continue
        det.source_url=det.source_url or url
        det.scraped_timestamp=det.scraped_timestamp or now_utc_iso()
        rows=normalize_detail(det,batch_id=batch_id)
        for k,v in rows.items(): buckets[k].extend(v)
        time.sleep(delay_sec)
    for tbl,arr in buckets.items():
        dump_json(struct_dir/f"{tbl}.json",arr)
    print(f"✅ Wrote JSON files to {struct_dir}")
    for tbl,arr in buckets.items():
        print(f"   {tbl}={len(arr)}")

# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--batch-id",default=None)
    ap.add_argument("--limit",type=int,default=10)
    ap.add_argument("--delay",type=float,default=1.0)
    ap.add_argument("--seed-limit",type=int,default=4)
    ap.add_argument("--new-batch",action="store_true")
    args=ap.parse_args()
    main(batch_id=args.batch_id,limit=args.limit,delay_sec=args.delay,seed_limit=args.seed_limit,new_batch=args.new_batch)
