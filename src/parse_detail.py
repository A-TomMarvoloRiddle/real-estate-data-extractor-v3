# src/parse_detail.py
# Robust parser for Zillow/Redfin detail pages (new schema, with strong fallbacks)

from __future__ import annotations
import hashlib, json, re, html
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from src.settings import PROJECT_ROOT, now_utc_iso

BATCHES_ROOT = PROJECT_ROOT / "data" / "batches"
NUM_RE = re.compile(r"[^\d\.]+")

# ---------------- utils ----------------
def to_int(x):
    if x is None: return None
    s = re.sub(NUM_RE, "", str(x))
    if not s: return None
    try: return int(float(s))
    except: return None

def to_float(x):
    if x is None: return None
    s = re.sub(NUM_RE, "", str(x))
    if not s: return None
    try: return float(s)
    except: return None

def s_trim(x):
    if x is None: return None
    t = str(x).strip()
    return t or None

def guess_source(u: str) -> str:
    return "zillow" if "zillow.com" in u else ("redfin" if "redfin.com" in u else "unknown")

def ext_id(u: str, sid: str) -> Optional[str]:
    if sid == "zillow":
        m = re.search(r"/(\d+)_zpid", u); return m.group(1) if m else None
    if sid == "redfin":
        m = re.search(r"/home/(\d+)", u); return m.group(1) if m else None
    return None

def stable_id(*parts: str) -> str:
    return hashlib.sha1("|".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:32]

def _latest_batch_dir() -> Path:
    ds = [p for p in (BATCHES_ROOT).iterdir() if p.is_dir()]
    if not ds: raise RuntimeError("No batches found. Run: python -m src.batch")
    return max(ds, key=lambda p: p.stat().st_mtime)

def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")

def _read_json(p: Path, default=None):
    if not p.exists(): return default
    try:
        return json.loads(_read_text(p))
    except Exception: return default

def _write_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def _blocked(html: str) -> bool:
    t = (html or "").lower()
    bad = ("captcha", "access denied", "forbidden", "unusual traffic", "are you a human", "bot detection")
    # صفحات حماية/خفيفة جداً
    return len(t) < 4000 or any(b in t for b in bad)

# ---------------- Zillow extractors ----------------
def zillow_from_apollo(soup: BeautifulSoup) -> Dict:
    out = {}
    tag = soup.find("script", id="hdpApolloPreloadedData")
    if not tag or not tag.string:
        return out
    try:
        data = json.loads(tag.string)
    except Exception:
        return out

    def walk(d):
        if isinstance(d, dict):
            addr = d.get("address")
            if isinstance(addr, dict):
                out.setdefault("street_address", s_trim(addr.get("streetAddress")))
                out.setdefault("city", s_trim(addr.get("city")))
                out.setdefault("state", s_trim(addr.get("state")))
                out.setdefault("postal_code", s_trim(addr.get("zipcode")))
                out.setdefault("latitude", to_float(addr.get("latitude")))
                out.setdefault("longitude", to_float(addr.get("longitude")))
            if "bedrooms" in d: out.setdefault("beds", to_float(d.get("bedrooms")))
            if "bathrooms" in d: out.setdefault("baths", to_float(d.get("bathrooms")))
            if "livingArea" in d: out.setdefault("interior_area_sqft", to_int(d.get("livingArea")))
            if "lotAreaValue" in d: out.setdefault("lot_size_sqft", to_int(d.get("lotAreaValue")))
            if "price" in d: out.setdefault("list_price", to_int(d.get("price")))
            if "yearBuilt" in d: out.setdefault("year_built", to_int(d.get("yearBuilt")))
            if "description" in d: out.setdefault("description", s_trim(d.get("description")))
            photos = d.get("photos") or d.get("image") or d.get("images")
            if isinstance(photos, list):
                out.setdefault("images", [])
                for ph in photos:
                    u = ph.get("url") if isinstance(ph, dict) else ph
                    if isinstance(u, str) and len(out["images"]) < 20 and u not in out["images"]:
                        out["images"].append(u)
            # agents (إن وجدت)
            for key in ("listingAgent", "agent", "agents"):
                ag = d.get(key)
                if ag:
                    out.setdefault("agents", [])
                    if isinstance(ag, list):
                        for a in ag:
                            if isinstance(a, dict):
                                out["agents"].append({
                                    "name": s_trim(a.get("name")),
                                    "phone": s_trim(a.get("phone")),
                                    "brokerage": s_trim(a.get("brokerageName") or a.get("brokerage")),
                                    "email": s_trim(a.get("email")),
                                })
                    elif isinstance(ag, dict):
                        out["agents"].append({
                            "name": s_trim(ag.get("name")),
                            "phone": s_trim(ag.get("phone")),
                            "brokerage": s_trim(ag.get("brokerageName") or ag.get("brokerage")),
                            "email": s_trim(ag.get("email")),
                        })
            # price history (إن وجدت)
            if isinstance(d.get("priceHistory"), list):
                out.setdefault("price_history", [])
                for ev in d["priceHistory"]:
                    if isinstance(ev, dict):
                        out["price_history"].append({
                            "event_date": s_trim(ev.get("date") or ev.get("eventDate")),
                            "event_type": s_trim(ev.get("event") or ev.get("eventType")),
                            "price": to_int(ev.get("price")),
                            "notes": s_trim(ev.get("description")),
                        })
            for v in d.values(): walk(v)
        elif isinstance(d, list):
            for v in d: walk(v)
    walk(data)
    return out

# ---------------- Redfin extractor ----------------
def redfin_from_nextdata(soup: BeautifulSoup) -> Dict:
    out = {}
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return out
    try:
        data = json.loads(tag.string)
    except Exception:
        return out

    def walk(d):
        if isinstance(d, dict):
            addr = d.get("address")
            if isinstance(addr, dict):
                out.setdefault("street_address", s_trim(addr.get("streetLine")))
                out.setdefault("city", s_trim(addr.get("city")))
                out.setdefault("state", s_trim(addr.get("stateCode")))
                out.setdefault("postal_code", s_trim(addr.get("zip")))
                out.setdefault("latitude", to_float(addr.get("lat")))
                out.setdefault("longitude", to_float(addr.get("lng")))
            if "beds" in d: out.setdefault("beds", to_float(d.get("beds")))
            if "baths" in d: out.setdefault("baths", to_float(d.get("baths")))
            if "sqFt" in d: out.setdefault("interior_area_sqft", to_int(d.get("sqFt")))
            if "lotSize" in d: out.setdefault("lot_size_sqft", to_int(d.get("lotSize")))
            if "price" in d: out.setdefault("list_price", to_int(d.get("price")))
            if "yearBuilt" in d: out.setdefault("year_built", to_int(d.get("yearBuilt")))
            if "description" in d: out.setdefault("description", s_trim(d.get("description")))
            # media
            photos = d.get("photos") or d.get("media") or []
            if isinstance(photos, list):
                out.setdefault("images", [])
                for ph in photos:
                    if isinstance(ph, dict):
                        u = ph.get("url") or ph.get("src")
                        if isinstance(u, str) and len(out["images"]) < 20 and u not in out["images"]:
                            out["images"].append(u)
            # agents
            listing_agent = d.get("listingAgent") or d.get("agent")
            if listing_agent:
                out.setdefault("agents", [])
                if isinstance(listing_agent, list):
                    for a in listing_agent:
                        if isinstance(a, dict):
                            out["agents"].append({
                                "name": s_trim(a.get("name")),
                                "phone": s_trim(a.get("phone")),
                                "brokerage": s_trim(a.get("brokerage")),
                                "email": s_trim(a.get("email")),
                            })
                elif isinstance(listing_agent, dict):
                    out["agents"].append({
                        "name": s_trim(listing_agent.get("name")),
                        "phone": s_trim(listing_agent.get("phone")),
                        "brokerage": s_trim(listing_agent.get("brokerage")),
                        "email": s_trim(listing_agent.get("email")),
                    })
            # price history
            if isinstance(d.get("priceHistory"), list):
                out.setdefault("price_history", [])
                for ev in d["priceHistory"]:
                    if isinstance(ev, dict):
                        out["price_history"].append({
                            "event_date": s_trim(ev.get("date") or ev.get("eventDate")),
                            "event_type": s_trim(ev.get("event") or ev.get("eventType")),
                            "price": to_int(ev.get("price")),
                            "notes": s_trim(ev.get("description")),
                        })
            for v in d.values(): walk(v)
        elif isinstance(d, list):
            for v in d: walk(v)
    walk(data)
    return out

# ---------------- JSON-LD fallback ----------------
def from_jsonld(soup: BeautifulSoup) -> Dict:
    out = {}
    blocks = []
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        txt = tag.string or tag.get_text("", strip=True)
        if not txt: continue
        try:
            data = json.loads(txt)
            if isinstance(data, dict): blocks.append(data)
            elif isinstance(data, list): blocks.extend([x for x in data if isinstance(x, dict)])
        except Exception: 
            continue

    def prefer(a,b):
        if a is None: return b
        if b is None: return a
        if isinstance(a,(int,float)) and isinstance(b,(int,float)): return a if a>=b else b
        if isinstance(a,str) and isinstance(b,str): return a if len(a)>=len(b) else b
        return a or b

    for d in blocks:
        name = d.get("name") or d.get("headline")
        out["title"] = prefer(out.get("title"), s_trim(name))
        desc = d.get("description")
        out["description"] = prefer(out.get("description"), s_trim(desc))
        addr = d.get("address")
        if isinstance(addr, dict):
            out["street_address"] = prefer(out.get("street_address"), s_trim(addr.get("streetAddress")))
            out["city"] = prefer(out.get("city"), s_trim(addr.get("addressLocality")))
            out["state"] = prefer(out.get("state"), s_trim(addr.get("addressRegion")))
            out["postal_code"] = prefer(out.get("postal_code"), s_trim(addr.get("postalCode")))
        out["beds"] = prefer(out.get("beds"), to_float(d.get("numberOfBedrooms") or d.get("numberOfRooms")))
        out["baths"] = prefer(out.get("baths"), to_float(d.get("numberOfBathroomsTotal") or d.get("numberOfBathrooms")))
        fs = d.get("floorSize")
        if isinstance(fs, dict):
            out["interior_area_sqft"] = prefer(out.get("interior_area_sqft"), to_int(fs.get("value")))
        offers = d.get("offers") or {}
        if isinstance(offers, dict):
            price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
            out["list_price"] = prefer(out.get("list_price"), to_int(price))
        imgs = d.get("image")
        if isinstance(imgs, list):
            out.setdefault("images", [])
            for im in imgs[:20]:
                if isinstance(im, str) and im not in out["images"]:
                    out["images"].append(im)
        elif isinstance(imgs, str):
            out["images"] = list({imgs})
    return out

# ---------------- meta/DOM fallbacks ----------------
META_NAME_KEYS = ("description", "twitter:description", "og:description")
META_TITLE_KEYS = ("og:title", "twitter:title")
META_IMAGE_KEYS = ("og:image", "twitter:image", "og:image:secure_url")

BED_RE   = re.compile(r'(\d+(?:\.\d+)?)\s*(?:bed|beds|bedroom)s?', re.I)
BATH_RE  = re.compile(r'(\d+(?:\.\d+)?)\s*(?:bath|baths|bathroom)s?', re.I)
SQFT_RE  = re.compile(r'(\d{3,}(?:,\d{3})*)\s*(?:sq\s*ft|sqft|ft²)', re.I)
PRICE_RE = re.compile(r'\$\s*([0-9]{1,3}(?:,[0-9]{3})+)', re.I)
VIEWS_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*)\s+views?', re.I)
SAVES_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*)\s+saves?', re.I)
FAVS_RE  = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*)\s+(?:favorites?|favorite|favs?)', re.I)
PHONE_RE = re.compile(r'(\(?\d{3}\)?[\s\-\.]?\d{3}[\-\.]?\d{4})')

def _text_all(soup: BeautifulSoup) -> str:
    return soup.get_text(" ", strip=True)

def extract_from_meta(soup: BeautifulSoup) -> Dict:
    out = {}
    def _get(name):
        tag = soup.find("meta", attrs={"name": name}) or soup.find("meta", attrs={"property": name})
        if tag and (tag.get("content")):
            return tag["content"].strip()
        return None
    # title
    for k in META_TITLE_KEYS:
        t = _get(k)
        if t:
            out["title"] = t
            break
    # description
    for k in META_NAME_KEYS:
        d = _get(k)
        if d:
            out.setdefault("description", d)
            break
    # og:image(s)
    imgs = []
    for k in META_IMAGE_KEYS:
        v = _get(k)
        if v:
            imgs.append(v)
    if imgs:
        out["images"] = list(dict.fromkeys(imgs))[:20]
    return out

def extract_from_dom_common(soup: BeautifulSoup, base_url: str) -> Dict:
    out = {}
    # عنوان صفحة: تجنّب العناوين العامة
    for tag in soup.find_all(["h1","h2"]):
        tt = tag.get_text(" ", strip=True)
        if not tt: 
            continue
        if tt.lower() in ("about this home", "about this house", "facts and features"):
            continue
        out["title"] = tt
        break

    # محاولات لالتقاط العنوان التفصيلي
    addr_candidates = []
    for sel in ["address", ".address", ".homeAddress", ".street-address", "[data-rf-test-id='abp-streetLine']"]:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if txt and len(txt) > 6:
                addr_candidates.append(txt)
    if addr_candidates:
        line = addr_candidates[0]
        out.setdefault("street_address", line)
        m = re.search(r",\s*([A-Za-z\.\s]+),\s*([A-Z]{2})\s+(\d{5})", line)
        if m:
            out["city"] = m.group(1).strip()
            out["state"] = m.group(2).strip()
            out["postal_code"] = m.group(3).strip()

    big = _text_all(soup)

    m = PRICE_RE.search(big)
    if m:
        out["list_price"] = int(m.group(1).replace(",", ""))

    m = BED_RE.search(big)
    if m:
        out["beds"] = float(m.group(1))

    m = BATH_RE.search(big)
    if m:
        out["baths"] = float(m.group(1))

    m = SQFT_RE.search(big)
    if m:
        try: out["interior_area_sqft"] = int(m.group(1).replace(",", ""))
        except: pass

    # صور من <img>
    imgs = out.get("images", []) or []
    for img in soup.find_all("img"):
        u = img.get("data-src") or img.get("src")
        if not u: continue
        u = html.unescape((u or "").strip())
        if not u: continue
        if u.startswith("//"): u = "https:" + u
        if u.startswith("/"):  u = urljoin(base_url, u)
        if u.lower().startswith("http"):
            imgs.append(u)
        if len(imgs) >= 30:
            break
    if imgs:
        out["images"] = list(dict.fromkeys(imgs))[:30]

    return out

def extract_engagement_dom(soup: BeautifulSoup) -> Dict:
    out = {}
    text = _text_all(soup)
    mv = VIEWS_RE.search(text)
    ms = SAVES_RE.search(text) or FAVS_RE.search(text)
    if mv:
        try: out["metrics_views"] = int(mv.group(1).replace(",", ""))
        except: pass
    if ms:
        try: out["metrics_saves"] = int(ms.group(1).replace(",", ""))
        except: pass
    return out

def extract_agents_dom(soup: BeautifulSoup) -> List[Dict]:
    agents = []
    for sel in [".agent", ".agent-card", ".listing-agent", "[data-testid='listing-agent']"]:
        for block in soup.select(sel):
            txt = block.get_text(" ", strip=True)
            if not txt or len(txt) < 3: 
                continue
            name = None; brokerage = None; phone = None
            name = txt.split(" - ")[0].strip() if " - " in txt else txt.split(",")[0].strip()
            pm = PHONE_RE.search(txt)
            if pm: phone = pm.group(1)
            if any(k in txt for k in ("Realty", "Broker", "Compass", "Keller", "Sotheby", "Douglas", "EXP")):
                brokerage = "Brokerage"
            agents.append({"name": s_trim(name), "phone": s_trim(phone), "brokerage": s_trim(brokerage), "email": None})
    uniq, seen = [], set()
    for a in agents:
        key = (a.get("name"), a.get("phone"))
        if key in seen: continue
        seen.add(key); uniq.append(a)
    return uniq[:5]

def extract_price_history_dom(soup: BeautifulSoup) -> List[Dict]:
    events = []
    text = _text_all(soup)
    line_re = re.compile(r'([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}).{0,40}\$\s*([0-9]{1,3}(?:,[0-9]{3})+)', re.S)
    for m in line_re.finditer(text):
        dt = m.group(1)
        price = int(m.group(2).replace(",", ""))
        chunk = text[m.start(): m.end()+40]
        et = "price_event"
        if "Listed" in chunk: et = "listed"
        elif "Sold" in chunk: et = "sold"
        elif "Price" in chunk: et = "price_change"
        events.append({"event_date": dt, "event_type": et, "price": price, "notes": None})
    return events[:20]

def url_address_fallback(url: str) -> dict:
    out = {}
    if not url: return out
    try:
        parts = [p for p in urlparse(url).path.split("/") if p]
        # Redfin: /NY/New-York/111-4th-Ave-10003/unit-3I/home/45142411
        if "redfin.com" in url and len(parts) >= 3:
            state = parts[0] if len(parts[0]) <= 3 else None
            city = parts[1].replace("-", " ")
            street_zip = parts[2]
            unit = parts[3] if len(parts) > 3 and parts[3].startswith("unit-") else None
            toks = street_zip.split("-")
            zipc = toks[-1] if toks[-1].isdigit() and len(toks[-1]) == 5 else None
            street_tokens = toks[:-1] if zipc else toks
            street = " ".join(t.replace("+", " ").replace("_", " ") for t in street_tokens)
            unit_val = unit.split("-",1)[1] if unit else None
            out["street_address"] = s_trim(street)
            out["postal_code"] = s_trim(zipc)
            out["city"] = s_trim(city.title())
            out["state"] = s_trim(state)
            out["unit_number"] = s_trim(unit_val)
    except Exception:
        pass
    return {k:v for k,v in out.items() if v}

# ---------------- main per-page ----------------
def parse_one_detail_html(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html or "", "html.parser")
    sid = guess_source(url)
    rec: Dict = {
        "source_id": sid, "source_url": url,
        "external_property_id": ext_id(url, sid),
        "scraped_timestamp": now_utc_iso(),
        "status": "ok",
    }
    if _blocked(html):
        rec["status"] = "blocked"
        return rec

    # 1) Rich JSON (primary)
    if sid == "zillow": rec.update({k:v for k,v in zillow_from_apollo(soup).items() if v not in (None,"",[],{})})
    if sid == "redfin": rec.update({k:v for k,v in redfin_from_nextdata(soup).items() if v not in (None,"",[],{})})

    # 2) JSON-LD (secondary)
    jl = from_jsonld(soup)
    for k,v in jl.items():
        if rec.get(k) in (None,"",[],{}):
            rec[k] = v

    # 3) Meta + DOM (fallbacks)
    meta_enrich = extract_from_meta(soup)
    for k, v in meta_enrich.items():
        if rec.get(k) in (None, "", [], {}):
            rec[k] = v

    dom_enrich = extract_from_dom_common(soup, url)
    for k, v in dom_enrich.items():
        if rec.get(k) in (None, "", [], {}):
            rec[k] = v

    # Engagement
    eng = extract_engagement_dom(soup)
    for k, v in eng.items():
        if rec.get(k) in (None, "", [], {}):
            rec[k] = v

    # Agents
    if rec.get("agents") in (None, [], {}):
        ags = extract_agents_dom(soup)
        if ags:
            rec["agents"] = ags

    # Price history
    if rec.get("price_history") in (None, [], {}):
        rec["price_history"] = extract_price_history_dom(soup)

    # 4) URL-derived address (last resort)
    if not rec.get("street_address") or not rec.get("postal_code") or not rec.get("city") or not rec.get("state"):
        url_addr = url_address_fallback(url)
        for k,v in url_addr.items():
            if rec.get(k) in (None, "", [], {}):
                rec[k] = v

    # normalize images
    imgs = rec.get("images")
    if isinstance(imgs, list):
        uniq, seen = [], set()
        for u in imgs:
            u = s_trim(u)
            if not u or u in seen: continue
            uniq.append(u); seen.add(u)
            if len(uniq) >= 30: break
        rec["images"] = uniq

    # تنظيف العنوان “About this home”
    if (rec.get("title") or "").strip().lower() in ("about this home", "about this house"):
        rec["title"] = None

    return rec

# ---------------- batch API ----------------
def parse_all_details(batch_id: Optional[str] = None, limit: int = 50) -> None:
    base = _latest_batch_dir() if batch_id is None else (BATCHES_ROOT / batch_id)
    raw, struct = base/"raw", base/"structured"
    files = sorted(raw.glob("1???_raw.html"))[:limit]
    if not files: raise FileNotFoundError("No raw detail files found")

    wrote=0
    for f in files:
        try:
            html=_read_text(f)
            meta=_read_json(raw/f.name.replace("_raw.html","_meta.json"),{}) or {}
            url=meta.get("final_url") or meta.get("requested_url") or ""
            rec=parse_one_detail_html(html,url)

            sid=rec.get("source_id") or "unknown"
            ext=rec.get("external_property_id") or ""
            surl=rec.get("source_url") or ""
            rec["listing_id"]=stable_id(sid,"listing",ext or surl)
            rec["property_id"]=stable_id(sid,"property",ext or surl)
            rec["crawl_method"]=meta.get("crawl_method") or "requests"
            rec["batch_id"]=base.name

            out=struct/f.name.replace("_raw.html",".json")
            _write_json(out,rec); wrote+=1
        except Exception as e:
            _write_json(struct/f.name.replace("_raw.html","_error.json"),{"error":str(e)})

    print(f"✅ parse_all_details wrote {wrote} detail records -> {struct}")

# ---------------- adapted rows for pipeline ----------------
def to_adapted_rows(rec: Dict) -> Dict[str, List[Dict]]:
    sid = s_trim(rec.get("source_id")) or "unknown"
    listing_id = rec.get("listing_id")
    property_id = rec.get("property_id")
    surl = s_trim(rec.get("source_url"))
    ts = rec.get("scraped_timestamp")
    status = rec.get("status") or "ok"

    street = s_trim(rec.get("street_address"))
    city = s_trim(rec.get("city"))
    state = s_trim(rec.get("state"))
    postal = s_trim(rec.get("postal_code"))
    lat = rec.get("latitude"); lon = rec.get("longitude")
    beds = to_float(rec.get("beds"))
    baths = to_float(rec.get("baths"))
    area = to_int(rec.get("interior_area_sqft"))
    price = to_int(rec.get("list_price"))
    ppsf = float(price)/float(area) if (price and area) else None

    listings = [{
        "listing_id": listing_id,
        "property_id": property_id,
        "batch_id": rec.get("batch_id") or "",
        "source_id": sid,
        "source_url": surl,
        "crawl_method": rec.get("crawl_method") or "requests",
        "scraped_timestamp": ts,
        "listing_type": "sell",
        "status": status,
        "title": s_trim(rec.get("title")),
        "description": s_trim(rec.get("description")),
        "list_price": price,
        "price_per_sqft": ppsf,
        "images_count": len(rec.get("images") or []),
    }]

    properties = [{
        "property_id": property_id,
        "street_address": street,
        "unit_number": s_trim(rec.get("unit_number")),
        "city": city,
        "state": state,
        "postal_code": postal,
        "latitude": lat,
        "longitude": lon,
        "interior_area_sqft": area,
        "lot_size_sqft": to_int(rec.get("lot_size_sqft")),
        "year_built": to_int(rec.get("year_built")),
        "beds": beds,
        "baths": baths,
        "features": rec.get("features") or {},
        "created_at": ts,
        "updated_at": ts,
        "property_type": s_trim(rec.get("property_type")),
        "property_subtype": s_trim(rec.get("property_subtype")),
        "condition": s_trim(rec.get("condition")),
    }]

    # media
    media = []
    for i, u in enumerate(rec.get("images") or []):
        media.append({
            "listing_id": listing_id,
            "media_url": u,
            "media_type": "image",
            "display_order": i,
            "is_primary": (i == 0),
        })

    # agents
    agents = []
    for a in rec.get("agents") or []:
        agents.append({
            "listing_id": listing_id,
            "agent_name": s_trim(a.get("name")),
            "phone": s_trim(a.get("phone")),
            "brokerage": s_trim(a.get("brokerage")),
            "email": s_trim(a.get("email")),
        })

    # price history
    price_history = []
    for ev in rec.get("price_history") or []:
        price_history.append({
            "listing_id": listing_id,
            "event_date": s_trim(ev.get("event_date")),
            "event_type": s_trim(ev.get("event_type")),
            "price": to_int(ev.get("price")),
            "notes": s_trim(ev.get("notes")),
        })

    # engagement
    engagement = [{
        "listing_id": listing_id,
        "views": to_int(rec.get("metrics_views")),
        "saves": to_int(rec.get("metrics_saves")),
        "shares": to_int(rec.get("metrics_shares")),
    }]

    # locations (كان فاضي: نعبّيه الآن)
    loc_key = "|".join([street or "", s_trim(rec.get("unit_number")) or "", city or "", state or "", postal or "", str(lat or ""), str(lon or "")])
    location_id = hashlib.sha1(loc_key.encode("utf-8")).hexdigest() if loc_key.strip("|") else stable_id(sid, "loc", property_id or surl or "")
    locations = [{
        "location_id": location_id,
        "street_address": street,
        "unit_number": s_trim(rec.get("unit_number")),
        "city": city,
        "state": state,
        "postal_code": postal,
        "latitude": lat,
        "longitude": lon,
    }]

    return {
        "listings": listings,
        "properties": properties,
        "media": media,
        "agents": agents,
        "price_history": price_history,
        "engagement": engagement,
        "financials": [],
        "community_attributes": [],
        "similar_properties": [],
        "locations": locations,
    }
