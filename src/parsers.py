# parsers.py
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from .extractors import (
    extract_json_ld,
    extract_og_meta,
    extract_title,
    extract_js_variable_object,
    pick_nonempty,
    ZPID_RE, REDFIN_ID_RE,
)
from .utils import safe_float, safe_int, hash_address


# =========================
# Data model (stable property slice)
# =========================

@dataclass
class PropStable:
    external_property_id: Optional[str]
    address: Optional[str]
    unit_number: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[int]      # int per Debayan spec
    latitude: Optional[float]
    longitude: Optional[float]
    beds: Optional[float]
    baths: Optional[float]
    interior_area: Optional[int]
    property_type: Optional[str]    # normalized enum-like string
    property_subtype: Optional[str]
    condition: Optional[str]
    year_built: Optional[int]


# =========================
# Helpers
# =========================

TYPE_MAP = {
    "single family residence": "single_family",
    "single-family": "single_family",
    "single family": "single_family",
    "house": "single_family",
    "condo": "condo",
    "apartment": "apartment",
    "townhouse": "townhouse",
    "multi-family": "multi_family",
    "multi family": "multi_family",
}

PHONE_RE = re.compile(r"(?:\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}")

def _try_external_id(url: str, html: str) -> Optional[str]:
    if "zillow.com" in (url or ""):
        m = ZPID_RE.search(url) or ZPID_RE.search(html or "")
        if m: return f"zpid:{m.group(1)}"
    if "redfin.com" in (url or ""):
        m = REDFIN_ID_RE.search(url) or REDFIN_ID_RE.search(html or "")
        if m: return f"redfin:{m.group(1)}"
    return None

def _parse_iso_like(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s): return s
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})", s)
    if m:
        months = {
            "january": "01","february": "02","march": "03","april": "04","may": "05","june": "06",
            "july": "07","august": "08","september": "09","october": "10","november": "11","december": "12"
        }
        mm = months.get(m.group(1).lower())
        if mm: return f"{m.group(3)}-{mm}-{int(m.group(2)):02d}"
    return None

def _extract_max_price(blob_md: str) -> Optional[float]:
    matches = re.findall(r"\$[\d,]+", blob_md)
    vals = []
    for m in matches:
        v = safe_float(m.replace("$","").replace(",",""))
        if v: vals.append(v)
    return max(vals) if vals else None

def _extract_markdown_images(blob_md: str, limit: int = 24) -> List[str]:
    """Keep only property photos; drop agent avatars/logos and partner showcase."""
    urls = []
    for m in re.finditer(r"!\[[^\]]*\]\((https?://[^\s)]+)\)", blob_md):
        u = m.group(1)
        if "photos.zillowstatic.com/fp/" in u:   # property photos
            urls.append(u)
        # skip agent avatars / partner showcase / logos
        if len(urls) >= limit:
            break
    return urls

def _normalize_address_from_blob(blob_md: str):
    """
    Return (address, city, state, postal:int) if found in markdown.
    Robust to '# ' headers and stray whitespace.
    """
    m = re.search(r"(\d+\s+[A-Za-z0-9\s\.#\-]+),\s*([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5})", blob_md)
    if not m:
        return None, None, None, None
    street, city, state, postal = m.groups()
    street = street.replace("\n", " ").strip()
    street = re.sub(r"^\#\s*", "", street)          # drop leading '# '
    street = re.sub(r"\s{2,}", " ", street)
    addr = f"{street}, {city.strip()}, {state} {postal}"
    return addr, city.strip(), state, safe_int(postal)

def _norm_property_type(raw: Optional[str]) -> Optional[str]:
    if not raw: return None
    key = raw.strip().lower()
    for k,v in TYPE_MAP.items():
        if k in key: return v
    if key in TYPE_MAP.values():
        return key
    if "residence" in key or "house" in key: return "single_family"
    return "other"

def _extract_agent_info(blob_md: str) -> Dict[str, Optional[str]]:
    """
    Try to get brokerage, agent_name, phone from lines like:
    'Listing by: Corcoran ... (917-573-5102) Douglas Brown' OR
    'Listed by Douglas Elliman 212-641-0096 - Eleonora Srugo'
    Heuristics: pick phone; words around it split into brokerage (left) and agent (right).
    """
    agent = {"brokerage": None, "agent_name": None, "phone": None}
    # Find the 'Listing by' block (one or two lines)
    m = re.search(r"(?:^|\n)\s*(?:Listing by|Listed by)\s*:?\s*(.+?)(?:\n|$)", blob_md, re.I)
    if not m:
        # sometimes it's under "Agent information" header
        m = re.search(r"##\s*Agent information[\s\S]+?(?:\n##|\Z)", blob_md, re.I)
        line = None
        if m:
            # take first non-empty line inside the section
            sect = m.group(0)
            for ln in sect.splitlines():
                ln = ln.strip()
                if ln and not ln.lower().startswith("##"):
                    line = ln
                    break
        else:
            line = None
    else:
        line = m.group(1).strip()

    if not line:
        return agent

    # phone
    ph = PHONE_RE.search(line)
    if ph:
        agent["phone"] = ph.group(0)

    # split brokerage (left of phone) / agent (right of phone)
    if agent["phone"]:
        parts = line.split(agent["phone"])
        left = parts[0].strip(" -–|•")
        right = parts[1].strip(" -–|•") if len(parts) > 1 else ""
        # brokerage: take first 1-4 capitalized tokens from left side
        # (very heuristic but robust enough for Zillow MD)
        brokerage = left
        # remove leading 'at/with/from' etc
        brokerage = re.sub(r"^(at|with|from)\s+", "", brokerage, flags=re.I).strip()
        agent_name = right if right else None
        agent["brokerage"] = brokerage or None
        agent["agent_name"] = agent_name or None
    else:
        # no phone: try split by hyphen
        chunks = re.split(r"\s[-–|]\s", line)
        if chunks:
            agent["brokerage"] = chunks[0].strip()
            if len(chunks) > 1:
                agent["agent_name"] = chunks[1].strip()

    return agent

def _extract_monthly_costs(blob_md: str) -> Dict[str, Any]:
    """
    Parse the 'Monthly cost' block into a dict suitable for both
    market.other_costs and extras.monthly_costs.
    """
    mc: Dict[str, Any] = {}
    m_mc = re.search(r"##\s*Monthly cost[\s\S]+?(?:\n##|\Z)", blob_md, re.I)
    if not m_mc:
        return mc
    sect = m_mc.group(0)

    def pick(label: str, key: str):
        mm = re.search(rf"{label}\s*\$([\d,]+)", sect, re.I)
        if mm:
            mc[key] = safe_float(mm.group(1).replace(",", ""))

    pick("Principal & interest", "principal_interest")
    pick("Mortgage insurance", "mortgage_insurance")
    pick("Property taxes", "property_taxes")
    pick("Home insurance", "home_insurance")
    pick("HOA fees", "hoa_fees")

    # utilities line (e.g., "Utilities Not included")
    mu = re.search(r"Utilities\s*([^\n]+)", sect, re.I)
    if mu:
        mc["utilities"] = mu.group(1).strip()

    mc["currency"] = "USD"
    return mc


# =========================
# Main parser
# =========================

def parse_detail(url: str, html: str, markdown: Optional[str]):
    og = extract_og_meta(html)
    title = extract_title(html) or ""
    blob_md = markdown or ""

    # init
    addr = unit = city = state = None
    postal: Optional[int] = None
    lat = lon = None
    ptype = subtype = cond = None
    year_built = None
    list_price = None
    status = None
    list_date = None
    listing_type = None
    description = None
    beds = baths = None
    interior_area = None
    js_state_hit = False

    # --- JSON-LD ---
    jlds = extract_json_ld(html)
    for j in jlds:
        a = j.get("address") if isinstance(j.get("address"), dict) else None
        if a:
            addr = pick_nonempty(addr, a.get("streetAddress"))
            city = pick_nonempty(city, a.get("addressLocality"))
            state = pick_nonempty(state, a.get("addressRegion"))
            if a.get("postalCode") and postal is None:
                postal = safe_int(str(a.get("postalCode")).strip())
        g = j.get("geo") if isinstance(j.get("geo"), dict) else None
        if g:
            lat = pick_nonempty(lat, safe_float(g.get("latitude")))
            lon = pick_nonempty(lon, safe_float(g.get("longitude")))
        beds = pick_nonempty(beds, safe_float(j.get("numberOfRooms") or j.get("numberOfBedrooms")))
        baths = pick_nonempty(baths, safe_float(j.get("numberOfBathroomsTotal") or j.get("numberOfBathrooms")))
        if isinstance(j.get("floorSize"), dict):
            interior_area = pick_nonempty(interior_area, safe_int(j["floorSize"].get("value")))
        ptype = pick_nonempty(ptype, j.get("@type"), j.get("propertyType"))
        if j.get("yearBuilt"):
            year_built = safe_int(j["yearBuilt"])
        offers = j.get("offers")
        if isinstance(offers, dict):
            price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
            if price:
                list_price = pick_nonempty(list_price, safe_float(price))
            listing_type = pick_nonempty(listing_type, "sell")
        if isinstance(j.get("description"), str) and not description:
            description = j["description"]

    # --- hidden JS state ---
    js_objs = extract_js_variable_object(html, ["__REDUX_STATE__", "__INITIAL_STATE__"])
    for obj in js_objs:
        js_state_hit = True
        dump = str(obj)
        if not list_price:
            mp = re.search(r'"price"\s*:\s*"?([\d,\.]+)"?', dump)
            if mp: list_price = safe_float(mp.group(1))
        if not beds:
            mb = re.search(r'"beds"\s*:\s*"?([\d\.]+)"?', dump)
            if mb: beds = safe_float(mb.group(1))

    # --- Markdown fallbacks ---
    if list_price is None:
        list_price = _extract_max_price(blob_md)

    if not addr or not postal:
        a, c, s, z = _normalize_address_from_blob(blob_md)
        if a:
            addr, city, state, postal = a, c, s, z

    if beds is None:
        m = re.search(r"(\d+)\s*beds?", blob_md, re.I)
        if m: beds = safe_float(m.group(1))
    if baths is None:
        m = re.search(r"(\d+)\s*baths?", blob_md, re.I)
        if m: baths = safe_float(m.group(1))
    if interior_area is None:
        m = re.search(r"([\d,]+)\s*sqft", blob_md, re.I)
        if m: interior_area = safe_int(m.group(1).replace(",",""))
    if year_built is None:
        m = re.search(r"Built in (\d{4})", blob_md, re.I)
        if m: year_built = safe_int(m.group(1))
    if not ptype:
        m = re.search(r"Single Family Residence|Condo|Apartment|Townhouse|House|Multi[- ]?Family", blob_md, re.I)
        if m: ptype = m.group(0)

    # status (normalize to lower)
    if not status:
        m = re.search(r"\bActive\b|\bPending\b|\bContingent\b|\bSold\b|\bWithdrawn\b", blob_md, re.I)
        if m: status = m.group(0).lower()

    # description (multi-line)
    if not description:
        m = re.search(r"## (?:What's special|Description)\s*\n+([\s\S]+?)(?:\n##|\Z)", blob_md, re.I)
        if m: description = m.group(1).strip()

    # engagement (allow thousands with commas and missing spaces)
    market_eng: Dict[str, Any] = {"views": None, "saves": None, "shares": None}
    m_days = re.search(r"\*\*([\d,]+)\*\*\s*days", blob_md, re.I)
    if m_days: market_eng["days_on_zillow"] = safe_int(m_days.group(1).replace(",",""))
    m_views = re.search(r"\*\*([\d,]+)\*\*\s*views", blob_md, re.I)
    if m_views: market_eng["views"] = safe_int(m_views.group(1).replace(",", ""))
    m_saves = re.search(r"\*\*([\d,]+)\*\*\s*saves", blob_md, re.I)
    if m_saves: market_eng["saves"] = safe_int(m_saves.group(1).replace(",", ""))

    # monthly cost breakdown (market.other_costs) + copy to extras.monthly_costs
    monthly_costs = _extract_monthly_costs(blob_md)

    # agent info
    agent_info = _extract_agent_info(blob_md)

    # images (property photos only)
    image_urls = _extract_markdown_images(blob_md)

    # normalize property type to enum-ish value
    ptype_norm = _norm_property_type(ptype)

    # --- assemble ---
    ext_id = _try_external_id(url, html)
    addr_hash = hash_address(addr) if addr else None
    external_property_id = ext_id or (f"addr:{addr_hash}" if addr_hash else None)

    # ensure postal_code is int (nullable allowed)
    postal_int = safe_int(str(postal)) if postal is not None else None

    prop = PropStable(
        external_property_id=external_property_id,
        address=addr,
        unit_number=None,
        city=city,
        state=state,
        postal_code=postal_int,
        latitude=lat,
        longitude=lon,
        beds=beds,
        baths=baths,
        interior_area=interior_area,
        property_type=ptype_norm,
        property_subtype=None,
        condition=None,
        year_built=year_built,
    )

    # days_on_market from engagement if available
    dom = market_eng.get("days_on_zillow")

    other = {"listing_type": listing_type, "description": description}
    market = {
        "status": status,                     # lower-case per spec
        "list_date": _parse_iso_like(list_date),
        "days_on_market": dom,
        "list_price": list_price,
        "price_per_sqft": (round(list_price / interior_area, 2) if list_price and interior_area else None),
        "other_costs": {k: v for k, v in monthly_costs.items() if k != "currency"},  # keep currency only in extras
        "engagement": market_eng,
    }
    media_items = [{"kind": "image", "url": u, "caption": None} for u in image_urls]
    extras = {
        "title": title,
        "json_ld_count": len(jlds),
        "js_state_hit": js_state_hit,
        "agent": agent_info,
        "monthly_costs": monthly_costs,
    }

    return prop, other, market, media_items, extras
