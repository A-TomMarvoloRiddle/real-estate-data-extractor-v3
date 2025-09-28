# steps/fetch_details.py
from __future__ import annotations

import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone

from ..firecrawl_client import Firecrawl
from ..parsers import parse_detail, PropStable
from ..settings import now_utc_iso
from ..utils import jitter_sleep, write_json
from ..schemas import (
    PropertyStable,
    ListingRow,
    MediaItem,
    PriceHistoryRow,
    AgentRow,
    MonthlyCostRow,
)

log = logging.getLogger(__name__)

# -------- helpers --------

def _parse_date_safe(iso_like: Optional[str]) -> Optional[datetime]:
    if not iso_like:
        return None
    try:
        return datetime.fromisoformat(iso_like.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _compute_days_on_market(list_date: Optional[str]) -> Optional[int]:
    dt = _parse_date_safe(list_date)
    if not dt:
        return None
    delta = datetime.now(timezone.utc) - dt
    return max(0, delta.days)

def _reason_for_reject(prop: PropStable, market: dict) -> Optional[str]:
    if not (prop.city or prop.postal_code):
        return "missing_location"
    if not market.get("list_price"):
        return "missing_price"
    if not any([prop.beds, prop.baths, prop.interior_area]):
        return "missing_specs"
    return None

# -------- main --------

def fetch_and_parse_details(
    fc: Firecrawl,
    detail_urls: List[str],
    batch_root: Path,
    crawl_method: str,
    sleep_range: Tuple[float, float],
    start_listing_id: int = 1000
) -> Tuple[Dict[str, dict], List[dict], List[dict], List[dict]]:
    raw_dir = batch_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    properties: Dict[str, dict] = {}
    listings: List[dict] = []
    media_rows: List[dict] = []
    price_history: List[dict] = []
    agent_rows: List[dict] = []
    monthly_rows: List[dict] = []
    rejects: List[dict] = []

    listing_id = start_listing_id
    for url in detail_urls:
        log.info(f"[detail] fetching listing_id={listing_id} url={url}")
        try:
            # Firecrawl v1: no only_main_content/params kwargs
            res = fc.scrape(url)
        except Exception as e:
            log.error(f"[detail] scrape failed url={url} err={e}")
            rejects.append({
                "listing_id": listing_id,
                "url": url,
                "reason": f"scrape_error:{e.__class__.__name__}"
            })
            listing_id += 1
            jitter_sleep(*sleep_range)
            continue

        # normalize response
        html = ""
        md = ""
        if isinstance(res, dict):
            html = (res.get("html") or res.get("raw_html") or "") or ""
            md = res.get("markdown") or ""
        else:
            log.warning(f"[detail] unexpected firecrawl response type: {type(res)}")

        # save raw
        try:
            (raw_dir / f"{listing_id}_raw.html").write_text(html, encoding="utf-8")
            (raw_dir / f"{listing_id}.md").write_text(md, encoding="utf-8")
        except Exception as e:
            log.warning(f"[detail] failed to write raw files listing_id={listing_id}: {e}")

        # parse
        try:
            prop, other, market, media, extras = parse_detail(url, html, md)
        except Exception as e:
            log.error(f"[detail] parse_detail crashed listing_id={listing_id} url={url} err={e}")
            rejects.append({
                "listing_id": listing_id,
                "url": url,
                "reason": f"parse_error:{e.__class__.__name__}"
            })
            listing_id += 1
            jitter_sleep(*sleep_range)
            continue

        reason = _reason_for_reject(prop, market)
        if reason:
            rejects.append({
                "listing_id": listing_id,
                "url": url,
                "reason": reason
            })
            listing_id += 1
            jitter_sleep(*sleep_range)
            continue

        # keep existing days_on_market from parser when present; otherwise derive from list_date
        if market.get("days_on_market") is None:
            dom = _compute_days_on_market(market.get("list_date"))
            if dom is not None:
                market["days_on_market"] = dom

        dedup_key = prop.external_property_id or hashlib.md5(url.encode()).hexdigest()[:10]

        if dedup_key not in properties:
            properties[dedup_key] = vars(prop).copy()
            properties[dedup_key]["dedup_key"] = dedup_key

        listings.append({
            "listing_id": listing_id,
            "source_id": "zillow" if "zillow.com" in url else "redfin",
            "source_url": url,
            "crawl_method": crawl_method,
            "scraped_timestamp": now_utc_iso(),
            "batch_id": batch_root.name,
            "dedup_key": dedup_key,
            "listing_type": other.get("listing_type"),
            "description": other.get("description"),
            "status": (market.get("status") or None),
            "list_date": market.get("list_date"),
            "days_on_market": market.get("days_on_market"),
            "list_price": market.get("list_price"),
            "price_per_sqft": market.get("price_per_sqft"),
            "engagement": market.get("engagement") or {},
            "other_costs": market.get("other_costs") or {},
            "extras": extras or {},
        })

        for m in media or []:
            url_m = m.get("url")
            if url_m:
                media_rows.append({
                    "listing_id": listing_id,
                    "dedup_key": dedup_key,
                    "kind": m.get("kind") or "image",
                    "url": url_m,
                    "caption": m.get("caption"),
                })

        # --- optional tables from extras ---
        # Agent
        agent_info = (extras or {}).get("agent")
        if isinstance(agent_info, dict) and any(agent_info.get(k) for k in ("brokerage", "agent_name", "phone")):
            try:
                agent_rows.append(AgentRow(
                    listing_id=listing_id,
                    dedup_key=dedup_key,
                    brokerage=agent_info.get("brokerage"),
                    agent_name=agent_info.get("agent_name"),
                    phone=agent_info.get("phone"),
                    source_id="zillow" if "zillow.com" in url else "redfin",
                    source_url=url,
                ).dict())
            except Exception as e:
                log.debug(f"[detail] skip agent row listing_id={listing_id}: {e}")

        # Monthly Costs
        monthly_costs = (extras or {}).get("monthly_costs")
        if isinstance(monthly_costs, dict) and any(
            monthly_costs.get(k) for k in (
                "principal_interest","mortgage_insurance","property_taxes","home_insurance","hoa_fees"
            )
        ):
            try:
                monthly_rows.append(MonthlyCostRow(
                    listing_id=listing_id,
                    dedup_key=dedup_key,
                    principal_interest=monthly_costs.get("principal_interest"),
                    mortgage_insurance=monthly_costs.get("mortgage_insurance"),
                    property_taxes=monthly_costs.get("property_taxes"),
                    home_insurance=monthly_costs.get("home_insurance"),
                    hoa_fees=monthly_costs.get("hoa_fees"),
                    utilities=monthly_costs.get("utilities"),
                    # currency is fixed in schema default
                ).dict())
            except Exception as e:
                log.debug(f"[detail] skip monthly row listing_id={listing_id}: {e}")

        listing_id += 1
        jitter_sleep(*sleep_range)

    # QA rejects file
    if rejects:
        write_json(batch_root / "qa_rejects.json", {"count": len(rejects), "items": rejects})

    # optional tables
    if agent_rows:
        write_json(batch_root / "agents.json", agent_rows)
    if monthly_rows:
        write_json(batch_root / "monthly_costs.json", monthly_rows)

    return properties, listings, media_rows, price_history
