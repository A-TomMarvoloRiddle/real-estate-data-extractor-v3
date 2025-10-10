"""Standalone property listing scrapers - Basic and Full tiers.

Basic Tier: Fast image extraction only (up to 6 images)
Full Tier: Full property details + comprehensive image gallery (all images)
"""

import os
import re
import json
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin, urlparse

from firecrawl import Firecrawl


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

IMAGE_EXTENSIONS = re.compile(
    r"(\.jpg|\.jpeg|\.png|\.webp|\.avif|\.heic|\.gif)(\?|$)", 
    re.IGNORECASE
)


def detect_site(url: str) -> str:
    """Detect site type from URL."""
    host = urlparse(url).netloc.lower()
    if "zillow" in host:
        return "zillow"
    if "redfin" in host:
        return "redfin"
    if "compass.com" in host:
        return "compass"
    return "unknown"


def is_likely_listing_image(url: str, site_type: str) -> bool:
    """Check if URL is likely a listing image."""
    if not url or not isinstance(url, str):
        return False
    if not url.startswith("http"):
        return False

    # Site-specific domain checks
    if site_type == "zillow":
        if "photos.zillowstatic.com" not in url:
            return False
    elif site_type == "redfin":
        if "ssl.cdn-redfin.com" not in url:
            return False
    elif site_type == "compass":
        if "compass.com" not in url:
            return False
    else:
        if not IMAGE_EXTENSIONS.search(url):
            return False

    # Filter out logos and icons
    low = url.lower()
    if "logo" in low or "icon" in low:
        return False
    return True


def upgrade_zillow_image_url(url: str) -> str:
    """Upgrade Zillow image URLs to higher resolution versions."""
    if "photos.zillowstatic.com" not in url:
        return url

    # Upgrade low-res cc_ft URLs to high-res uncropped_scaled versions
    if any(size in url for size in ["cc_ft_576", "cc_ft_960", "cc_ft_768", "cc_ft_384"]):
        return re.sub(r"-cc_ft_\d+", "-uncropped_scaled_within_1536_1152", url)

    return url


def _quality_score_for_url(src: str) -> int:
    """Calculate quality score for image URL."""
    quality = 0
    if "origin.webp" in src:
        quality = 10
    elif "1500x1000" in src:
        quality = 8
    elif any(k in src for k in ["_xl", "large", "_lg"]):
        quality = 5
    elif any(k in src for k in ["_l", "medium", "_md"]):
        quality = 4
    elif any(k in src for k in ["_m", "_med"]):
        quality = 3
    elif any(k in src for k in ["_s", "small"]):
        quality = 2
    else:
        quality = 1

    # Adjust based on pixel dimensions
    m = re.search(r"([0-9]+)x([0-9]+)", src)
    if m:
        w = int(m.group(1))
        h = int(m.group(2))
        pixels = w * h
        if pixels > 1_000_000:
            quality += 2
        elif pixels > 500_000:
            quality += 1
        elif pixels < 50_000:
            quality = max(1, quality - 2)
    return quality


def deduplicate_compass_images(urls: List[str]) -> List[str]:
    """Deduplicate Compass images, keeping highest quality versions."""
    images_by_quality: Dict[str, tuple[str, int]] = {}
    enhanced_urls: List[str] = []
    
    # First, enhance small thumbnails to origin.webp
    for src in urls:
        if "165x165.webp" in src or ("x" in src and src.endswith(".webp") and re.search(r"/[0-9]+x[0-9]+\.webp$", src)):
            base = re.sub(r"/[0-9]+x[0-9]+\.webp$", "", src)
            enhanced_urls.append(base + "/origin.webp")
        else:
            enhanced_urls.append(src)

    # Deduplicate by base ID, keeping highest quality
    for src in enhanced_urls:
        base_id = src
        m_compass = re.search(r"([a-f0-9]{32,})_img_(\d+)_[a-f0-9]+", src)
        if m_compass:
            base_id = f"{m_compass.group(1)}_img_{m_compass.group(2)}"
        else:
            m_uuid = re.search(r"([a-f0-9-]{30,})", src)
            if m_uuid:
                base_id = m_uuid.group(1)
            else:
                m_path = re.search(r"/([^/]+)\.(jpg|jpeg|png|webp)", src, re.IGNORECASE)
                if m_path:
                    base_id = m_path.group(1)

        q = _quality_score_for_url(src)
        if base_id not in images_by_quality or images_by_quality[base_id][1] < q:
            images_by_quality[base_id] = (src, q)

    return [u for (u, _q) in images_by_quality.values()]


# ============================================================================
# IMAGE EXTRACTION
# ============================================================================

def extract_images_from_property_data(property_data: Dict[str, Any], site_type: str) -> List[str]:
    """Walk nested JSON to collect likely image URLs."""
    urls: List[str] = []

    def walk(val: Any):
        if not val:
            return
        if isinstance(val, str):
            s = val.strip()
            if is_likely_listing_image(s, site_type):
                urls.append(s)
            return
        if isinstance(val, list):
            for x in val:
                walk(x)
            return
        if isinstance(val, dict):
            for v in val.values():
                walk(v)

    walk(property_data)
    unique = list(dict.fromkeys(urls))

    # Upgrade Zillow URLs to higher resolution
    if site_type == "zillow":
        unique = [upgrade_zillow_image_url(u) for u in unique]

    if site_type == "compass":
        return deduplicate_compass_images(unique)
    
    return unique


def extract_images_from_html(page_url: str, site_type: str) -> List[str]:
    """Fetch page via Firecrawl and extract image URLs from HTML."""
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        return []

    try:
        fc = Firecrawl(api_key=api_key)
        doc = fc.scrape(
            page_url, 
            formats=["html"], 
            only_main_content=True, 
            max_age=3600000  # 1 hour cache
        )
        html = getattr(doc, "html", None) or ""
        if not html:
            return []
    except Exception:
        return []

    # Extract image URLs from HTML
    candidates: List[str] = []
    
    # Find <img src="...">
    for m in re.finditer(r"<img[^>]+src=\"([^\"]+)\"", html, re.IGNORECASE):
        src = m.group(1)
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = urljoin(page_url, src)
        elif not src.startswith("http"):
            src = urljoin(page_url, src)
        candidates.append(src)

    # Find data-src="..."
    for m in re.finditer(r"data-src=\"([^\"]+)\"", html, re.IGNORECASE):
        src = m.group(1)
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = urljoin(page_url, src)
        elif not src.startswith("http"):
            src = urljoin(page_url, src)
        candidates.append(src)

    # Filter and dedupe
    filtered = [u for u in candidates if is_likely_listing_image(u, site_type)]
    unique = list(dict.fromkeys(filtered))

    if site_type == "compass":
        return deduplicate_compass_images(unique)
    
    return unique


# ============================================================================
# BASIC TIER SCRAPER
# ============================================================================

def scrape_basic_tier(url: str) -> Dict[str, Any]:
    """
    BASIC TIER: Fast image-only extraction.
    
    Returns up to 6 images with minimal metadata.
    Uses 1-hour cache for faster performance.
    """
    api_key = os.getenv("FIRECRAWL_API_KEY", "")
    if not api_key:
        raise ValueError("FIRECRAWL_API_KEY environment variable is required")

    site_type = detect_site(url)
    fc = Firecrawl(api_key=api_key)

    # Fast image-only extraction
    prompt = "Extract all high-resolution property image URLs from this listing."
    
    try:
        from firecrawl.v2.types import JsonFormat
        doc = fc.scrape(
            url,
            formats=[JsonFormat(type="json", prompt=prompt)],
            only_main_content=True,
            max_age=3600000  # 1 hour cache for 500% speedup
        )
        
        property_data = getattr(doc, "json", None) or getattr(doc, "data", None) or {}
        if isinstance(property_data, str):
            try:
                property_data = json.loads(property_data)
            except Exception:
                property_data = {}
    except Exception as e:
        raise RuntimeError(f"Firecrawl extraction failed: {e}")

    # Extract initial images from property data
    images = extract_images_from_property_data(property_data, site_type)[:10]

    # If we got less than 4 images, try HTML fallback
    if len(images) < 4:
        html_images = extract_images_from_html(url, site_type)
        images = list(dict.fromkeys([*images, *html_images]))

    # Limit to 6 images for basic tier
    images = images[:6]

    return {
        "images": images,
        "image_count": len(images),
        "site_type": site_type,
        "tier": "basic",
        "extraction_source": "initial_only" if len(images) >= 4 else "property_data+html_fallback"
    }


# ============================================================================
# FULL TIER SCRAPER
# ============================================================================

def scrape_efficient_tier(url: str) -> Dict[str, Any]:
    """
    EFFICIENT TIER: Full property details with fast image extraction.
    
    Returns up to 6 high-quality images and detailed property information.
    Uses 1-hour cache for faster performance.
    """
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise ValueError("FIRECRAWL_API_KEY environment variable is required")

    site_type = detect_site(url)
    fc = Firecrawl(api_key=api_key)

    # Comprehensive property data extraction
    prompt = (
        "Extract detailed property information including full address (street, city, state, zip), "
        "price, bedrooms/beds, bathrooms/baths, square footage/sqft, property type, lot size, year built, "
        "days on market, MLS number, listing agent details (name, company/brokerage, phone, email), "
        "property description, and all image URLs."
    )
    
    try:
        from firecrawl.v2.types import JsonFormat
        doc = fc.scrape(
            url,
            formats=[JsonFormat(type="json", prompt=prompt)],
            only_main_content=True,
            max_age=3600000  # 1 hour cache
        )
        
        property_data = getattr(doc, "json", None) or getattr(doc, "data", None) or {}
        if isinstance(property_data, str):
            try:
                property_data = json.loads(property_data)
            except Exception:
                property_data = {}
    except Exception as e:
        raise RuntimeError(f"Firecrawl extraction failed: {e}")

    # Extract initial images from property data (limited to 6)
    initial_images = extract_images_from_property_data(property_data, site_type)[:6]

    # If we got less than 4 images, try HTML fallback
    if len(initial_images) < 4:
        html_images = extract_images_from_html(url, site_type)
        images = list(dict.fromkeys([*initial_images, *html_images]))[:6]
    else:
        images = initial_images

    # Normalize property data
    extracted = _normalize_property_data(property_data, site_type)

    return {
        "images": images,
        "image_count": len(images),
        "site_type": site_type,
        "tier": "efficient",
        "extraction_source": "initial_only" if len(initial_images) >= 4 else "property_data+html_fallback",
        "property_data": extracted,
        "raw_data": property_data
    }


def scrape_full_tier(url: str) -> Dict[str, Any]:
    """
    FULL TIER: Comprehensive property data + full image gallery.
    
    Returns all available images and detailed property information.
    Uses 1-hour cache for faster performance.
    """
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise ValueError("FIRECRAWL_API_KEY environment variable is required")

    site_type = detect_site(url)
    fc = Firecrawl(api_key=api_key)

    # Comprehensive property data extraction
    prompt = (
        "Extract detailed property information including full address (street, city, state, zip), "
        "price, bedrooms/beds, bathrooms/baths, square footage/sqft, property type, lot size, year built, "
        "days on market, MLS number, listing agent details (name, company/brokerage, phone, email), "
        "property description, and all image URLs."
    )
    
    try:
        from firecrawl.v2.types import JsonFormat
        doc = fc.scrape(
            url,
            formats=[JsonFormat(type="json", prompt=prompt)],
            only_main_content=True,
            max_age=3600000  # 1 hour cache
        )
        
        property_data = getattr(doc, "json", None) or getattr(doc, "data", None) or {}
        if isinstance(property_data, str):
            try:
                property_data = json.loads(property_data)
            except Exception:
                property_data = {}
    except Exception as e:
        raise RuntimeError(f"Firecrawl extraction failed: {e}")

    # Extract initial images from property data
    initial_images = extract_images_from_property_data(property_data, site_type)

    # Full gallery scrape from HTML
    all_images = extract_images_from_html(url, site_type)
    
    # Merge images (prioritize HTML gallery as it's usually more complete)
    images = list(dict.fromkeys([*all_images, *initial_images]))

    # Normalize property data
    extracted = _normalize_property_data(property_data, site_type)

    return {
        "images": images,
        "image_count": len(images),
        "site_type": site_type,
        "tier": "full",
        "extraction_source": "full_gallery",
        "property_data": extracted,
        "raw_data": property_data
    }


def _normalize_property_data(raw: Optional[Dict[str, Any]], site_type: str) -> Dict[str, Any]:
    """Normalize property data from different sources."""
    raw = raw or {}

    # Handle address - can be string or nested object
    address_value = raw.get("address")
    if isinstance(address_value, str):
        full_address = address_value
        city = raw.get("city", "")
        state = raw.get("state", "")
        zip_code = raw.get("zip") or raw.get("zipcode", "")
    elif isinstance(address_value, dict):
        full_address = address_value.get("street") or address_value.get("line1", "")
        city = address_value.get("city") or raw.get("city", "")
        state = address_value.get("state") or raw.get("state", "")
        zip_code = (address_value.get("zip") or address_value.get("zipcode") or raw.get("zip") or raw.get("zipcode", ""))
    else:
        full_address = raw.get("fullAddress", "")
        city = raw.get("city", "")
        state = raw.get("state", "")
        zip_code = raw.get("zip") or raw.get("zipcode", "")

    # Handle price - can be string, number, or nested object
    price_value = raw.get("price")
    if isinstance(price_value, str):
        price = price_value
    elif isinstance(price_value, (int, float)):
        price = f"${int(price_value):,}"
    elif isinstance(price_value, dict) and "amount" in price_value:
        price = f"${int(price_value['amount']):,}"
    else:
        price = ""

    return {
        "fullAddress": full_address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "price": price,
        "bedrooms": raw.get("bedrooms") or raw.get("beds", 0),
        "bathrooms": raw.get("bathrooms") or raw.get("baths", 0),
        "sqft": raw.get("sqft") or raw.get("square_footage") or raw.get("squareFeet", 0),
        "propertyType": raw.get("property_type") or raw.get("propertyType", "Property"),
        "lotSize": raw.get("lot_size") or raw.get("lotSize"),
        "yearBuilt": raw.get("year_built") or raw.get("yearBuilt"),
        "daysOnMarket": raw.get("days_on_market") or raw.get("daysOnMarket"),
        "mlsNumber": raw.get("mls_number") or raw.get("mlsNumber", ""),
        "description": (raw.get("description") or raw.get("propertyDescription") or raw.get("summary", "")),
        "agent": {
            "name": raw.get("agent", {}).get("name", "") if isinstance(raw.get("agent"), dict) else "",
            "company": raw.get("agent", {}).get("company", "") if isinstance(raw.get("agent"), dict) else "",
            "phone": raw.get("agent", {}).get("phone", "") if isinstance(raw.get("agent"), dict) else "",
            "email": raw.get("agent", {}).get("email", "") if isinstance(raw.get("agent"), dict) else ""
        },
        "siteType": site_type,
    }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python Mike_scrapers.py <url> [basic|efficient|full]")
        sys.exit(1)

    url = sys.argv[1]
    tier = sys.argv[2] if len(sys.argv) > 2 else "basic"

    if tier == "basic":
        result = scrape_basic_tier(url)
    elif tier == "efficient":
        result = scrape_efficient_tier(url)
    else:
        result = scrape_full_tier(url)

    # Print the result to console
    print(json.dumps(result, indent=2))

    # Save result to JSON file named <address>_result.json
    address = result.get("property_data", {}).get("fullAddress", "")
    if not address:
        # fallback to site type and tier if no address found
        address = f"{result.get('site_type', 'unknown')}_{tier}"
    # If address is a dict (unexpected), convert to string
    if isinstance(address, dict):
        address = json.dumps(address)
    # Clean address to be a valid filename
    filename = re.sub(r'[^a-zA-Z0-9_-]', '_', address) + "_result.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Results saved to {filename}")
