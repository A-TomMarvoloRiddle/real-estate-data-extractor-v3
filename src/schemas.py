# src/schemas.py
from __future__ import annotations
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, RootModel, validator

# -----------------------------
# Core row models (backward-compatible)
# -----------------------------

class PropertyStable(BaseModel):
    external_property_id: Optional[str] = Field(None, description="zpid/redfin or address hash")
    address: Optional[str] = None
    unit_number: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[int] = Field(None, description="5-digit ZIP")  # int per spec
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    interior_area: Optional[int] = None
    property_type: Optional[str] = None  # normalized string (e.g., single_family)
    property_subtype: Optional[str] = None
    condition: Optional[str] = None
    year_built: Optional[int] = None
    dedup_key: Optional[str] = None

    @validator("postal_code")
    def _zip_len(cls, v):
        if v is None:
            return v
        if v < 0 or v > 99999:
            raise ValueError("postal_code must be a 5-digit positive integer")
        return v


class ListingRow(BaseModel):
    listing_id: int
    source_id: str
    source_url: str
    crawl_method: str
    scraped_timestamp: str
    batch_id: str
    dedup_key: str

    # optional/derived
    listing_type: Optional[str] = None  # sell/rent
    description: Optional[str] = None

    # market
    status: Optional[str] = None
    list_date: Optional[str] = None   # YYYY-MM-DD
    days_on_market: Optional[int] = None
    list_price: Optional[float] = None
    price_per_sqft: Optional[float] = None

    engagement: Dict[str, Any] = Field(default_factory=dict)
    other_costs: Dict[str, Any] = Field(default_factory=dict)
    extras: Dict[str, Any] = Field(default_factory=dict)

    @validator("list_price")
    def _check_price(cls, v):
        if v is not None and v < 0:
            raise ValueError("list_price must be positive")
        return v


class MediaItem(BaseModel):
    listing_id: int
    dedup_key: str
    kind: str = "image"  # image|video|floorplan
    url: str
    caption: Optional[str] = None


class PriceHistoryRow(BaseModel):
    listing_id: int
    dedup_key: str
    date: Optional[str] = None  # YYYY-MM-DD
    price: Optional[float] = None
    note: Optional[str] = None


# -----------------------------
# NEW optional tables
# -----------------------------

class AgentRow(BaseModel):
    listing_id: int
    dedup_key: str
    brokerage: Optional[str] = None
    agent_name: Optional[str] = None
    phone: Optional[str] = None
    source_id: Optional[str] = None  # zillow/redfin
    source_url: Optional[str] = None


class MonthlyCostRow(BaseModel):
    listing_id: int
    dedup_key: str
    principal_interest: Optional[float] = None
    mortgage_insurance: Optional[float] = None
    property_taxes: Optional[float] = None
    home_insurance: Optional[float] = None
    hoa_fees: Optional[float] = None
    utilities: Optional[str] = None  # e.g., "Not included" if shown
    currency: str = "USD"


# -----------------------------
# File containers (RootModel)
# -----------------------------

class PropertiesFile(RootModel[List[PropertyStable]]): ...
class ListingsFile(RootModel[List[ListingRow]]): ...
class MediaFile(RootModel[List[MediaItem]]): ...
class PriceHistoryFile(RootModel[List[PriceHistoryRow]]): ...
class AgentsFile(RootModel[List[AgentRow]]): ...
class MonthlyCostsFile(RootModel[List[MonthlyCostRow]]): ...
