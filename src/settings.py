# src/settings.py
# Purpose: Centralized settings & helpers (load config, paths, headers, timestamps).

from __future__ import annotations
import json
import datetime
import os
from pathlib import Path
from typing import Dict, Any, Tuple

# ---------- config loading ----------
def get_project_root() -> Path:
    try:
        if "get_ipython" in globals():
            return Path(os.getcwd()).parent
        else:
            return Path(__file__).resolve().parent.parent
    except NameError:
        return Path(os.getcwd()).parent

PROJECT_ROOT = get_project_root()
CONFIG_PATH = PROJECT_ROOT / "config" / "listings_config.json"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.json"  

def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found at {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

CFG = load_config()

# ---------- runtime knobs ----------
REQUEST_TIMEOUT_SEC: int = int(CFG["run"].get("request_timeout_sec", 30))
SLEEP_RANGE_SEC: Tuple[float, float] = tuple(CFG["run"].get("sleep_range_sec", [1.2, 2.8]))
USER_AGENT: str = CFG["run"].get("user_agent", "Mozilla/5.0")

# ---------- convenience getters ----------
def get_target_areas() -> list[Dict[str, Any]]:
    """Return list of areas (cities, states, zips) from config."""
    return CFG.get("areas", [])

def get_seeds() -> Dict[str, Any]:
    """Return base seed URLs (zillow, redfin...) from config."""
    return CFG.get("seeds", {})

# ---------- time helpers ----------
def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def today_ymd() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")

# ---------- batch folders ----------
def make_batch_dirs(batch_id: str) -> Dict[str, Path]:
    base = PROJECT_ROOT / "data" / "batches" / batch_id
    raw = base / "raw"
    structured = base / "structured"
    qa = base / "qa"
    for p in (raw, structured, qa):
        p.mkdir(parents=True, exist_ok=True)
    return {"base": base, "raw": raw, "structured": structured, "qa": qa}

# ---------- HTTP headers ----------
def default_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }
