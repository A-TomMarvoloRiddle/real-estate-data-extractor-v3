# settings.py
from __future__ import annotations
import os
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# -------- paths --------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
ENV_PATH = PROJECT_ROOT / ".env"

# -------- tiny .env loader (no external deps) --------

def _load_dotenv(path: Path) -> None:
    """
    Minimal dotenv loader: reads KEY=VALUE pairs (ignores comments/empty lines).
    Only sets keys that are not already present in os.environ.
    """
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            # strip optional quotes
            v = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = v
    except Exception:
        # fail silently; we still allow env vars coming from the OS
        pass

# load .env at import time
_load_dotenv(ENV_PATH)

# -------- defaults --------

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC") or 30)
# (lo, hi) jitter range in seconds
SLEEP_RANGE_SEC = (
    float((os.getenv("SLEEP_LO") or 1.2)),
    float((os.getenv("SLEEP_HI") or 2.8)),
)

default_headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# -------- helpers --------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_batch_dirs(base: Path) -> None:
    (base / "raw").mkdir(parents=True, exist_ok=True)


def load_listings_config() -> Dict[str, Any]:
    cfg_path = CONFIG_DIR / "listings_config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config file: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_firecrawl_api_key() -> Optional[str]:
    """
    Primary source: environment (possibly loaded from .env).
    Fallback: 'secrets.firecrawl_api_key' in config/listings_config.json if present.
    """
    key = os.getenv("FIRECRAWL_API_KEY")
    if key:
        return key
    try:
        cfg = load_listings_config()
        secrets = cfg.get("secrets") or {}
        if isinstance(secrets, dict):
            alt = secrets.get("firecrawl_api_key")
            if alt:
                # also export to process env for consistency
                os.environ["FIRECRAWL_API_KEY"] = str(alt)
                return str(alt)
    except Exception:
        pass
    return None
