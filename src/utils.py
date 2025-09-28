# utils.py
from __future__ import annotations
import json
import random
import time
import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Any

# -------- JSON helpers --------

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# -------- safe conversions --------

def safe_int(x) -> int | None:
    try:
        if x is None or (isinstance(x, str) and not x.strip()):
            return None
        return int(str(x).replace(",", "").strip())
    except Exception:
        return None

def safe_float(x) -> float | None:
    try:
        if x is None or (isinstance(x, str) and not x.strip()):
            return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

# -------- timing --------

def jitter_sleep(lo: float, hi: float) -> None:
    """Sleep random uniform between lo and hi seconds."""
    time.sleep(random.uniform(lo, hi))

# -------- dirs --------

def ensure_dir(path: Path) -> Path:
    """Ensure directory exists and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path

# -------- hashing helpers --------

_ADDR_CLEAN_RE = re.compile(r"[^a-z0-9]+", re.I)

def _normalize_addr(addr: str) -> str:
    # normalize unicode, lowercase, strip, collapse whitespace/punct
    s = unicodedata.normalize("NFKD", addr or "").strip().lower()
    s = _ADDR_CLEAN_RE.sub(" ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def hash_address(addr: str | None, length: int = 12) -> str | None:
    """
    Stable short hash for address strings used as dedup keys fallback.
    Returns None if addr is falsy.
    """
    if not addr:
        return None
    norm = _normalize_addr(addr)
    h = hashlib.md5(norm.encode("utf-8")).hexdigest()
    return h[:length]
