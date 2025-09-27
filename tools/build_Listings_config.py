# build_config.py
# Enrich CFG areas with Zillow & Redfin URLs automatically (no manual editing).
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ---- Defaults ----
DEFAULT_CONFIG_PATHS = [
    Path("config/listings_config.json"),   # preferred
    Path("listings_config.json"),          # fallback
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ===================== basic IO =====================

def _load_config(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _dump_config(path: Path, obj: Dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def resolve_config_path(explicit: Optional[str]) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        return p
    for p in DEFAULT_CONFIG_PATHS:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find listings_config.json (looked in config/ and current dir).")

# ===================== helpers =====================

def _norm_city_dash(city: str) -> str:
    v = re.sub(r"\s+", "-", (city or "").strip())
    v = re.sub(r"[^A-Za-z0-9\-]", "", v)
    return v

def _zillow_url(city: str, state_code: str) -> str:
    return f"https://www.zillow.com/{_norm_city_dash(city).lower()}-{state_code.upper()}/"

def _rf_headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.redfin.com/",
    }

# ===================== Redfin lookup =====================

def _redfin_autocomplete(city: str, state_code: str) -> Optional[str]:
    """
    Resolve a Redfin city URL by:
      1) Bootstrapping a session (homepage) to get cookies,
      2) Calling 'stingray/do/location-autocomplete' with same session,
      3) Picking first CITY row and returning its URL or building from id.
    Returns None if blocked or not found.
    """
    q = f"{city}, {state_code}"
    ac_url = "https://www.redfin.com/stingray/do/location-autocomplete"
    params = {"location": q, "start": 0, "count": 10, "v": 2}

    s = requests.Session()
    s.headers.update(_rf_headers())

    # 1) bootstrap cookies
    try:
        home = s.get("https://www.redfin.com/", timeout=20)
        home.raise_for_status()
    except requests.RequestException as e:
        print(f"[warn] Redfin bootstrap failed for {q}: {e}")
        return None

    # 2) autocomplete (with optional XHR-like headers on 403)
    try:
        r = s.get(ac_url, params=params, timeout=20)
        if r.status_code == 403:
            xhr_headers = _rf_headers()
            xhr_headers.update({
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://www.redfin.com",
            })
            r = s.get(ac_url, params=params, headers=xhr_headers, timeout=20)

        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        print(f"[warn] Redfin lookup failed for {q}: {e}")
        return None
    except ValueError:
        # JSON decode error
        print(f"[warn] Redfin lookup failed for {q}: invalid JSON")
        return None

    payload = data.get("payload") or {}
    sections = payload.get("sections") or []
    rows: List[Dict] = []
    for sec in sections:
        rows.extend(sec.get("rows", []))

    for row in rows:
        if (row.get("type") or "").upper() != "CITY":
            continue
        # prefer direct url
        u = row.get("url")
        if isinstance(u, str):
            return f"https://www.redfin.com{u}" if u.startswith("/") else u
        # build from id
        city_id = row.get("id") or row.get("cityId") or row.get("cityID")
        if city_id:
            cdash = _norm_city_dash(city)
            return f"https://www.redfin.com/city/{city_id}/{state_code.upper()}/{cdash}"
    return None

# ===================== cache support =====================

def _load_redfin_cache(cache_path: Optional[str]) -> Dict[str, int]:
    if not cache_path:
        return {}
    p = Path(cache_path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

# ===================== enrichment =====================

def enrich_config(cfg: Dict, cache_map: Dict[str, int]) -> Tuple[Dict, List[str]]:
    """
    Returns (new_cfg, logs). Order of Redfin resolution:
      1) If area.redfin_url exists → keep it.
      2) Else if cache contains "City, ST" → compose /city/{id}/{ST}/{City-Dash}.
      3) Else try _redfin_autocomplete(city, state_code).
      4) Else warn and skip (do not break pipeline).
    Also fills zillow_url if missing.
    """
    logs: List[str] = []
    areas = cfg.get("areas") or []
    changed = False

    for i, area in enumerate(areas):
        city = (area.get("city") or "").strip()
        state_code = (area.get("state_code") or area.get("state") or "").strip()
        if not city or not state_code:
            logs.append(f"[skip] areas[{i}] missing city/state_code")
            continue

        # Always ensure Zillow URL
        if not area.get("zillow_url"):
            zurl = _zillow_url(city, state_code)
            area["zillow_url"] = zurl
            changed = True
            logs.append(f"[zillow] areas[{i}] → {zurl}")

        # Redfin priority chain
        if area.get("redfin_url"):
            logs.append(f"[redfin.keep] areas[{i}] keep existing")
        else:
            key = f"{city}, {state_code}"
            cid = cache_map.get(key)
            if cid:
                rfurl = f"https://www.redfin.com/city/{cid}/{state_code.upper()}/{_norm_city_dash(city)}"
                area["redfin_url"] = rfurl
                changed = True
                logs.append(f"[redfin.cache] areas[{i}] → {rfurl}")
            else:
                rf = _redfin_autocomplete(city, state_code)
                if rf:
                    area["redfin_url"] = rf
                    changed = True
                    logs.append(f"[redfin.auto] areas[{i}] → {rf}")
                else:
                    logs.append(f"[warn] areas[{i}] Redfin lookup failed for {key}")

    if changed:
        cfg["areas"] = areas
    return cfg, logs

# ===================== CLI =====================

def main(argv=None):
    ap = argparse.ArgumentParser(description="Enrich listings_config.json with Zillow & Redfin URLs")
    ap.add_argument("--config", type=str, help="Path to listings_config.json (input & default output)")
    ap.add_argument("--out", type=str, help="Write enriched config to this path (optional)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write file; just print changes")
    ap.add_argument("--commit", action="store_true", help="Write back to file (default if --out provided)")
    ap.add_argument("--cache", type=str, help="Path to a JSON map: {'City, ST': city_id, ...} (optional)")
    args = ap.parse_args(argv)

    # Load inputs
    cfg_path = resolve_config_path(args.config)
    cfg = _load_config(cfg_path)
    cache_map = _load_redfin_cache(args.cache)

    # Enrich
    new_cfg, logs = enrich_config(cfg, cache_map)

    # Decide output path
    out_path = Path(args.out) if args.out else cfg_path

    # Dry run?
    if args.dry_run and not args.commit and not args.out:
        print(f"== DRY RUN (no write) ==\nConfig: {cfg_path}\nWould write to: {out_path}\n")
        for line in logs:
            print(line)
        return 0

    # Write
    _dump_config(out_path, new_cfg)
    print(f"[build-config] wrote: {out_path}")
    for line in logs:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
