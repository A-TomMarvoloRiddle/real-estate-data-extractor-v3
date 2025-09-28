# extractors.py
# Robust helpers & extractors used across the pipeline.
# - Exposes regex constants used by parsers.py
# - Extracts JSON-LD safely (tolerant to minor JSON issues)
# - Extracts hidden JS state (e.g., window.__REDUX_STATE__ / __INITIAL_STATE__)
# - Provides simple meta & utility helpers

from __future__ import annotations
import re
import json
import html
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ========================= Regex constants (API relied on by parsers.py) =========================

PRICE_RE = re.compile(r"\$?\s?([0-9]{1,3}(?:[,\s][0-9]{3})+|\d+)(?:\.\d{1,2})?")
BEDS_RE  = re.compile(r"(\d+(?:\.\d+)?)\s*bed", re.I)
BATHS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*bath", re.I)
AREA_RE  = re.compile(r"([\d,]+)\s*(?:sq\s?ft|ft²|square\s?feet)", re.I)
ZIP_RE   = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
CITY_STATE_RE = re.compile(r"([A-Za-z .'-]+),\s*([A-Z]{2})\b")
ZPID_RE  = re.compile(r"/(\d+)_zpid", re.I)
REDFIN_ID_RE = re.compile(r"/home/(\d+)", re.I)

# ========================= Utilities =========================

def pick_nonempty(*vals):
    """Return the first non-empty (non-blank string, non-None) value."""
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str):
            s = v.strip()
            if s:
                return s
        else:
            return v
    return None

# ========================= Meta tag & title helpers =========================

_META_RE = re.compile(
    r'<meta\s+(?:property|name)=["\'](?P<k>[^"\']+)["\']\s+content=["\'](?P<v>[^"\']*)["\']\s*/?>',
    re.I
)

_TITLE_RE = re.compile(r'<title[^>]*>(?P<t>.*?)</title>', re.I | re.S)

def extract_og_meta(html_text: str) -> Dict[str, str]:
    metas: Dict[str, str] = {}
    for m in _META_RE.finditer(html_text or ""):
        k = m.group("k").strip().lower()
        v = html.unescape(m.group("v")).strip()
        metas[k] = v
    return metas

def extract_title(html_text: str) -> Optional[str]:
    m = _TITLE_RE.search(html_text or "")
    if not m:
        return None
    return re.sub(r"\s+", " ", html.unescape(m.group("t"))).strip()

# ========================= Safe JSON from JS helpers =========================

def _find_balanced_braces(s: str, start_pos: int = 0) -> Optional[str]:
    """Return the substring that is a balanced {...} starting from first '{' at/after start_pos."""
    i = s.find("{", start_pos)
    if i == -1:
        return None
    depth = 0
    in_str: Optional[str] = None
    esc = False
    for j in range(i, len(s)):
        ch = s[j]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch in ('"', "'"):
            if not in_str:
                in_str = ch
            elif in_str == ch:
                in_str = None
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return s[i:j+1]
    return None

def _try_clean_js_json(text: str) -> str:
    """Heuristic cleanup to turn JS-ish objects into valid(er) JSON."""
    # strip comments
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)
    text = re.sub(r'//.*?(?=\n|$)', '', text)
    # remove trailing commas before } or ]
    text = re.sub(r',\s*(?=[}\]])', '', text)
    # 'key': -> "key":
    text = re.sub(r'\'([A-Za-z0-9_\-]+)\'\s*:', r'"\1":', text)
    # 'value' (simple) -> "value"
    text = re.sub(r'(?<=[:\[,]\s*)\'([^\'\\]*(?:\\.[^\'\\]*)*)\'(?=\s*[,}\]])', r'"\1"', text)
    return text

def safe_json_loads(s: str):
    """Try multiple strategies to parse possibly JS-ish content to JSON (dict/list)."""
    if not s:
        return None
    # direct
    try:
        return json.loads(s)
    except Exception:
        pass
    # cleaned
    try:
        cleaned = _try_clean_js_json(s)
        return json.loads(cleaned)
    except Exception:
        pass
    # balanced slice
    obj_slice = _find_balanced_braces(s)
    if obj_slice:
        try:
            cleaned = _try_clean_js_json(obj_slice)
            return json.loads(cleaned)
        except Exception:
            pass
    return None

def extract_js_variable_object(html_text: str, var_identifiers: List[str]) -> List[Dict[str, Any]]:
    """
    Extract assignments like:
      window.__REDUX_STATE__ = {...};
      __INITIAL_STATE__ = {...}
      var SOME = {...}
    Returns list of parsed dicts (may be empty).
    """
    out: List[Dict[str, Any]] = []
    if not html_text:
        return out

    for v in var_identifiers:
        for m in re.finditer(re.escape(v), html_text, re.I):
            # look for '=' near identifier (forward preference)
            eqpos = html_text.find("=", m.end(), m.end() + 200)
            if eqpos == -1:
                # maybe assignment earlier
                eqpos = html_text.find("=", max(0, m.start() - 40), m.end() + 240)
                if eqpos == -1:
                    continue
            obj = _find_balanced_braces(html_text, eqpos)
            if not obj:
                continue
            parsed = safe_json_loads(obj)
            if isinstance(parsed, dict):
                out.append(parsed)
    return out

# ========================= JSON-LD extraction =========================

_SCRIPT_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(?P<blob>.*?)</script>',
    re.I | re.S,
)

def extract_json_ld(html_text: str) -> List[Dict[str, Any]]:
    """Extract & parse all <script type="application/ld+json"> blocks, robustly."""
    out: List[Dict[str, Any]] = []
    if not html_text:
        return out

    for m in _SCRIPT_LD_RE.finditer(html_text):
        blob = html.unescape(m.group("blob")).strip()
        if not blob:
            continue
        parsed = safe_json_loads(blob)
        if parsed is None:
            logger.debug("extract_json_ld: failed to parse a blob")
            continue
        if isinstance(parsed, dict):
            out.append(parsed)
        elif isinstance(parsed, list):
            out.extend([x for x in parsed if isinstance(x, dict)])
        # ignore other types
    return out
