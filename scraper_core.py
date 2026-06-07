"""
scraper_core.py
================
Core logic for IndiaMart Lead Enricher.
Contains:
  - Address → locality keyword extraction
  - Serper API search (URL fetching)
  - IndiaMart URL validation & cleaning
  - Phone number extraction via requests + BeautifulSoup
  - Match type assignment

Designed to be imported by app.py; no Streamlit dependencies here.
"""

import re
import time
import random
import logging
from typing import Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("scraper_core")

# ──────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

SERPER_ENDPOINT = "https://google.serper.dev/search"

# IndiaMart's own helpline — never store this as a company phone
INDIAMART_HELPLINE_DIGITS = {"09696969696", "9696969696"}

# Domains to skip from Serper results (not IndiaMart seller pages)
SKIP_DOMAINS: set = {
    "google.com", "google.co.in", "googleapis.com",
    "youtube.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "linkedin.com", "wikipedia.org", "amazon.in", "amazon.com", "flipkart.com",
    "justdial.com", "sulekha.com", "tradeindia.com", "exportersindia.com",
    "yellowpages.in", "zaubacorp.com", "tofler.in", "dnb.com",
}

# IndiaMart URL filters
IM_INVALID_SUBDOMAINS = {"dir", "my", "m", "events", "connect", "buy", "sell",
                          "blog", "help", "developer", "api"}
IM_INVALID_PATH_STARTS = {"proddetail", "impcat", "city", "company", "isearch",
                            "search", "categories", "indianexporters", "buy", "sell",
                            "trade-leads", "trade_leads", "catalogs", "mcat"}
STRIP_PARAMS = {"srsltid", "utm_source", "utm_medium", "utm_campaign",
                "utm_term", "utm_content", "pos", "ref", "gclid"}

# Noise words for address parsing
NOISE_ADDRESS_WORDS = {
    "no", "plot", "flat", "floor", "house", "building", "bldg", "shop",
    "unit", "office", "near", "opp", "opposite", "behind", "above",
    "beside", "next", "road", "rd", "street", "st", "lane", "ln",
    "nagar", "nagr", "marg", "cross", "main", "block", "sector",
    "phase", "wing", "tower", "complex", "colony", "enclave", "layout",
    "extension", "ext", "industrial", "estate", "area", "zone", "park",
    "avenue", "ave", "square", "sq", "circle", "chowk", "gali", "wadi",
    "pvt", "ltd", "llp", "inc", "corp", "co", "and", "the", "of", "a",
    "east", "west", "north", "south", "new", "old",
}

# Realistic browser headers for scraping
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Indian phone regex patterns
PHONE_PATTERNS = [
    # Mobile: 6-9 followed by 9 digits (with optional +91 or 0 prefix)
    re.compile(r"(?<!\d)(?:\+91[-\s]?|0)?[6-9]\d{9}(?!\d)"),
    # STD landlines: 2-4 digit code + 6-8 digit number
    re.compile(r"(?<!\d)0[1-9]\d{1,3}[-\s]\d{6,8}(?!\d)"),
    # 08XXXXXXXX format (IndiaMart toll-free style)
    re.compile(r"(?<!\d)08\d{8}(?!\d)"),
]

# Regex patterns for locality extraction
_PINCODE_RE = re.compile(r"\b\d{6}\b")
_NUM_TOKEN_RE = re.compile(r"^[\d\-/]+[a-zA-Z]{0,2}$")

MAX_QUERIES_PER_COMPANY = 4
NUM_RESULTS_PER_QUERY = 5
MAX_RETRIES_ON_ERROR = 2


# ──────────────────────────────────────────────────────────────────────────────
#  CUSTOM EXCEPTIONS
# ──────────────────────────────────────────────────────────────────────────────

class QuotaExhaustedError(Exception):
    """Serper API monthly quota is exhausted."""

class InvalidKeyError(Exception):
    """Serper API key is invalid or inactive."""


# ──────────────────────────────────────────────────────────────────────────────
#  ADDRESS → LOCALITY KEYWORD EXTRACTOR
# ──────────────────────────────────────────────────────────────────────────────

def extract_locality_keywords(address: str, pincode: str = "") -> list:
    """
    Extracts up to 3 ranked locality keywords from an Indian address string.
    e.g. "201, Radhey Avenue 2, Jagabhai Park, Rambaug, Maninagar-380008"
         → ["Maninagar", "Rambaug", "Jagabhai Park"]
    """
    if not address:
        return []

    # Remove pincode digits
    clean = _PINCODE_RE.sub("", address)
    if pincode:
        clean = clean.replace(str(pincode).split(".")[0], "")

    fragments = re.split(r"[,/|\n]+", clean)
    candidates = []

    for frag in fragments:
        frag = frag.strip(" -\t")
        if not frag:
            continue

        frag = re.sub(r"\s+", " ", frag)
        words = frag.split()
        meaningful = [
            w for w in words
            if len(w) >= 3
            and not _NUM_TOKEN_RE.match(w)
            and w.lower() not in NOISE_ADDRESS_WORDS
        ]
        if not meaningful:
            continue

        cleaned_frag = " ".join(meaningful)
        if len(cleaned_frag) < 4:
            continue
        if re.match(r"^(no\.?|no\s|shed|plot|flat|unit|door)\b", cleaned_frag, re.I):
            continue

        candidates.append(cleaned_frag)

    # De-duplicate (case-insensitive)
    seen = set()
    unique = []
    for c in candidates:
        key = c.lower()
        if key not in seen:
            seen.add(key)
            unique.append(c)

    # Prefer multi-word (more specific), then longer
    unique.sort(key=lambda x: (-len(x.split()), -len(x)))
    return unique[:3]


# ──────────────────────────────────────────────────────────────────────────────
#  QUERY BUILDER
# ──────────────────────────────────────────────────────────────────────────────

def build_queries(name: str, pincode: str, address: str) -> list:
    """
    Returns a list of (query_string, match_type_label) in priority order.
    Stops on first hit — never fires unnecessary queries.

    Priority:
      Q1  "name" locality1 site:indiamart.com   ← most precise
      Q2  "name" pincode   site:indiamart.com   ← pincode fallback
      Q3  "name" locality2 site:indiamart.com   ← alt locality
      Q4  "name"           site:indiamart.com   ← broadest fallback
    """
    name = name.strip()
    pincode = str(pincode).strip().split(".")[0]
    localities = extract_locality_keywords(address, pincode)

    queries = []

    if localities:
        queries.append((
            f'"{name}" {localities[0]} site:indiamart.com',
            f"name+locality",
        ))

    if pincode and len(pincode) == 6:
        queries.append((
            f'"{name}" {pincode} site:indiamart.com',
            "name+pincode",
        ))

    if len(localities) >= 2:
        queries.append((
            f'"{name}" {localities[1]} site:indiamart.com',
            f"name+locality",
        ))

    queries.append((
        f'"{name}" site:indiamart.com',
        "name_only",
    ))

    return queries


# ──────────────────────────────────────────────────────────────────────────────
#  URL UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def clean_indiamart_url(url: str) -> str:
    """Strips tracking/noise params; returns canonical IndiaMart URL."""
    try:
        parsed = urlparse(url)
        qs = {k: v for k, v in parse_qs(parsed.query).items()
              if k.lower() not in STRIP_PARAMS}
        new_query = urlencode(qs, doseq=True)
        return urlunparse((
            "https", "www.indiamart.com",
            parsed.path.rstrip("/"),
            parsed.params, new_query, "",
        ))
    except Exception:
        return url


def is_valid_indiamart_profile(url: str) -> bool:
    """
    Returns True only for genuine seller-profile pages on indiamart.com.
    Validates domain, path structure, and slug format.
    """
    if not url or "indiamart.com" not in url.lower():
        return False

    url_lower = url.lower()
    try:
        parsed = urlparse(url)

        # Subdomain check — only allow www.indiamart.com or indiamart.com
        netloc = parsed.netloc.lower()
        host_parts = netloc.replace("www.", "").split(".")
        if len(host_parts) > 2:
            if host_parts[0] in IM_INVALID_SUBDOMAINS:
                return False

        # Must have a path (the seller slug)
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts:
            return False

        # First path segment must not be a system route
        first = path_parts[0].lower()
        if first in IM_INVALID_PATH_STARTS:
            return False

        # Full URL must not contain known bad fragments
        bad_fragments = [
            "/proddetail/", "/impcat/", "/categories/", "/indianexporters/",
            "/isearch.php", "/search.html", "/trade-leads", "/catalogs/",
            "/mcat/", "/buy-",
        ]
        if any(frag in url_lower for frag in bad_fragments):
            return False

        # Slug sanity: must contain at least one letter, not purely numeric
        slug = path_parts[0]
        if not re.search(r"[a-zA-Z]", slug):
            return False
        if re.match(r"^\d+$", slug):
            return False

        return True

    except Exception:
        return False


def _netloc(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def should_skip_domain(url: str) -> bool:
    if not url or not url.startswith("http"):
        return True
    h = _netloc(url)
    return any(h == d or h.endswith("." + d) for d in SKIP_DOMAINS)


def get_profile_url(url: str) -> str:
    """
    Converts any IndiaMart URL to its /profile.html canonical form.
    /members/<slug>  →  /<slug>/profile.html
    /<slug>          →  /<slug>/profile.html
    """
    url = url.strip().rstrip("/")
    base = "https://www.indiamart.com"

    m = re.match(r"https?://www\.indiamart\.com/members/([^/?#]+)", url)
    if m:
        return f"{base}/{m.group(1)}/profile.html"

    if url.endswith("/profile.html") or url.endswith("/profile"):
        return url

    m2 = re.match(r"https?://www\.indiamart\.com/([^/?#]+)/(.+)", url)
    if m2:
        slug = m2.group(1)
        if slug not in ("data2", "images"):
            return f"{base}/{slug}/profile.html"

    m3 = re.match(r"https?://www\.indiamart\.com/([^/?#]+)$", url)
    if m3:
        return f"{base}/{m3.group(1)}/profile.html"

    return url


# ──────────────────────────────────────────────────────────────────────────────
#  SERPER API
# ──────────────────────────────────────────────────────────────────────────────

def serper_search(query: str, api_key: str) -> list:
    """
    Calls Serper Google Search API and returns a list of result URLs.
    Raises QuotaExhaustedError or InvalidKeyError on those conditions.
    """
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "q":   query,
        "gl":  "in",   # India locale
        "hl":  "en",
        "num": NUM_RESULTS_PER_QUERY,
    }

    for attempt in range(1, MAX_RETRIES_ON_ERROR + 2):
        try:
            resp = requests.post(
                SERPER_ENDPOINT, headers=headers, json=payload, timeout=30
            )

            if resp.status_code == 401:
                raise InvalidKeyError("Serper API key is invalid or not activated.")
            if resp.status_code == 403:
                raise InvalidKeyError("Serper API key forbidden — check your account.")
            if resp.status_code == 429:
                raise QuotaExhaustedError("Serper API monthly credit quota exhausted.")

            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                err = str(data["error"]).lower()
                if any(k in err for k in ("quota", "limit", "credit", "insufficient")):
                    raise QuotaExhaustedError(f"Serper quota error: {data['error']}")
                log.warning("Serper API body error: %s", data["error"])
                return []

            return [r.get("link", "") for r in data.get("organic", []) if r.get("link")]

        except (QuotaExhaustedError, InvalidKeyError):
            raise
        except requests.exceptions.Timeout:
            log.warning("Timeout attempt %d/%d", attempt, MAX_RETRIES_ON_ERROR + 1)
            if attempt <= MAX_RETRIES_ON_ERROR:
                time.sleep(3 * attempt)
        except requests.exceptions.RequestException as exc:
            log.warning("Network error attempt %d: %s", attempt, exc)
            if attempt <= MAX_RETRIES_ON_ERROR:
                time.sleep(3 * attempt)

    return []


def validate_api_key(api_key: str) -> tuple:
    """
    Quick validation of Serper API key.
    Returns (is_valid: bool, message: str).
    """
    if not api_key or len(api_key.strip()) < 10:
        return False, "API key too short."
    try:
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        payload = {"q": "test", "gl": "in", "num": 1}
        resp = requests.post(SERPER_ENDPOINT, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            return True, "API key is valid ✓"
        if resp.status_code in (401, 403):
            return False, "Invalid or inactive API key."
        if resp.status_code == 429:
            return False, "Quota exhausted — enter a new key."
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, f"Connection error: {str(e)[:60]}"


# ──────────────────────────────────────────────────────────────────────────────
#  PHONE NUMBER EXTRACTION
# ──────────────────────────────────────────────────────────────────────────────

def _is_valid_phone_number(digits: str) -> bool:
    """Validates extracted digit string as an Indian phone number."""
    d = digits.lstrip("0")
    if d in {"9696969696"}:  # IndiaMart helpline
        return False
    # Mobile: 10 digits starting with 6-9
    if len(d) == 10 and d[0] in "6789":
        return True
    # Landline with STD: 10-12 digits starting with 0
    if len(digits) in (10, 11, 12) and digits.startswith("0"):
        return True
    # 08xxxxxxxx style (toll-free prefix)
    if len(digits) == 10 and digits.startswith("08"):
        return True
    return False


def extract_phones_from_html(html: str) -> list:
    """
    Extracts all valid Indian phone numbers from raw HTML.
    Scoped to the 'Reach Us' / contact section when possible.
    Returns a deduplicated list of phone strings.
    """
    soup = BeautifulSoup(html, "lxml")

    # Priority: search within the company's own contact/reach-us section
    contact_sections = soup.select(
        "section.footer__col--reach, div.footer__col--reach, "
        ".footer__col--reach, div.footer__phone, "
        "[class*='contact'], [class*='reach'], [class*='phone']"
    )

    search_scope = contact_sections if contact_sections else [soup]

    found_phones = []
    seen_digits = set()

    for scope in search_scope:
        scope_text = scope.get_text(" ")

        # Check tel: links first (most reliable)
        for link in scope.find_all("a", href=True):
            href = link.get("href", "")
            if href.startswith("tel:"):
                digits = re.sub(r"\D", "", href.replace("tel:", ""))
                if digits not in seen_digits and _is_valid_phone_number(digits):
                    seen_digits.add(digits)
                    found_phones.append(digits)

        # Check data attributes
        for elem in scope.find_all(True):
            for attr in ("data-pnsno", "data-phone", "data-mobile", "data-tel"):
                val = elem.get(attr, "")
                if val:
                    digits = re.sub(r"\D", "", val)
                    if digits not in seen_digits and _is_valid_phone_number(digits):
                        seen_digits.add(digits)
                        found_phones.append(digits)

        # Regex on text
        for pattern in PHONE_PATTERNS:
            for match in pattern.findall(scope_text):
                digits = re.sub(r"\D", "", match)
                if digits not in seen_digits and _is_valid_phone_number(digits):
                    seen_digits.add(digits)
                    found_phones.append(digits)

        if found_phones:
            break  # Stop at first section that yields results

    return found_phones


def scrape_phone_from_url(url: str, session: Optional[requests.Session] = None) -> str:
    """
    Fetches the IndiaMart seller profile page and extracts phone number(s).
    Tries the profile.html URL first; falls back to original if needed.
    Returns comma-separated phone numbers, or "Not Found" / "Error: ...".
    """
    if not url or not url.startswith("http"):
        return "No URL"

    sess = session or requests.Session()
    profile_url = get_profile_url(url)

    urls_to_try = [profile_url]
    if profile_url != url:
        urls_to_try.append(url)

    for try_url in urls_to_try:
        try:
            # Add jitter to be polite
            time.sleep(random.uniform(0.5, 1.2))

            resp = sess.get(try_url, headers=SCRAPER_HEADERS, timeout=20, allow_redirects=True)
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                log.warning("HTTP %d for %s", resp.status_code, try_url)
                continue

            phones = extract_phones_from_html(resp.text)
            if phones:
                return ", ".join(phones[:3])  # Return up to 3 numbers

        except requests.exceptions.Timeout:
            log.warning("Timeout scraping %s", try_url)
        except Exception as exc:
            log.warning("Scrape error for %s: %s", try_url, str(exc)[:80])

    return "Not Found"


# ──────────────────────────────────────────────────────────────────────────────
#  CORE PER-COMPANY PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def process_company(
    name: str,
    pincode: str,
    address: str,
    api_key: str,
    session: Optional[requests.Session] = None,
) -> dict:
    """
    Full pipeline for one company:
      1. Build queries → search Serper → get best IndiaMart URL
      2. Scrape phone from that URL

    Returns a dict with:
      url_fetched, match_type, contact_number, error, credits_used
    """
    result = {
        "url_fetched": "",
        "match_type": "",
        "contact_number": "",
        "error": "",
        "credits_used": 0,
    }

    queries = build_queries(name, pincode, address)[:MAX_QUERIES_PER_COMPANY]

    # ── Step 1: URL fetching via Serper ────────────────────────────────────────
    for qi, (query, label) in enumerate(queries, 1):
        try:
            raw_urls = serper_search(query, api_key)
            result["credits_used"] += 1
        except (QuotaExhaustedError, InvalidKeyError):
            raise  # Bubble up — handled in caller

        for raw_url in raw_urls:
            if should_skip_domain(raw_url):
                continue
            if is_valid_indiamart_profile(raw_url):
                result["url_fetched"] = clean_indiamart_url(raw_url)
                result["match_type"] = label
                break

        if result["url_fetched"]:
            break  # Found a match — stop querying

    # ── Step 2: Phone scraping ─────────────────────────────────────────────────
    if result["url_fetched"]:
        try:
            result["contact_number"] = scrape_phone_from_url(
                result["url_fetched"], session=session
            )
        except Exception as exc:
            result["contact_number"] = "Scrape Error"
            result["error"] = str(exc)[:100]
    else:
        result["contact_number"] = ""

    return result