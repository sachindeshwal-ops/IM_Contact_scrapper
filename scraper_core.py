"""
scraper_core.py
================
Core logic for IndiaMart Lead Enricher.
Contains:
  - Address → locality keyword extraction
  - Serper API search (URL fetching)
  - IndiaMart URL validation & cleaning
  - Phone number extraction via Selenium (primary) or requests fallback
  - Match type assignment
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

INDIAMART_HELPLINE_DIGITS = {"09696969696", "9696969696"}

SKIP_DOMAINS: set = {
    "google.com", "google.co.in", "googleapis.com",
    "youtube.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "linkedin.com", "wikipedia.org", "amazon.in", "amazon.com", "flipkart.com",
    "justdial.com", "sulekha.com", "tradeindia.com", "exportersindia.com",
    "yellowpages.in", "zaubacorp.com", "tofler.in", "dnb.com",
}

IM_INVALID_SUBDOMAINS = {"dir", "my", "m", "events", "connect", "buy", "sell",
                          "blog", "help", "developer", "api"}
IM_INVALID_PATH_STARTS = {"proddetail", "impcat", "city", "company", "isearch",
                            "search", "categories", "indianexporters", "buy", "sell",
                            "trade-leads", "trade_leads", "catalogs", "mcat"}
STRIP_PARAMS = {"srsltid", "utm_source", "utm_medium", "utm_campaign",
                "utm_term", "utm_content", "pos", "ref", "gclid"}

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

PHONE_PATTERNS = [
    re.compile(r"(?<!\d)(?:\+91[-\s]?|0)?[6-9]\d{9}(?!\d)"),
    re.compile(r"(?<!\d)0[1-9]\d{1,3}[-\s]\d{6,8}(?!\d)"),
    re.compile(r"(?<!\d)08\d{8}(?!\d)"),
]

_PINCODE_RE = re.compile(r"\b\d{6}\b")
_NUM_TOKEN_RE = re.compile(r"^[\d\-/]+[a-zA-Z]{0,2}$")

MAX_QUERIES_PER_COMPANY = 4
NUM_RESULTS_PER_QUERY   = 5
MAX_RETRIES_ON_ERROR    = 2

# Selenium timing constants (seconds)
_PAGE_LOAD_WAIT = 6
_REVEAL_WAIT    = 5


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
    if not address:
        return []
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
    seen = set()
    unique = []
    for c in candidates:
        key = c.lower()
        if key not in seen:
            seen.add(key)
            unique.append(c)
    unique.sort(key=lambda x: (-len(x.split()), -len(x)))
    return unique[:3]


# ──────────────────────────────────────────────────────────────────────────────
#  QUERY BUILDER
# ──────────────────────────────────────────────────────────────────────────────

def build_queries(name: str, pincode: str, address: str) -> list:
    name     = name.strip()
    pincode  = str(pincode).strip().split(".")[0]
    localities = extract_locality_keywords(address, pincode)
    queries = []
    if localities:
        queries.append((f'"{name}" {localities[0]} site:indiamart.com', "name+locality"))
    if pincode and len(pincode) == 6:
        queries.append((f'"{name}" {pincode} site:indiamart.com', "name+pincode"))
    if len(localities) >= 2:
        queries.append((f'"{name}" {localities[1]} site:indiamart.com', "name+locality"))
    queries.append((f'"{name}" site:indiamart.com', "name_only"))
    return queries


# ──────────────────────────────────────────────────────────────────────────────
#  URL UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def clean_indiamart_url(url: str) -> str:
    try:
        parsed   = urlparse(url)
        qs       = {k: v for k, v in parse_qs(parsed.query).items()
                    if k.lower() not in STRIP_PARAMS}
        new_query = urlencode(qs, doseq=True)
        return urlunparse(("https", "www.indiamart.com",
                           parsed.path.rstrip("/"), parsed.params, new_query, ""))
    except Exception:
        return url


def is_valid_indiamart_profile(url: str) -> bool:
    if not url or "indiamart.com" not in url.lower():
        return False
    url_lower = url.lower()
    try:
        parsed     = urlparse(url)
        netloc     = parsed.netloc.lower()
        host_parts = netloc.replace("www.", "").split(".")
        if len(host_parts) > 2 and host_parts[0] in IM_INVALID_SUBDOMAINS:
            return False
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts:
            return False
        first = path_parts[0].lower()
        if first in IM_INVALID_PATH_STARTS:
            return False
        bad_fragments = [
            "/proddetail/", "/impcat/", "/categories/", "/indianexporters/",
            "/isearch.php", "/search.html", "/trade-leads", "/catalogs/",
            "/mcat/", "/buy-",
        ]
        if any(frag in url_lower for frag in bad_fragments):
            return False
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
    url  = url.strip().rstrip("/")
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
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": query, "gl": "in", "hl": "en", "num": NUM_RESULTS_PER_QUERY}
    for attempt in range(1, MAX_RETRIES_ON_ERROR + 2):
        try:
            resp = requests.post(SERPER_ENDPOINT, headers=headers,
                                 json=payload, timeout=30)
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
    if not api_key or len(api_key.strip()) < 10:
        return False, "API key too short."
    try:
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        payload = {"q": "test", "gl": "in", "num": 1}
        resp = requests.post(SERPER_ENDPOINT, headers=headers,
                             json=payload, timeout=10)
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
#  PHONE EXTRACTION — STATIC FALLBACK (requests + BeautifulSoup)
# ──────────────────────────────────────────────────────────────────────────────

def _is_valid_phone_number(digits: str) -> bool:
    d = digits.lstrip("0")
    if d in {"9696969696"}:
        return False
    if len(d) == 10 and d[0] in "6789":
        return True
    if len(digits) in (10, 11, 12) and digits.startswith("0"):
        return True
    if len(digits) == 10 and digits.startswith("08"):
        return True
    return False


def extract_phones_from_html(html: str) -> list:
    """
    Extracts valid Indian phone numbers from raw static HTML.
    Scoped to the Reach Us / contact section when present.
    Used as a fallback when Selenium is unavailable.
    """
    soup = BeautifulSoup(html, "lxml")
    contact_sections = soup.select(
        "section.footer__col--reach, div.footer__col--reach, "
        ".footer__col--reach, div.footer__phone, "
        "[class*='contact'], [class*='reach'], [class*='phone']"
    )
    search_scope = contact_sections if contact_sections else [soup]
    found_phones = []
    seen_digits  = set()
    for scope in search_scope:
        scope_text = scope.get_text(" ")
        for link in scope.find_all("a", href=True):
            href = link.get("href", "")
            if href.startswith("tel:"):
                digits = re.sub(r"\D", "", href.replace("tel:", ""))
                if digits not in seen_digits and _is_valid_phone_number(digits):
                    seen_digits.add(digits)
                    found_phones.append(digits)
        for elem in scope.find_all(True):
            for attr in ("data-pnsno", "data-phone", "data-mobile", "data-tel"):
                val = elem.get(attr, "")
                if val:
                    digits = re.sub(r"\D", "", val)
                    if digits not in seen_digits and _is_valid_phone_number(digits):
                        seen_digits.add(digits)
                        found_phones.append(digits)
        for pattern in PHONE_PATTERNS:
            for match in pattern.findall(scope_text):
                digits = re.sub(r"\D", "", match)
                if digits not in seen_digits and _is_valid_phone_number(digits):
                    seen_digits.add(digits)
                    found_phones.append(digits)
        if found_phones:
            break
    return found_phones


def _scrape_phone_static(url: str, session: Optional[requests.Session] = None) -> str:
    """
    Requests-based phone scraping (no JS execution).
    Only works if the phone number happens to be in the initial HTML —
    increasingly rare on IndiaMart since they added the Click-to-Reveal button.
    Used as a last-resort fallback when Selenium is not available.
    """
    if not url or not url.startswith("http"):
        return "No URL"
    sess        = session or requests.Session()
    profile_url = get_profile_url(url)
    urls_to_try = [profile_url]
    if profile_url != url:
        urls_to_try.append(url)
    for try_url in urls_to_try:
        try:
            time.sleep(random.uniform(0.5, 1.2))
            resp = sess.get(try_url, headers=SCRAPER_HEADERS,
                            timeout=20, allow_redirects=True)
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                log.warning("HTTP %d for %s", resp.status_code, try_url)
                continue
            phones = extract_phones_from_html(resp.text)
            if phones:
                return ", ".join(phones[:3])
        except requests.exceptions.Timeout:
            log.warning("Timeout scraping %s", try_url)
        except Exception as exc:
            log.warning("Scrape error for %s: %s", try_url, str(exc)[:80])
    return "Not Found"


# ──────────────────────────────────────────────────────────────────────────────
#  PHONE EXTRACTION — SELENIUM (primary, handles Click-to-Reveal)
# ──────────────────────────────────────────────────────────────────────────────

def create_selenium_driver(headless: bool = True):
    """
    Creates and returns a configured Chrome WebDriver.
    Raises ImportError if selenium / webdriver_manager are not installed.
    Raises RuntimeError if ChromeDriver cannot be started.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError as exc:
        raise ImportError(
            "Selenium dependencies missing. "
            "Run: pip install selenium webdriver-manager"
        ) from exc

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=opts
        )
        return driver
    except Exception as exc:
        raise RuntimeError(f"Could not start ChromeDriver: {exc}") from exc


def _clean_phone(raw: str) -> str:
    """Strip 'Call ' prefix and extension suffixes."""
    s = re.sub(r"(?i)^call\s+", "", raw.strip())
    if "(" in s:
        s = s.split("(")[0].strip()
    return s


def _scroll_to_reach_us(driver) -> bool:
    """Scroll to the company Reach Us section to trigger lazy loading."""
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException

    for css in [
        "section.footer__col--reach",
        "div.footer__col--reach",
        ".footer__col--reach",
        "div.footer__phone",
        "div.footer__banner",
    ]:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, css)
            driver.execute_script(
                "arguments[0].scrollIntoView({behavior:'instant', block:'center'});",
                elem
            )
            time.sleep(0.8)
            return True
        except NoSuchElementException:
            continue

    # Fallback: scroll through page to trigger lazy loading
    height = driver.execute_script("return document.body.scrollHeight")
    pos = 0
    while pos < height and pos < 25_000:
        driver.execute_script(f"window.scrollTo(0, {pos});")
        time.sleep(0.15)
        pos += 500
        height = driver.execute_script("return document.body.scrollHeight")
    time.sleep(1)
    return False


def _extract_phone_selenium(driver) -> str:
    """
    Extract phone from the company's Reach Us section.
    Clicks the 'Call Now' button and waits for the number to be revealed.
    Strictly scoped to footer__col--reach — never reads the site-wide helpline.
    """
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException

    REACH_CSS = (
        "section.footer__col--reach, "
        "div.footer__col--reach, "
        ".footer__col--reach"
    )

    # Locate the Reach Us container
    reach_us = None
    for css in REACH_CSS.split(","):
        try:
            reach_us = driver.find_element(By.CSS_SELECTOR, css.strip())
            break
        except NoSuchElementException:
            continue

    if reach_us is None:
        return "Not Found"

    # Step 1 — number may already be revealed (e.g. after a prior click on same session)
    try:
        span = reach_us.find_element(By.CSS_SELECTOR, "span.phone-reveal__line1")
        text = span.text.strip()
        if text and "Call Now" not in text:
            cleaned = _clean_phone(text)
            if _is_valid_phone_number(re.sub(r"\D", "", cleaned)):
                return cleaned
    except NoSuchElementException:
        pass

    # Step 2 — click the "Call Now" button and wait for the number to appear
    try:
        btn = reach_us.find_element(By.CSS_SELECTOR, "button.phone-reveal-btn")
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior:'instant', block:'center'});", btn
        )
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", btn)

        deadline = time.time() + _REVEAL_WAIT
        while time.time() < deadline:
            time.sleep(0.4)
            try:
                reach_us = driver.find_element(By.CSS_SELECTOR, REACH_CSS)
                span = reach_us.find_element(By.CSS_SELECTOR, "span.phone-reveal__line1")
                text = span.text.strip()
                if text and "Call Now" not in text and len(re.sub(r"\D", "", text)) >= 7:
                    cleaned = _clean_phone(text)
                    if _is_valid_phone_number(re.sub(r"\D", "", cleaned)):
                        return cleaned
            except (NoSuchElementException, StaleElementReferenceException):
                pass

    except (NoSuchElementException, StaleElementReferenceException):
        pass

    # Step 3 — data-pnsno attribute
    try:
        reach_us = driver.find_element(By.CSS_SELECTOR, REACH_CSS)
        elem = reach_us.find_element(By.XPATH, ".//*[@data-pnsno]")
        val  = (elem.get_attribute("data-pnsno") or "").strip()
        if val and _is_valid_phone_number(re.sub(r"\D", "", val)):
            return _clean_phone(val)
    except (NoSuchElementException, StaleElementReferenceException):
        pass

    # Step 4 — tel: links inside reach_us
    try:
        reach_us = driver.find_element(By.CSS_SELECTOR, REACH_CSS)
        links = reach_us.find_elements(By.XPATH, ".//a[starts-with(@href,'tel:')]")
        for link in links:
            val = link.get_attribute("href").replace("tel:", "").strip()
            digits = re.sub(r"\D", "", val)
            if val and _is_valid_phone_number(digits):
                return _clean_phone(val)
    except (NoSuchElementException, StaleElementReferenceException):
        pass

    return "Not Found"


def scrape_phone_selenium(url: str, driver) -> str:
    """
    Load an IndiaMart profile page with a running Selenium driver,
    scroll to Reach Us, click Call Now, return the phone number.
    Tries original URL; falls back to /profile.html if needed.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    if not url or not url.startswith("http"):
        return "No URL"

    profile_url = get_profile_url(url)
    urls_to_try = [profile_url]
    if profile_url != url:
        urls_to_try.append(url)

    for try_url in urls_to_try:
        try:
            driver.get(try_url)
            # Wait for page body to be present
            try:
                WebDriverWait(driver, _PAGE_LOAD_WAIT + 4).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "footer"))
                )
            except TimeoutException:
                time.sleep(_PAGE_LOAD_WAIT)

            _scroll_to_reach_us(driver)
            time.sleep(0.5)  # allow lazy components to initialise

            phone = _extract_phone_selenium(driver)
            if _is_valid_phone_number(re.sub(r"\D", "", phone)):
                return phone
            # If we got Not Found on profile.html try the original
            if "Not Found" in phone and len(urls_to_try) == 1:
                return phone  # nothing else to try

        except Exception as exc:
            log.warning("Selenium page error for %s: %s", try_url, str(exc)[:80])
            return "Page Load Error"

    return "Not Found"


# ──────────────────────────────────────────────────────────────────────────────
#  UNIFIED PHONE SCRAPER (Selenium → static fallback)
# ──────────────────────────────────────────────────────────────────────────────

def scrape_phone_from_url(
    url: str,
    session: Optional[requests.Session] = None,
    driver=None,
) -> str:
    """
    Scrape phone number from an IndiaMart URL.

    Priority:
      1. Selenium driver (if provided) — handles the Click-to-Reveal button
      2. Static requests fallback (rarely works on modern IndiaMart pages)

    Returns a phone number string, "Not Found", "No URL", or "Scrape Error".
    """
    if not url or not url.startswith("http"):
        return "No URL"

    if driver is not None:
        try:
            return scrape_phone_selenium(url, driver)
        except Exception as exc:
            log.warning("Selenium scrape failed, falling back to static: %s", exc)
            # Fall through to static

    return _scrape_phone_static(url, session=session)


# ──────────────────────────────────────────────────────────────────────────────
#  CORE PER-COMPANY PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def process_company(
    name: str,
    pincode: str,
    address: str,
    api_key: str,
    session: Optional[requests.Session] = None,
    driver=None,
) -> dict:
    """
    Full pipeline for one company:
      1. Build queries → search Serper → get best IndiaMart URL
      2. Scrape phone from that URL via Selenium (if driver given) or requests

    Returns a dict with:
      url_fetched, match_type, contact_number, error, credits_used
    """
    result = {
        "url_fetched":    "",
        "match_type":     "",
        "contact_number": "",
        "error":          "",
        "credits_used":   0,
    }

    queries = build_queries(name, pincode, address)[:MAX_QUERIES_PER_COMPANY]

    # ── Step 1: URL fetching via Serper ────────────────────────────────────────
    for qi, (query, label) in enumerate(queries, 1):
        try:
            raw_urls = serper_search(query, api_key)
            result["credits_used"] += 1
        except (QuotaExhaustedError, InvalidKeyError):
            raise

        for raw_url in raw_urls:
            if should_skip_domain(raw_url):
                continue
            if is_valid_indiamart_profile(raw_url):
                result["url_fetched"] = clean_indiamart_url(raw_url)
                result["match_type"]  = label
                break

        if result["url_fetched"]:
            break

    # ── Step 2: Phone scraping ─────────────────────────────────────────────────
    if result["url_fetched"]:
        try:
            result["contact_number"] = scrape_phone_from_url(
                result["url_fetched"],
                session=session,
                driver=driver,
            )
        except Exception as exc:
            result["contact_number"] = "Scrape Error"
            result["error"]          = str(exc)[:100]
    else:
        result["contact_number"] = ""

    return result