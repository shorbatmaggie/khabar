import csv
from pathlib import Path
import feedparser
from dateutil import parser as dateparser
from datetime import datetime, timedelta
import requests
import string
import time
import re
import unicodedata
import cloudscraper
import asyncio
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse  

# ---------- Config ----------
scraper = cloudscraper.create_scraper()

RUN_DATE = datetime.now().strftime("%Y-%m-%d")

MAX_SNIPPET_LEN = 400
DAYS_LIMIT = 1 # ignore most old news. consider changing to 0 if there are too many duplicates in dedupe stage
MAX_ENTRIES = 300  # prevent issues with huge feeds
REAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive",
}

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "config/news_rss_availability.csv"
KEYWORD_CSV = BASE_DIR / "config/news_rss_keywords.csv"
HARDENED_FEEDS = BASE_DIR / "config/news_playwright_rss_list.csv"

ARTICLES_DIR = BASE_DIR / "data/digests/rss_digests"
ERROR_DIR = BASE_DIR / "data/error_logs/rss_errors"

OUTPUT_CSV = ARTICLES_DIR / f"rss_articles_{RUN_DATE}.csv"
OUTPUT_ERROR_LOG = ERROR_DIR / f"rss_fetch_and_parse_errors_{RUN_DATE}.csv"

def _normalize_url(u: str) -> str:
    """Lowercase scheme/host, strip trailing slash on path (except root), keep query."""
    if not u:
        return ""
    u = u.strip()
    try:
        p = urlparse(u)
        path = p.path[:-1] if p.path.endswith("/") and p.path != "/" else p.path
        return f"{p.scheme.lower()}://{p.netloc.lower()}{path}{('?' + p.query) if p.query else ''}"
    except Exception:
        return u.strip().lower()

def _domain_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def _load_hardened_feeds(csv_path: str):
    urls = set()
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        col = "feed_url" if "feed_url" in (reader.fieldnames or []) else (reader.fieldnames or [""])[0]
        for row in reader:
            val = (row.get(col) or "").strip()
            if val:
                urls.add(_normalize_url(val))
    domains = { _domain_of(u) for u in urls if u }
    return urls, domains

# Load hardened feeds once from the CSV path
HARDENED_FEED_URLS, HARDENED_DOMAINS = _load_hardened_feeds(HARDENED_FEEDS)

def _is_hardened(target_url: str) -> bool:
    """True if the exact URL or its domain appears in the hardened feeds CSV."""
    nu = _normalize_url(target_url)
    return (nu in HARDENED_FEED_URLS) or (_domain_of(nu) in HARDENED_DOMAINS)

def load_keywords(path):
    keywords = set()
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            word = row['word'].strip().lower()
            if word:
                keywords.add(word)
    return keywords

def normalize(text):
    text = unicodedata.normalize("NFKC", (text or "")).lower()
    text = re.sub(r'(\w)[\"\'“”‘’]+(?=\s|$)', r'\1', text)
    text = re.sub(r'(\w),(?=\s|$)', r'\1', text)
    return text

def find_trigger_keywords(text, keywords):
    if not text:
        return []
    norm_text = normalize(text)
    triggered = []
    for kw in keywords:
        if "&" in kw:
            parts = [p.strip() for p in kw.split("&") if p.strip()]
            if parts and all(re.search(r'\b{}\b'.format(re.escape(part)), norm_text) for part in parts):
                triggered.append(kw)

        else:
            # Single keyword: whole word match only
            if re.search(r'\b{}\b'.format(re.escape(kw)), norm_text):
                triggered.append(kw)
    return triggered

def is_recent(pubdate):
    """
    Keep items that are:
      - within the past DAYS_LIMIT *calendar days* (inclusive),
      - OR exactly one calendar day in the "future"
    All checks are done as DATE-ONLY (no timezone normalization)
    """
    try:
        # Parse whatever we get; many feeds provide date-only strings.
        # Using .date() makes this robust to missing times/tzinfo.
        parsed = dateparser.parse(pubdate)
        if parsed is None:
            return False

        pub_date = parsed.date()
        today = datetime.now().date()

        # Past window: 0..DAYS_LIMIT days old (inclusive)
        past_days = (today - pub_date).days
        if 0 <= past_days <= DAYS_LIMIT:
            return True

        # Future allowance: exactly "tomorrow" relative to local date
        future_days = (pub_date - today).days
        if future_days == 1:
            return True

        return False
    except Exception:
        return False


def fetch_feed_with_timeout(feed_url, timeout=15):
    if _is_hardened(feed_url):
        return fetch_with_playwright(feed_url, timeout=timeout)
    try:
        resp = requests.get(feed_url, headers=REAL_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content, None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            try:
                resp = scraper.get(feed_url, timeout=timeout)
                resp.raise_for_status()
                return resp.content, None
            except Exception as ce:
                return None, f"❌ Cloudscraper failed: {ce}"
        else:
            return None, f"❌ Requests failed: {e}"
    except Exception as e:
        return None, f"❌ Requests exception: {e}"
      
def fetch_with_playwright(feed_url, timeout=15):
    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(timeout * 1000)  # ms

            # Use request API instead of page.goto for downloadable content
            response = page.request.get(feed_url, headers=REAL_HEADERS)
            if not response.ok:
                return None, f"❌ Playwright request failed: {response.status}"
            content = response.text()
            browser.close()
            return content.encode("utf-8"), None
    except Exception as e:
        return None, f"❌ Playwright failed: {e}"

def main():
    # Load keywords from CSV
    KEYWORDS = load_keywords(KEYWORD_CSV)

    # Read only feed_url and domain from CSV, where rss == "yes" and feed_url is not blank
    feeds = []
    with open(CSV_PATH, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            feed_url = row.get('feed_url', '').strip()
            rss_status = row.get('rss', '').strip().lower()
            domain = row.get('domain', '').strip()
            if rss_status == "yes" and feed_url:
                feeds.append((domain, feed_url))

    total_feeds = len(feeds)
    successful_feeds = 0
    all_articles = []
    seen_urls = set()
    seen_titles = set()
    error_log = []

    for domain, feed_url in feeds:
        print(f"\nProcessing {feed_url} ({domain})...")
        try:
            feed_content, fetch_error = fetch_feed_with_timeout(feed_url)
            if fetch_error:
                print(fetch_error)
                # Add to error log:
                error_log.append({
                    "feed_url": feed_url,
                    "error_type": "fetch",
                    "error_message": fetch_error
                })
                continue
            else:
                print("✅ Feed fetched")
            # Now, try to parse
            d = feedparser.parse(feed_content)
            bozo = getattr(d, "bozo", 0)
            bozo_exception = getattr(d, "bozo_exception", None)
            entries_count = len(getattr(d, "entries", []))
            if entries_count == 0:
                print("❌ Failed to parse feed: No entries found.")
                # log error as "no entries found"
                error_log.append({
                    "feed_url": feed_url,
                    "error_type": "parse",
                    "error_message": str(bozo_exception)
                })
                continue
            elif bozo:
                print(f"⚠️ Parse warning: {bozo_exception} (but entries found!)")
                # optionally log as warning, but proceed
            else:
                print("✅ Feed parsed")
                successful_feeds += 1

            from bs4 import BeautifulSoup
            for entry in d.entries[:MAX_ENTRIES]:
                try:
                    title = entry.title
                    link = entry.link
                    date = getattr(entry, 'published', getattr(entry, 'updated', None))
                    snippet = getattr(entry, 'summary', "")
                    if not (title and link and date):
                        continue
                    if not is_recent(date):
                        continue
                    snippet = BeautifulSoup(snippet, "html.parser").get_text()[:MAX_SNIPPET_LEN] if snippet else ""

                    # Find keywords that triggered this article
                    triggered_title = find_trigger_keywords(title, KEYWORDS)
                    triggered_snippet = find_trigger_keywords(snippet, KEYWORDS)
                    triggered = list(set(triggered_title + triggered_snippet))
                    if not triggered:
                        continue  # skip articles with no hits

                    ukey = link.strip()
                    unique_id = (title.strip().lower(), dateparser.parse(date).strftime("%Y-%m-%d"))
                    if ukey in seen_urls or unique_id in seen_titles:
                        continue
                    seen_urls.add(ukey)
                    seen_titles.add(unique_id)

                    all_articles.append({
                        "date_published": dateparser.parse(date).strftime("%Y-%m-%d"),
                        "trigger_keywords": ", ".join(sorted(triggered)),
                        "title": title.strip(),
                        "snippet": snippet.strip(),
                        "url": link.strip(),
                        "source_domain": domain,
                    })
                except Exception as e:
                    print(f"    Error parsing entry: {e}")
                    continue
        except Exception as e:
            print(f"!! Exception for feed {feed_url} ({domain}): {e}")
            error_log.append({
                "feed_url": feed_url,
                "error_type": "exception",
                "error_message": str(e)
             })
            continue
        time.sleep(0.5)
        
    start_time = time.time()
    elapsed = time.time() - start_time

    print(f"\nSuccessfully fetched {successful_feeds} out of {total_feeds} feeds.")
    print(f"\nTotal relevant articles found: {len(all_articles)}")

    # Output CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trigger_keywords",
                "title",
                "snippet",
                "date_published",
                "source_domain",
                "url"
            ]
        )
        writer.writeheader()
        # Sort alphabetically by trigger_keywords before writing 
        for art in sorted(all_articles, key=lambda x: x["trigger_keywords"].strip().lower()):
            writer.writerow(art)
    
    # Output Error Log
    with open(OUTPUT_ERROR_LOG, "w", encoding="utf-8", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["feed_url", "error_type", "error_message"])
        writer.writeheader()
        for row in error_log:
            writer.writerow(row)


    print(f"Saved articles to {OUTPUT_CSV}. Saved error log to {OUTPUT_ERROR_LOG}.")

if __name__ == "__main__":
    _t0 = time.perf_counter()
    try:
        main()
    finally:
        _elapsed = time.perf_counter() - _t0
        print(f"Script runtime: {_elapsed:.2f} seconds ({_elapsed/60:.2f} minutes)")

