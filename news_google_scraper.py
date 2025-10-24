import html
import csv
import feedparser
from dateutil import parser as dateparser
from datetime import datetime, timedelta 
from pathlib import Path
from urllib.parse import urlparse
from urllib.parse import parse_qs
import requests
import time
import re
import unicodedata
from bs4 import BeautifulSoup

# ---------- Config ----------
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

MAX_SNIPPET_LEN = 400
DAYS_LIMIT = 1 # ignore most old news. consider changing to 0 if there are too many duplicates in dedupe stage
MAX_ENTRIES = 300  # prevent issues with huge feeds (not really necessary but a good precaution)      

REAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive",
}

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "config/news_google_rss_list.csv"
ARTICLES_DIR = BASE_DIR / "data/digests/google_digests"
ERROR_DIR = BASE_DIR / "data/error_logs/google_errors"

OUTPUT_CSV = ARTICLES_DIR / f"google_alerts_articles_{RUN_DATE}.csv"
OUTPUT_ERROR_LOG = ERROR_DIR / f"google_fetch_and_parse_errors_{RUN_DATE}.csv"


# ---------- Helpers tuned for Google Alerts ----------

def _html_to_text(s: str) -> str:
    if not s:
        return ""
    text = BeautifulSoup(s, "html.parser").get_text()
    text = html.unescape(text)
    text = unicodedata.normalize("NFKC", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _extract_best_link(entry) -> str:
    """
    Google Alerts often uses https://www.google.com/url?... with real target in q= or url=.
    Prefer entry.link; fallback to first href in entry.links.
    """
    link = getattr(entry, "link", "") or ""
    if not link:
        links = getattr(entry, "links", []) or []
        if links and isinstance(links, list):
            href = links[0].get("href") or ""
            link = href

    link = link.strip()
    if not link:
        return ""

    # If it's a Google redirect, pull the real URL from query params.
    try:
        pu = urlparse(link)
        if pu.netloc.endswith("google.com") and pu.path.startswith("/url"):
            qs = parse_qs(pu.query)
            # Common params carrying the target
            real = (qs.get("q") or qs.get("url") or [""])[0]
            if real:
                return real.strip()
    except Exception:
        pass

    return link

def _extract_source_domain(url: str) -> str:
    if not url:
        return ""
    try:
        domain = urlparse(url).netloc.lower()
        if not domain:
            return ""
        if domain.startswith("www."):
            domain = domain[4:]
        return domain.split(":")[0]
    except Exception:
        return ""
    
def _raw_date_for_recency(entry) -> str | None:
    """Return a raw date string in order of preference: published, updated, else from *_parsed."""
    for field in ("published", "updated"):
        val = getattr(entry, field, None)
        if val:
            return val
    for field in ("published_parsed", "updated_parsed"):
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime(*val[:6]).isoformat()
            except Exception:
                continue
    return None

def _iso_date(entry) -> str | None:
    """Return ISO date (YYYY-MM-DD) using published/updated or their *_parsed variants."""
    for field in ("published", "updated"):
        val = getattr(entry, field, None)
        if val:
            try:
                return dateparser.parse(val).strftime("%Y-%m-%d")
            except Exception:
                pass
    for field in ("published_parsed", "updated_parsed"):
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime(*val[:6]).strftime("%Y-%m-%d")
            except Exception:
                pass
    return None

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

def _extract_snippet(entry) -> str:
    """
    For Google Alerts Atom, prefer content[0].value (HTML), then summary/description.
    """
    content = getattr(entry, "content", None)
    if content and isinstance(content, list) and content and "value" in content[0]:
        return _html_to_text(content[0]["value"])[:MAX_SNIPPET_LEN]

    for field in ("summary", "description"):
        val = getattr(entry, field, None)
        if val:
            return _html_to_text(val)[:MAX_SNIPPET_LEN]
    return ""
# -----------------------------------------------------


def fetch_feed_bytes(feed_url: str, timeout: int = 15):
    """Plain requests fetch; no playwright/cloudscraper per your requirement."""
    try:
        resp = requests.get(feed_url, headers=REAL_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.content, None
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        return None, f"❌ Requests failed (HTTP {code}): {e}"
    except Exception as e:
        return None, f"❌ Requests exception: {e}"


def main():
    t0 = time.perf_counter()

    # Read feeds CSV (expects headers: feed_url, keywords, source)
    feeds = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        if reader.fieldnames:
            reader.fieldnames = [ (h or "").lstrip("\ufeff").strip().lower() for h in reader.fieldnames ]

        for raw in reader:
            row = { (k or "").strip().lower(): (v or "").strip() for k, v in raw.items() }

            feed_url = row.get("feed_url", "")
            keywords = row.get("keywords", "")
            source   = row.get("source", "")  # not used downstream rn

            if feed_url:
                feeds.append({"feed_url": feed_url, "keywords": keywords, "source": source})


    total_feeds = len(feeds)
    successful_feeds = 0
    all_articles = []
    seen_urls = set()
    seen_title_date = set()
    error_log = []

    for idx, item in enumerate(feeds, start=1):
        feed_url = item["feed_url"]
        keywords = item["keywords"]

        print(f"\n[{idx}/{total_feeds}] Processing {feed_url}  (keywords: {keywords})...")
        content, fetch_err = fetch_feed_bytes(feed_url)
        if fetch_err:
            print(fetch_err)
            error_log.append({
                "feed_url": feed_url,
                "keywords": keywords,
                "error_type": "fetch",
                "error_message": fetch_err
            })
            continue

        d = feedparser.parse(content)
        bozo = getattr(d, "bozo", 0)
        bozo_exception = getattr(d, "bozo_exception", None)
        entries = getattr(d, "entries", [])

        if not entries:
            msg = f"ℹ️ No entries found. bozo={bozo}, exc={bozo_exception}"
            print(msg)
            if bozo_exception:
                error_log.append({
                    "feed_url": feed_url,
                    "keywords": keywords,
                    "error_type": "parse",
                    "error_message": str(bozo_exception)
                })
            continue

        if bozo:
            print(f"⚠️ Parse warning: {bozo_exception} (continuing; entries present)")

        print("✅ Feed parsed")
        successful_feeds += 1

        for entry in entries[:MAX_ENTRIES]:
            try:
                raw_date = _raw_date_for_recency(entry)
                if not raw_date or not is_recent(raw_date):
                    continue

                date_iso = _iso_date(entry)
                if not date_iso:
                    # If can't normalize to ISO, skip
                    continue

                title_raw = getattr(entry, "title", "") or ""
                title = html.unescape(title_raw).strip()
                link = _extract_best_link(entry)
                if not title or not link:
                    continue

                snippet = _extract_snippet(entry)

                # De-dupe by final link and by (title, date)
                if link in seen_urls:
                    continue
                key_title_date = (title.lower(), date_iso)
                if key_title_date in seen_title_date:
                    continue

                seen_urls.add(link)
                seen_title_date.add(key_title_date)

                all_articles.append({
                    "keywords": keywords,
                    "title": title,
                    "snippet": snippet,
                    "date_published": date_iso,
                    "source_domain": _extract_source_domain(link),
                    "url": link,
                    "in_roundup": ""
                })
            except Exception as e:
                print(f"    Error parsing entry: {e}")
                continue

        time.sleep(0.3)  # be polite

    # Sort output — by title (asc)
    all_articles.sort(key=lambda x: x["title"].lower())

    print(f"\nSuccessfully fetched {successful_feeds} out of {total_feeds} feeds.")
    print(f"Total articles (last {DAYS_LIMIT} days): {len(all_articles)}")

    # Output CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["keywords", "title", "snippet", "date_published", "source_domain", "url", "in_roundup"]
        )
        writer.writeheader()
        for row in all_articles:
            writer.writerow(row)

    # Error log
    with open(OUTPUT_ERROR_LOG, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["feed_url", "keywords", "error_type", "error_message"]
        )
        writer.writeheader()
        for row in error_log:
            writer.writerow(row)

    elapsed = time.perf_counter() - t0
    print(f"\nSaved articles to {OUTPUT_CSV}.")
    print(f"Saved error log to {OUTPUT_ERROR_LOG}.")
    print(f"Script runtime: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")


if __name__ == "__main__":
    main()
