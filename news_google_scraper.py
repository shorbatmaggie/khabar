import html
import csv
import json
import feedparser
from dateutil import parser as dateparser
from datetime import datetime
import requests
import time
import re
import unicodedata
from bs4 import BeautifulSoup

# ---------- Config ----------
RUN_DATE = datetime.now().strftime("%Y-%m-%d")
CSV_PATH = "/Users/maggie/Documents/PIL/news_roundup/news_google_rss_list.csv"

MAX_SNIPPET_LEN = 400
DAYS_LIMIT = 10            
MAX_ENTRIES = 200          

REAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Connection": "keep-alive",
}

OUTPUT_JSON = f"articles_json/google_alerts_articles_{RUN_DATE}.json"
OUTPUT_CSV = f"articles_csv/google_alerts_articles_{RUN_DATE}.csv"
OUTPUT_ERROR_LOG = f"error_logs/google_fetch_and_parse_errors{RUN_DATE}.csv"


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

def _is_recent(raw_dt: str) -> bool:
    try:
        dt = dateparser.parse(raw_dt)
        now = datetime.now(dt.tzinfo) if dt and dt.tzinfo else datetime.utcnow()
        return (now - dt).days <= DAYS_LIMIT
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

    # Read feeds CSV (expects headers: feed_url, alert, source)
    feeds = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        if reader.fieldnames:
            reader.fieldnames = [ (h or "").lstrip("\ufeff").strip().lower() for h in reader.fieldnames ]

        for raw in reader:
            row = { (k or "").strip().lower(): (v or "").strip() for k, v in raw.items() }

            feed_url = row.get("feed_url", "")
            alert    = row.get("alert", "")
            source   = row.get("source", "")  # not used downstream rn

            if feed_url:
                feeds.append({"feed_url": feed_url, "alert": alert, "source": source})


    total_feeds = len(feeds)
    successful_feeds = 0
    all_articles = []
    seen_urls = set()
    seen_title_date = set()
    error_log = []

    for idx, item in enumerate(feeds, start=1):
        feed_url = item["feed_url"]
        alert = item["alert"]

        print(f"\n[{idx}/{total_feeds}] Processing {feed_url}  (alert: {alert})...")
        content, fetch_err = fetch_feed_bytes(feed_url)
        if fetch_err:
            print(fetch_err)
            error_log.append({
                "feed_url": feed_url,
                "alert": alert,
                "error_type": "fetch",
                "error_message": fetch_err
            })
            continue

        d = feedparser.parse(content)
        bozo = getattr(d, "bozo", 0)
        bozo_exception = getattr(d, "bozo_exception", None)
        entries = getattr(d, "entries", [])

        if not entries:
            msg = f"❌ No entries found. bozo={bozo}, exc={bozo_exception}"
            print(msg)
            error_log.append({
                "feed_url": feed_url,
                "alert": alert,
                "error_type": "parse",
                "error_message": str(bozo_exception) if bozo_exception else "no entries"
            })
            continue

        if bozo:
            print(f"⚠️ Parse warning: {bozo_exception} (continuing; entries present)")

        print("✅ Feed parsed")
        successful_feeds += 1

        for entry in entries[:MAX_ENTRIES]:
            try:
                raw_date = _raw_date_for_recency(entry)
                if not raw_date or not _is_recent(raw_date):
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
                    "alert": alert,
                    "title": title,
                    "snippet": snippet,
                    "date_published": date_iso,
                    "url": link
                })
            except Exception as e:
                print(f"    Error parsing entry: {e}")
                continue

        time.sleep(0.3)  # be polite

    # Sort output — by alert (asc), date (desc), then title (asc)
    all_articles.sort(key=lambda x: (x["alert"].lower(), x["date_published"], x["title"].lower()))

    print(f"\nSuccessfully fetched {successful_feeds} out of {total_feeds} feeds.")
    print(f"Total articles (last {DAYS_LIMIT} days): {len(all_articles)}")

    # Output JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, indent=2, ensure_ascii=False)

    # Output CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["alert", "title", "snippet", "date_published", "url"]
        )
        writer.writeheader()
        for row in all_articles:
            writer.writerow(row)

    # Error log
    with open(OUTPUT_ERROR_LOG, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["feed_url", "alert", "error_type", "error_message"]
        )
        writer.writeheader()
        for row in error_log:
            writer.writerow(row)

    elapsed = time.perf_counter() - t0
    print(f"\nSaved articles to {OUTPUT_JSON} and {OUTPUT_CSV}.")
    print(f"Saved error log to {OUTPUT_ERROR_LOG}.")
    print(f"Script runtime: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")


if __name__ == "__main__":
    main()