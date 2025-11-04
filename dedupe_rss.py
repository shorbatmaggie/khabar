from __future__ import annotations
import csv
import re
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple

# === CONFIG ===
BASE_DIR = Path(__file__).resolve().parent
ARTICLES_DIR = BASE_DIR / "data/digests/rss_digests"

# Expected fields/order
FIELDS = ["keywords", "title", "snippet", "date_published", "source_domain", "url", "in_roundup"]

# Regex to extract date from filenames
DATE_RE = re.compile(r"rss_articles_(\d{4}-\d{2}-\d{2})\.csv", re.IGNORECASE)


# === DATE HELPERS ===
def parse_date_str(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")

def daterange(start: datetime, end: datetime) -> List[str]:
    """Inclusive list of YYYY-MM-DD from start..end (start <= end)."""
    days = (end.date() - start.date()).days
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days + 1)]

def prompt_start_date() -> datetime:
    """Prompt until a valid YYYY-MM-DD that is not in the future."""
    today = datetime.now().date()
    while True:
        s = input("Enter start date (YYYY-MM-DD): ").strip()
        try:
            d = parse_date_str(s).date()
        except ValueError:
            print("Invalid format. Please use YYYY-MM-DD.")
            continue
        if d > today:
            print("Start date cannot be in the future.")
            continue
        return datetime.combine(d, datetime.min.time())


# === FILE SELECTION ===
def select_files_in_dates(folder: Path, ext: str, allowed_dates: Set[str]) -> List[Path]:
    picks: List[Tuple[str, Path]] = []
    for p in folder.glob(f"rss_articles_*.{ext}"):
        m = DATE_RE.search(p.name)
        if not m:
            continue
        dstr = m.group(1)
        if dstr in allowed_dates:
            picks.append((dstr, p))
    picks.sort(key=lambda x: (x[0], x[1].name))
    return [p for _, p in picks]


# === DEDUPE HELPER ===
def build_master_csv(csv_files: List[Path], out_path: Path) -> tuple[int, int, int]:
    """
    Returns (unique_count, total_rows_read, duplicate_count).
    """
    seen: Set[Tuple[str, ...]] = set()
    unique_rows: List[Dict[str, str]] = []
    total_rows = 0

    for fp in csv_files:
        with fp.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            missing = [k for k in FIELDS if k not in (reader.fieldnames or [])]
            if missing:
                raise ValueError(f"{fp} missing expected fields: {missing}")

            for row in reader:
                total_rows += 1
                identity = tuple((row.get(k, "") or "").strip() for k in FIELDS)
                if identity in seen:
                    continue
                seen.add(identity)
                unique_rows.append({k: row.get(k, "") for k in FIELDS})

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in unique_rows:
            writer.writerow(r)

    unique_count = len(unique_rows)
    dup_count = max(0, total_rows - unique_count)
    return unique_count, total_rows, dup_count

# === MAIN ===
def main() -> None:
    # Support optional CLI arg: --start YYYY-MM-DD
    start_dt: datetime
    if len(sys.argv) >= 3 and sys.argv[1] == "--start":
        try:
            start_dt = parse_date_str(sys.argv[2])
        except ValueError:
            print("Invalid --start date. Use YYYY-MM-DD.")
            sys.exit(2)
        today_dt = datetime.now()
        if start_dt.date() > today_dt.date():
            print("Start date cannot be in the future.")
            sys.exit(2)
    else:
        start_dt = prompt_start_date()
        today_dt = datetime.now()

    # Build allowed date set (inclusive)
    date_list = daterange(start_dt, today_dt)
    allowed_dates = set(date_list)
    start_str, end_str = date_list[0], date_list[-1]

    # Select inputs
    csv_inputs = select_files_in_dates(ARTICLES_DIR, "csv", allowed_dates)

    # Plan outputs
    csv_out  = ARTICLES_DIR  / f"deduped_candidate_articles_{start_str}_to_{end_str}.csv"

    # Build masters
    if csv_inputs:
        csv_unique, csv_total, csv_dups = build_master_csv(csv_inputs, csv_out)
    else:
        csv_unique, csv_total, csv_dups = 0, 0, 0

    # Summary (stdout) — matches Google style
    print(f"[CSV]  {len(csv_inputs)} files -> {csv_dups} duplicates out of {csv_total} rows. "
        f"{csv_unique} unique rows saved to {csv_out}")

 
if __name__ == "__main__":
    main()