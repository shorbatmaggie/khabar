from __future__ import annotations
import csv
import re
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Set

# === CONFIG ===
BASE_DIR = Path(__file__).resolve().parent
ARTICLES_DIR = BASE_DIR / "data/digests/rss_digests"

# Expected fields/order
FIELDS = ["keywords", "title", "snippet", "date_published", "source_domain", "url"]

PATTERN_STEM = "rss_articles"
OUTPUT_PREFIX = "deduped_candidate_articles"

# Regex helpers for raw and deduped files
DATE_RE = re.compile(rf"{PATTERN_STEM}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", re.IGNORECASE)
DEDUPED_RE = re.compile(
    rf"{OUTPUT_PREFIX}_(\d{{4}}-\d{{2}}-\d{{2}})_to_(\d{{4}}-\d{{2}}-\d{{2}})\.csv",
    re.IGNORECASE,
)


# === DATE HELPERS ===
def parse_date_str(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


# === FILE SELECTION ===
def list_csv_files_with_dates(folder: Path) -> List[Tuple[datetime, Path]]:
    picks: List[Tuple[datetime, Path]] = []
    for p in folder.glob(f"{PATTERN_STEM}_*.csv"):
        m = DATE_RE.search(p.name)
        if not m:
            continue
        dt = parse_date_str(m.group(1))
        picks.append((dt, p))
    picks.sort(key=lambda x: (x[0], x[1].name))
    return picks


def find_latest_deduped_end_date(folder: Path) -> Optional[datetime]:
    latest_end: Optional[datetime] = None
    for p in folder.glob(f"{OUTPUT_PREFIX}_*_to_*.csv"):
        m = DEDUPED_RE.fullmatch(p.name)
        if not m:
            continue
        end_dt = parse_date_str(m.group(2))
        if latest_end is None or end_dt > latest_end:
            latest_end = end_dt
    return latest_end


def determine_start_date(
    csv_entries: List[Tuple[datetime, Path]],
    cli_start: Optional[datetime]
) -> datetime:
    if cli_start is not None:
        return cli_start

    latest_end = find_latest_deduped_end_date(ARTICLES_DIR)
    if latest_end is not None:
        return latest_end + timedelta(days=1)

    # No deduped files yet; start from earliest available CSV.
    return csv_entries[0][0]


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
    cli_start: Optional[datetime] = None
    if len(sys.argv) >= 3 and sys.argv[1] == "--start":
        try:
            cli_start = parse_date_str(sys.argv[2])
        except ValueError:
            print("Invalid --start date. Use YYYY-MM-DD.")
            sys.exit(2)
        today_dt = datetime.now()
        if cli_start.date() > today_dt.date():
            print("Start date cannot be in the future.")
            sys.exit(2)

    csv_entries = list_csv_files_with_dates(ARTICLES_DIR)
    if not csv_entries:
        print(f"No CSV files found in {ARTICLES_DIR}")
        return

    start_dt = determine_start_date(csv_entries, cli_start)

    selected = [(dt, path) for dt, path in csv_entries if dt >= start_dt]
    if not selected:
        print(f"No CSV files found on or after {start_dt.strftime('%Y-%m-%d')}")
        return

    start_str = selected[0][0].strftime("%Y-%m-%d")
    end_str = selected[-1][0].strftime("%Y-%m-%d")
    csv_inputs = [path for _, path in selected]

    # Plan outputs
    csv_out = ARTICLES_DIR / f"{OUTPUT_PREFIX}_{start_str}_to_{end_str}.csv"

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
