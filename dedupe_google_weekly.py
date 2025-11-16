from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Set

# === CONFIG ===
BASE_DIR = Path(__file__).resolve().parent

# Default input dir for daily digests (used if --input-dir is not provided)
DEFAULT_INPUT_DIR = BASE_DIR / "data/digests/google_digests"

# Weekly output dir
WEEKLY_DIR = BASE_DIR / "data/digests/weekly"

# Expected fields/order for CSV (same as your original script)
FIELDS = ["keywords", "title", "snippet", "date_published", "source_domain", "url", "in_roundup"]

# Filenames: google_alerts_articles_YYYY-MM-DD.csv
PATTERN_STEM = "google_alerts_articles"
DATE_RE = re.compile(rf"{PATTERN_STEM}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", re.IGNORECASE)


# === DATE HELPERS ===
def parse_date_str(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def most_recent_friday(on_or_before: date) -> date:
    """
    Given a date, return the most recent Friday on or before that date.
    Python weekday(): Monday=0, ..., Friday=4, Sunday=6.
    """
    offset = (on_or_before.weekday() - 4) % 7
    return on_or_before - timedelta(days=offset)


# === FILE SELECTION ===
def list_csv_files_with_dates(folder: Path) -> List[Tuple[date, Path]]:
    """
    Scan the folder for files named google_alerts_articles_YYYY-MM-DD.csv
    and return a sorted list of (date, path).
    """
    picks: List[Tuple[date, Path]] = []
    for p in folder.glob(f"{PATTERN_STEM}_*.csv"):
        m = DATE_RE.fullmatch(p.name)
        if not m:
            continue
        dt = parse_date_str(m.group(1)).date()
        picks.append((dt, p))
    picks.sort(key=lambda x: (x[0], x[1].name))
    return picks


def filter_files_for_week(
    entries: List[Tuple[date, Path]],
    week_ending: date,
    window_days: int = 7,
) -> List[Tuple[date, Path]]:
    """
    Given (date, path) entries and a week-ending date (typically Friday),
    return those whose date is in [week_ending - (window_days-1), week_ending].
    Default window is 7 days (Sat–Fri).
    """
    if window_days <= 0:
        raise ValueError("window_days must be positive")

    start_date = week_ending - timedelta(days=window_days - 1)
    return [
        (dt, path)
        for dt, path in entries
        if start_date <= dt <= week_ending
    ]


# === DEDUP HELPER ===
def build_master_csv(csv_files: List[Path], out_path: Path) -> tuple[int, int, int]:
    """
    Read all csv_files, deduplicate rows based on FIELDS tuple identity, and write out_path.
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
                # Preserve original values (no stripping on write, only on identity)
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


# === ARG PARSING (minimal, non-interactive) ===
def parse_args(argv: List[str]) -> tuple[Path, Optional[date]]:
    """
    Very simple CLI parser for:
      --input-dir PATH      (optional, default DEFAULT_INPUT_DIR)
      --week-ending YYYY-MM-DD  (optional, default = most recent Friday <= today)
    """
    input_dir: Path = DEFAULT_INPUT_DIR
    week_ending: Optional[date] = None

    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--input-dir" and i + 1 < len(argv):
            input_dir = Path(argv[i + 1]).resolve()
            i += 2
        elif arg == "--week-ending" and i + 1 < len(argv):
            try:
                week_ending = parse_date_str(argv[i + 1]).date()
            except ValueError:
                print("Invalid --week-ending date. Use YYYY-MM-DD.")
                sys.exit(2)
            i += 2
        else:
            print(f"Unknown argument: {arg}")
            print("Usage: python dedupe_google_weekly.py [--input-dir PATH] [--week-ending YYYY-MM-DD]")
            sys.exit(2)

    return input_dir, week_ending


# === MAIN ===
def main() -> None:
    input_dir, week_ending = parse_args(sys.argv)

    if not input_dir.is_dir():
        print(f"Input directory does not exist or is not a directory: {input_dir}")
        sys.exit(1)

    # Determine week-ending date (Friday) if not provided
    today = datetime.now().date()
    if week_ending is None:
        week_ending = most_recent_friday(today)

    # List candidate CSV files
    csv_entries = list_csv_files_with_dates(input_dir)
    if not csv_entries:
        print(f"No CSV files found in {input_dir}")
        return

    # Filter to Sat–Fri window (7 days)
    selected = filter_files_for_week(csv_entries, week_ending, window_days=7)
    if not selected:
        print(
            f"No CSV files in {input_dir} for week ending {week_ending.isoformat()} "
            f"(Sat–Fri window)."
        )
        return

    csv_inputs = [path for _, path in selected]

    # Weekly output file: google_weekly_<weekending>.csv
    end_str = week_ending.strftime("%Y-%m-%d")
    csv_out = WEEKLY_DIR / f"google_weekly_{end_str}.csv"

    csv_unique, csv_total, csv_dups = build_master_csv(csv_inputs, csv_out)

    # Summary
    print(
        f"[CSV] week ending {end_str} | {len(csv_inputs)} files | "
        f"{csv_dups} duplicates out of {csv_total} rows. "
        f"{csv_unique} unique rows saved to {csv_out}"
    )


if __name__ == "__main__":
    main()
