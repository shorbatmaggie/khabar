from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Drive scopes
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def most_recent_friday(on_or_before: date) -> date:
    """
    Given a date, return the most recent Friday on or before that date.
    Monday=0, ..., Friday=4, Sunday=6.
    """
    offset = (on_or_before.weekday() - 4) % 7
    return on_or_before - timedelta(days=offset)


def compute_week_window(today: date) -> Tuple[date, date]:
    """
    Return (start_date, end_date) for the Sat–Fri window ending
    on the most recent Friday on or before `today`.
    """
    end_date = most_recent_friday(today)
    start_date = end_date - timedelta(days=6)  # 7 days: Sat–Fri
    return start_date, end_date


def get_drive_service(credentials_path: str):
    """
    Build an authenticated Drive v3 service using a service account JSON file.
    """
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=DRIVE_SCOPES,
    )
    return build("drive", "v3", credentials=creds)


def list_candidate_files(
    service,
    folder_id: str,
    date_re: re.Pattern,
) -> List[Tuple[str, str]]:
    """
    List files in the given folder and return those whose names match date_re.

    Returns a list of (file_id, file_name).
    """
    candidates: List[Tuple[str, str]] = []

    query = f"'{folder_id}' in parents and trashed = false"

    page_token = None
    while True:
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        for f in response.get("files", []):
            fid = f.get("id")
            name = f.get("name", "")
            if date_re.fullmatch(name):
                candidates.append((fid, name))

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    print(
        f"Found {len(candidates)} candidate files with pattern {date_re.pattern!r} "
        f"in folder {folder_id}"
    )
    return candidates


def filter_files_for_week(
    candidates: List[Tuple[str, str]],
    date_re: re.Pattern,
    start_date: date,
    end_date: date,
) -> List[Tuple[str, str, date]]:
    """
    Given a list of (file_id, file_name), return those whose filename date
    is in [start_date, end_date]. Returns (file_id, file_name, file_date).
    """
    picked: List[Tuple[str, str, date]] = []

    for file_id, name in candidates:
        m = date_re.fullmatch(name)
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        if start_date <= dt <= end_date:
            picked.append((file_id, name, dt))

    picked.sort(key=lambda x: (x[2], x[1]))
    return picked


def download_files(
    service,
    files: List[Tuple[str, str, date]],
    dest_dir: Path,
) -> None:
    """
    Download each file in `files` (file_id, name, date) into dest_dir.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    for file_id, name, dt in files:
        local_path = dest_dir / name
        print(f"Downloading {name} ({dt.isoformat()}) -> {local_path}")

        request = service.files().get_media(
            fileId=file_id,
            supportsAllDrives=True,
        )
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                # Uncomment for progress
                # print(f"  Download {int(status.progress() * 100)}%")
                pass

    print(f"Downloaded {len(files)} files into {dest_dir}")


def main() -> None:
    """
    Usage:
      python fetch_week_from_drive.py <dest_dir> <pattern_stem> <folder_env_var>

    Examples:
      # Google
      python fetch_week_from_drive.py weekly_input/google google_alerts_articles GDRIVE_GOOGLE_FOLDER_ID

      # RSS
      python fetch_week_from_drive.py weekly_input/rss rss_articles GDRIVE_RSS_FOLDER_ID
    """
    if len(sys.argv) < 4:
        print(
            "Usage: python fetch_week_from_drive.py <dest_dir> <pattern_stem> <folder_env_var>"
        )
        sys.exit(2)

    dest_dir = Path(sys.argv[1]).resolve()
    pattern_stem = sys.argv[2]
    folder_env_var = sys.argv[3]

    # Pattern: <pattern_stem>_YYYY-MM-DD.csv
    date_re = re.compile(
        rf"{re.escape(pattern_stem)}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv",
        re.IGNORECASE,
    )

    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    folder_id = os.environ.get(folder_env_var)

    if not credentials_path or not Path(credentials_path).is_file():
        print(
            "GOOGLE_APPLICATION_CREDENTIALS is not set or does not point to an existing file."
        )
        sys.exit(1)

    if not folder_id:
        print(f"{folder_env_var} environment variable is not set.")
        sys.exit(1)

    today = date.today()
    start_date, end_date = compute_week_window(today)
    print(
        f"Computing weekly window for Sat–Fri: "
        f"{start_date.isoformat()} to {end_date.isoformat()} "
        f"(week ending {end_date.isoformat()})"
    )

    service = get_drive_service(credentials_path)

    print(f"Listing candidate files in Drive folder {folder_id}...")
    candidates = list_candidate_files(service, folder_id, date_re)

    week_files = filter_files_for_week(candidates, date_re, start_date, end_date)

    if not week_files:
        print(
            "No files found for week window "
            f"{start_date.isoformat()} to {end_date.isoformat()}."
        )
        sys.exit(0)

    print("Files selected for this week:")
    for _, name, dt in week_files:
        print(f"  {dt.isoformat()} -> {name}")

    download_files(service, week_files, dest_dir)


if __name__ == "__main__":
    main()
