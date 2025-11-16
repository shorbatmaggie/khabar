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

# --- CONFIG ---

# Same naming pattern as your deduper
PATTERN_STEM = "google_alerts_articles"
DATE_RE = re.compile(rf"{PATTERN_STEM}_(\d{{4}}-\d{{2}}-\d{{2}})\.csv", re.IGNORECASE)

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
    start_date = end_date - timedelta(days=6)  # 7 days total: Sat–Fri
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
) -> List[Tuple[str, str]]:
    """
    List CSV files in the given folder that match google_alerts_articles_YYYY-MM-DD.csv.

    Returns a list of (file_id, file_name).
    """
    results: List[Tuple[str, str]] = []

    # Drive query: in folder, not trashed, name contains pattern, mimeType text/csv-ish
    # We keep the mimeType condition loose because some tools set slightly different types.
    query = (
        f"'{folder_id}' in parents and "
        "trashed = false and "
        f"name contains '{PATTERN_STEM}_'"
    )

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
            name = f.get("name", "")
            if DATE_RE.fullmatch(name):
                results.append((f["id"], name))

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return results


def filter_files_for_week(
    candidates: List[Tuple[str, str]],
    start_date: date,
    end_date: date,
) -> List[Tuple[str, str, date]]:
    """
    Given a list of (file_id, file_name), return those whose filename date
    is in [start_date, end_date]. Returns (file_id, file_name, file_date).
    """
    picked: List[Tuple[str, str, date]] = []

    for file_id, name in candidates:
        m = DATE_RE.fullmatch(name)
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        if start_date <= dt <= end_date:
            picked.append((file_id, name, dt))

    # Sort by date then name, to match your dedupe script behavior
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

        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                # You can uncomment if you want progress in logs
                # print(f"  Download {int(status.progress() * 100)}%.")
                pass

    print(f"Downloaded {len(files)} files into {dest_dir}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python fetch_google_week_from_drive.py <dest_dir>")
        sys.exit(2)

    dest_dir = Path(sys.argv[1]).resolve()

    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    folder_id = os.environ.get("GDRIVE_GOOGLE_FOLDER_ID")

    if not credentials_path or not Path(credentials_path).is_file():
        print(
            "GOOGLE_APPLICATION_CREDENTIALS is not set or does not point to an existing file."
        )
        sys.exit(1)

    if not folder_id:
        print("GDRIVE_GOOGLE_FOLDER_ID environment variable is not set.")
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
    candidates = list_candidate_files(service, folder_id)
    print(f"Found {len(candidates)} candidate files with matching pattern.")

    week_files = filter_files_for_week(candidates, start_date, end_date)

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