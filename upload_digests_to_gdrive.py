import os
import sys
import json
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


def get_drive_client(service_account_json: str):
    """
    Build an authenticated Google Drive client from GDRIVE_SERVICE_ACCOUNT_JSON.
    """
    try:
        info = json.loads(service_account_json)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse GDRIVE_SERVICE_ACCOUNT_JSON: {e}")
        sys.exit(1)

    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def upload_file(
    drive,
    local_path: Path,
    folder_id: str,
    mime_type: str = "text/csv",
    overwrite: bool = True,
):
    """
    Upload a single file to the given Drive folder.

    If overwrite=True, delete any existing files with the same name in that folder
    before uploading. This lets you rerun the collector without accumulating dupes.
    """
    file_name = local_path.name

    if overwrite:
        # Remove existing files with same name in target folder
        query = (
            f"name = '{file_name}' and "
            f"'{folder_id}' in parents and "
            f"trashed = false"
        )
        existing = (
            drive.files()
            .list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="allDrives",
            )
            .execute()
        )
        for f in existing.get("files", []):
            print(f"🗑️ Deleting existing Drive file with same name: {f['name']} ({f['id']})")
            try:
                drive.files().delete(fileId=f["id"]).execute()
            except HttpError as exc:
                if exc.resp.status == 404:
                    print(f"⚠️ File already removed or missing: {f['id']}")
                else:
                    raise

    file_metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaFileUpload(str(local_path), mimetype=mime_type)

    uploaded = (
        drive.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
            supportsAllDrives=True,
        )
        .execute()
    )
    print(f"✅ Uploaded {file_name} to Drive with ID: {uploaded['id']}")


def main():
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    sa_json = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON")

    if not folder_id:
        print("❌ Missing GDRIVE_FOLDER_ID env var.")
        sys.exit(1)
    if not sa_json:
        print("❌ Missing GDRIVE_SERVICE_ACCOUNT_JSON env var.")
        sys.exit(1)

    # No positional args → nothing to upload, but that's not an error.
    if len(sys.argv) < 2:
        print("ℹ️ No file paths supplied to upload_to_gdrive.py; nothing to upload.")
        sys.exit(0)

    drive = get_drive_client(sa_json)

    for arg in sys.argv[1:]:
        path = Path(arg)
        if not path.is_file():
            print(f"⚠️ Skipping non-existent file: {path}")
            continue
        upload_file(drive, path, folder_id)


if __name__ == "__main__":
    main()
