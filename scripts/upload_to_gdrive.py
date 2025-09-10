#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Upload a file or a directory (optionally zipped) to Google Drive.

Auth options:
- OAuth client (interactive the first time): provide client config JSON via
  --credentials or env GDRIVE_CREDENTIALS (default: credentials/credentials.json)
  token will be saved to credentials/token.json

Usage examples:
  # upload directory as zip into folder id
  python scripts/upload_to_gdrive.py --src data/openFDA_drug_event --zip --folder-id <GDRIVE_FOLDER_ID>

  # upload a single file as-is
  python scripts/upload_to_gdrive.py --src data/openFDA_drug_event/er_tables/report.csv.gz --folder-id <ID>
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from tqdm import tqdm


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def zip_dir(src: Path, dst_zip: Path) -> Path:
    base = str(dst_zip.with_suffix(""))
    # shutil.make_archive adds .zip automatically when format='zip'
    out = shutil.make_archive(base, "zip", root_dir=str(src))
    return Path(out)


def _find_child_folder(drive: GoogleDrive, name: str, parent_id: str) -> str | None:
    # Query existing folder with same name under parent to avoid duplicates
    q = (
        f"'{parent_id}' in parents and "
        f"mimeType='application/vnd.google-apps.folder' and "
        f"trashed=false and title='{name}'"
    )
    lst = drive.ListFile({'q': q}).GetList()
    return lst[0]['id'] if lst else None


def get_or_create_folder(drive: GoogleDrive, name: str, parent_id: str) -> str:
    exist = _find_child_folder(drive, name, parent_id)
    if exist:
        return exist
    meta = {
        "title": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [{"id": parent_id}],
    }
    f = drive.CreateFile(meta)
    f.Upload()
    return f["id"]


def _find_child_file(drive: GoogleDrive, name: str, parent_id: str) -> str | None:
    q = (
        f"'{parent_id}' in parents and "
        f"mimeType!='application/vnd.google-apps.folder' and "
        f"trashed=false and title='{name}'"
    )
    lst = drive.ListFile({'q': q}).GetList()
    return lst[0]['id'] if lst else None


def upload_file(drive: GoogleDrive, local_path: Path, name: str, parent_id: str) -> str:
    # If a file with the same name already exists in the target folder, overwrite content instead of duplicating
    exist_id = _find_child_file(drive, name, parent_id)
    if exist_id:
        f = drive.CreateFile({'id': exist_id})
        f.SetContentFile(str(local_path))
        f.Upload()
        return exist_id
    meta = {"title": name, "parents": [{"id": parent_id}]}
    f = drive.CreateFile(meta)
    f.SetContentFile(str(local_path))
    f.Upload()
    return f["id"]


def build_gauth(creds_path: Path, token_path: Path) -> GoogleAuth:
    gauth = GoogleAuth()
    # Tell pydrive2 where client secrets are
    gauth.LoadClientConfigFile(str(creds_path))
    # Try existing token
    if token_path.exists():
        gauth.LoadCredentialsFile(str(token_path))
        if gauth.credentials is None:
            gauth.CommandLineAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
    else:
        # First time: interactive flow (console based)
        try:
            gauth.LocalWebserverAuth()
        except Exception:
            gauth.CommandLineAuth()
    # Save token
    ensure_dir(token_path.parent)
    gauth.SaveCredentialsFile(str(token_path))
    return gauth


def main():
    ap = argparse.ArgumentParser(description="Upload file/dir to Google Drive")
    ap.add_argument("--src", required=True, help="Source file or directory to upload")
    ap.add_argument("--folder-id", required=True, help="Destination Google Drive folder id")
    ap.add_argument("--name", default=None, help="Name to show on Drive (default: source filename)")
    ap.add_argument("--zip", action="store_true", help="Zip directory before upload (if omitted for a directory, uploads recursively)")
    ap.add_argument("--credentials", default=os.getenv("GDRIVE_CREDENTIALS", "credentials/credentials.json"))
    ap.add_argument("--token", default=os.getenv("GDRIVE_TOKEN", "credentials/token.json"))
    args = ap.parse_args()

    src = Path(args.src)
    if not src.exists():
        raise SystemExit(f"Source not found: {src}")

    upload_path: Path
    tmpdir = None
    if src.is_dir() and args.zip:
        tmpdir = tempfile.TemporaryDirectory()
        zip_dst = Path(tmpdir.name) / ((args.name or src.name) + ".zip")
        upload_path = zip_dir(src, zip_dst)
        name = args.name or upload_path.name
    elif src.is_dir():
        upload_path = None  # we will upload recursively
        name = args.name or src.name
    else:
        upload_path = src
        name = args.name or upload_path.name

    creds_path = Path(args.credentials)
    token_path = Path(args.token)
    if not creds_path.exists():
        raise SystemExit(f"Credentials not found: {creds_path}. Provide OAuth client JSON via --credentials")

    gauth = build_gauth(creds_path, token_path)
    drive = GoogleDrive(gauth)

    if src.is_dir() and not args.zip:
        # Ensure or reuse root folder under destination (avoid duplicates)
        root_id = get_or_create_folder(drive, name, args.folder_id)
        print(f"[upload] Root folder '{name}' id={root_id}")

        # Cache for folder path -> id to avoid repeat GDrive queries
        folder_cache: dict[Path, str] = {Path('.'): root_id}

        def ensure_folder_path(rel_path: Path) -> str:
            cur = Path('.')
            parent_id = root_id
            for part in rel_path.parts:
                cur = cur / part
                if cur in folder_cache:
                    parent_id = folder_cache[cur]
                    continue
                new_id = get_or_create_folder(drive, part, parent_id)
                folder_cache[cur] = new_id
                parent_id = new_id
            return parent_id

        # First ensure all directories exist (stable order)
        for dirpath, dirnames, filenames in os.walk(src):
            rel_dir = Path(dirpath).relative_to(src)
            if rel_dir != Path('.'):
                ensure_folder_path(rel_dir)

        # Then upload files with progress
        all_files = []
        for dirpath, dirnames, filenames in os.walk(src):
            for fn in filenames:
                all_files.append((Path(dirpath), fn))

        pbar = tqdm(total=len(all_files), desc="Uploading", unit="file")
        for dirpath, fn in all_files:
            rel_dir = Path(dirpath).relative_to(src)
            parent_id = folder_cache.get(rel_dir, root_id)
            local = Path(dirpath) / fn
            file_id = upload_file(drive, local, fn, parent_id)
            pbar.set_postfix_str(str(local.relative_to(src)))
            pbar.update(1)
        pbar.close()
        print("[upload] Completed recursive upload without duplicates.")
    else:
        meta = {"title": name, "parents": [{"id": args.folder_id}]}
        f = drive.CreateFile(meta)
        f.SetContentFile(str(upload_path))
        print(f"[upload] Uploading {upload_path} → folder {args.folder_id} as '{name}' ...")
        f.Upload()
        print(f"[upload] Done. File id: {f['id']}")

    if tmpdir is not None:
        tmpdir.cleanup()


if __name__ == "__main__":
    main()
