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
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

from pydrive2.auth import GoogleAuth
from pydrive2 import settings as pydrive2_settings
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


def load_json_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def build_gauth(
    creds_path: Path,
    token_path: Path,
    auth_mode: str = "auto",  # auto|cli|localweb|service
    service_account: Path | None = None,
    reauth: bool = False,
) -> GoogleAuth:
    """Build GoogleAuth with sensible defaults for headless use.

    - Ensures refresh_token is requested and saved.
    - Supports service account (no browser).
    - Allows forcing CLI auth (no browser) or local webserver.
    """
    gauth = GoogleAuth()

    if service_account is not None:
        # Service account auth (share destination folder with SA email beforehand)
        service_cfg = {"client_json_file_path": str(service_account)}
        # Some PyDrive2 versions require 'client_user_email' when domain-wide delegation is in use.
        # Respect optional env/CLI value GDRIVE_IMPERSONATE / --impersonate.
        imp = os.getenv("GDRIVE_IMPERSONATE")
        if imp:
            service_cfg["client_user_email"] = imp
        gauth.settings.update(
            {
                "client_config_backend": "service",
                "service_config": service_cfg,
            }
        )
        try:
            gauth.ServiceAuth()
        except pydrive2_settings.InvalidConfigError as e:
            raise SystemExit(
                "Service account auth failed: {}\n".format(e)
                + "Hints:\n"
                + "- If your PyDrive2 requires client_user_email, pass --impersonate <user@domain> and enable Domain-wide Delegation for the service account with Drive scope.\n"
                + "- Or share the destination Drive folder with the service account email and use a PyDrive2 version that doesn't require impersonation.\n"
            )
        return gauth

    # OAuth (user) flow with refresh token
    gauth.settings.update(
        {
            "client_config_backend": "file",
            "client_config_file": str(creds_path),
            # Request refresh token explicitly
            "get_refresh_token": True,
            # Drive scope (full access to allow upload/overwrite)
            "oauth_scope": [
                "https://www.googleapis.com/auth/drive",
            ],
            # Persist credentials
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": str(token_path),
        }
    )

    # Try existing token unless user forces reauth
    if token_path.exists() and not reauth:
        gauth.LoadCredentialsFile(str(token_path))
        try:
            if gauth.credentials is None:
                raise RuntimeError("no creds")
            if gauth.access_token_expired:
                gauth.Refresh()
            else:
                gauth.Authorize()
        except Exception:
            # Fall through to interactive auth below
            pass

    if gauth.credentials is None:
        # First-time or reauth
        if auth_mode in ("localweb", "auto"):
            try:
                # Will attempt to open a browser; if not possible, fall back to CLI
                gauth.LocalWebserverAuth()
            except Exception:
                gauth.CommandLineAuth()
        elif auth_mode == "cli":
            gauth.CommandLineAuth()
        else:
            # default fallback
            gauth.CommandLineAuth()

    # Ensure token saved (includes refresh_token when get_refresh_token=True)
    ensure_dir(token_path.parent)
    gauth.SaveCredentialsFile(str(token_path))
    return gauth


def main():
    ap = argparse.ArgumentParser(description="Upload file/dir to Google Drive")
    # Config-first parsing
    ap.add_argument("--config", default=os.getenv("GDRIVE_CONFIG", "scripts/gdrive_upload.json"), help="Path to JSON config (optional). CLI overrides values from config. If the default path exists, it will be used.")
    # Define all options with non-required first pass; we will re-parse after loading config
    ap.add_argument("--src", required=False, help="Source file or directory to upload")
    ap.add_argument("--folder-id", required=False, help="Destination Google Drive folder id")
    ap.add_argument("--name", default=None, help="Name to show on Drive (default: source filename)")
    ap.add_argument("--zip", action="store_true", help="Zip directory before upload (if omitted for a directory, uploads recursively)")
    ap.add_argument("--zip-out", default=None, help="Optional explicit path for the zip file (useful when controlling disk location)")
    ap.add_argument("--credentials", default=os.getenv("GDRIVE_CREDENTIALS", "credentials/credentials.json"))
    ap.add_argument("--token", default=os.getenv("GDRIVE_TOKEN", "credentials/token.json"))
    ap.add_argument("--auth-mode", choices=["auto", "cli", "localweb", "service"], default=os.getenv("GDRIVE_AUTH_MODE", "auto"), help="Auth method: 'cli' avoids opening browser; 'service' uses a service account JSON")
    ap.add_argument("--service-account", default=os.getenv("GDRIVE_SERVICE_ACCOUNT", None), help="Path to service account JSON (implies --auth-mode service)")
    ap.add_argument("--impersonate", default=os.getenv("GDRIVE_IMPERSONATE", None), help="User email to impersonate for service account (requires domain-wide delegation)")
    ap.add_argument("--reauth", action="store_true", help="Force re-authentication and refresh-token acquisition")

    # First pass to get config path
    pre_args, _ = ap.parse_known_args()

    # If config exists, load and set defaults
    if pre_args.config and Path(pre_args.config).exists():
        cfg = load_json_config(Path(pre_args.config))
        # Map config keys directly to argparse defaults when names match
        valid_keys = {
            "src",
            "folder_id",
            "name",
            "zip",
            "zip_out",
            "credentials",
            "token",
            "auth_mode",
            "service_account",
            "impersonate",
            "reauth",
        }
        # Support both dash and underscore keys
        normalized = {}
        for k, v in cfg.items():
            nk = k.replace("-", "_")
            if nk in valid_keys:
                normalized[nk] = v
        ap.set_defaults(**normalized)

    # Now parse final args (CLI overrides defaults/config)
    args = ap.parse_args()

    src = Path(args.src)
    if not src.exists():
        raise SystemExit(f"Source not found: {src}")

    upload_path: Path
    tmpdir = None
    if src.is_dir() and args.zip:
        if args.zip_out:
            # Use explicit user-provided path, ensure parent exists
            zip_dst = Path(args.zip_out)
            ensure_dir(zip_dst.parent)
            upload_path = zip_dir(src, zip_dst)
            tmpdir = None
        else:
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

    # Resolve auth mode
    auth_mode = args.auth_mode
    sa_path = Path(args.service_account) if args.service_account else None
    if sa_path is not None:
        auth_mode = "service"

    # Only require OAuth client credentials when not using service account auth
    if auth_mode != "service" and not creds_path.exists():
        raise SystemExit(
            f"Credentials not found: {creds_path}. Provide OAuth client JSON via --credentials"
        )

    # When service account is used, some PyDrive2 versions require client_user_email
    # if domain-wide delegation is expected. Pass it when provided via --impersonate.
    if auth_mode == "service" and sa_path is None:
        raise SystemExit("--service-account is required when --auth-mode service")

    # Map CLI --impersonate to env for build_gauth
    if auth_mode == "service" and args.impersonate:
        os.environ["GDRIVE_IMPERSONATE"] = args.impersonate

    gauth = build_gauth(
        creds_path,
        token_path,
        auth_mode=auth_mode,
        service_account=sa_path,
        reauth=args.reauth,
    )
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
