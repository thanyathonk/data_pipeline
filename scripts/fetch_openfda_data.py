#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import hashlib
import os
import sys
import tarfile
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm


def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def is_archive(name: str) -> bool:
    name = name.lower()
    return any(
        name.endswith(ext)
        for ext in (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz")
    )


def extract(src: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if zipfile.is_zipfile(src):
        with zipfile.ZipFile(src, "r") as zf:
            zf.extractall(out_dir)
        return
    # tar-like
    try:
        with tarfile.open(src, "r:*") as tf:
            tf.extractall(out_dir)
        return
    except tarfile.ReadError:
        pass
    raise SystemExit(f"Unsupported archive format: {src}")


def http_download(url: str, dst: Path) -> None:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        pbar = tqdm(total=total or None, unit="B", unit_scale=True, desc="Downloading", disable=False)
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))
        pbar.close()


def maybe_gdown(url: str, dst: Path) -> bool:
    # try gdown if available and url is google drive
    if "drive.google.com" not in url.lower():
        return False
    try:
        import gdown  # type: ignore

        gdown.download(url, str(dst), quiet=False, fuzzy=True)
        return True
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser(description="Fetch openFDA data/archive into local data folder")
    ap.add_argument("--url", required=True, help="HTTP(S) URL or Google Drive link to archive")
    ap.add_argument("--out", default="data/openFDA_drug_event", help="Output folder")
    ap.add_argument("--sha256", default=None, help="Optional SHA256 checksum of the file")
    ap.add_argument("--force", action="store_true", help="Overwrite existing output")
    ap.add_argument("--skip-extract", action="store_true", help="Do not extract; just download")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Determine local filename
    name = os.path.basename(args.url.split("?")[0]) or "openfda_data"
    if not is_archive(name) and not args.skip_extract:
        # assume zip by default for single-file naming
        name += ".zip"
    tmp = out_dir.parent / f".{name}.part"
    dst = out_dir.parent / name

    print(f"[fetch] URL: {args.url}")
    print(f"[fetch] Save: {dst}")

    if dst.exists() and not args.force:
        print("[fetch] File exists; skip download")
    else:
        if not maybe_gdown(args.url, tmp):
            http_download(args.url, tmp)
        tmp.rename(dst)

    if args.sha256:
        print("[fetch] Verifying SHA256...")
        got = sha256sum(dst)
        if got.lower() != args.sha256.lower():
            raise SystemExit(f"SHA256 mismatch: got {got} expected {args.sha256}")
        print("[fetch] OK")

    if args.skip_extract:
        print("[fetch] Done (no extract)")
        return

    if is_archive(dst.name):
        print(f"[fetch] Extracting to {out_dir} ...")
        extract(dst, out_dir)
        print("[fetch] Extracted")
    else:
        print("[fetch] Not an archive; nothing to extract.")

    print("[fetch] Completed.")


if __name__ == "__main__":
    main()
