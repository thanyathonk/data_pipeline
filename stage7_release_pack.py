#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage-7: Packaging & Release
- Collect key outputs and write MANIFEST.json (size, sha256, paths)
"""

import os, sys, argparse, json, hashlib, datetime, shutil

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def sha256_file(path, buf=1024*1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(buf)
            if not b: break
            h.update(b)
    return h.hexdigest()

DEFAULT_FILES = [
    "data/processed/FDA_patient_drug_report_reaction.csv.gz",
    "data/processed/pediatric.csv.gz",
    "data/processed/adults.csv.gz",
    "data/processed/merge_coverage.json",
    "data/processed/split_stats.json",
    "data/qa/unmatched_products.csv.gz",
    "data/qa/unmapped_adr.csv.gz",
    "data/qa/dupe_pairs.csv.gz",
    "data/qa/summary.json"
]

def main(a):
    day = a.date or datetime.datetime.now().strftime("%Y%m%d")
    out_dir = os.path.join("data","release", day)
    ensure_dir(out_dir)

    # collect files
    files = a.files or DEFAULT_FILES
    files = [f.strip() for f in files if f.strip()]
    found, missing = [], []
    for p in files:
        if os.path.exists(p):
            found.append(p)
        else:
            missing.append(p)

    # copy files into release dir (preserve basename)
    copied = []
    for p in found:
        dst = os.path.join(out_dir, os.path.basename(p))
        shutil.copy2(p, dst)
        copied.append(dst)

    # manifest
    items = []
    for p in copied:
        st = os.stat(p)
        items.append({
            "file": p,
            "size_bytes": st.st_size,
            "sha256": sha256_file(p)
        })

    manifest = {
        "release_date": day,
        "generated_at": datetime.datetime.now().isoformat(),
        "outputs": items,
        "missing": missing
    }
    manifest_path = os.path.join(out_dir, "MANIFEST.json")
    open(manifest_path, "w", encoding="utf-8").write(json.dumps(manifest, ensure_ascii=False, indent=2))

    print(f"[OK] Stage-7 release → {out_dir}")
    if missing:
        print("[WARN] Missing files not packaged:")
        for m in missing: print(" -", m)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="release folder name (YYYYMMDD). default: today")
    ap.add_argument("--files", nargs="*", help="override file list to package")
    args = ap.parse_args()
    main(args)
