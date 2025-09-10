#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage-5: Split cohorts by age with NICHD categories
- Input: merged file from Stage-4
- Age column: patient_custom_master_age (year as float)
"""

import os, sys, argparse, json
import numpy as np
import pandas as pd

def ensure_dir(p): os.makedirs(os.path.dirname(p), exist_ok=True)
def read_csv_any(path): return pd.read_csv(path, low_memory=False)
def write_csv_gz(df, path):
    ensure_dir(path); comp = "gzip" if path.endswith((".gz",".gzip")) else None
    df.to_csv(path, index=False, compression=comp)
def dump_json(obj, path):
    ensure_dir(path); open(path,"w",encoding="utf-8").write(json.dumps(obj, ensure_ascii=False, indent=2))

def to_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan

def label_nichd(age_years: float) -> str|None:
    if not np.isfinite(age_years): return None
    # ตามเงื่อนไขที่ให้มา
    if age_years > 0 and age_years <= (1/12): return "term_neonatal"
    if age_years > (1/12) and age_years <= 1:  return "infancy"
    if age_years > 1 and age_years <= 2:       return "toddler"
    if age_years > 2 and age_years <= 5:       return "early_childhood"
    if age_years > 5 and age_years <= 11:      return "middle_childhood"
    if age_years > 11 and age_years <= 18:     return "early_adolescence"
    if age_years > 18 and age_years <= 21:     return "late_adolescence"
    return None

def main(a):
    df = read_csv_any(a.input)
    age_col = a.age_col
    if age_col not in df.columns:
        raise SystemExit(f"[split] not found age column: {age_col}")

    ages = df[age_col].apply(to_float)
    nichd = ages.apply(label_nichd)
    df["nichd"] = nichd

    pediatric = df[df["nichd"].notna()].copy()        # >0..<=21 ตามกฎ
    adults    = df[ages > 21].copy()                   # >21 = ผู้ใหญ่
    unknown   = df[df["nichd"].isna() & ~(ages > 21)].copy()  # อายุน้อย/ศูนย์/หาย

    write_csv_gz(pediatric, a.out_pediatric)
    write_csv_gz(adults,    a.out_adults)
    write_csv_gz(unknown,   a.out_unknown)

    stats = {
        "input_rows": int(len(df)),
        "age_col": age_col,
        "counts": {
            "pediatric": int(len(pediatric)),
            "adults": int(len(adults)),
            "unknown": int(len(unknown))
        },
        "nichd_breakdown": pediatric["nichd"].value_counts(dropna=False).to_dict(),
        "outputs": {
            "pediatric": os.path.abspath(a.out_pediatric),
            "adults": os.path.abspath(a.out_adults),
            "unknown": os.path.abspath(a.out_unknown)
        }
    }
    dump_json(stats, a.out_stats)
    print(f"[OK] Stage-5 split → {a.out_pediatric} , {a.out_adults}")
    print(f"[OK] Stats → {a.out_stats}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/processed/FDA_patient_drug_report_reaction.csv.gz")
    ap.add_argument("--age-col", default="patient_custom_master_age")
    ap.add_argument("--out-pediatric", default="data/processed/pediatric.csv.gz")
    ap.add_argument("--out-adults", default="data/processed/adults.csv.gz")
    ap.add_argument("--out-unknown", default="data/processed/age_unknown.csv.gz")
    ap.add_argument("--out-stats", default="data/processed/split_stats.json")
    args = ap.parse_args()
    main(args)
