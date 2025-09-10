#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
merge_data_all_ages.py
------------------------
รวมตาราง Entity (patient + report + report_serious + reporter) จากขั้นตอน Entity (ER tables)
เพื่อสร้างฐานข้อมูลรวม "ทุกอายุ" สำหรับนำไปแตก Drug/ADR ในสเต็ปถัดไป (ยังไม่กรองอายุ/ยังไม่ map Drug/ADR)

Usage:
  python merge_data_all_ages.py \
    --er-root ../../data/openFDA_drug_event/er_tables_memory_efficient \
    --out-dir ../../data \
    --join-type inner

Input (ภายใต้ --er-root):
  - patient.csv.gz
  - report.csv.gz
  - report_serious.csv.gz
  - reporter.csv.gz

Output (ภายใต้ --out-dir):
  - patients_report_serious_reporter.csv.gz
  - patients_report_serious_reporter_stats.json
"""

import os
import json
import argparse
import pandas as pd


def _to_str_sid(s):
    """แปลง safetyreportid ให้เป็นสตริงสม่ำเสมอ (ตัด .0 ที่พ่วงมาจาก float)"""
    if pd.isna(s):
        return None
    try:
        v = str(s).strip()
        if v.endswith(".0"):
            v = v[:-2]
        return v
    except Exception:
        return str(s)


def read_entity(er_root: str, name: str, low_memory: bool = False) -> pd.DataFrame:
    """อ่านไฟล์ entity จากโฟลเดอร์ ER tables และบังคับ safetyreportid เป็น str"""
    path = os.path.join(er_root, f"{name}.csv.gz")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Not found: {path}")
    df = pd.read_csv(path, low_memory=low_memory)
    if "safetyreportid" in df.columns:
        df["safetyreportid"] = df["safetyreportid"].map(_to_str_sid)
    return df


def add_prefix(
    df: pd.DataFrame, prefix: str, except_cols=("safetyreportid",)
) -> pd.DataFrame:
    """กันชื่อคอลัมน์ชนกันด้วยการเติม prefix (ยกเว้นคีย์รวม)"""
    ren = {}
    for c in df.columns:
        if c not in except_cols:
            ren[c] = f"{prefix}{c}"
    return df.rename(columns=ren)


def main(er_root: str, out_dir: str, join_type: str = "inner"):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลด entity tables
    patient = read_entity(er_root, "patient")
    report = read_entity(er_root, "report")
    report_serious = read_entity(er_root, "report_serious")
    reporter = read_entity(er_root, "reporter")

    # 2) เก็บจำนวนก่อน merge
    stats = {
        "counts": {
            "patient": int(len(patient)),
            "report": int(len(report)),
            "report_serious": int(len(report_serious)),
            "reporter": int(len(reporter)),
        },
        "join_type": join_type,
    }

    # 3) กันชื่อคอลัมน์ชน: เติม prefix (ยกเว้น safetyreportid)
    patient_pref = add_prefix(patient, "patient_")
    report_pref = add_prefix(report, "report_")
    serious_pref = add_prefix(report_serious, "serious_")
    reporter_pref = add_prefix(reporter, "reportr_")

    # 4) Merge ตามลำดับ (ทุกอายุ)
    base = patient_pref.merge(report_pref, on="safetyreportid", how=join_type)
    base = base.merge(serious_pref, on="safetyreportid", how=join_type)
    base = base.merge(reporter_pref, on="safetyreportid", how=join_type)

    # 5) จัด dtype เบื้องต้น (เตรียมไว้สำหรับการ split ทีหลัง)
    age_like_cols = [c for c in base.columns if "patient_custom_master_age" in c]
    for c in age_like_cols:
        base[c] = pd.to_numeric(base[c], errors="coerce")

    # 6) บันทึกผลลัพธ์
    out_csv = os.path.join(out_dir, "patients_report_serious_reporter.csv.gz")
    base.to_csv(out_csv, index=False, compression="gzip")

    stats["counts"]["merged_rows"] = int(len(base))
    stats["nulls"] = {
        "patient_custom_master_age_null": (
            int(base[age_like_cols[0]].isna().sum()) if age_like_cols else None
        )
    }

    out_stats = os.path.join(out_dir, "patients_report_serious_reporter_stats.json")
    with open(out_stats, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote CSV : {out_csv}")
    print(f"[OK] Wrote STAT: {out_stats}")
    print(f"[OK] join_type={join_type}  rows={len(base)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--er-root",
        required=True,
        help="Path to ER tables directory (e.g., …/er_tables",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory to write merged outputs",
    )
    parser.add_argument(
        "--join-type",
        default="inner",
        choices=["inner", "left", "right", "outer"],
        help="Join type for merges (default: inner)",
    )
    args = parser.parse_args()
    main(args.er_root, args.out_dir, args.join_type)
