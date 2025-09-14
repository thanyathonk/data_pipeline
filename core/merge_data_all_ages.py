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


def read_entity(er_root: str, name: str, low_memory: bool = False, dtype_str: bool = True) -> pd.DataFrame:
    """อ่านไฟล์ entity จากโฟลเดอร์ ER tables และบังคับ safetyreportid เป็น str"""
    path = os.path.join(er_root, f"{name}.csv.gz")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Not found: {path}")
    df = pd.read_csv(path, low_memory=low_memory, dtype=str if dtype_str else None)
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


def _filter_by_ids(path: str, id_set: set[str]) -> pd.DataFrame:
    """อ่านไฟล์ใหญ่แบบ chunk แล้วคัดแถวที่ safetyreportid อยู่ใน id_set (dtype=str)."""
    keep = []
    for ch in pd.read_csv(path, dtype=str, chunksize=1_000_000, low_memory=False):
        if "safetyreportid" not in ch.columns:
            continue
        ch["safetyreportid"] = ch["safetyreportid"].map(_to_str_sid)
        ch = ch[ch["safetyreportid"].isin(id_set)]
        if not ch.empty:
            keep.append(ch)
    return pd.concat(keep, ignore_index=True) if keep else pd.DataFrame(columns=["safetyreportid"]) 


def main(er_root: str, out_dir: str, join_type: str = "inner"):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลด entity tables แบบ dtype=str (ลดปัญหาแปลง float) และใช้กลยุทธ์ stream join เพื่อลด memory
    patient_path = os.path.join(er_root, "patient.csv.gz")
    report_path = os.path.join(er_root, "report.csv.gz")
    serious_path = os.path.join(er_root, "report_serious.csv.gz")
    reporter_path = os.path.join(er_root, "reporter.csv.gz")
    for p in (patient_path, report_path, serious_path, reporter_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Not found: {p}")

    # pre-count sizes for stats (cheap sampling)
    def _count_rows(path):
        c = 0
        for ch in pd.read_csv(path, dtype=str, chunksize=2_000_000, low_memory=False):
            c += len(ch)
        return c

    patient_n = _count_rows(patient_path)
    report_n = _count_rows(report_path)
    serious_n = _count_rows(serious_path)
    reporter_n = _count_rows(reporter_path)

    # 2) เก็บจำนวนก่อน merge
    stats = {
        "counts": {
            "patient": int(patient_n),
            "report": int(report_n),
            "report_serious": int(serious_n),
            "reporter": int(reporter_n),
        },
        "join_type": join_type,
    }

    # 3) stream join เป็นก้อนตาม patient chunk
    out_csv = os.path.join(out_dir, "patients_report_serious_reporter.csv.gz")
    wrote_header = False
    merged_total = 0
    age_null_total = 0

    for pch in pd.read_csv(patient_path, dtype=str, chunksize=2_000_000, low_memory=False):
        if pch.empty:
            continue
        pch["safetyreportid"] = pch["safetyreportid"].map(_to_str_sid)
        ids = set(pch["safetyreportid"].dropna().astype(str))

        # filter other tables by ids in this chunk
        rep = _filter_by_ids(report_path, ids)
        ser = _filter_by_ids(serious_path, ids)
        rtr = _filter_by_ids(reporter_path, ids)

        # prefix columns
        p_pref = add_prefix(pch, "patient_")
        rep_pref = add_prefix(rep, "report_")
        ser_pref = add_prefix(ser, "serious_")
        rtr_pref = add_prefix(rtr, "reportr_")

        # merge in-memory for this chunk
        base = p_pref.merge(rep_pref, on="safetyreportid", how=join_type)
        base = base.merge(ser_pref, on="safetyreportid", how=join_type)
        base = base.merge(rtr_pref, on="safetyreportid", how=join_type)

        # numeric coercion for age column(s) if exist
        age_like_cols = [c for c in base.columns if "patient_custom_master_age" in c]
        for c in age_like_cols:
            base[c] = pd.to_numeric(base[c], errors="coerce")
        if age_like_cols:
            age_null_total += int(base[age_like_cols[0]].isna().sum())

        # append to gzip file
        mode = "wb" if not wrote_header else "ab"
        import gzip as _gz
        with _gz.open(out_csv, mode) as f:
            base.to_csv(f, index=False, header=not wrote_header)
        wrote_header = True
        merged_total += len(base)

        # free explicitly
        del pch, rep, ser, rtr, p_pref, rep_pref, ser_pref, rtr_pref, base

    out_stats = os.path.join(out_dir, "patients_report_serious_reporter_stats.json")
    with open(out_stats, "w", encoding="utf-8") as f:
        stats["counts"]["merged_rows"] = int(merged_total)
        stats["nulls"] = {"patient_custom_master_age_null": age_null_total}
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote CSV : {out_csv}")
    print(f"[OK] Wrote STAT: {out_stats}")
    print(f"[OK] join_type={join_type}  rows={merged_total}")


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
