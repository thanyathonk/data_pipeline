# split_drug.py
import os, argparse, pandas as pd
from common_utils import to_str_sid, slug_key, read_csv_any, write_csv_gz, dump_json

def main(er_root: str, out_dir: str, base_csv: str=None, suspect_only: bool=False, dedup: bool=True):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลดจากไฟล์มาตรฐานเดียว
    drug_path = os.path.join(er_root, "drugcharacteristics.csv.gz")
    if not os.path.exists(drug_path):
        raise SystemExit(f"Expected file not found: {drug_path} (from ER step)")

    df = read_csv_any(drug_path)
    if "safetyreportid" not in df.columns or "medicinal_product" not in df.columns:
        raise SystemExit("drugcharacteristics.csv.gz must contain 'safetyreportid' and 'medicinal_product'")

    df["safetyreportid"] = df["safetyreportid"].map(to_str_sid)

    # 2) จำกัดให้อยู่ใน baseline (ถ้ามี)
    if base_csv:
        base = read_csv_any(base_csv)
        keep = set(base["safetyreportid"].map(to_str_sid).dropna().astype(str))
        df = df[df["safetyreportid"].isin(keep)]

    # 3) เฉพาะ suspect (ตัวเลือกเสริม)
    if suspect_only and "drug_characterization" in df.columns:
        df = df[df["drug_characterization"].str.contains("suspect", case=False, na=False)]

    # 4) สร้าง map และ dict
    cols_keep = ["safetyreportid", "medicinal_product"]
    if "drug_characterization" in df.columns:
        cols_keep.append("drug_characterization")

    drug_map = df[cols_keep].dropna(subset=["medicinal_product"]).copy()
    if dedup:
        drug_map = drug_map.drop_duplicates(subset=["safetyreportid", "medicinal_product"])

    drug_map["product_key"] = drug_map["medicinal_product"].map(slug_key)
    drug_dict = drug_map[["medicinal_product", "product_key"]].drop_duplicates()

    # 5) เขียนผลลัพธ์
    write_csv_gz(drug_map,  os.path.join(out_dir, "drug_map.csv.gz"))
    write_csv_gz(drug_dict, os.path.join(out_dir, "drug_dict.csv.gz"))
    dump_json(
        {
            "source": os.path.relpath(drug_path, er_root),
            "rows": {"drug_map": int(len(drug_map)), "drug_dict": int(len(drug_dict))},
            "suspect_only": suspect_only,
            "dedup_pair": dedup
        },
        os.path.join(out_dir, "drug_split_stats.json")
    )
    print("[ok] split_drug: wrote drug_map.csv.gz and drug_dict.csv.gz")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--er-root", required=True, help=".../data/openFDA_drug_event/er_tables")
    ap.add_argument("--out-dir", required=True, help="e.g., ./data/split")
    ap.add_argument("--base-csv", help="baseline patients_report_serious_reporter.csv.gz (optional)")
    ap.add_argument("--suspect-only", action="store_true")
    ap.add_argument("--no-dedup", action="store_true")
    args = ap.parse_args()
    main(args.er_root, args.out_dir, args.base_csv, suspect_only=args.suspect_only, dedup=not args.no_dedup)
