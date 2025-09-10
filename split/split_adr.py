# split_adr.py
import os, argparse, pandas as pd
from common_utils import to_str_sid, slug_key, read_csv_any, write_csv_gz, dump_json

def main(er_root: str, out_dir: str, base_csv: str=None, dedup: bool=True):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลดจากไฟล์มาตรฐานเดียว
    reac_path = os.path.join(er_root, "reactions.csv.gz")
    if not os.path.exists(reac_path):
        raise SystemExit(f"Expected file not found: {reac_path} (from ER step)")

    df = read_csv_any(reac_path)
    required = ["safetyreportid", "reaction_meddrapt"]
    for c in required:
        if c not in df.columns:
            raise SystemExit(f"reactions.csv.gz must contain '{c}'")

    df["safetyreportid"] = df["safetyreportid"].map(to_str_sid)

    # 2) จำกัดให้อยู่ใน baseline (ถ้ามี)
    if base_csv:
        base = read_csv_any(base_csv)
        keep = set(base["safetyreportid"].map(to_str_sid).dropna().astype(str))
        df = df[df["safetyreportid"].isin(keep)]

    # 3) สร้าง map และ dict
    cols_keep = ["safetyreportid", "reaction_meddrapt"]
    if "reaction_outcome" in df.columns:
        cols_keep.append("reaction_outcome")

    adr_map = df[cols_keep].dropna(subset=["reaction_meddrapt"]).copy()
    if dedup:
        adr_map = adr_map.drop_duplicates(subset=["safetyreportid", "reaction_meddrapt"])

    adr_map["reaction_key"] = adr_map["reaction_meddrapt"].map(slug_key)
    adr_dict = adr_map[["reaction_meddrapt", "reaction_key"]].drop_duplicates()

    # 4) เขียนผลลัพธ์
    write_csv_gz(adr_map,  os.path.join(out_dir, "adr_map.csv.gz"))
    write_csv_gz(adr_dict, os.path.join(out_dir, "adr_dict.csv.gz"))
    dump_json(
        {
            "source": os.path.relpath(reac_path, er_root),
            "rows": {"adr_map": int(len(adr_map)), "adr_dict": int(len(adr_dict))}
        },
        os.path.join(out_dir, "adr_split_stats.json")
    )
    print("[ok] split_adr: wrote adr_map.csv.gz and adr_dict.csv.gz")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--er-root", required=True, help=".../data/openFDA_drug_event/er_tables")
    ap.add_argument("--out-dir", required=True, help="e.g., ./data/split")
    ap.add_argument("--base-csv", help="baseline patients_report_serious_reporter.csv.gz (optional)")
    ap.add_argument("--no-dedup", action="store_true")
    args = ap.parse_args()
    main(args.er_root, args.out_dir, args.base_csv, dedup=not args.no_dedup)
