# split_drug.py
import os, argparse, pandas as pd, gzip, sqlite3
from common_utils import to_str_sid, slug_key, read_csv_any, write_csv_gz, dump_json

def main(er_root: str, out_dir: str, base_csv: str=None, suspect_only: bool=False, dedup: bool=True):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลดจากไฟล์มาตรฐานเดียว
    drug_path = os.path.join(er_root, "drugcharacteristics.csv.gz")
    if not os.path.exists(drug_path):
        raise SystemExit(f"Expected file not found: {drug_path} (from ER step)")

    # Stream in chunks to reduce memory
    if base_csv and os.path.exists(base_csv):
        # Build sqlite index of baseline SIDs to avoid loading all into RAM
        sid_db = os.path.join(out_dir, "drug_sid_index.sqlite")
        if os.path.exists(sid_db):
            os.remove(sid_db)
        conn = sqlite3.connect(sid_db)
        cur = conn.cursor()
        cur.execute("CREATE TABLE sids (sid TEXT PRIMARY KEY)")
        for ch in pd.read_csv(base_csv, usecols=["safetyreportid"], chunksize=2_000_000, low_memory=False):
            ch["safetyreportid"] = ch["safetyreportid"].map(to_str_sid)
            vals = [(str(x),) for x in ch["safetyreportid"].dropna().astype(str).unique().tolist()]
            if vals:
                cur.executemany("INSERT OR IGNORE INTO sids(sid) VALUES(?)", vals)
        conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sid ON sids(sid)")
        conn.commit()

        # Inspect columns to decide usecols
        hdr = pd.read_csv(drug_path, nrows=0)
        cols = list(hdr.columns)
        usecols = [c for c in ["safetyreportid", "medicinal_product", "drug_characterization"] if c in cols]
        if "safetyreportid" not in usecols or "medicinal_product" not in usecols:
            raise SystemExit("drugcharacteristics.csv.gz must contain 'safetyreportid' and 'medicinal_product'")

        out_map_path = os.path.join(out_dir, "drug_map.csv.gz")
        wrote_header = False
        dict_map = {}
        total_rows = 0

        with gzip.open(out_map_path, mode="wt", encoding="utf-8") as gz:
            for ch in pd.read_csv(drug_path, usecols=usecols, chunksize=200_000, low_memory=False):
                ch["safetyreportid"] = ch["safetyreportid"].map(to_str_sid)
                # Filter by baseline SIDs using sqlite IN batches
                uniq = ch["safetyreportid"].dropna().astype(str).unique().tolist()
                keep_sids = set()
                step = 50_000
                for i in range(0, len(uniq), step):
                    sub = uniq[i:i+step]
                    qmarks = ",".join(["?"]*len(sub))
                    cur.execute(f"SELECT sid FROM sids WHERE sid IN ({qmarks})", sub)
                    keep_sids.update([r[0] for r in cur.fetchall()])
                if keep_sids:
                    ch = ch[ch["safetyreportid"].isin(keep_sids)]
                else:
                    continue
                if ch.empty:
                    continue

                # Optional suspect filter
                if suspect_only and "drug_characterization" in ch.columns:
                    ch = ch[ch["drug_characterization"].str.contains("suspect", case=False, na=False)]
                    if ch.empty:
                        continue

                # Prepare chunk map and product_key (per chunk to limit memory)
                chunk_map = ch[[c for c in ["safetyreportid","medicinal_product","drug_characterization"] if c in ch.columns]].dropna(subset=["medicinal_product"]).copy()
                # Dedup within chunk to reduce volume
                if dedup:
                    chunk_map = chunk_map.drop_duplicates(subset=["safetyreportid","medicinal_product"]) 
                # compute product_key per chunk
                chunk_map["product_key"] = chunk_map["medicinal_product"].map(slug_key)
                # update dict_map for drug_dict
                for v in chunk_map["medicinal_product"].dropna().astype(str).unique():
                    if v not in dict_map:
                        dict_map[v] = slug_key(v)

                # write chunk
                chunk_map.to_csv(gz, index=False, header=not wrote_header)
                wrote_header = True
                total_rows += len(chunk_map)

        # build drug_dict from dict_map
        drug_dict = pd.DataFrame({
            "medicinal_product": list(dict_map.keys()),
            "product_key": list(dict_map.values())
        })
        try:
            conn.close()
        except Exception:
            pass
    else:
        # Fallback: read entire file (may be large)
        df = read_csv_any(drug_path)
        if "safetyreportid" not in df.columns or "medicinal_product" not in df.columns:
            raise SystemExit("drugcharacteristics.csv.gz must contain 'safetyreportid' and 'medicinal_product'")
        df["safetyreportid"] = df["safetyreportid"].map(to_str_sid)

    # 3) เฉพาะ suspect (ตัวเลือกเสริม)
    # handled inside streaming branch; for non-stream fallback, filter here
    if not (base_csv and os.path.exists(base_csv)):
        if suspect_only and "drug_characterization" in df.columns:
            df = df[df["drug_characterization"].str.contains("suspect", case=False, na=False)]

    # 4) สร้าง map และ dict (non-stream fallback)
    cols_keep = ["safetyreportid", "medicinal_product"]
    if not (base_csv and os.path.exists(base_csv)) and "drug_characterization" in df.columns:
        cols_keep.append("drug_characterization")

    if base_csv and os.path.exists(base_csv):
        # drug_map already written in streaming branch
        pass
    else:
        drug_map = df[cols_keep].dropna(subset=["medicinal_product"]).copy()
        if dedup:
            drug_map = drug_map.drop_duplicates(subset=["safetyreportid", "medicinal_product"])
        drug_map["product_key"] = drug_map["medicinal_product"].map(slug_key)
        drug_dict = drug_map[["medicinal_product", "product_key"]].drop_duplicates()
        total_rows = len(drug_map)

    # 5) เขียนผลลัพธ์
    if not (base_csv and os.path.exists(base_csv)):
        write_csv_gz(drug_map,  os.path.join(out_dir, "drug_map.csv.gz"))
    write_csv_gz(drug_dict, os.path.join(out_dir, "drug_dict.csv.gz"))
    dump_json(
        {
            "source": os.path.relpath(drug_path, er_root),
            "rows": {"drug_map": int(total_rows), "drug_dict": int(len(drug_dict))},
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
