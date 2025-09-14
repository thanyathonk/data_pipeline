# split_adr.py
import os, argparse, pandas as pd, gzip, sqlite3, tempfile
from common_utils import to_str_sid, slug_key, read_csv_any, write_csv_gz, dump_json

def main(er_root: str, out_dir: str, base_csv: str=None, dedup: bool=True):
    os.makedirs(out_dir, exist_ok=True)

    # 1) โหลดจากไฟล์มาตรฐานเดียว
    reac_path = os.path.join(er_root, "reactions.csv.gz")
    if not os.path.exists(reac_path):
        raise SystemExit(f"Expected file not found: {reac_path} (from ER step)")

    # 2) จำกัดให้อยู่ใน baseline (ถ้ามี) และสตรีม reactions เป็น chunks เพื่อประหยัดหน่วยความจำ
    required_cols = ["safetyreportid", "reaction_meddrapt"]
    # เตรียม index ของ SIDs จาก baseline โดยใช้ sqlite เพื่อลดหน่วยความจำ
    sid_db = None
    sid_conn = None
    if base_csv and os.path.exists(base_csv):
        sid_db = os.path.join(out_dir, "adr_sid_index.sqlite")
        if os.path.exists(sid_db):
            os.remove(sid_db)
        sid_conn = sqlite3.connect(sid_db)
        cur = sid_conn.cursor()
        cur.execute("CREATE TABLE sids (sid TEXT PRIMARY KEY)")
        # อ่าน baseline เป็นก้อนเฉพาะคอลัมน์ safetyreportid
        for ch in pd.read_csv(base_csv, usecols=["safetyreportid"], chunksize=2_000_000, low_memory=False):
            ch["safetyreportid"] = ch["safetyreportid"].map(to_str_sid)
            vals = [(str(x),) for x in ch["safetyreportid"].dropna().astype(str).unique().tolist()]
            cur.executemany("INSERT OR IGNORE INTO sids(sid) VALUES(?)", vals)
        sid_conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sid ON sids(sid)")
        sid_conn.commit()

    # 3) stream สร้าง adr_map.csv.gz และ adr_dict.csv.gz
    map_path = os.path.join(out_dir, "adr_map.csv.gz")
    wrote_header = False
    dict_map = {}
    total_rows = 0

    with gzip.open(map_path, mode="wt", encoding="utf-8") as gz:
        # ตรวจคอลัมน์ reactions เพื่อประกอบ usecols
        hdr = pd.read_csv(reac_path, nrows=0)
        cols = list(hdr.columns)
        usecols = [c for c in required_cols + ["reaction_outcome"] if c in cols]
        if not set(required_cols).issubset(set(usecols)):
            missing = set(required_cols) - set(usecols)
            raise SystemExit(f"reactions.csv.gz missing columns: {missing}")

        for ch in pd.read_csv(reac_path, usecols=usecols, chunksize=1_000_000, low_memory=False):
            ch["safetyreportid"] = ch["safetyreportid"].map(to_str_sid)
            ch = ch.dropna(subset=["safetyreportid","reaction_meddrapt"]) 
            if sid_conn is not None:
                # ดึง set ของ sid ที่มีใน chunk นี้จาก sqlite เพื่อลดจำนวน
                uniq = ch["safetyreportid"].astype(str).unique().tolist()
                # แบ่งเป็นชุดย่อยเพื่อลด place holders
                keep_sids = set()
                cur = sid_conn.cursor()
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
            # drop duplicates ภายใน chunk เพื่อลดปริมาณ
            if dedup:
                ch = ch.drop_duplicates(subset=["safetyreportid","reaction_meddrapt"]) 
            # reaction_key
            ch["reaction_key"] = ch["reaction_meddrapt"].map(slug_key)
            # อัปเดต dict_map
            for v in ch["reaction_meddrapt"].dropna().astype(str).unique():
                if v not in dict_map:
                    dict_map[v] = slug_key(v)
            # เขียนต่อท้ายไฟล์ map
            ch.to_csv(gz, index=False, header=not wrote_header)
            wrote_header = True
            total_rows += len(ch)

    # เขียน adr_dict จาก dict_map
    adr_dict = pd.DataFrame({
        "reaction_meddrapt": list(dict_map.keys()),
        "reaction_key": list(dict_map.values())
    })
    write_csv_gz(adr_dict, os.path.join(out_dir, "adr_dict.csv.gz"))
    dump_json(
        {
            "source": os.path.relpath(reac_path, er_root),
            "rows": {"adr_map": int(total_rows), "adr_dict": int(len(adr_dict))}
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
