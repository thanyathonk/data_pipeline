#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage-6: QA & Diagnostics
- Unmatched product keys (drug_dict vs drug_rxnorm)
- Unmapped ADR terms (adr_map vs standard_reactions)
- Duplicate pairs in final merged
"""

import os, sys, argparse, json, glob
import pandas as pd

def ensure_dir(p): os.makedirs(os.path.dirname(p), exist_ok=True)
def read_csv_any(p): return pd.read_csv(p, low_memory=False)
def write_csv_gz(df, p):
    ensure_dir(p); comp = "gzip" if p.endswith((".gz",".gzip")) else None
    df.to_csv(p, index=False, compression=comp)
def dump_json(obj, p):
    ensure_dir(p); open(p,"w",encoding="utf-8").write(json.dumps(obj, ensure_ascii=False, indent=2))

def load_rxnorm(rx_input: str) -> pd.DataFrame:
    if os.path.isdir(rx_input):
        paths = sorted(glob.glob(os.path.join(rx_input, "drug_rxnorm*.csv.gz")))
    else:
        paths = sorted(glob.glob(rx_input)) if any(ch in rx_input for ch in "*?[") else [rx_input]
    if not paths: return pd.DataFrame(columns=["product_key","INN"])
    rx = pd.concat([read_csv_any(p) for p in paths], ignore_index=True).drop_duplicates()
    cols = set(rx.columns)
    need = {"product_key","INN"}
    if not need.issubset(cols): return pd.DataFrame(columns=["product_key","INN"])
    rx = rx.dropna(subset=["product_key"]).sort_values("product_key").drop_duplicates("product_key")
    return rx[["product_key","INN"]]

def main(a):
    # unmatched products
    drug_dict = read_csv_any(a.drug_dict)
    need = {"medicinal_product","product_key"}
    if not need.issubset(drug_dict.columns):
        raise SystemExit(f"[drug_dict] missing {need}")

    rx = load_rxnorm(a.rxnorm)  # may be empty if Stage-3B skipped
    if rx.empty:
        unmatched_prod = drug_dict[["medicinal_product","product_key"]].drop_duplicates()
    else:
        unmatched_prod = drug_dict.merge(rx, on="product_key", how="left")
        unmatched_prod = unmatched_prod[unmatched_prod["INN"].isna()][["medicinal_product","product_key"]].drop_duplicates()

    # add frequency from drug_map (เพื่อจัดลำดับ)
    if os.path.exists(a.drug_map):
        dm = read_csv_any(a.drug_map)[["safetyreportid","medicinal_product"]]
        freq = dm.groupby("medicinal_product").size().reset_index(name="count_in_reports")
        unmatched_prod = unmatched_prod.merge(freq, on="medicinal_product", how="left").sort_values(
            ["count_in_reports","medicinal_product"], ascending=[False, True])

    write_csv_gz(unmatched_prod, a.out_unmatched_products)

    # unmapped ADR
    adr_map = read_csv_any(a.adr_map)[["reaction_meddrapt"]].dropna().drop_duplicates()
    std_pt = os.path.join(a.meddra_dir, "standard_reactions.csv.gz")
    if os.path.exists(std_pt):
        pt = read_csv_any(std_pt)[["reaction_meddrapt"]].dropna().drop_duplicates()
        unmapped_adr = adr_map.merge(pt, on="reaction_meddrapt", how="left")
        unmapped_adr = unmapped_adr[unmapped_adr["reaction_meddrapt"].duplicated(keep=False) == False]  # keep all
        unmapped_adr = unmapped_adr[unmapped_adr.isna().any(axis=1)][["reaction_meddrapt"]]
    else:
        unmapped_adr = adr_map.copy()  # if no MedDRA file yet
    # add frequency
    adr_full = read_csv_any(a.adr_map)
    freq_adr = adr_full.groupby("reaction_meddrapt").size().reset_index(name="count_in_reports")
    unmapped_adr = unmapped_adr.merge(freq_adr, on="reaction_meddrapt", how="left").sort_values(
        ["count_in_reports","reaction_meddrapt"], ascending=[False, True])
    write_csv_gz(unmapped_adr, a.out_unmapped_adr)

    # duplicate pairs in final merged
    final_path = a.final_csv
    if os.path.exists(final_path):
        final_df = read_csv_any(final_path)
        keys = ["safetyreportid","medicinal_product","reaction_meddrapt"]
        if all(k in final_df.columns for k in keys):
            dupes = final_df[final_df.duplicated(keys, keep=False)].sort_values(keys)
            write_csv_gz(dupes, a.out_dupes)
            dup_count = int(len(dupes))
        else:
            dup_count = None
    else:
        dup_count = None

    summary = {
        "outputs": {
            "unmatched_products": os.path.abspath(a.out_unmatched_products),
            "unmapped_adr": os.path.abspath(a.out_unmapped_adr),
            "dupe_pairs": os.path.abspath(a.out_dupes),
        },
        "counts": {
            "unmatched_products": int(len(unmatched_prod)),
            "unmapped_adr": int(len(unmapped_adr)),
            "dupe_pairs": dup_count,
        }
    }
    dump_json(summary, a.out_summary)
    print(f"[OK] Stage-6 QA → {a.out_summary}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--drug-dict", default="data/split/drug_dict.csv.gz")
    ap.add_argument("--drug-map",  default="data/split/drug_map.csv.gz")
    ap.add_argument("--rxnorm",    default="data/enrich")
    ap.add_argument("--adr-map",   default="data/split/adr_map.csv.gz")
    ap.add_argument("--meddra-dir",default="data/enrich")
    ap.add_argument("--final-csv", default="data/processed/FDA_patient_drug_report_reaction.csv.gz")
    ap.add_argument("--out-unmatched-products", default="data/qa/unmatched_products.csv.gz")
    ap.add_argument("--out-unmapped-adr",      default="data/qa/unmapped_adr.csv.gz")
    ap.add_argument("--out-dupes",             default="data/qa/dupe_pairs.csv.gz")
    ap.add_argument("--out-summary",           default="data/qa/summary.json")
    args = ap.parse_args()
    main(args)
