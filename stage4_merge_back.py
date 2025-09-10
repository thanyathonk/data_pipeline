#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage-4: Merge back to combined dataset
- baseline + drug (INN only) + ADR (PT/SOC minimal fields)
"""

import os, sys, argparse, glob, json
import pandas as pd

def to_str_sid(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if s.endswith(".0"): s = s[:-2]
    return s

def ensure_dir(p): os.makedirs(os.path.dirname(p), exist_ok=True)
def read_csv_any(path: str) -> pd.DataFrame: return pd.read_csv(path, low_memory=False)
def write_csv_gz(df: pd.DataFrame, path: str):
    ensure_dir(path); comp = "gzip" if path.endswith((".gz",".gzip")) else None
    df.to_csv(path, index=False, compression=comp)
def dump_json(obj, path: str):
    ensure_dir(path); open(path, "w", encoding="utf-8").write(json.dumps(obj, ensure_ascii=False, indent=2))

def load_rxnorm(rx_input: str) -> pd.DataFrame:
    if os.path.isdir(rx_input):
        paths = sorted(glob.glob(os.path.join(rx_input, "drug_rxnorm*.csv.gz")))
    else:
        paths = sorted(glob.glob(rx_input)) if any(ch in rx_input for ch in "*?[") else [rx_input]
    if not paths: raise SystemExit(f"[rxnorm] No input files at: {rx_input}")
    rx = pd.concat([read_csv_any(p) for p in paths], ignore_index=True).drop_duplicates()
    need = {"product_key","INN"}
    if not need.issubset(rx.columns): raise SystemExit("[rxnorm] need columns: product_key, INN")
    rx = rx.dropna(subset=["product_key"]).sort_values("product_key").drop_duplicates("product_key")
    return rx[["product_key","INN"]]

def load_meddra_joiner(meddra_dir: str):
    std_pt = os.path.join(meddra_dir, "standard_reactions.csv.gz")
    std_soc = os.path.join(meddra_dir, "standard_reactions_meddra_soc.csv.gz")
    if not (os.path.exists(std_pt) and os.path.exists(std_soc)):
        raise SystemExit(f"[meddra] missing: {std_pt} or {std_soc}")

    pt = read_csv_any(std_pt)
    soc = read_csv_any(std_soc)

    miss_pt = {"reaction_meddrapt","MedDRA_concept_id","MedDRA_concept_code"} - set(pt.columns)
    if miss_pt: raise SystemExit(f"[meddra] standard_reactions missing {miss_pt}")

    # heuristic column pick for SOC map
    def pick(cols, *cand):
        for c in cand:
            if c in cols: return c
        return None
    cols = set(soc.columns)
    pt_id_col   = pick(cols, "MedDRA_concept_id_1","pt_id")
    pt_code_col = pick(cols, "MedDRA_concept_code_1","pt_code")
    soc_id_col  = pick(cols, "MedDRA_concept_id","soc_id")
    soc_code_col= pick(cols, "MedDRA_concept_code","soc_code")
    soc_name_col= pick(cols, "MedDRA_concept_name","soc_name")
    for nm,val in [("pt_id_col",pt_id_col),("soc_id_col",soc_id_col),
                   ("soc_code_col",soc_code_col),("soc_name_col",soc_name_col)]:
        if not val: raise SystemExit(f"[meddra] cannot determine {nm}")

    pt_map = pt[["reaction_meddrapt","MedDRA_concept_id","MedDRA_concept_code"]].drop_duplicates()
    pt_map = pt_map.rename(columns={"MedDRA_concept_id":"pt_id","MedDRA_concept_code":"pt_code"})
    soc_map = soc[[pt_id_col,soc_id_col,soc_code_col,soc_name_col]].drop_duplicates().rename(
        columns={pt_id_col:"pt_id", soc_id_col:"soc_id", soc_code_col:"soc_code", soc_name_col:"soc_name"})

    def join_adr(adr: pd.DataFrame) -> pd.DataFrame:
        out = adr.merge(pt_map, on="reaction_meddrapt", how="inner").merge(soc_map, on="pt_id", how="left")
        return out

    return join_adr

def pairs_count(drug_sid: pd.DataFrame, adr_sid: pd.DataFrame) -> int:
    dc = drug_sid.groupby("safetyreportid").size()
    ac = adr_sid.groupby("safetyreportid").size()
    j = dc.to_frame("n_drug").join(ac.to_frame("n_adr"), how="inner")
    return 0 if j.empty else int((j["n_drug"] * j["n_adr"]).sum())

def main(a):
    # load baseline
    base = read_csv_any(a.baseline)
    if "safetyreportid" not in base.columns: raise SystemExit("[baseline] missing safetyreportid")
    base["safetyreportid"] = base["safetyreportid"].map(to_str_sid)

    # load drug side
    drug_map = read_csv_any(a.drug_map)
    need = {"safetyreportid","medicinal_product"}
    if not need.issubset(drug_map.columns): raise SystemExit(f"[drug_map] missing {need}")
    if "product_key" not in drug_map.columns:
        import re, unicodedata
        def slug_key(t):
            s = unicodedata.normalize("NFKC", str(t)).lower()
            s = re.sub(r"[^a-z0-9\s]"," ", s); s = re.sub(r"\s+","_", s).strip("_"); return s or None
        drug_map["product_key"] = drug_map["medicinal_product"].map(slug_key)
    drug_map["safetyreportid"] = drug_map["safetyreportid"].map(to_str_sid)
    drug_map = drug_map.dropna(subset=["safetyreportid","medicinal_product","product_key"]).drop_duplicates(
        ["safetyreportid","medicinal_product"])

    rx = load_rxnorm(a.rxnorm)  # product_key, INN (unique per key)
    rx = rx.dropna(subset=["INN"])
    drug_mapped = drug_map.merge(rx, on="product_key", how="inner")[["safetyreportid","medicinal_product","INN"]].drop_duplicates()

    # Optional DrugBank filter/join
    db_map = None
    if getattr(a, "drugbank", None):
        if os.path.isdir(a.drugbank):
            cand = os.path.join(a.drugbank, "drugbank_results.csv")
            if os.path.exists(cand):
                db_path = cand
            else:
                # any csv in dir
                files = sorted(glob.glob(os.path.join(a.drugbank, "*.csv")))
                db_path = files[0] if files else None
        else:
            db_path = a.drugbank
        if not db_path or not os.path.exists(db_path):
            raise SystemExit(f"[drugbank] mapping not found at: {a.drugbank}")
        db = read_csv_any(db_path)
        # standardize columns
        cols = {c.lower(): c for c in db.columns}
        inn_col = None
        for k in ("inn","ingredient","clean_name"):
            if k in cols: inn_col = cols[k]; break
        id_col = None
        for k in ("drugbank_id","dbid","drugbankid"):
            if k in cols: id_col = cols[k]; break
        if not inn_col or not id_col:
            raise SystemExit("[drugbank] need INN/ingredient and drugbank_id columns")
        db_map = db[[inn_col, id_col]].dropna().drop_duplicates()
        db_map = db_map.rename(columns={inn_col: "INN", id_col: "drugbank_id"})
        db_map["INN"] = db_map["INN"].astype(str)
        # attach to drug_mapped
        drug_mapped["INN"] = drug_mapped["INN"].astype(str)
        drug_mapped = drug_mapped.merge(db_map, on="INN", how="left")
        if getattr(a, "filter_drugbank", False):
            # treat 'nan' string as NaN
            drug_mapped.loc[drug_mapped["drugbank_id"].astype(str).str.lower()=="nan", "drugbank_id"] = pd.NA
            drug_mapped = drug_mapped.dropna(subset=["drugbank_id"]).drop_duplicates()

    # load ADR + join MedDRA
    adr_map = read_csv_any(a.adr_map)
    need = {"safetyreportid","reaction_meddrapt"}
    if not need.issubset(adr_map.columns): raise SystemExit(f"[adr_map] missing {need}")
    adr_map["safetyreportid"] = adr_map["safetyreportid"].map(to_str_sid)
    adr_map = adr_map.dropna(subset=["safetyreportid","reaction_meddrapt"]).drop_duplicates(
        ["safetyreportid","reaction_meddrapt"])

    join_adr = load_meddra_joiner(a.meddra_dir)
    adr_mapped = join_adr(adr_map)
    keep_adr = ["safetyreportid","reaction_meddrapt","pt_id","pt_code","soc_id","soc_code","soc_name"]
    miss = set(keep_adr) - set(adr_mapped.columns)
    if miss: raise SystemExit(f"[ADR mapped] missing {miss}")
    adr_mapped = adr_mapped[keep_adr].drop_duplicates()

    # coverage (estimates pre/final)
    pairs_pre = pairs_count(drug_map[["safetyreportid"]].drop_duplicates(),
                            adr_map[["safetyreportid"]].drop_duplicates())
    pairs_after_inn = pairs_count(drug_mapped[["safetyreportid"]].drop_duplicates(),
                                  adr_map[["safetyreportid"]].drop_duplicates())
    pairs_after_pt  = pairs_count(drug_mapped[["safetyreportid"]].drop_duplicates(),
                                  adr_mapped[["safetyreportid"]].drop_duplicates())

    # materialize pairs and attach baseline
    pairs = drug_mapped.merge(adr_mapped, on="safetyreportid", how="inner") \
                       .drop_duplicates(["safetyreportid","medicinal_product","reaction_meddrapt"])
    final_df = base.merge(pairs, on="safetyreportid", how="inner")

    write_csv_gz(final_df, a.out_csv)
    stats = {
        "rows": {
            "baseline": int(len(base)),
            "drug_map": int(len(drug_map)),
            "adr_map": int(len(adr_map)),
            "pairs_pre_enrich_est": int(pairs_pre),
            "pairs_after_inn_est": int(pairs_after_inn),
            "pairs_after_pt_est": int(pairs_after_pt),
            "final_rows_written": int(len(final_df))
        },
        "unique": {
            "reports_in_final": int(final_df["safetyreportid"].nunique()),
            "products_in_final": int(final_df["medicinal_product"].nunique()),
            "reactions_in_final": int(final_df["reaction_meddrapt"].nunique()),
        },
        "added_columns": {"drug":["INN"] + (["drugbank_id"] if db_map is not None else []), "adr":["pt_id","pt_code","soc_id","soc_code","soc_name"]},
        "inputs": {
            "baseline": os.path.abspath(a.baseline),
            "drug_map": os.path.abspath(a.drug_map),
            "rxnorm"  : os.path.abspath(a.rxnorm),
            "adr_map" : os.path.abspath(a.adr_map),
            "meddra_dir": os.path.abspath(a.meddra_dir)
        },
        "output": {"csv": os.path.abspath(a.out_csv)}
    }
    dump_json(stats, a.out_stats)
    print(f"[OK] Stage-4 merged → {a.out_csv}")
    print(f"[OK] Coverage → {a.out_stats}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline",  default="data/baseline/patients_report_serious_reporter.csv.gz")
    ap.add_argument("--drug-map",  default="data/split/drug_map.csv.gz")
    ap.add_argument("--rxnorm",    default="data/enrich")  # dir or glob or file
    ap.add_argument("--adr-map",   default="data/split/adr_map.csv.gz")
    ap.add_argument("--meddra-dir",default="data/enrich")
    ap.add_argument("--drugbank",  default=None, help="DrugBank mapping CSV or dir (expects drugbank_results.csv)")
    ap.add_argument("--filter-drugbank", action="store_true", help="Keep only rows with DrugBank ID")
    ap.add_argument("--out-csv",   default="data/processed/FDA_patient_drug_report_reaction.csv.gz")
    ap.add_argument("--out-stats", default="data/processed/merge_coverage.json")
    args = ap.parse_args()
    main(args)
