#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ADR.py
Map ADR dictionary (reaction_meddrapt) -> MedDRA PT + SOC using OMOP vocab.

Inputs:
  --in       : data/split/adr_dict.csv.gz  (columns: reaction_meddrapt, reaction_key)
  --out-dir  : data/enrich/
  --vocab    : vocab/<...>/  (folder that contains CONCEPT.csv[.gz], CONCEPT_ANCESTOR.csv[.gz])

Outputs (under --out-dir):
  - standard_reactions.csv.gz
        columns: reaction_meddrapt, MedDRA_concept_id, MedDRA_concept_code, MedDRA_concept_name
        (map raw PT text -> PT concept)
  - standard_reactions_meddra_soc.csv.gz
        columns: MedDRA_concept_id_1 (PT id), MedDRA_concept_code_1 (PT code),
                 MedDRA_concept_id (SOC id), MedDRA_concept_code (SOC code), MedDRA_concept_name (SOC name)
        (PT -> SOC; ชื่อคอลัมน์ออกแบบให้เข้ากับ stage4_merge_back.py โดยตรง)
"""

import os
import argparse
import pandas as pd
import numpy as np
import unicodedata

def ensure_dir(p): os.makedirs(p, exist_ok=True)

# ---------- IO helpers ----------
def _read_table_auto(path):
    for sep in ("\t", ",", "|"):
        try:
            return pd.read_csv(path, sep=sep, low_memory=False, dtype=str)
        except Exception:
            continue
    return pd.read_csv(path, low_memory=False, dtype=str)

def _find_vocab_file(vocab_dir, base):
    cands = [os.path.join(vocab_dir, base), os.path.join(vocab_dir, base + ".gz")]
    if not any(os.path.exists(p) for p in cands):
        if os.path.isdir(vocab_dir):
            for name in os.listdir(vocab_dir):
                sub = os.path.join(vocab_dir, name)
                if os.path.isdir(sub):
                    p1 = os.path.join(sub, base)
                    p2 = os.path.join(sub, base + ".gz")
                    if os.path.exists(p1): return p1
                    if os.path.exists(p2): return p2
    for p in cands:
        if os.path.exists(p): return p
    return None

def read_vocab_table(vocab_dir, base):
    path = _find_vocab_file(vocab_dir, base)
    if not path:
        raise SystemExit(f"[vocab] Not found: {base} under {vocab_dir}")
    return _read_table_auto(path)

def read_csv_any(path): return pd.read_csv(path, low_memory=False, dtype=str)

def write_csv_gz(df, path):
    ensure_dir(os.path.dirname(path))
    comp = "gzip" if path.endswith((".gz",".gzip")) else None
    df.to_csv(path, index=False, compression=comp)

# ---------- text normalization ----------
def norm_key(s):
    if s is None or (isinstance(s, float) and pd.isna(s)): return None
    s = unicodedata.normalize("NFKC", str(s)).casefold()
    return " ".join(s.split())

# ---------- main ----------
def main(adr_dict_path: str, out_dir: str, vocab_dir: str):
    ensure_dir(out_dir)

    # 0) load adr dict (unique PT text)
    adr = read_csv_any(adr_dict_path)
    need = {"reaction_meddrapt"}
    miss = need - set(adr.columns)
    if miss:
        raise SystemExit(f"[ADR] input missing columns: {miss}")
    adr = adr.dropna(subset=["reaction_meddrapt"]).drop_duplicates("reaction_meddrapt").copy()
    adr["concept_name_key"] = adr["reaction_meddrapt"].map(norm_key)

    # 1) vocab
    concept  = read_vocab_table(vocab_dir, "CONCEPT.csv")
    concept.columns  = [c.strip() for c in concept.columns]
    reqc = {"concept_id","concept_name","concept_code","concept_class_id","vocabulary_id"}
    if not reqc.issubset(concept.columns): raise SystemExit(f"[vocab] CONCEPT.csv missing: {reqc - set(concept.columns)}")

    ancestor = read_vocab_table(vocab_dir, "CONCEPT_ANCESTOR.csv")
    ancestor.columns = [c.strip() for c in ancestor.columns]
    reqa = {"ancestor_concept_id","descendant_concept_id","min_levels_of_separation","max_levels_of_separation"}
    if not reqa.issubset(ancestor.columns): raise SystemExit(f"[vocab] CONCEPT_ANCESTOR.csv missing: {reqa - set(ancestor.columns)}")

    concept["concept_name_key"] = concept["concept_name"].map(norm_key)
    meddra       = concept[concept["vocabulary_id"].str.upper()=="MEDDRA"].copy()
    meddra_pt    = meddra[meddra["concept_class_id"].str.upper()=="PT"].copy()
    meddra_soc   = meddra[meddra["concept_class_id"].str.upper()=="SOC"].copy()

    # 2) map PT by normalized name
    pt_match = adr.merge(
        meddra_pt[["concept_id","concept_code","concept_name","concept_name_key","concept_class_id"]],
        on="concept_name_key", how="left"
    )

    # 3) standard_reactions.csv.gz (reaction -> PT)
    std_pt = pt_match.rename(columns={
        "concept_id":"MedDRA_concept_id",
        "concept_code":"MedDRA_concept_code",
        "concept_name":"MedDRA_concept_name"
    })[["reaction_meddrapt","MedDRA_concept_id","MedDRA_concept_code","MedDRA_concept_name"]].drop_duplicates()
    write_csv_gz(std_pt, os.path.join(out_dir, "standard_reactions.csv.gz"))

    # 4) PT -> SOC via CONCEPT_ANCESTOR
    pt_ids = pt_match["concept_id"].dropna().astype(str).unique().tolist()
    ca = ancestor[ancestor["descendant_concept_id"].astype(str).isin(pt_ids)].copy()
    soc = meddra_soc[["concept_id","concept_code","concept_name"]].rename(columns={
        "concept_id":"SOC_concept_id","concept_code":"SOC_concept_code","concept_name":"SOC_concept_name"
    })
    ca = ca.merge(soc, left_on="ancestor_concept_id", right_on="SOC_concept_id", how="inner")

    pt_info = meddra_pt[["concept_id","concept_code","concept_name"]].rename(columns={
        "concept_id":"PT_concept_id","concept_code":"PT_concept_code","concept_name":"PT_concept_name"
    })
    ca = ca.merge(pt_info, left_on="descendant_concept_id", right_on="PT_concept_id", how="left")

    ca["min_levels_of_separation"] = pd.to_numeric(ca["min_levels_of_separation"], errors="coerce")
    ca = ca.sort_values(["PT_concept_id","min_levels_of_separation","SOC_concept_id"]).dropna(subset=["PT_concept_id"])
    best = ca.groupby("PT_concept_id", as_index=False).first()

    std_soc = best.rename(columns={
        "PT_concept_id":"MedDRA_concept_id_1",
        "PT_concept_code":"MedDRA_concept_code_1",
        "SOC_concept_id":"MedDRA_concept_id",
        "SOC_concept_code":"MedDRA_concept_code",
        "SOC_concept_name":"MedDRA_concept_name"
    })[["MedDRA_concept_id_1","MedDRA_concept_code_1","MedDRA_concept_id","MedDRA_concept_code","MedDRA_concept_name"]].drop_duplicates()

    write_csv_gz(std_soc, os.path.join(out_dir, "standard_reactions_meddra_soc.csv.gz"))

    # summary
    n_in  = len(adr)
    n_pt  = std_pt["MedDRA_concept_id"].notna().sum()
    n_soc = std_soc["MedDRA_concept_id"].notna().sum()
    print(f"[ADR] input unique PT text : {n_in:,}")
    print(f"[ADR] mapped PT concepts  : {n_pt:,}")
    print(f"[ADR] PT with SOC chosen  : {n_soc:,}")
    print(f"[OK] wrote: {os.path.join(out_dir,'standard_reactions.csv.gz')}")
    print(f"[OK] wrote: {os.path.join(out_dir,'standard_reactions_meddra_soc.csv.gz')}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="adr_dict.csv.gz")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--vocab", required=True, help="folder containing CONCEPT.csv and CONCEPT_ANCESTOR.csv")
    args = ap.parse_args()
    main(args.in_path, args.out_dir, args.vocab)
