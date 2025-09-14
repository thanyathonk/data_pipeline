#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
run_pipeline_validation.py — end-to-end pipeline validation on a small sample

Pipeline stages (reference):
  1) core/Parsing.py
  2) core/openFDA_Entity_Relationship_Tables.py     -> ER tables (REQUIRED for smoke)
  3) core/merge_data_all_ages.py                     -> baseline (here we build a sample inside)
  4) split/split_adr.py + split/split_drug.py
  5) enrich/ADR.py                                   -> MedDRA map
  6) enrich/clean_drug.py -> enrich/rxnav_enrich.py  -> INN/RxCUI
  7) stage4_merge_back.py
  8) stage5_split_cohorts.py
  9) stage6_qa_checks.py
 10) stage7_release_pack.py
"""

import os, sys, argparse, logging, datetime, subprocess, random, json
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
def rel(*p): return os.path.join(ROOT, *p)
def ensure_dir(p):
    d = p if os.path.splitext(p)[1]=="" else os.path.dirname(p)
    if d: os.makedirs(d, exist_ok=True)
    return p

# ---------------- helpers: runner & env ----------------
def _inject_unbuffered(cmd):
    try: exe = os.path.basename(cmd[0]).lower()
    except Exception: exe = ""
    if "python" in exe and "-u" not in cmd[1:3]:
        return [cmd[0], "-u"] + cmd[1:]
    return cmd

def child_env_for_imports():
    """Add project dirs so child scripts can import shared modules (merge/common_utils.py)."""
    extras = []
    for d in (ROOT, rel("core"), rel("split"), rel("enrich"), rel("merge")):
        if os.path.isdir(d):
            extras.append(d)
    prev = os.environ.get("PYTHONPATH", "")
    new_pp = os.pathsep.join(extras + ([prev] if prev else []))
    return {"PYTHONPATH": new_pp, "PYTHONUNBUFFERED":"1", "PYTHONIOENCODING":"utf-8"}

def run_step(cmd, step_name, logger, steps_dir, cwd=None, env_extra=None):
    step_log = os.path.join(steps_dir, f"{TS}_{step_name}.log")
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED","1"); env.setdefault("PYTHONIOENCODING","utf-8")
    if env_extra: env.update(env_extra)
    cmd = _inject_unbuffered(cmd)
    logger.debug(f"[STEP:{step_name}] RUN: {' '.join(cmd)} (cwd={cwd or ROOT})")
    logger.debug(f"[STEP:{step_name}] Log -> {os.path.relpath(step_log, ROOT)}")
    def _scrub_console(s: str) -> str:
        try:
            enc = getattr(sys.stdout, "encoding", None) or "ascii"
            s.encode(enc)
            return s
        except Exception:
            try:
                return s.encode("ascii", "ignore").decode("ascii")
            except Exception:
                return s

    with open(step_log,"wb") as sf:
        p = subprocess.Popen(cmd, cwd=cwd or ROOT, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, bufsize=0, env=env)
        buf=b""
        while True:
            ch = p.stdout.read(1)
            if not ch: break
            if ch in (b"\n", b"\r"):
                if buf:
                    line = buf.decode("utf-8","replace").rstrip()
                    logger.info(_scrub_console(line)); sf.write((line+"\n").encode("utf-8")); sf.flush(); buf=b""
            else:
                buf += ch
        if buf:
            line = buf.decode("utf-8","replace").rstrip()
            logger.info(_scrub_console(line)); sf.write((line+"\n").encode("utf-8")); sf.flush()
        code = p.wait()
        if code!=0:
            raise SystemExit(f"[STEP:{step_name}] Command failed ({code}): {' '.join(cmd)}")
    return step_log

# ---------------- helpers: locate, IO ----------------
def find_first(*cands):
    for c in cands:
        p = rel(c)
        if os.path.exists(p): return p
    return None

def detect_er_root():
    cands = [
        rel("data","openFDA_drug_event","er_tables"),
        rel("data","openFDA_drug_event","er_tables_memory_efficient"),
        os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event","er_tables"),
        os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event","er_tables_memory_efficient"),
    ]
    for p in cands:
        if os.path.isdir(p): return p
    for base in (rel("data","openFDA_drug_event"),
                 os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event")):
        if os.path.isdir(base):
            for name in os.listdir(base):
                if name.startswith("er_tables"):
                    q = os.path.join(base, name)
                    if os.path.isdir(q): return q
    return None

def read_csv_any(path, usecols=None, chunksize=None):
    return pd.read_csv(path, usecols=usecols, low_memory=False, chunksize=chunksize)

def write_csv(df, path):
    ensure_dir(path)
    comp = "gzip" if path.endswith((".gz",".gzip")) else None
    df.to_csv(path, index=False, compression=comp)

def load_first_existing(er_root, names):
    for n in names:
        p = os.path.join(er_root, n)
        if os.path.exists(p): return p
    return None

# ---------------- ER sampling & baseline sample ----------------
def sample_sids_from_er(er_root, sample_size=100, source="report.csv.gz", seed=42):
    path = load_first_existing(er_root, [source, "report.csv.gz"])
    if not path:
        raise SystemExit(f"[SMOKE] ER file not found for sampling: {source}")
    sids=set(); random.seed(seed)
    for ch in read_csv_any(path, usecols=["safetyreportid"], chunksize=50_000):
        vals = ch["safetyreportid"].dropna().astype(str).unique().tolist()
        random.shuffle(vals)
        for v in vals:
            sids.add(v)
            if len(sids) >= sample_size:
                return list(sids)
    return list(sids)

def build_baseline_sample_from_er(er_root, sids, out_path, logger):
    patient_p  = load_first_existing(er_root, ["patient.csv.gz"])
    report_p   = load_first_existing(er_root, ["report.csv.gz"])
    serious_p  = load_first_existing(er_root, ["report_serious.csv.gz"])
    reporter_p = load_first_existing(er_root, ["reporter.csv.gz"])
    for need,p in [("patient.csv.gz",patient_p),("report.csv.gz",report_p),
                   ("report_serious.csv.gz",serious_p),("reporter.csv.gz",reporter_p)]:
        if not p: raise SystemExit(f"[SMOKE] ER missing table: {need}")

    sidset = set(str(x) for x in sids)

    def filt(path, usecols=None):
        keep=[]
        for ch in read_csv_any(path, usecols=usecols, chunksize=200_000):
            ch["safetyreportid"] = ch["safetyreportid"].astype(str)
            keep.append(ch[ch["safetyreportid"].isin(sidset)])
        return pd.concat(keep, ignore_index=True) if keep else pd.DataFrame()

    logger.info("[SMOKE] Loading ER core tables filtered by sampled SIDs ...")
    patient  = filt(patient_p)
    report   = filt(report_p)
    serious  = filt(serious_p)
    reporter = filt(reporter_p)
    logger.info(f"[SMOKE] sizes: patient={len(patient)}, report={len(report)}, serious={len(serious)}, reporter={len(reporter)}")

    df = (patient.merge(report,   on="safetyreportid", how="inner")
                 .merge(serious,  on="safetyreportid", how="inner")
                 .merge(reporter, on="safetyreportid", how="inner"))
    write_csv(df, out_path)
    logger.info(f"[SMOKE] baseline_sample rows={len(df)} -> {os.path.relpath(out_path, ROOT)}")
    return out_path

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--er-root", help="Path to ER tables (from Stage-2). If omitted, auto-detect.")
    ap.add_argument("--sample-size", type=int, default=100, help="Number of safetyreportid to sample.")
    ap.add_argument("--age-col", default="patient_custom_master_age")
    ap.add_argument("--vocab-dir", default=rel("vocab"), help="MedDRA vocab directory.")
    ap.add_argument("--qps", type=float, default=4.0, help="RxNav API throttle (queries per second).")
    ap.add_argument("--max-workers", type=int, default=8, help="RxNav concurrent workers.")
    ap.add_argument("--suspect-only", action="store_true", help="Pass through to split_drug (optional).")
    args = ap.parse_args()

    # logging + dated test folders
    global TS
    now = datetime.datetime.now()
    TS = now.strftime("%Y%m%d_%H%M%S")
    DATE_PATH = os.path.join(now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"))
    logs_dir  = ensure_dir(rel("data","logs", DATE_PATH))
    steps_dir = ensure_dir(os.path.join(logs_dir, "steps"))
    pipe_log  = os.path.join(logs_dir, f"validation_{TS}.log")
    logger = logging.getLogger("validation"); logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(pipe_log, encoding="utf-8"); fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout);              ch.setLevel(logging.INFO)
    fmt= logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(ch)

    logger.info("=== PIPELINE VALIDATION (Stages 3->10 on a sampled baseline; ER from Stage-2 must exist) ===")
    logger.info(f"[LOG] pipeline : {os.path.relpath(pipe_log, ROOT)}")
    logger.info(f"[LOG] step dir : {os.path.relpath(steps_dir, ROOT)}/")

    # detect ER
    er_root = args.er_root or detect_er_root()
    if not er_root:
        raise SystemExit(
            "[SMOKE] ER tables not found. Please run Stage-1/2 to build ER, "
            "or download prepared ER into data/openFDA_drug_event/ before running validation."
        )
    logger.info(f"[ER] Using: {er_root}")

    # sandbox dirs (group by YYYY/MM/DD/TS)
    sand_root = ensure_dir(rel("data","test_runs", DATE_PATH, TS))
    dirs = {
        "baseline": ensure_dir(os.path.join(sand_root,"baseline")),
        "split":    ensure_dir(os.path.join(sand_root,"split")),
        "enrich":   ensure_dir(os.path.join(sand_root,"enrich")),
        "processed":ensure_dir(os.path.join(sand_root,"processed")),
        "qa":       ensure_dir(os.path.join(sand_root,"qa")),
    }
    release_dir = ensure_dir(rel("data","release", DATE_PATH, TS))

    # locate scripts
    split_adr     = find_first("split/split_adr.py")
    split_drug    = find_first("split/split_drug.py")
    adr_map_py    = find_first("enrich/ADR.py")
    clean_drug_py = find_first("enrich/clean_drug.py")
    rxnav_py      = find_first("enrich/rxnav_enrich.py")
    drugbank_py   = find_first("enrich/production_drugbank_scraper.py")
    stg4_merge    = find_first("stage4_merge_back.py")
    stg5_split    = find_first("stage5_split_cohorts.py")
    stg6_qa       = find_first("stage6_qa_checks.py")
    stg7_rel      = find_first("stage7_release_pack.py")
    for need, path in [
        ("split_adr", split_adr), ("split_drug", split_drug),
        ("ADR.py", adr_map_py), ("clean_drug", clean_drug_py), ("rxnav", rxnav_py), ("drugbank", drugbank_py),
        ("stage4", stg4_merge), ("stage5", stg5_split), ("stage6", stg6_qa), ("stage7", stg7_rel)
    ]:
        if not path:
            raise SystemExit(f"[SMOKE] Missing script: {need}")

    # ---------- Stage-3 (sample baseline built here) ----------
    logger.info("[SMOKE] Stage-3: Build baseline SAMPLE from ER")
    sids = sample_sids_from_er(er_root, sample_size=args.sample_size, source="report.csv.gz")
    # Use production-like baseline filename for consistency
    base_csv = os.path.join(dirs["baseline"], "patients_report_serious_reporter.csv.gz")
    build_baseline_sample_from_er(er_root, sids, base_csv, logger)

    # ---------- Stage-4: Split dictionaries ----------
    logger.info("[SMOKE] Stage-4: split dicts (ADR then Drug)")
    run_step([sys.executable, split_adr,
              "--er-root", er_root, "--out-dir", dirs["split"], "--base-csv", base_csv],
             "smk_stage4_split_adr", logger, steps_dir, env_extra=child_env_for_imports())

    cmd_drug = [sys.executable, split_drug,
                "--er-root", er_root, "--out-dir", dirs["split"], "--base-csv", base_csv]
    if args.suspect_only: cmd_drug.append("--suspect-only")
    run_step(cmd_drug, "smk_stage4_split_drug", logger, steps_dir, env_extra=child_env_for_imports())

    adr_dict  = os.path.join(dirs["split"], "adr_dict.csv.gz")
    adr_map   = os.path.join(dirs["split"], "adr_map.csv.gz")
    drug_dict = os.path.join(dirs["split"], "drug_dict.csv.gz")
    drug_map  = os.path.join(dirs["split"], "drug_map.csv.gz")

    # ---------- Stage-5: ADR mapping ----------
    logger.info("[SMOKE] Stage-5: MedDRA mapping (ADR.py)")
    vocab_dir = args.vocab_dir if os.path.isdir(args.vocab_dir) else rel("vocab")
    if not os.path.isdir(vocab_dir):
        raise SystemExit(f"[SMOKE] Vocab directory not found: {vocab_dir}")
    run_step([sys.executable, adr_map_py, "--in", adr_dict, "--out-dir", dirs["enrich"], "--vocab", vocab_dir],
             "smk_stage5_adr_mapping", logger, steps_dir, env_extra=child_env_for_imports())

    # ---------- Stage-6: Drug (clean -> RxNav; REAL APIs) ----------
    logger.info("[SMOKE] Stage-6: Drug (clean -> RxNav)")
    out_clean = os.path.join(dirs["enrich"], "drug_clean.csv.gz")
    run_step([sys.executable, clean_drug_py,
              "--in-drug-dict", drug_dict,
              "--out-drug-clean", out_clean,
              "--shards","1","--shard-id","0"],
             "smk_stage6_drug_clean", logger, steps_dir, env_extra=child_env_for_imports())

    out_rx = os.path.join(dirs["enrich"], "drug_rxnorm.csv.gz")
    run_step([sys.executable, rxnav_py,
              "--in-drug-clean", out_clean,
              "--out-drug-rxnorm", out_rx,
              "--cache-db", os.path.join(dirs["enrich"], "rxnav_cache.sqlite"),
              "--shards","1","--shard-id","0",
              "--max-workers", str(args.max_workers),
              "--qps", str(args.qps)],
             "smk_stage6_rxnav", logger, steps_dir, env_extra=child_env_for_imports())

    # ---------- Stage-6C: DrugBank (demo) ----------
    logger.info("[SMOKE] Stage-6C: DrugBank demo (scrape/mapping)")
    drugbank_dir = os.path.join(dirs["enrich"], "drugbank")
    # ถ้าเซ็ต CHROME_BIN ไว้ ให้ส่งผ่านไปยังสคริปต์
    chrome_bin = os.environ.get("CHROME_BIN")
    cmd_db = [sys.executable, drugbank_py,
              "--input-file", out_rx,
              "--output-dir", drugbank_dir,
              "--sessions", "4",
              "--init-visible",
              "--interactive-init"]
    if chrome_bin:
        cmd_db += ["--chrome-binary", chrome_bin]
    # Run real DrugBank scraping in interactive mode (no demo fallback here)
    run_step(cmd_db,
             "smk_stage6_drugbank", logger, steps_dir, env_extra=child_env_for_imports())
    # Standardize DrugBank output name and columns for downstream use
    try:
        import pandas as _pd
        _res_csv = os.path.join(drugbank_dir, "drugbank_results.csv")
        if os.path.exists(_res_csv):
            _df = _pd.read_csv(_res_csv)
            if "modality" in _df.columns and "drug_type" not in _df.columns:
                _df["drug_type"] = _df["modality"].astype(str)
            if "molecular_weight" not in _df.columns:
                aw = _df["average_weight"] if "average_weight" in _df.columns else None
                mw = _df["monoisotopic_weight"] if "monoisotopic_weight" in _df.columns else None
                if aw is not None or mw is not None:
                    _df["molecular_weight"] = (aw.fillna(mw) if aw is not None else mw)
            if "url" not in _df.columns:
                _df["url"] = ""
            keep = [
                "ingredient","drugbank_id","smiles","molecular_weight","drug_type","url",
                "actual_drug_name","unii","cas_number","inchi_key","iupac_name"
            ]
            _out = os.path.join(dirs["enrich"], "drug_drugbank.csv.gz")
            _df[[c for c in keep if c in _df.columns]].to_csv(_out, index=False, compression="gzip")
            logger.info(f"[SMOKE] Standardized DrugBank -> {_out}")
    except Exception as e:
        logger.warning(f"[SMOKE] Unable to standardize DrugBank output: {e}")

    # ---------- Stage-7: Merge-back ----------
    logger.info("[SMOKE] Stage-7: Merge-back")
    out_csv   = os.path.join(dirs["processed"], "FDA_patient_drug_report_reaction.csv.gz")
    out_stats = os.path.join(dirs["processed"], "merge_coverage.json")
    run_step([sys.executable, stg4_merge,
              "--baseline", base_csv,
              "--drug-map", drug_map,
              "--rxnorm", out_rx,
              "--adr-map", adr_map,
              "--meddra-dir", dirs["enrich"],
              "--drugbank", drugbank_dir,
              "--filter-drugbank",
              "--out-csv", out_csv,
              "--out-stats", out_stats],
             "smk_stage7_merge_back", logger, steps_dir, env_extra=child_env_for_imports())

    # ---------- Stage-8: Cohorts split ----------
    logger.info("[SMOKE] Stage-8: Split cohorts (NICHD)")
    out_ped = os.path.join(dirs["processed"], "pediatric.csv.gz")
    out_adu = os.path.join(dirs["processed"], "adults.csv.gz")
    out_unk = os.path.join(dirs["processed"], "age_unknown.csv.gz")
    out_sst = os.path.join(dirs["processed"], "split_stats.json")
    run_step([sys.executable, stg5_split,
              "--input", out_csv, "--age-col", args.age_col,
              "--out-pediatric", out_ped, "--out-adults", out_adu,
              "--out-unknown", out_unk, "--out-stats", out_sst],
             "smk_stage8_split", logger, steps_dir, env_extra=child_env_for_imports())

    # ---------- Stage-9: QA ----------
    logger.info("[SMOKE] Stage-9: QA checks")
    out_unprod = os.path.join(dirs["qa"], "unmatched_products.csv.gz")
    out_unadr  = os.path.join(dirs["qa"], "unmapped_adr.csv.gz")
    out_dupes  = os.path.join(dirs["qa"], "dupe_pairs.csv.gz")
    out_qsum   = os.path.join(dirs["qa"], "summary.json")
    run_step([sys.executable, stg6_qa,
              "--drug-dict", drug_dict,
              "--drug-map",  drug_map,
              "--rxnorm",    out_rx,
              "--adr-map",   adr_map,
              "--meddra-dir", dirs["enrich"],
              "--final-csv", out_csv,
              "--out-unmatched-products", out_unprod,
              "--out-unmapped-adr", out_unadr,
              "--out-dupes", out_dupes,
              "--out-summary", out_qsum],
             "smk_stage9_qa", logger, steps_dir, env_extra=child_env_for_imports())

    # ---------- Stage-10: Release (test bundle) ----------
    logger.info("[SMOKE] Stage-10: Release bundle (test)")
    run_step([sys.executable, stg7_rel,
              "--date", TS,
              "--files",
              out_csv, out_ped, out_adu, out_sst, out_stats,
              out_unprod, out_unadr, out_dupes, out_qsum],
             "smk_stage10_release", logger, steps_dir, env_extra=child_env_for_imports())

    # Prepare delivery folder with subset for the team
    delivery_dir = ensure_dir(rel("data","deliveries", TS))
    try:
        from shutil import copy2
        deliver = [
            out_csv,
            out_ped, out_adu,
            out_sst,
            os.path.join(dirs["enrich"], "standard_reactions.csv.gz"),
            os.path.join(dirs["enrich"], "standard_reactions_meddra_soc.csv.gz"),
            out_rx,
            os.path.join(dirs["enrich"], "drug_drugbank.csv.gz"),
        ]
        for p in deliver:
            if os.path.exists(p):
                copy2(p, os.path.join(delivery_dir, os.path.basename(p)))
        logger.info(f"[SMOKE] Delivery folder -> {delivery_dir}")
    except Exception as e:
        logger.warning(f"[SMOKE] Unable to assemble delivery folder: {e}")

    summary = {
        "sandbox_root": os.path.relpath(sand_root, ROOT),
        "release_dir":  os.path.relpath(release_dir, ROOT),
        "logs": {
            "pipeline": os.path.relpath(pipe_log, ROOT),
            "steps_dir": os.path.relpath(steps_dir, ROOT)
        },
        "inputs": {
            "er_root": er_root,
            "sample_size": args.sample_size
        },
        "artifacts": {
            "baseline_csv": os.path.relpath(base_csv, ROOT),
            "final_csv": os.path.relpath(out_csv, ROOT),
            "pediatric_csv": os.path.relpath(out_ped, ROOT),
            "adults_csv": os.path.relpath(out_adu, ROOT)
        }
    }
    ensure_dir(os.path.join(sand_root, "SMOKE_SUMMARY.json"))
    with open(os.path.join(sand_root, "SMOKE_SUMMARY.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info("[OK] PIPELINE VALIDATION DONE")
    logger.info(f"Sandbox : {os.path.relpath(sand_root, ROOT)}")
    logger.info(f"Release : {os.path.relpath(release_dir, ROOT)}")

if __name__ == "__main__":
    main()
