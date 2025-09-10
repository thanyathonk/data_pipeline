#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
run_pipeline.py — Stages (updated):
  1) core/Parsing.py                               -> prepare openFDA raw
  2) core/openFDA_Entity_Relationship_Tables.py    -> build ER tables
  3) core/merge_data_all_ages.py                   -> baseline CSV
  4) split/split_adr.py + split/split_drug.py      -> dictionaries
  5) enrich/ADR.py                                  -> MedDRA mapping
  6) enrich/clean_drug.py -> enrich/rxnav_enrich.py -> INN/RxCUI
  7) stage4_merge_back.py                           -> merged dataset
  8) stage5_split_cohorts.py                        -> pediatric/adults
  9) stage6_qa_checks.py                            -> QA artifacts
 10) stage7_release_pack.py                         -> release bundle

Flags:
  --skip-stage1          skip Parsing
  --skip-stage2          skip Entity (ER)
  --until-stage N        stop after stage N (1..10)
"""

import os, sys, argparse, subprocess, logging, datetime, gzip

ROOT = os.path.dirname(os.path.abspath(__file__))
def rel(*p): return os.path.join(ROOT, *p)
def ensure_dir(p): os.makedirs(p, exist_ok=True); return p

# ---------------- Runner & ENV ----------------
def _inject_unbuffered(cmd):
    try: exe = os.path.basename(cmd[0]).lower()
    except Exception: exe = ""
    if "python" in exe and "-u" not in cmd[1:3]:
        return [cmd[0], "-u"] + cmd[1:]
    return cmd

def child_env_for_imports():
    """Add project module dirs to PYTHONPATH so child scripts can import shared modules."""
    extras = []
    for d in (ROOT, rel("core"), rel("split"), rel("enrich"), rel("merge")):
        if os.path.isdir(d):
            extras.append(d)
    prev = os.environ.get("PYTHONPATH", "")
    new_pp = os.pathsep.join(extras + ([prev] if prev else []))
    return {
        "PYTHONPATH": new_pp,
        "PYTHONUNBUFFERED": "1",
        "PYTHONIOENCODING": "utf-8",
    }

def run_step(cmd, step_name, logger, steps_dir, cwd=None, env_extra=None):
    step_log = os.path.join(steps_dir, f"{TS}_{step_name}.log")
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED","1")
    env.setdefault("PYTHONIOENCODING","utf-8")
    if env_extra: env.update(env_extra)
    cmd = _inject_unbuffered(cmd)

    logger.debug(f"[STEP:{step_name}] RUN: {' '.join(cmd)} (cwd={cwd or ROOT})")
    logger.debug(f"[STEP:{step_name}] Log -> {os.path.relpath(step_log, ROOT)}")

    with open(step_log, "wb") as sf:
        p = subprocess.Popen(cmd, cwd=cwd or ROOT, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, bufsize=0, env=env)
        buf=b""
        while True:
            ch = p.stdout.read(1)
            if not ch: break
            if ch in (b"\n", b"\r"):
                if buf:
                    line = buf.decode("utf-8","replace").rstrip()
                    logger.info(line); sf.write((line+"\n").encode("utf-8")); sf.flush(); buf=b""
            else:
                buf += ch
        if buf:
            line = buf.decode("utf-8","replace").rstrip()
            logger.info(line); sf.write((line+"\n").encode("utf-8")); sf.flush()
        code = p.wait()
        if code != 0:
            raise SystemExit(f"[STEP:{step_name}] Command failed ({code}): {' '.join(cmd)}")
    return step_log

# ---------------- Utils ----------------
def find_first(*cands):
    for c in cands:
        p = rel(c)
        if os.path.exists(p):
            return p
    return None

def detect_er_root(logger):
    cands = [
        rel("data","openFDA_drug_event","er_tables"),
        rel("data","openFDA_drug_event","er_tables_memory_efficient"),
        os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event","er_tables"),
        os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event","er_tables_memory_efficient"),
    ]
    for p in cands:
        if os.path.isdir(p):
            logger.info(f"[ER] Found: {p}")
            return p
    # shallow scan
    for base in (rel("data","openFDA_drug_event"),
                 os.path.join(os.path.dirname(ROOT), "data","openFDA_drug_event")):
        if os.path.isdir(base):
            for name in os.listdir(base):
                if name.startswith("er_tables"):
                    p = os.path.join(base, name)
                    if os.path.isdir(p):
                        logger.info(f"[ER] Found by scan: {p}")
                        return p
    return None

def path_info(p):
    if not os.path.exists(p): return "MISSING"
    try:
        size = os.path.getsize(p)/1024/1024
        return f"OK ({size:.1f} MB)"
    except Exception:
        return "OK"

def resolve_vocab_dir(user_path):
    cands = [user_path, rel("vocab"),
             rel("vocab","SNOMED_MEDDRA_RxNorm_ATC"),
             rel("vocab","vocabulary_SNOMED_MEDDRA_RxNorm_ATC")]
    for p in cands:
        if p and os.path.isdir(p):
            return p
    return user_path

# ---- CRC preflight (catch corrupted .gz early) ----
def verify_gz_crc(path, logger=None, chunk=1<<20):
    try:
        with gzip.open(path, "rb") as f:
            while f.read(chunk):
                pass
        if logger: logger.debug(f"[CRC] OK: {path}")
        return True
    except Exception as e:
        if logger: logger.error(f"[CRC] BAD: {path}  <-- {type(e).__name__}: {e}")
        return False

def preflight_after_stage3(logger, er_root, baseline_csv):
    """Verify essential ER tables and baseline after Stage-3."""
    required_er = [
        "patient.csv.gz", "report.csv.gz",
        "report_serious.csv.gz", "reporter.csv.gz",
        "patient_reaction.csv.gz", "patient_drug.csv.gz",
    ]
    bad = []
    if not os.path.exists(baseline_csv) or not verify_gz_crc(baseline_csv, logger):
        bad.append(baseline_csv)
    for fn in required_er:
        p = os.path.join(er_root, fn)
        if not os.path.exists(p) or not verify_gz_crc(p, logger):
            bad.append(p)
    return bad

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-stage1", action="store_true", help="Skip Stage-1 (Parsing).")
    ap.add_argument("--skip-stage2", action="store_true", help="Skip Stage-2 (Entity/ER).")
    ap.add_argument("--until-stage", type=int, default=10, choices=range(1,11),
                    help="Stop after this stage (1..10). Default: 10 (run all).")
    # knobs
    ap.add_argument("--suspect-only", action="store_true")
    ap.add_argument("--use-llm", action="store_true")
    ap.add_argument("--llm-module", default="clean_drug_gpt5")
    ap.add_argument("--vocab-dir", default=rel("vocab"))
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--shard-id", type=int, default=0)
    ap.add_argument("--qps", type=float, default=4.0)
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("-y","--yes", action="store_true", help="Auto-confirm before Drug stage")
    ap.add_argument("--no-confirm", action="store_true")
    ap.add_argument("--age-col", default="patient_custom_master_age")
    args = ap.parse_args()

    # logging
    global TS
    TS = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir  = ensure_dir(rel("data","logs"))
    steps_dir = ensure_dir(rel("data","logs","steps"))
    pipe_log  = os.path.join(logs_dir, f"pipeline_{TS}.log")
    logger = logging.getLogger("pipeline"); logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(pipe_log, encoding="utf-8"); fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout);            ch.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.handlers.clear(); logger.addHandler(fh); logger.addHandler(ch)

    logger.info("=== RUN PIPELINE: 1 Parsing → 2 ER → 3 Baseline → 4 Split → 5 ADR → 6 Drug → 7 Merge → 8 Cohorts → 9 QA → 10 Release ===")
    logger.info(f"[LOG] Pipeline log: {os.path.relpath(pipe_log, ROOT)}")
    logger.info(f"[LOG] Step logs   : {os.path.relpath(steps_dir, ROOT)}/")

    # locate scripts
    parsing       = find_first("core/Parsing.py")
    entity        = find_first("core/openFDA_Entity_Relationship_Tables.py")
    merge_base    = find_first("core/merge_data_all_ages.py")
    split_adr     = find_first("split/split_adr.py")
    split_drug    = find_first("split/split_drug.py")
    adr_map_py    = find_first("enrich/ADR.py")
    clean_drug_py = find_first("enrich/clean_drug.py")
    rxnav_py      = find_first("enrich/rxnav_enrich.py")
    stg4_merge    = find_first("stage4_merge_back.py")
    stg5_split    = find_first("stage5_split_cohorts.py")
    stg6_qa       = find_first("stage6_qa_checks.py")
    stg7_rel      = find_first("stage7_release_pack.py")

    for need, path in [
        ("core/Parsing.py", parsing),
        ("core/openFDA_Entity_Relationship_Tables.py", entity),
        ("core/merge_data_all_ages.py", merge_base),
        ("split/split_adr.py", split_adr),
        ("split/split_drug.py", split_drug),
        ("enrich/ADR.py", adr_map_py),
        ("enrich/clean_drug.py", clean_drug_py),
        ("enrich/rxnav_enrich.py", rxnav_py),
        ("stage4_merge_back.py", stg4_merge),
        ("stage5_split_cohorts.py", stg5_split),
        ("stage6_qa_checks.py", stg6_qa),
        ("stage7_release_pack.py", stg7_rel),
    ]:
        if not path:
            raise SystemExit(f"Missing script: {need}")

    # dirs
    baseline_dir  = ensure_dir(rel("data","baseline"))
    split_dir     = ensure_dir(rel("data","split"))
    enrich_dir    = ensure_dir(rel("data","enrich"))
    processed_dir = ensure_dir(rel("data","processed"))
    qa_dir        = ensure_dir(rel("data","qa"))
    baseline_csv  = os.path.join(baseline_dir, "patients_report_serious_reporter.csv.gz")

    # If ER already exists, we can skip Stage-1/2 automatically (unless user forces otherwise)
    existing_er = detect_er_root(logger)

    # ---------------- Stage-1: Parsing ----------------
    if args.until_stage >= 1:
        if existing_er:
            logger.info("Stage-1: SKIPPED (ER found; assuming raw prepared earlier)")
        elif not args.skip_stage1:
            logger.info("Stage-1: Parsing (download/prepare raw openFDA)")
            run_step([sys.executable, parsing], "stage1_parsing", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-1: SKIPPED (flag)")

    if args.until_stage == 1:
        logger.info("Stop requested at Stage-1. Done."); return

    # ---------------- Stage-2: Entity -> ER ----------------
    if args.until_stage >= 2:
        existing_er = detect_er_root(logger)
        if existing_er:
            logger.info("Stage-2: SKIPPED (ER found)")
        elif not args.skip_stage2:
            logger.info("Stage-2: Entity → build ER tables")
            run_step([sys.executable, entity], "stage2_entity", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-2: SKIPPED (flag)")

    # Detect ER after Stage-2
    er_root = detect_er_root(logger)
    if not er_root:
        raise SystemExit("ER tables not found. Run Stage-2 (Entity) or provide ER path.")

    if args.until_stage == 2:
        logger.info("Stop requested at Stage-2. Done."); return

    # ---------------- Stage-3: Merge baseline ----------------
    if args.until_stage >= 3:
        logger.info("Stage-3: Merge baseline (patients + report + serious + reporter)")
        run_step([sys.executable, merge_base, "--er-root", er_root, "--out-dir", baseline_dir, "--join-type", "inner"],
                 "stage3_merge_baseline", logger, steps_dir, env_extra=child_env_for_imports())
        logger.info(f"Baseline CSV: {os.path.relpath(baseline_csv, ROOT)}  [{path_info(baseline_csv)}]")

        # Preflight CRC check (baseline + ER core tables)
        bad = preflight_after_stage3(logger, er_root, baseline_csv)
        if bad:
            logger.error("Preflight CRC failed. The following files are missing or corrupted:")
            for p in bad: logger.error(f"  - {p}")
            raise SystemExit(2)

    if args.until_stage == 3:
        logger.info("Stop requested at Stage-3. Done."); return

    # ---------------- Stage-4: Split dictionaries ----------------
    if args.until_stage >= 4:
        logger.info("Stage-4: Split dictionaries (ADR then Drug)")
        run_step([sys.executable, split_adr,
                  "--er-root", er_root, "--out-dir", split_dir, "--base-csv", baseline_csv],
                 "stage4_split_adr", logger, steps_dir, env_extra=child_env_for_imports())

        cmd_drug = [sys.executable, split_drug,
                    "--er-root", er_root, "--out-dir", split_dir, "--base-csv", baseline_csv]
        if args.suspect_only: cmd_drug.append("--suspect-only")
        run_step(cmd_drug, "stage4_split_drug", logger, steps_dir, env_extra=child_env_for_imports())

        adr_dict  = os.path.join(split_dir, "adr_dict.csv.gz")
        adr_map   = os.path.join(split_dir, "adr_map.csv.gz")
        drug_dict = os.path.join(split_dir, "drug_dict.csv.gz")
        drug_map  = os.path.join(split_dir, "drug_map.csv.gz")
        logger.info(f"ADR dict  : {os.path.relpath(adr_dict,  ROOT)} [{path_info(adr_dict)}]")
        logger.info(f"ADR map   : {os.path.relpath(adr_map,   ROOT)} [{path_info(adr_map)}]")
        logger.info(f"Drug dict : {os.path.relpath(drug_dict, ROOT)} [{path_info(drug_dict)}]")
        logger.info(f"Drug map  : {os.path.relpath(drug_map,  ROOT)} [{path_info(drug_map)}]")
    else:
        adr_dict  = os.path.join(split_dir, "adr_dict.csv.gz")
        adr_map   = os.path.join(split_dir, "adr_map.csv.gz")
        drug_dict = os.path.join(split_dir, "drug_dict.csv.gz")
        drug_map  = os.path.join(split_dir, "drug_map.csv.gz")

    if args.until_stage == 4:
        logger.info("Stop requested at Stage-4. Done."); return

    # ---------------- Stage-5: ADR mapping ----------------
    if args.until_stage >= 5:
        vocab_dir = resolve_vocab_dir(args.vocab_dir)
        logger.info(f"Stage-5: ADR mapping (MedDRA) — vocab_dir={vocab_dir}")
        if not os.path.isdir(vocab_dir):
            raise SystemExit(f"Vocab directory not found: {vocab_dir}")
        run_step([sys.executable, adr_map_py, "--in", adr_dict, "--out-dir", enrich_dir, "--vocab", vocab_dir],
                 "stage5_adr_mapping", logger, steps_dir, env_extra=child_env_for_imports())

        std_react = os.path.join(enrich_dir, "standard_reactions.csv.gz")
        soc_map   = os.path.join(enrich_dir, "standard_reactions_meddra_soc.csv.gz")
        logger.info(f"MedDRA PT : {os.path.relpath(std_react, ROOT)}  [{path_info(std_react)}]")
        logger.info(f"PT→SOC map: {os.path.relpath(soc_map,   ROOT)}  [{path_info(soc_map)}]")

        if not args.no_confirm:
            print("\n[CONFIRM] ตรวจผล ADR mapping แล้วหรือยัง?")
            print(f"  - PT table : {os.path.relpath(std_react, ROOT)} [{path_info(std_react)}]")
            print(f"  - PT→SOC   : {os.path.relpath(soc_map, ROOT)} [{path_info(soc_map)}]")
            ans = "y" if args.yes else input("Proceed to Stage-6 Drug? [y/N]: ").strip().lower()
            logger.info(f"[confirm] proceed={ans!r}")
            if ans not in ("y","yes"):
                print("\n[STOP] Stopped before Drug stage as requested.\n"); return

    if args.until_stage == 5:
        logger.info("Stop requested at Stage-5. Done."); return

    # ---------------- Stage-6: Drug (clean -> RxNav) ----------------
    if args.until_stage >= 6:
        logger.info("Stage-6: Drug (clean → RxNav)")
        out_clean = (os.path.join(enrich_dir, f"drug_clean_sh{args.shard_id}.csv.gz")
                     if args.shards>1 else os.path.join(enrich_dir, "drug_clean.csv.gz"))
        cmd_clean = [sys.executable, clean_drug_py,
                     "--in-drug-dict", drug_dict,
                     "--out-drug-clean", out_clean,
                     "--shards", str(args.shards),
                     "--shard-id", str(args.shard_id)]
        if args.use_llm: cmd_clean += ["--use-llm", "--llm-module", args.llm_module]
        run_step(cmd_clean, "stage6_drug_clean", logger, steps_dir, env_extra=child_env_for_imports())

        out_rx = (os.path.join(enrich_dir, f"drug_rxnorm_sh{args.shard_id}.csv.gz")
                  if args.shards>1 else os.path.join(enrich_dir, "drug_rxnorm.csv.gz"))
        cache_db = os.path.join(enrich_dir, "rxnav_cache.sqlite")
        cmd_rx = [sys.executable, rxnav_py,
                  "--in-drug-clean", out_clean,
                  "--out-drug-rxnorm", out_rx,
                  "--cache-db", cache_db,
                  "--shards", str(args.shards),
                  "--shard-id", str(args.shard_id),
                  "--max-workers", str(args.max_workers),
                  "--qps", str(args.qps)]
        run_step(cmd_rx, "stage6_rxnav_enrich", logger, steps_dir, env_extra=child_env_for_imports())
    else:
        out_rx = os.path.join(enrich_dir, "drug_rxnorm.csv.gz")

    if args.until_stage == 6:
        logger.info("Stop requested at Stage-6. Done."); return

    # ---------------- Stage-7: Merge-back ----------------
    if args.until_stage >= 7:
        out_csv   = os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz")
        out_stats = os.path.join(processed_dir, "merge_coverage.json")
        run_step([sys.executable, stg4_merge,
                  "--baseline", baseline_csv,
                  "--drug-map",  os.path.join(split_dir, "drug_map.csv.gz"),
                  "--rxnorm",    out_rx,
                  "--adr-map",   os.path.join(split_dir, "adr_map.csv.gz"),
                  "--meddra-dir",enrich_dir,
                  "--out-csv",   out_csv,
                  "--out-stats", out_stats],
                 "stage7_merge_back", logger, steps_dir, env_extra=child_env_for_imports())
    else:
        out_csv = os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz")

    if args.until_stage == 7:
        logger.info("Stop requested at Stage-7. Done."); return

    # ---------------- Stage-8: Split cohorts ----------------
    if args.until_stage >= 8:
        out_ped = os.path.join(processed_dir, "pediatric.csv.gz")
        out_adu = os.path.join(processed_dir, "adults.csv.gz")
        out_unk = os.path.join(processed_dir, "age_unknown.csv.gz")
        out_sst = os.path.join(processed_dir, "split_stats.json")
        run_step([sys.executable, stg5_split,
                  "--input", out_csv, "--age-col", args.age_col,
                  "--out-pediatric", out_ped, "--out-adults", out_adu,
                  "--out-unknown", out_unk, "--out-stats", out_sst],
                 "stage8_split_cohorts", logger, steps_dir, env_extra=child_env_for_imports())

    if args.until_stage == 8:
        logger.info("Stop requested at Stage-8. Done."); return

    # ---------------- Stage-9: QA ----------------
    if args.until_stage >= 9:
        out_unprod = os.path.join(qa_dir, "unmatched_products.csv.gz")
        out_unadr  = os.path.join(qa_dir, "unmapped_adr.csv.gz")
        out_dupes  = os.path.join(qa_dir, "dupe_pairs.csv.gz")
        out_qsum   = os.path.join(qa_dir, "summary.json")
        run_step([sys.executable, stg6_qa,
                  "--drug-dict", os.path.join(split_dir,"drug_dict.csv.gz"),
                  "--drug-map",  os.path.join(split_dir,"drug_map.csv.gz"),
                  "--rxnorm",    out_rx,
                  "--adr-map",   os.path.join(split_dir,"adr_map.csv.gz"),
                  "--meddra-dir",enrich_dir,
                  "--final-csv", os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz"),
                  "--out-unmatched-products", out_unprod,
                  "--out-unmapped-adr", out_unadr,
                  "--out-dupes", out_dupes,
                  "--out-summary", out_qsum],
                 "stage9_qa_checks", logger, steps_dir, env_extra=child_env_for_imports())

    if args.until_stage == 9:
        logger.info("Stop requested at Stage-9. Done."); return

    # ---------------- Stage-10: Release ----------------
    if args.until_stage >= 10:
        release_date = TS
        run_step([sys.executable, stg7_rel,
                  "--date", release_date,
                  "--files",
                  os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz"),
                  os.path.join(processed_dir, "pediatric.csv.gz"),
                  os.path.join(processed_dir, "adults.csv.gz"),
                  os.path.join(processed_dir, "split_stats.json"),
                  os.path.join(processed_dir, "merge_coverage.json"),
                  os.path.join(qa_dir, "unmatched_products.csv.gz"),
                  os.path.join(qa_dir, "unmapped_adr.csv.gz"),
                  os.path.join(qa_dir, "dupe_pairs.csv.gz"),
                  os.path.join(qa_dir, "summary.json")],
                 "stage10_release_pack", logger, steps_dir, env_extra=child_env_for_imports())

    logger.info("=== DONE: Stage-1 .. Stage-10 ===")
    logger.info(f"Pipeline log: {os.path.relpath(pipe_log, ROOT)}")
    logger.info(f"Step logs   : {os.path.relpath(steps_dir, ROOT)}/")
    print(f"\n[OK] Completed.\n- Pipeline log: {os.path.relpath(pipe_log, ROOT)}\n- Step logs  : {os.path.relpath(steps_dir, ROOT)}/")

if __name__ == "__main__":
    main()
