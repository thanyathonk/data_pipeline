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

import os, sys, argparse, subprocess, logging, datetime, gzip, csv, sqlite3, io

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
    # accept alternative filenames from different ER builders
    alt = {
        "patient.csv.gz": ["patient.csv.gz"],
        "report.csv.gz": ["report.csv.gz"],
        "report_serious.csv.gz": ["report_serious.csv.gz"],
        "reporter.csv.gz": ["reporter.csv.gz"],
        # reactions
        "patient_reaction.csv.gz": ["patient_reaction.csv.gz", "reactions.csv.gz"],
        # drugs
        "patient_drug.csv.gz": ["patient_drug.csv.gz", "drugcharacteristics.csv.gz", "drugs.csv.gz"],
    }
    required_er = list(alt.keys())
    bad = []
    if not os.path.exists(baseline_csv) or not verify_gz_crc(baseline_csv, logger):
        bad.append(baseline_csv)
    for logical in required_er:
        candidates = alt.get(logical, [logical])
        ok = False
        for fn in candidates:
            p = os.path.join(er_root, fn)
            if os.path.exists(p) and verify_gz_crc(p, logger):
                ok = True
                break
        if not ok:
            bad.append(os.path.join(er_root, logical))
    return bad

# ---- lightweight checks / helpers ----
def gz_has_data(path, peek_bytes=1<<14):
    try:
        with gzip.open(path, "rb") as f:
            chunk = f.read(peek_bytes)
            return bool(chunk)
    except Exception:
        return False

def file_ready(path):
    return os.path.exists(path) and gz_has_data(path)

def rxnorm_existing(enrich_dir, shards=1, shard_id=0):
    if shards and int(shards) > 1:
        p = os.path.join(enrich_dir, f"drug_rxnorm_sh{int(shard_id)}.csv.gz")
        return p if file_ready(p) else None
    p = os.path.join(enrich_dir, "drug_rxnorm.csv.gz")
    return p if file_ready(p) else None

def build_rxnorm_demo(drug_dict_path: str, out_clean: str, out_rx: str, logger):
    """Offline/demo: derive a minimal product_key -> INN mapping without pandas.
    - INN is approximated by uppercased product_key tokens (placeholder but stable).
    Produces both drug_clean.csv.gz and drug_rxnorm.csv.gz with minimal columns.
    """
    def slug_to_inn(slug: str) -> str:
        # convert spaces to '_' and upper for readability
        return (slug or "").strip().replace(" ", "_").upper()

    os.makedirs(os.path.dirname(out_clean), exist_ok=True)
    os.makedirs(os.path.dirname(out_rx), exist_ok=True)

    uniq = {}
    # Read drug_dict.csv.gz with csv module
    with gzip.open(drug_dict_path, "rt", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            mp = row.get("medicinal_product")
            pk = row.get("product_key")
            if not pk:
                # try alternative header casing if any
                pk = row.get("product_key".upper()) or row.get("PRODUCT_KEY")
            if pk and pk not in uniq:
                uniq[pk] = str(mp) if mp is not None else ""

    # Write drug_clean.csv.gz (medicinal_product,product_key,clean_name)
    with gzip.open(out_clean, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["medicinal_product","product_key","clean_name"])
        for pk, mp in uniq.items():
            # cleaned name is a very basic lowercase alnum collapse (replicate roughly)
            cleaned = (mp or "").lower()
            w.writerow([mp, pk, cleaned])

    # Write drug_rxnorm.csv.gz (product_key, INN) minimal but include extra cols for convenience
    with gzip.open(out_rx, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["product_key","INN"])  # only columns required downstream
        for pk in uniq.keys():
            w.writerow([pk, slug_to_inn(pk)])

    logger.info(f"[DEMO] Built minimal RxNorm mapping: {os.path.relpath(out_rx, ROOT)}")
    logger.info(f"[DEMO] Built clean list          : {os.path.relpath(out_clean, ROOT)}")
    return out_rx

def has_pandas() -> bool:
    try:
        import importlib.util as _iu
        return _iu.find_spec('pandas') is not None
    except Exception:
        return False

# ---- Fallback: Stage-7 (merge) and Stage-8 (split) without pandas ----
def _csv_reader_gz(path):
    with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            yield row

def _csv_writer_gz(path, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = gzip.open(path, 'wt', encoding='utf-8', newline='')
    w = csv.writer(f)
    w.writerow(header)
    return f, w

def fallback_stage7_and_stage8(baseline_csv, drug_map_csv, rxnorm_csv, adr_map_csv, meddra_dir,
                               processed_dir, age_col, logger):
    logger.info('[FB] Stage-7/8 fallback: streaming merge + cohort split (no pandas)')
    os.makedirs(processed_dir, exist_ok=True)

    # Load RxNorm product_key -> INN
    rx_map = {}
    with gzip.open(rxnorm_csv, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        pk = 'product_key'; innk = 'INN'
        for r in rdr:
            k = r.get(pk)
            v = r.get(innk)
            if k and (k not in rx_map):
                rx_map[k] = v
    logger.info(f"[FB] RxNorm map loaded: {len(rx_map):,} keys")

    # Load MedDRA maps
    std_pt = os.path.join(meddra_dir, 'standard_reactions.csv.gz')
    std_soc = os.path.join(meddra_dir, 'standard_reactions_meddra_soc.csv.gz')
    pt_map = {}  # reaction_meddrapt -> (pt_id, pt_code)
    with gzip.open(std_pt, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            t = r.get('reaction_meddrapt')
            pid = r.get('MedDRA_concept_id')
            pcode = r.get('MedDRA_concept_code')
            if t and pid:
                pt_map.setdefault(t, (pid, pcode))
    soc_map = {}  # pt_id -> (soc_id, soc_code, soc_name)
    with gzip.open(std_soc, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        # support both column name variants
        for r in rdr:
            ptid = r.get('MedDRA_concept_id_1') or r.get('pt_id')
            soc_id = r.get('MedDRA_concept_id') or r.get('soc_id')
            soc_code = r.get('MedDRA_concept_code') or r.get('soc_code')
            soc_name = r.get('MedDRA_concept_name') or r.get('soc_name')
            if ptid and soc_id:
                soc_map.setdefault(ptid, (soc_id, soc_code, soc_name))
    logger.info(f"[FB] MedDRA maps loaded: PT={len(pt_map):,} SOC={len(soc_map):,}")

    # Build sqlite index for drug and adr
    idx_db = os.path.join(processed_dir, 'fb_merge_index.sqlite')
    if os.path.exists(idx_db):
        try: os.remove(idx_db)
        except Exception: pass
    conn = sqlite3.connect(idx_db)
    cur = conn.cursor()
    cur.executescript('''
        PRAGMA journal_mode=MEMORY;
        PRAGMA synchronous=OFF;
        CREATE TABLE drug (sid TEXT, medicinal_product TEXT, inn TEXT);
        CREATE TABLE adr  (sid TEXT, reaction_meddrapt TEXT, pt_id TEXT, pt_code TEXT, soc_id TEXT, soc_code TEXT, soc_name TEXT);
        CREATE TABLE pair_counts (sid TEXT, medicinal_product TEXT, reaction_meddrapt TEXT, cnt INTEGER, PRIMARY KEY (sid, medicinal_product, reaction_meddrapt));
    ''')
    conn.commit()

    # Ingest drug_map with INN
    logger.info('[FB] Ingest drug_map …')
    batch = []
    for r in _csv_reader_gz(drug_map_csv):
        sid = (r.get('safetyreportid') or '').strip()
        mp = r.get('medicinal_product')
        pk = r.get('product_key')
        inn = rx_map.get(pk)
        if sid and mp and inn:
            batch.append((sid, mp, inn))
            if len(batch) >= 50000:
                cur.executemany('INSERT INTO drug(sid,medicinal_product,inn) VALUES(?,?,?)', batch)
                conn.commit(); batch = []
    if batch:
        cur.executemany('INSERT INTO drug(sid,medicinal_product,inn) VALUES(?,?,?)', batch)
        conn.commit(); batch = []
    cur.execute('CREATE INDEX IF NOT EXISTS ix_drug_sid ON drug(sid)')
    conn.commit()

    # Ingest adr_map with PT/SOC
    logger.info('[FB] Ingest adr_map …')
    batch = []
    for r in _csv_reader_gz(adr_map_csv):
        sid = (r.get('safetyreportid') or '').strip()
        reac = r.get('reaction_meddrapt')
        if not sid or not reac:
            continue
        pt = pt_map.get(reac)
        if not pt:
            continue  # keep inner join semantics like original
        pid, pcode = pt
        soc = soc_map.get(pid, (None, None, None))
        batch.append((sid, reac, pid, pcode, soc[0], soc[1], soc[2]))
        if len(batch) >= 50000:
            cur.executemany('INSERT INTO adr(sid,reaction_meddrapt,pt_id,pt_code,soc_id,soc_code,soc_name) VALUES(?,?,?,?,?,?,?)', batch)
            conn.commit(); batch = []
    if batch:
        cur.executemany('INSERT INTO adr(sid,reaction_meddrapt,pt_id,pt_code,soc_id,soc_code,soc_name) VALUES(?,?,?,?,?,?,?)', batch)
        conn.commit(); batch = []
    cur.execute('CREATE INDEX IF NOT EXISTS ix_adr_sid ON adr(sid)')
    conn.commit()

    # Prepare outputs
    out_final = os.path.join(processed_dir, 'FDA_patient_drug_report_reaction.csv.gz')
    # Header = baseline columns + additions
    with gzip.open(baseline_csv, 'rt', encoding='utf-8', newline='') as f:
        hdr_rdr = csv.reader(f)
        base_header = next(hdr_rdr)
    add_cols = ['medicinal_product','INN','reaction_meddrapt','pt_id','pt_code','soc_id','soc_code','soc_name']
    final_f, final_w = _csv_writer_gz(out_final, base_header + add_cols)

    ped_f, ped_w = _csv_writer_gz(os.path.join(processed_dir, 'pediatric.csv.gz'), base_header + add_cols)
    adu_f, adu_w = _csv_writer_gz(os.path.join(processed_dir, 'adults.csv.gz'), base_header + add_cols)
    unk_f, unk_w = _csv_writer_gz(os.path.join(processed_dir, 'age_unknown.csv.gz'), base_header + add_cols)

    def to_float(v):
        try: return float(v)
        except Exception: return float('nan')

    def nichd_label(age):
        if not (age==age):
            return None
        if age>0 and age<= (1/12): return 'term_neonatal'
        if age> (1/12) and age<=1: return 'infancy'
        if age>1 and age<=2: return 'toddler'
        if age>2 and age<=5: return 'early_childhood'
        if age>5 and age<=11: return 'middle_childhood'
        if age>11 and age<=18: return 'early_adolescence'
        if age>18 and age<=21: return 'late_adolescence'
        return None

    # Stream baseline and join
    total_rows = 0
    ped_n = adu_n = unk_n = 0
    with gzip.open(baseline_csv, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            sid = (row.get('safetyreportid') or '').strip()
            if not sid:
                continue
            # fetch drug/adr lists
            meds = cur.execute('SELECT medicinal_product, inn FROM drug WHERE sid=?', (sid,)).fetchall()
            adrs = cur.execute('SELECT reaction_meddrapt, pt_id, pt_code, soc_id, soc_code, soc_name FROM adr WHERE sid=?', (sid,)).fetchall()
            if not meds or not adrs:
                continue
            base_vals = [row.get(c, '') for c in base_header]
            # age
            try:
                age = to_float(row.get(age_col))
            except Exception:
                age = float('nan')
            nichd = nichd_label(age)
            for mp, inn in meds:
                for reac, pid, pcode, sid_soc, scode, sname in adrs:
                    final_w.writerow(base_vals + [mp, inn, reac, pid, pcode, sid_soc, scode, sname])
                    # dupes count
                    try:
                        cur.execute('INSERT INTO pair_counts(sid,medicinal_product,reaction_meddrapt,cnt) VALUES(?,?,?,1) ON CONFLICT(sid,medicinal_product,reaction_meddrapt) DO UPDATE SET cnt=cnt+1',
                                    (sid, mp, reac))
                    except Exception:
                        pass
                    if nichd is not None:
                        ped_w.writerow(base_vals + [mp, inn, reac, pid, pcode, sid_soc, scode, sname]); ped_n += 1
                    elif age>21:
                        adu_w.writerow(base_vals + [mp, inn, reac, pid, pcode, sid_soc, scode, sname]); adu_n += 1
                    else:
                        unk_w.writerow(base_vals + [mp, inn, reac, pid, pcode, sid_soc, scode, sname]); unk_n += 1
                    total_rows += 1
            if total_rows % 500000 == 0 and total_rows>0:
                logger.info(f"[FB] merged rows written: {total_rows:,}")
                conn.commit()

    conn.commit()
    final_f.close(); ped_f.close(); adu_f.close(); unk_f.close()
    logger.info(f"[FB] Stage-7/8 done. Final rows={total_rows:,} pediatric={ped_n:,} adults={adu_n:,} unknown={unk_n:,}")

    # produce simple stats jsons for compatibility
    import json as _json
    with open(os.path.join(processed_dir, 'merge_coverage.json'), 'w', encoding='utf-8') as f:
        f.write(_json.dumps({
            'rows': {'final_rows_written': int(total_rows)},
            'note': 'fallback generated (no pandas)'
        }, ensure_ascii=False, indent=2))
    with open(os.path.join(processed_dir, 'split_stats.json'), 'w', encoding='utf-8') as f:
        f.write(_json.dumps({
            'counts': {'pediatric': int(ped_n), 'adults': int(adu_n), 'unknown': int(unk_n)},
            'age_col': age_col,
            'note': 'fallback generated (no pandas)'
        }, ensure_ascii=False, indent=2))

    return idx_db  # return sqlite path for QA dupes

def fallback_stage9_qa(drug_dict, drug_map, rxnorm_csv, adr_map, meddra_dir, final_csv, qa_dir, idx_db, logger):
    logger.info('[FB] Stage-9 fallback: QA outputs (no pandas)')
    os.makedirs(qa_dir, exist_ok=True)
    # load rxnorm
    rx_map = {}
    with gzip.open(rxnorm_csv, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if r.get('product_key'):
                rx_map[r['product_key']] = r.get('INN')
    # unmatched products
    out_unprod = os.path.join(qa_dir, 'unmatched_products.csv.gz')
    f_out, w_out = _csv_writer_gz(out_unprod, ['medicinal_product','product_key','count_in_reports'])
    # build freq of meds from drug_map
    freq = {}
    for r in _csv_reader_gz(drug_map):
        mp = r.get('medicinal_product')
        if mp:
            freq[mp] = freq.get(mp, 0) + 1
    # now scan drug_dict for ones without INN
    rows = []
    for r in _csv_reader_gz(drug_dict):
        mp = r.get('medicinal_product'); pk = r.get('product_key')
        inn = rx_map.get(pk)
        if pk and (not inn or inn.strip()=='' or inn.lower()=='nan'):
            rows.append((mp, pk, freq.get(mp, 0)))
    rows.sort(key=lambda x: (-x[2], x[0] or ''))
    for row in rows:
        w_out.writerow(row)
    f_out.close()

    # unmapped ADR
    std_pt = os.path.join(meddra_dir, 'standard_reactions.csv.gz')
    known = set()
    with gzip.open(std_pt, 'rt', encoding='utf-8', newline='') as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if r.get('reaction_meddrapt'):
                known.add(r['reaction_meddrapt'])
    counts = {}
    for r in _csv_reader_gz(adr_map):
        t = r.get('reaction_meddrapt')
        if t:
            counts[t] = counts.get(t, 0) + 1
    out_unadr = os.path.join(qa_dir, 'unmapped_adr.csv.gz')
    f2, w2 = _csv_writer_gz(out_unadr, ['reaction_meddrapt','count_in_reports'])
    um = [(t,c) for t,c in counts.items() if t not in known]
    um.sort(key=lambda x: (-x[1], x[0]))
    for t,c in um:
        w2.writerow([t,c])
    f2.close()

    # dupes: scan final using pair_counts in idx_db
    out_dupes = os.path.join(qa_dir, 'dupe_pairs.csv.gz')
    f3, w3 = _csv_writer_gz(out_dupes, [])  # header will be same as final
    f3.close()  # re-open with real header after we peek final header
    # Load pair_counts
    conn = sqlite3.connect(idx_db)
    cur = conn.cursor()
    dup_keys = set()
    for sid, mp, reac, cnt in cur.execute('SELECT sid, medicinal_product, reaction_meddrapt, cnt FROM pair_counts WHERE cnt>1'):
        dup_keys.add((sid, mp, reac))
    if dup_keys:
        with gzip.open(final_csv, 'rt', encoding='utf-8', newline='') as f:
            rdr = csv.DictReader(f)
            hdr = rdr.fieldnames
            f3, w3 = _csv_writer_gz(out_dupes, hdr)
            for row in rdr:
                k = (row.get('safetyreportid'), row.get('medicinal_product'), row.get('reaction_meddrapt'))
                if k in dup_keys:
                    w3.writerow([row.get(c,'') for c in hdr])
            f3.close()
    else:
        # write empty with header = final header
        with gzip.open(final_csv, 'rt', encoding='utf-8', newline='') as f:
            rdr = csv.DictReader(f)
            hdr = rdr.fieldnames
        f3, w3 = _csv_writer_gz(out_dupes, hdr)
        f3.close()

    # summary
    import json as _json
    with open(os.path.join(qa_dir, 'summary.json'), 'w', encoding='utf-8') as f:
        f.write(_json.dumps({
            'outputs': {
                'unmatched_products': os.path.abspath(out_unprod),
                'unmapped_adr': os.path.abspath(out_unadr),
                'dupe_pairs': os.path.abspath(out_dupes)
            },
            'counts': {
                'unmatched_products': int(len(rows)),
                'unmapped_adr': int(len(um)),
                'dupe_pairs': None  # unknown cheaply
            },
            'note': 'fallback generated (no pandas)'
        }, ensure_ascii=False, indent=2))

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
    ap.add_argument("--demo-rxnav", action="store_true", help="Use offline/demo RxNorm fallback (no pandas/requests)")
    ap.add_argument("--force", action="store_true", help="Force re-run steps even if outputs exist")
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

    logger.info("=== RUN PIPELINE: 1 Parsing -> 2 ER -> 3 Baseline -> 4 Split -> 5 ADR -> 6 Drug -> 7 Merge -> 8 Cohorts -> 9 QA -> 10 Release ===")
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
            logger.info("Stage-2: Entity -> build ER tables")
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
        need_rerun = args.force or (not file_ready(baseline_csv))
        if need_rerun and args.force and not has_pandas() and file_ready(baseline_csv):
            logger.info("Stage-3: SKIP forced re-run (pandas missing). Using existing baseline.")
            need_rerun = False
        if not need_rerun:
            bad = preflight_after_stage3(logger, er_root, baseline_csv)
            need_rerun = bool(bad)
            if bad:
                logger.warning("Stage-3 preflight indicates issues; will re-run merge")
        if need_rerun:
            logger.info("Stage-3: Merge baseline (patients + report + serious + reporter)")
            run_step([sys.executable, merge_base, "--er-root", er_root, "--out-dir", baseline_dir, "--join-type", "inner"],
                     "stage3_merge_baseline", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-3: SKIPPED (baseline file exists and passed preflight)")
        logger.info(f"Baseline CSV: {os.path.relpath(baseline_csv, ROOT)}  [{path_info(baseline_csv)}]")

        # Preflight CRC check (baseline + ER core tables) after potential run
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
        adr_dict  = os.path.join(split_dir, "adr_dict.csv.gz")
        adr_map   = os.path.join(split_dir, "adr_map.csv.gz")
        drug_dict = os.path.join(split_dir, "drug_dict.csv.gz")
        drug_map  = os.path.join(split_dir, "drug_map.csv.gz")

        need_adr = (not (file_ready(adr_dict) and file_ready(adr_map))) or (args.force and has_pandas())
        if need_adr:
            run_step([sys.executable, split_adr,
                      "--er-root", er_root, "--out-dir", split_dir, "--base-csv", baseline_csv],
                     "stage4_split_adr", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-4 ADR: SKIPPED (outputs exist)")

        need_drug = (not (file_ready(drug_dict) and file_ready(drug_map))) or (args.force and has_pandas())
        if need_drug:
            cmd_drug = [sys.executable, split_drug,
                        "--er-root", er_root, "--out-dir", split_dir, "--base-csv", baseline_csv]
            if args.suspect_only: cmd_drug.append("--suspect-only")
            run_step(cmd_drug, "stage4_split_drug", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-4 Drug: SKIPPED (outputs exist)")

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
        std_react = os.path.join(enrich_dir, "standard_reactions.csv.gz")
        soc_map   = os.path.join(enrich_dir, "standard_reactions_meddra_soc.csv.gz")
        need_adr_map = (not (file_ready(std_react) and file_ready(soc_map))) or (args.force and has_pandas())
        if need_adr_map:
            vocab_dir = resolve_vocab_dir(args.vocab_dir)
            logger.info(f"Stage-5: ADR mapping (MedDRA) — vocab_dir={vocab_dir}")
            if not os.path.isdir(vocab_dir):
                raise SystemExit(f"Vocab directory not found: {vocab_dir}")
            run_step([sys.executable, adr_map_py, "--in", adr_dict, "--out-dir", enrich_dir, "--vocab", vocab_dir],
                     "stage5_adr_mapping", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-5: SKIPPED (MedDRA outputs exist)")

        logger.info(f"MedDRA PT : {os.path.relpath(std_react, ROOT)}  [{path_info(std_react)}]")
        logger.info(f"PT->SOC map: {os.path.relpath(soc_map,   ROOT)}  [{path_info(soc_map)}]")

        if not args.no_confirm and need_adr_map:
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
        logger.info("Stage-6: Drug (clean -> RxNav)")
        # If rxnorm output already exists, skip heavy work
        pre_rx = rxnorm_existing(enrich_dir, args.shards, args.shard_id)
        if pre_rx and not args.force:
            out_rx = pre_rx
            logger.info(f"Stage-6: SKIPPED (found existing RxNorm: {os.path.relpath(out_rx, ROOT)})")
        else:
            out_clean = (os.path.join(enrich_dir, f"drug_clean_sh{args.shard_id}.csv.gz")
                         if args.shards>1 else os.path.join(enrich_dir, "drug_clean.csv.gz"))
            out_rx = (os.path.join(enrich_dir, f"drug_rxnorm_sh{args.shard_id}.csv.gz")
                      if args.shards>1 else os.path.join(enrich_dir, "drug_rxnorm.csv.gz"))
            if args.demo_rxnav:
                logger.info("Stage-6: DEMO mode -> building minimal RxNorm mapping without network/pandas")
                build_rxnorm_demo(drug_dict, out_clean, out_rx, logger)
            else:
                # Normal path using child scripts
                cmd_clean = [sys.executable, clean_drug_py,
                             "--in-drug-dict", drug_dict,
                             "--out-drug-clean", out_clean,
                             "--shards", str(args.shards),
                             "--shard-id", str(args.shard_id)]
                if args.use_llm: cmd_clean += ["--use-llm", "--llm-module", args.llm_module]
                run_step(cmd_clean, "stage6_drug_clean", logger, steps_dir, env_extra=child_env_for_imports())

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
    pandas_ok = has_pandas()
    fb_idx_db = None
    if args.until_stage >= 7:
        out_csv   = os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz")
        out_stats = os.path.join(processed_dir, "merge_coverage.json")
        if pandas_ok and not args.force:
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
            logger.info("Stage-7: Using fallback merger (pandas unavailable or --force)")
            fb_idx_db = fallback_stage7_and_stage8(
                baseline_csv,
                os.path.join(split_dir, "drug_map.csv.gz"),
                out_rx,
                os.path.join(split_dir, "adr_map.csv.gz"),
                enrich_dir,
                processed_dir,
                args.age_col,
                logger
            )
    else:
        out_csv = os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz")

    if args.until_stage == 7:
        logger.info("Stop requested at Stage-7. Done."); return

    # ---------------- Stage-8: Split cohorts ----------------
    if args.until_stage >= 8:
        if pandas_ok and fb_idx_db is None and not args.force:
            out_ped = os.path.join(processed_dir, "pediatric.csv.gz")
            out_adu = os.path.join(processed_dir, "adults.csv.gz")
            out_unk = os.path.join(processed_dir, "age_unknown.csv.gz")
            out_sst = os.path.join(processed_dir, "split_stats.json")
            run_step([sys.executable, stg5_split,
                      "--input", out_csv, "--age-col", args.age_col,
                      "--out-pediatric", out_ped, "--out-adults", out_adu,
                      "--out-unknown", out_unk, "--out-stats", out_sst],
                     "stage8_split_cohorts", logger, steps_dir, env_extra=child_env_for_imports())
        else:
            logger.info("Stage-8: SKIPPED (already produced by fallback Stage-7/8)")

    if args.until_stage == 8:
        logger.info("Stop requested at Stage-8. Done."); return

    # ---------------- Stage-9: QA ----------------
    if args.until_stage >= 9:
        if pandas_ok and fb_idx_db is None and not args.force:
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
        else:
            logger.info("Stage-9: Using fallback QA (no pandas or fallback path)")
            final_csv = os.path.join(processed_dir, "FDA_patient_drug_report_reaction.csv.gz")
            fallback_stage9_qa(os.path.join(split_dir,"drug_dict.csv.gz"),
                               os.path.join(split_dir,"drug_map.csv.gz"),
                               out_rx,
                               os.path.join(split_dir,"adr_map.csv.gz"),
                               enrich_dir,
                               final_csv,
                               qa_dir,
                               fb_idx_db,
                               logger)

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



