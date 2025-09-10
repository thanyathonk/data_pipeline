#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, io, re, sys, json, time, shutil, datetime as dt, traceback, unicodedata, subprocess
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from openai import OpenAI
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
import openai

# ================== CONFIG ==================
OUTPUT_ROOT = "output"
MODEL = os.getenv("OPENAI_MODEL", "gpt-5-nano")  # Responses API (ไม่ส่ง temperature)
TEXT_COL_DEFAULT = "medicinal_product"
UNIQUE_CHUNK_SIZE = int(os.getenv("UNIQUE_CHUNK_SIZE", "300"))
WIRE_SEP = "\t"  # ส่งกับโมเดลเป็น TSV เพื่อลดปัญหาคอมมา

SYSTEM_INSTRUCTIONS = """
You are a data-cleaning assistant for medicinal PRODUCT names used for RxNorm search.
Goal: minimally clean each raw product string so it can be used as the RxNorm query.

Hard constraints:
- NEVER invent new tokens or brand spellings not present in the input. Output must be derived only by:
  (a) lowercasing; (b) deleting noise tokens; (c) normalizing delimiters.
- DO NOT convert brand/product to INN/generic. Do NOT translate.
- Preserve original token order (only deletions allowed).

Noise you may delete:
- dose/strength/units (mg, mcg, g, %, mg/mL, mcg/mL, IU, units, mEq, mmol, etc.),
  dosage forms (tablet/tab, capsule/cap, inj, solution, suspension, syrup, spray, patch, cream, gel, ointment, granules, powder, elixir),
  release types (EC, XR, SR, DR, CR, ER, MR, OD),
  routes (oral, PO, IV, IM, SC, SL, PR, ophthalmic/oph, otic, nasal, topical, inhalation),
  pack sizes, frequency, lot/batch numbers, and trailing marketing codes not part of the base name.

Delimiters:
- Replace any of these as token separators: "\\", "/", "+", "," with a SINGLE "/" (no spaces).
- Collapse duplicates: "A//B" → "A/B".
- If string repeats same spelling (e.g., "peg-l-asparaginase peg-l-asparaginase"), keep one.

Parentheses:
- If parentheses only echo descriptors or same concept (e.g., "(isoflurane)"), drop the parentheses content.

Ambiguity:
- If unsure, copy the ORIGINAL input value unchanged (except trimming). Never return empty or 'NaN'.

Output contract (TSV ONLY):
- Return TSV (tab-separated) with EXACTLY the input columns plus ONE NEW LAST COLUMN: clean_med_product
- Single header row (TAB-separated). No markdown/code fences/comments.
- Do NOT put TAB inside values—replace with a space.
"""

# ================== PRE-CLEAN (deterministic ช่วยโมเดลเข้าใจง่าย) ==================
DELIM_CLASS = r"[\\/+,]+"
DOSE_UNITS = r"(mcg|mg|g|kg|ml|l|%|iu|units|meq|mmol)"
FORM_WORDS = r"(tab(let)?s?|cap(s|sule)?s?|inj(ection)?|amp(oule)?|vial|susp(ension)?|sol(ution)?|sirup|syrup|drop(s)?|spray|aerosol|cream|gel|ointment|patch|granule(s)?|powder|elixir|lozenge|chewable|suppository|film-?coated)"
ROUTE_WORDS = r"(oral|po|iv|im|sc|subcut|pr|sl|oph(thalmic)?|otic|nasal|topical|inhal(ation)?|nebuliz(e|er)d?)"
RELEASE_WORDS = r"(xr|sr|er|dr|cr|ec|mr|od)"
FREQ_WORDS = r"(bid|tid|qid|q\d+h|qhs|hs|qod|prn)"
MARKETING_CODE = r"(/?\d{5,}[/\)]?)"
DELIM_RE = re.compile(DELIM_CLASS, flags=re.I)
SPACE_RE = re.compile(r"\s+")
PARENS_GENERIC_RE = re.compile(r"\([^)]*\)", flags=re.I)
DOSE_PATTERNS = [
    re.compile(rf"\b\d+(\.\d+)?\s*{DOSE_UNITS}\b", flags=re.I),
    re.compile(
        rf"\b\d+(\.\d+)?\s*{DOSE_UNITS}\s*/\s*\d+(\.\d+)?\s*(ml|l)\b", flags=re.I
    ),
]
NOISE_TOKENS_RE = re.compile(
    rf"\b({FORM_WORDS}|{ROUTE_WORDS}|{RELEASE_WORDS}|{FREQ_WORDS})\b", flags=re.I
)


def preclean_for_model(s: str) -> str:
    if s is None:
        return ""
    t = (
        unicodedata.normalize("NFKC", str(s))
        .replace("\u00a0", " ")
        .replace("\ufeff", " ")
    )
    t = re.sub(MARKETING_CODE, " ", t)
    for pat in DOSE_PATTERNS:
        t = pat.sub(" ", t)
    t = NOISE_TOKENS_RE.sub(" ", t)
    t = DELIM_RE.sub("/", t)
    t2 = PARENS_GENERIC_RE.sub(" ", t).strip()
    if t2:
        t = t2
    t = re.sub(r"[^\w\s/.\-]", " ", t)
    t = SPACE_RE.sub(" ", t).strip()
    t = t.replace(" /", "/").replace("/ ", "/")
    while "//" in t:
        t = t.replace("//", "/")
    return t.lower()


# ================== HELPERS ==================
def load_env_and_client():
    load_dotenv()
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("Please set OPENAI_API_KEY in .env")
    return OpenAI(api_key=key)


def ensure_dirs(version: str, clear_cache: bool):
    out_dir = os.path.join(OUTPUT_ROOT, f"v{version}")
    cache_dir = os.path.join(out_dir, ".cache")
    log_dir = os.path.join(out_dir, "logs")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    if clear_cache:
        shutil.rmtree(cache_dir, ignore_errors=True)
        os.makedirs(cache_dir, exist_ok=True)
    return out_dir, cache_dir, log_dir


def df_to_tsv_text(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False, sep="\t")
    return buf.getvalue()


# ---- Lenient TSV parser (แถว = ต้นฉบับ + มี key แน่) ----
def parse_tsv_lenient(
    tsv_text: str,
    expected_headers: list[str],
    original_part: pd.DataFrame,
    key_col: str,
) -> pd.DataFrame:
    text = tsv_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln for ln in text.split("\n") if ln.strip() != ""]
    exp_n = len(expected_headers)

    header = expected_headers[:]
    rows = []
    if lines:
        for ln in lines[1:]:
            cols = ln.split("\t")
            if len(cols) < exp_n:
                cols += [""] * (exp_n - len(cols))
            elif len(cols) > exp_n:
                head = cols[: exp_n - 1]
                tail = cols[exp_n - 1 :]
                cols = head + [" ".join(tail)]
            rows.append(cols)

    df = pd.DataFrame(rows, columns=header) if rows else pd.DataFrame(columns=header)

    target_n = len(original_part)
    cur_n = len(df)
    if cur_n > target_n:
        df = df.head(target_n).reset_index(drop=True)
    elif cur_n < target_n:
        pad_n = target_n - cur_n
        pad_df = pd.DataFrame({col: [""] * pad_n for col in header})
        df = pd.concat([df.reset_index(drop=True), pad_df], ignore_index=True)

    df[key_col] = original_part[key_col].values
    if "clean_med_product" not in df.columns:
        df["clean_med_product"] = ""
    return df[header]


# ---- normalize & fallback ----
DELIM_RE2 = re.compile(DELIM_CLASS, flags=re.I)
SPACE_RE2 = re.compile(r"\s+")


def fallback_lower(original_val: str) -> str:
    if original_val is None:
        return ""
    s = (
        unicodedata.normalize("NFKC", str(original_val))
        .replace("\u00a0", " ")
        .replace("\ufeff", "")
    )
    return s.strip().lower()


def normalize_clean_name(clean_val: str, original_val: str) -> str:
    if clean_val is None:
        return fallback_lower(original_val)
    s_in = str(clean_val)
    if s_in.strip() == "" or s_in.strip().lower() == "nan":
        return fallback_lower(original_val)
    s = unicodedata.normalize("NFKC", s_in).lower()
    s = DELIM_RE2.sub("/", s).replace(" /", "/").replace("/ ", "/")
    while "//" in s:
        s = s.replace("//", "/")
    s = SPACE_RE2.sub(" ", s).strip()
    toks, dedup = s.split(" "), []
    for t in toks:
        if not dedup or t != dedup[-1]:
            dedup.append(t)
    s = " ".join(dedup).strip()
    return s if s else fallback_lower(original_val)


# ===== Search-friendly refinement (ตัด descriptor เพิ่มเติม) =====
SEARCH_STOPWORDS = {
    "solution",
    "suspension",
    "syrup",
    "elixir",
    "drops",
    "drop",
    "spray",
    "cream",
    "gel",
    "ointment",
    "patch",
    "granules",
    "granule",
    "powder",
    "tablet",
    "tablets",
    "tab",
    "tabs",
    "capsule",
    "capsules",
    "cap",
    "caps",
    "lozenge",
    "chewable",
    "suppository",
    "oral",
    "po",
    "iv",
    "im",
    "sc",
    "sl",
    "pr",
    "oph",
    "ophthalmic",
    "otic",
    "nasal",
    "topical",
    "inhalation",
    "nebulized",
    "nebulizer",
    "xr",
    "sr",
    "er",
    "dr",
    "cr",
    "ec",
    "mr",
    "od",
    "for",
    "sterile",
}
UNIT_WORDS = {"mcg", "mg", "g", "kg", "ml", "l", "%", "iu", "units", "meq", "mmol"}


def make_search_friendly(name: str, original_val: str) -> str:
    raw = fallback_lower(name) or fallback_lower(original_val)
    x = re.sub(r"\([^)]*\)", " ", raw)
    x = re.sub(
        r"\b\d+(\.\d+)?\s*(mcg|mg|g|kg|ml|l|%|iu|units|meq|mmol)\b", " ", x, flags=re.I
    )
    x = re.sub(
        r"\b\d+(\.\d+)?\s*(mcg|mg|g|kg)\s*/\s*\d+(\.\d+)?\s*(ml|l)\b",
        " ",
        x,
        flags=re.I,
    )
    x = re.sub(r"/\s*(ml|l)\b", " ", x, flags=re.I)
    x = re.sub(r"[\\+,]+", " ", x).replace(" /", "/").replace("/ ", "/")
    x = re.sub(r"\s+", " ", x).strip()
    parts = []
    for chunk in re.split(r"/", x):
        toks = [
            t
            for t in re.split(r"\s+", chunk.strip())
            if t and t not in SEARCH_STOPWORDS and t not in UNIT_WORDS
        ]
        if toks:
            parts.append(" ".join(toks))
    return "/".join(parts) if parts else fallback_lower(original_val)


# ---------- Guard: อนุญาตเฉพาะ “ลบ” จาก preclean ----------
THAI_RANGE = "\u0e00-\u0e7f"
TOKENIZE_RE = re.compile(rf"[^\w/\- {THAI_RANGE}]+", flags=re.UNICODE)


def tokenset(s: str) -> set[str]:
    if s is None:
        return set()
    x = unicodedata.normalize("NFKC", str(s)).lower()
    x = re.sub(r"[\\/+,]+", " ", x)
    x = TOKENIZE_RE.sub(" ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return set(t for t in x.split(" ") if t)


def guard_monotone_delete(
    candidate: str, preclean_input: str, original_raw: str
) -> str:
    cand = (candidate or "").strip()
    if not cand or cand.lower() == "nan":
        return fallback_lower(original_raw)
    cs = tokenset(cand)
    os_ = tokenset(preclean_input)
    return candidate if cs and cs.issubset(os_) else fallback_lower(original_raw)


# ================== MODEL CALL ==================
@retry(
    reraise=True,
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(
        (
            openai.APIConnectionError,
            openai.RateLimitError,
            openai.APITimeoutError,
            openai.InternalServerError,
        )
    ),
)
def call_model_tsv(
    client: OpenAI, tsv_text: str, text_col: str, expected_headers: list[str]
) -> str:
    headers_str = "\t".join(expected_headers)
    user_msg = (
        f"You are given a TSV chunk (tab-separated). The raw product-name column is '{text_col}'. "
        f"Return the ENTIRE TSV with exactly ONE new last column named 'clean_med_product'. "
        f"THE RETURNED TSV MUST HAVE THIS EXACT SINGLE HEADER LINE:\n{headers_str}\n"
        f"Do not add/remove/reorder columns. Do NOT put TAB characters inside field values—replace tabs with a space. "
        f"If unsure, copy the ORIGINAL input value unchanged. Never output empty or 'NaN'. "
        f"Output TSV only (no markdown or commentary):\n\n{tsv_text}"
    )
    try:
        resp = client.responses.create(
            model=MODEL,
            input=[
                {"role": "system", "content": SYSTEM_INSTRUCTIONS},
                {"role": "user", "content": user_msg},
            ],
        )
    except openai.BadRequestError:
        raise
    return getattr(resp, "output_text", None) or resp.output[0].content[0].text


# ================== CACHE ==================
def cache_paths(cache_dir: str):
    return (os.path.join(cache_dir, "clean_cache.tsv"),)


def load_cache_clean(cache_dir: str, key_col: str) -> pd.DataFrame:
    (cache_map_path,) = cache_paths(cache_dir)
    if os.path.exists(cache_map_path):
        df = pd.read_csv(
            cache_map_path, sep="\t", keep_default_na=False, na_filter=False, dtype=str
        )
        for c in [key_col, "clean_med_product"]:
            if c not in df.columns:
                df[c] = ""
        return df[[key_col, "clean_med_product"]].drop_duplicates()
    return pd.DataFrame(columns=[key_col, "clean_med_product"])


def append_cache_clean(cache_dir: str, key_col: str, df_new: pd.DataFrame):
    (cache_map_path,) = cache_paths(cache_dir)
    df_new = df_new[[key_col, "clean_med_product"]].drop_duplicates()
    if os.path.exists(cache_map_path):
        old = pd.read_csv(
            cache_map_path, sep="\t", keep_default_na=False, na_filter=False, dtype=str
        )
        all_ = pd.concat([old, df_new], ignore_index=True)
        all_.drop_duplicates(subset=[key_col], keep="last", inplace=True)
        all_.to_csv(cache_map_path, index=False, sep="\t")
    else:
        df_new.to_csv(cache_map_path, index=False, sep="\t")


# ================== MAIN ==================
def main():
    import argparse

    ap = argparse.ArgumentParser(
        description="LLM cleaning → clean_med_product (no NaN), versioned, cache, logs. Optionally chain RxNav."
    )
    ap.add_argument("--input", required=True, help="path to input CSV")
    ap.add_argument(
        "--version", required=True, help="version tag, e.g. V3 or 2025.08.31-1"
    )
    ap.add_argument(
        "--text-col",
        default=TEXT_COL_DEFAULT,
        help="column name (default: medicinal_product)",
    )
    ap.add_argument(
        "--chunk",
        type=int,
        default=UNIQUE_CHUNK_SIZE,
        help="unique-name chunk size per model call",
    )
    ap.add_argument(
        "--clear-cache-first",
        action="store_true",
        help="clear cache directory at start",
    )
    ap.add_argument(
        "--keep-cache",
        action="store_true",
        help="keep cache after finish (default: delete)",
    )
    ap.add_argument(
        "--run-rxnav",
        action="store_true",
        help="chain to rxnav_lookup.py after LLM step",
    )
    ap.add_argument(
        "--rx-concurrency",
        type=int,
        default=20,
        help="concurrency for rxnav_lookup if chained",
    )
    args = ap.parse_args()

    client = load_env_and_client()
    out_dir, cache_dir, log_dir = ensure_dirs(
        args.version, clear_cache=args.clear_cache_first
    )
    error_log_path = os.path.join(log_dir, "error.log")
    had_error = False
    rejected_by_guard = 0

    # 1) อ่านอินพุต
    df_in = pd.read_csv(args.input, keep_default_na=False, na_filter=False, dtype=str)
    if args.text_col not in df_in.columns:
        raise SystemExit(f"Column '{args.text_col}' not found in {args.input}")
    key_col = args.text_col

    # 2) unique + preclean
    uniq_vals = pd.Series(df_in[key_col].astype(str).fillna("").unique(), name=key_col)
    df_uniques = pd.DataFrame({key_col: uniq_vals})
    df_uniques["_pre_for_model"] = df_uniques[key_col].apply(preclean_for_model)

    # 3) cache
    cache_df = load_cache_clean(cache_dir, key_col)
    pending = df_uniques.merge(cache_df, on=key_col, how="left")
    to_do = pending[pending["clean_med_product"].isna()].drop(
        columns=["clean_med_product"]
    )

    print(f"Total unique products: {len(df_uniques):,}")
    print(
        f"Cached clean: {len(df_uniques) - len(to_do):,} | To process now: {len(to_do):,}"
    )

    # 4) ประมวลผล LLM เป็นก้อน ๆ
    with tqdm(total=len(to_do), desc="LLM cleaning", unit="rows") as pbar:
        for i in range(0, len(to_do), args.chunk):
            part = to_do.iloc[i : i + args.chunk].copy()
            expected_headers = [key_col, "clean_med_product"]
            part_to_send = pd.DataFrame({key_col: part["_pre_for_model"].values})
            tsv_in = df_to_tsv_text(part_to_send)
            try:
                tsv_out = call_model_tsv(client, tsv_in, key_col, expected_headers)
                df_part = parse_tsv_lenient(
                    tsv_out, expected_headers, part[[key_col]], key_col
                )

                # Guard + normalize + search-friendly + fallback ไม่ให้ว่าง
                guarded = []
                for j in range(len(part)):
                    cand = (
                        df_part.get("clean_med_product", [""])[j]
                        if "clean_med_product" in df_part.columns
                        else ""
                    )
                    kept = guard_monotone_delete(
                        cand, part["_pre_for_model"].values[j], part[key_col].values[j]
                    )
                    if (cand or "").strip() and kept == fallback_lower(
                        part[key_col].values[j]
                    ):
                        rejected_by_guard += 1
                    guarded.append(kept)

                cleaned_ready = [
                    make_search_friendly(
                        normalize_clean_name(c, part[key_col].values[j]),
                        part[key_col].values[j],
                    )
                    for j, c in enumerate(guarded)
                ]
                df_part["clean_med_product"] = cleaned_ready
                append_cache_clean(
                    cache_dir, key_col, df_part[[key_col, "clean_med_product"]]
                )
            except Exception:
                had_error = True
                with open(error_log_path, "a", encoding="utf-8") as f:
                    f.write(
                        f"\n--- {dt.datetime.now().isoformat()} | Error at chunk {i}-{i + args.chunk} ---\n"
                    )
                    f.write(traceback.format_exc())
                print(f"Error logged at {error_log_path}")
            finally:
                pbar.update(len(part))
                time.sleep(0.25)

    # 5) รวม clean + กัน NaN
    final_map = load_cache_clean(cache_dir, key_col)
    df_out = df_in.merge(final_map, on=key_col, how="left")
    mask = (
        df_out["clean_med_product"].isna()
        | df_out["clean_med_product"].astype(str).str.strip().eq("")
        | df_out["clean_med_product"].astype(str).str.lower().eq("nan")
    )
    df_out.loc[mask, "clean_med_product"] = df_out.loc[mask, key_col].apply(
        fallback_lower
    )

    # 6) เซฟผลลัพธ์ + manifest
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"cleaned_{args.version}.csv")
    df_out.to_csv(out_csv, index=False)
    manifest = {
        "version": args.version,
        "input": os.path.abspath(args.input),
        "output_csv": os.path.abspath(out_csv),
        "rows_in": int(len(df_in)),
        "rows_unique": int(df_in[key_col].nunique()),
        "model": MODEL,
        "text_col": args.text_col,
        "chunk": args.chunk,
        "wire_format": "pre-clean→TSV-to-model; CSV final",
        "generated_at": dt.datetime.now().isoformat(),
        "guard_rejected": int(rejected_by_guard),
        "notes": "LLM minimal clean; guard; search-friendly; fallback to lowercase original (no NaN).",
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\nSaved CSV  → {out_csv}")
    print(f"Manifest   → {os.path.join(out_dir, 'manifest.json')}")
    print(f"Guard rejections: {rejected_by_guard:,}")

    # 7) ล้าง cache ถ้าไม่เก็บ
    if not args.keep_cache:
        shutil.rmtree(cache_dir, ignore_errors=True)
        print("Clean cache cleared.")

    # 8) ถ้าต้องการ chain ไป RxNav ต่อทันที
    if args.run_rxnav:
        cmd = [
            sys.executable,
            "rxnav_lookup.py",
            "--version",
            args.version,
            "--rx-concurrency",
            str(getattr(args, "rx_concurrency", 20)),
        ]
        print("\n→ Chaining to RxNav:", " ".join(cmd))
        subprocess.run(cmd, check=False)


if __name__ == "__main__":
    main()

# python clean_drug_gpt5.py \
#   --input ./pediatric_drugs.csv \
#   --version V4 \
#   --text-col medicinal_product \
#   --chunk 300 \
#   --run-rxnav \
#   --rx-concurrency 20
