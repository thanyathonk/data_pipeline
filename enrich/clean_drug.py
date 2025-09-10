# clean_drug.py
import os, re, argparse, importlib, pandas as pd
from common_utils import read_csv_any, write_csv_gz, shard_of

def rule_clean(name: str) -> str|None:
    if not isinstance(name, str) or not name.strip(): return None
    s = name.lower()
    s = re.sub(r"[^a-z0-9\s\-\+]", " ", s)
    s = re.sub(r"\b(\d+(\.\d+)?(mg|mcg|g|kg|ml|l|iu))\b", " ", s)
    s = re.sub(r"\b(tablet|capsule|suspension|solution|cream|ointment|injection|syrup|tab|cap)\b"," ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def need_llm(cleaned: str|None) -> bool:
    if not cleaned: return True
    # ถ้ายังยาว/หลวม/มี + หลายตัว → ให้ LLM ช่วย
    tokens = cleaned.split()
    return (len(tokens) >= 5) or ("+" in cleaned)

def call_llm(names: list[str], module: str):
    mod = importlib.import_module(module)
    if not hasattr(mod, "llm_clean"):
        raise SystemExit(f"{module}.llm_clean(list)->list not found")
    return mod.llm_clean(names)

def main(in_dict, out_clean, use_llm=False, llm_module="clean_drug_gpt5", shards=1, shard_id=0):
    df = read_csv_any(in_dict)[["medicinal_product","product_key"]].drop_duplicates()
    if shards > 1:
        df = df[df["product_key"].astype(str).apply(lambda x: shard_of(x, shards)==int(shard_id))]

    rb = df["medicinal_product"].astype(str).map(rule_clean)
    out = df.copy()
    out["clean_name"] = rb

    if use_llm:
        mask = out["clean_name"].map(need_llm)
        to_fix = out.loc[mask, "medicinal_product"].astype(str).tolist()
        if to_fix:
            llm_results = call_llm(to_fix, llm_module)
            out.loc[mask, "clean_name"] = llm_results

    write_csv_gz(out, out_clean)
    print(f"[ok] wrote {out_clean} rows={len(out)} (shard {shard_id}/{shards})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-drug-dict", required=True)
    ap.add_argument("--out-drug-clean", required=True)
    ap.add_argument("--use-llm", action="store_true")
    ap.add_argument("--llm-module", default="clean_drug_gpt5")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--shard-id", type=int, default=0)
    args = ap.parse_args()
    main(args.in_drug_dict, args.out_drug_clean, args.use_llm, args.llm_module, args.shards, args.shard_id)
