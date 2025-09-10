# rxnav_enrich.py
import os, time, argparse, requests, hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from common_utils import read_csv_any, write_csv_gz, ensure_dir, RxCache, RateLimiter, shard_of

BASE = "https://rxnav.nlm.nih.gov/REST"
HEAD = {"User-Agent": "faers-pipeline/1.0"}
OFFLINE = str(os.getenv("RXNAV_OFFLINE", "")).lower() in ("1","true","yes","y")
DEMO = False  # will be set from CLI

def _get_json(url, params=None, limiter: RateLimiter|None=None, tries=4, backoff=1.6):
    # Demo mode: จำลองการเรียก API โดยไม่แตะเครือข่าย (เลียนแบบออนไลน์)
    if DEMO:
        return None
    if OFFLINE:
        # Offline mode: skip network calls; behave as if not found
        return None
    for i in range(tries):
        try:
            if limiter: limiter.wait()
            r = requests.get(url, params=params, headers=HEAD, timeout=20)
            if r.status_code == 404: return None
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == tries-1: raise
            time.sleep(backoff*(i+1))

def rxcui_by_name(name: str, limiter: RateLimiter):
    if not name: return None
    if DEMO:
        # แปลงชื่อ -> rxcui แบบกำหนดแน่นอน (จำลองผลลัพธ์ที่เสถียร)
        h = hashlib.md5(name.encode("utf-8")).hexdigest()
        return str(int(h[:8], 16) % 100000000)
    j = _get_json(f"{BASE}/rxcui.json", {"name": name, "search": "2"}, limiter)
    try:
        ids = j.get("idGroup", {}).get("rxnormId")
        if ids: return ids[0]
    except Exception: pass
    j = _get_json(f"{BASE}/approximateTerm.json", {"term": name, "maxEntries": 1}, limiter)
    try:
        cand = j.get("approximateGroup", {}).get("candidate")
        if cand: return str(cand[0]["rxcui"])
    except Exception: pass
    return None

def ingredients_by_rxcui(rxcui: str, limiter: RateLimiter):
    if not rxcui: return (None, None)
    if DEMO:
        # ใช้ rxcui เป็นฐาน สร้าง INN จำลองที่เสถียร
        try:
            n = int(str(rxcui))
        except Exception:
            n = int(hashlib.md5(str(rxcui).encode("utf-8")).hexdigest()[:6], 16)
        inn = f"ING-{n%100000:05d}"
        return inn, str((n*7) % 100000000)
    j = _get_json(f"{BASE}/rxcui/{rxcui}/related.json", {"tty": "IN"}, limiter)
    try:
        for g in j.get("relatedGroup", {}).get("conceptGroup", []) or []:
            for cp in g.get("conceptProperties", []) or []:
                return cp.get("name"), cp.get("rxcui")
    except Exception: pass
    return (None, None)

def main(in_clean, out_rx, cache_db="data/enrich/rxnav_cache.sqlite",
         shards=1, shard_id=0, max_workers=8, qps=4.0, demo=False):
    global DEMO
    DEMO = bool(demo)
    ensure_dir(os.path.dirname(out_rx))
    cache = RxCache(cache_db)
    limiter = RateLimiter(qps=qps)

    src = read_csv_any(in_clean)[["medicinal_product","product_key","clean_name"]].drop_duplicates()
    if shards > 1:
        src = src[src["product_key"].astype(str).apply(lambda x: shard_of(x, shards)==int(shard_id))]

    # 1) resolve rxcui (from cache first)
    names = src["clean_name"].astype(str).tolist()
    cached = cache.get_rxcui_many(names)
    need = [(i,n) for i,n in enumerate(names) if n not in cached]

    def task_name(name):
        return name, rxcui_by_name(name, limiter)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(task_name, n) for _,n in need]
        for f in as_completed(futs):
            n, rid = f.result()
            cache.upsert_rxcui(n, rid)

    # 2) fetch ingredients (from cache first)
    src["rxcui"] = src["clean_name"].map(lambda n: cache.get_rxcui_many([n]).get(n))
    rxs = sorted({r for r in src["rxcui"].astype(str).tolist() if r and r != "nan"})
    ing_cached = cache.get_ing_many(rxs)
    missing = [r for r in rxs if r not in ing_cached]

    def task_ing(r):
        return r, ingredients_by_rxcui(r, limiter)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(task_ing, r) for r in missing]
        for f in as_completed(futs):
            r, (inn, inn_rx) = f.result()
            cache.upsert_ing(r, inn, inn_rx)

    # 3) build output
    src["rxcui"] = src["clean_name"].map(lambda n: cache.get_rxcui_many([n]).get(n))
    src["INN"], src["ingredient_rxcui"] = zip(*[
        (cache.get_ing_many([r]).get(r, (None,None)))
        for r in src["rxcui"].astype(str).tolist()
    ])

    write_csv_gz(src, out_rx)
    print(f"[ok] wrote {out_rx} rows={len(src)} (shard {shard_id}/{shards})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-drug-clean", required=True)
    ap.add_argument("--out-drug-rxnorm", required=True)
    ap.add_argument("--cache-db", default="data/enrich/rxnav_cache.sqlite")
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--shard-id", type=int, default=0)
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--qps", type=float, default=4.0)  # ปรับตามกฎของ API
    ap.add_argument("--demo", action="store_true", help="โหมดจำลองออนไลน์: ไม่เรียก API จริง แต่สร้าง rxcui/INN จำลอง")
    args = ap.parse_args()
    main(args.in_drug_clean, args.out_drug_rxnorm, args.cache_db, args.shards, args.shard_id,
         args.max_workers, args.qps, args.demo)
