#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import hashlib
import sqlite3
import time
from typing import Iterable
import re
import json
import pandas as pd

# ---------- FS helpers ----------
def ensure_dir(path: str) -> str:
    """
    Ensure parent directory exists for a file path, or create the dir itself
    if 'path' is a directory path.
    """
    # if path looks like a file (has extension), ensure its parent
    base, ext = os.path.splitext(path)
    d = path if ext == "" else os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    return path

# ---------- Small utils ----------
def to_str_sid(x) -> str | None:
    """Normalize safetyreportid to string (keep None as None)."""
    if x is None:
        return None
    try:
        return str(int(str(x).strip()))
    except Exception:
        return str(x).strip()

def slug_key(s: str | None) -> str | None:
    """
    Lowercase, remove non [a-z0-9 ] characters, collapse spaces.
    (ใช้สำหรับทำคีย์แบบหลวม ๆ)
    """
    if s is None:
        return None
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)  # ← แก้ quote เกินที่นี่
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ---------- IO ----------
def read_csv_any(path: str, **kwargs) -> pd.DataFrame | pd.io.parsers.TextFileReader:
    """
    Thin wrapper over pandas.read_csv with low_memory=False by default.
    Pass chunksize=... เพื่ออ่านเป็น iterator
    """
    kwargs.setdefault("low_memory", False)
    return pd.read_csv(path, **kwargs)

def write_csv_gz(df: pd.DataFrame, path: str) -> None:
    """Write CSV (gzip if .gz/.gzip)."""
    ensure_dir(path)
    comp = "gzip" if path.endswith((".gz", ".gzip")) else None
    df.to_csv(path, index=False, compression=comp)

def dump_json(obj, path: str) -> None:
    """Write JSON UTF-8 with indent."""
    ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# ---------- Sharding ----------
def shard_of(key: str | int, shards: int) -> int:
    """
    Stable shard assignment in [0, shards).
    Uses md5 on the stringified key to avoid Python's randomized hash.
    """
    s = str(key).encode("utf-8", errors="ignore")
    h = hashlib.md5(s).hexdigest()
    return int(h[:8], 16) % int(shards)


# ---------- Rate limiter ----------
class RateLimiter:
    """Simple wall-clock rate limiter (qps = queries per second)."""

    def __init__(self, qps: float = 0.0):
        self.qps = float(qps)
        self.min_interval = 1.0 / self.qps if self.qps > 0 else 0.0
        self._next = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        if self._next > now:
            time.sleep(self._next - now)
            now = time.monotonic()
        self._next = max(self._next, now) + self.min_interval


# ---------- RxCache (sqlite) ----------
class RxCache:
    """
    Lightweight SQLite-backed cache for RxNav lookups.
    Tables:
      - rxcui_by_name(name TEXT PRIMARY KEY, rxcui TEXT)
      - ing_by_rxcui(rxcui TEXT PRIMARY KEY, inn TEXT, inn_rxcui TEXT)
    """

    def __init__(self, path: str = "data/enrich/rxnav_cache.sqlite"):
        ensure_dir(path)
        self.conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._init_db()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rxcui_by_name (
                name TEXT PRIMARY KEY,
                rxcui TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ing_by_rxcui (
                rxcui TEXT PRIMARY KEY,
                inn TEXT,
                inn_rxcui TEXT
            )
            """
        )
        self.conn.commit()

    # ---- rxcui_by_name ----
    def get_rxcui_many(self, names: Iterable[str]) -> dict[str, str | None]:
        names = [str(n) for n in names]
        out: dict[str, str | None] = {}
        if not names:
            return out
        q = "SELECT name, rxcui FROM rxcui_by_name WHERE name IN ({})"
        CH = 900
        for i in range(0, len(names), CH):
            batch = names[i : i + CH]
            cur = self.conn.execute(q.format(",".join(["?"] * len(batch))), batch)
            for name, rxcui in cur.fetchall():
                out[name] = rxcui
        return out

    def upsert_rxcui(self, name: str, rxcui: str | None) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO rxcui_by_name(name, rxcui) VALUES(?, ?)",
            (str(name), None if rxcui is None else str(rxcui)),
        )
        self.conn.commit()

    # ---- ing_by_rxcui ----
    def get_ing_many(self, rxs: Iterable[str]) -> dict[str, tuple[str | None, str | None]]:
        rxs = [str(r) for r in rxs]
        out: dict[str, tuple[str | None, str | None]] = {}
        if not rxs:
            return out
        q = "SELECT rxcui, inn, inn_rxcui FROM ing_by_rxcui WHERE rxcui IN ({})"
        CH = 900
        for i in range(0, len(rxs), CH):
            batch = rxs[i : i + CH]
            cur = self.conn.execute(q.format(",".join(["?"] * len(batch))), batch)
            for rxcui, inn, inn_rx in cur.fetchall():
                out[str(rxcui)] = (inn, inn_rx)
        return out

    def upsert_ing(self, rxcui: str, inn: str | None, inn_rxcui: str | None) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO ing_by_rxcui(rxcui, inn, inn_rxcui) VALUES(?, ?, ?)",
            (str(rxcui), None if inn is None else str(inn), None if inn_rxcui is None else str(inn_rxcui)),
        )
        self.conn.commit()
