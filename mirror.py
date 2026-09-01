#!/usr/bin/env python3
"""LOCAL ANALYTICAL MIRROR — the fix for running analysis against a 1 GB nano.

THE PROBLEM THIS SOLVES
-----------------------
The live Supabase instance (t3a.nano, ~224 MB shared_buffers) can serve the collectors OR big
analytical reads, not both. Every `sb_all` that pulls a 1M-row table (~88 MB) evicts `trending_bars`
from cache, forces the collectors onto throttled disk IO, and drains the burstable CPU credits until
the whole instance queues. Measured 2026-09-01: a one-row indexed read timed out while three
collectors and a backfill ran.

THE FIX
-------
The live DB becomes a WRITE-ONLY ingestion sink. All analysis reads from a local DuckDB file that
this script keeps in sync. DuckDB is columnar and on local disk, so the profiling and backtests get
faster AND stop competing with the collectors for the nano's cache.

HOW IT STAYS GENTLE
-------------------
Incremental by a per-table "sync clock" — the column that reflects when OUR pipeline learned or last
touched a row, NOT the domain event time (A8: a bar's `ts` is the bar's own time, so a backfill of
old bars would be invisible to a `ts > watermark` sync). Each run pulls only rows past the stored
watermark, in small pages with a sleep between them, so even the first seed is a series of tiny
indexed reads that yield CPU to the collectors rather than one table-eviction.

`trending_bars` is special: it has no insert clock. Once `prune_bars_materialised` is on, the LIVE
bars table is a small rolling window, so `mirror.py bars` re-pulls that whole small window each run
and upserts it into the mirror's accumulated history (the mirror keeps history; the live table does
not). The one-time historical SEED (`--seed bars`) pages the full table once, and should be run when
the dashboard CPU is green.

Usage:
    python mirror.py                 # sync every table incrementally (safe to run any time)
    python mirror.py --seed bars     # one-time full crawl of a table (run off-peak)
    python mirror.py --status        # show row counts and watermarks, no DB reads of data

Env: SUPABASE_URL, SUPABASE_KEY, MIRROR_DB (default ~/crypto/mirror.duckdb),
     PAGE (default 1000), SLEEP (default 0.4s between pages), MAX_SECONDS (default 0 = no limit).
"""
import json, os, sys, time, urllib.error, urllib.request

import duckdb

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
DB = os.environ.get("MIRROR_DB", os.path.expanduser("~/crypto/mirror.duckdb"))
PAGE = min(int(os.environ.get("PAGE", "1000")), 1000)
SLEEP = float(os.environ.get("SLEEP", "0.4"))
MAX_SECONDS = int(os.environ.get("MAX_SECONDS", "0"))

# table -> (clock column that advances when we WRITE the row, primary-key columns).
# The clock must be monotonic in OUR ingest order, not the market's event order.
TABLES = {
    "trending_snapshots": ("captured_at", ["mint", "captured_at", "source"]),
    "trending_pools":     ("resolved_at", ["mint"]),
    "rh_launches":        ("first_seen_at", ["mint"]),
    "rh_tape":            ("computed_at", ["mint", "as_of", "window_s"]),
    "pump_launches":      ("created_at", ["mint"]),
    "candidate_universe": ("captured_at", ["pool_address", "captured_at"]),
    "trending_paths":     ("last_bar_ts", ["mint"]),
    # trending_bars is handled specially (no insert clock) — see sync_bars().
}


def http(path, timeout=180):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "User-Agent": "ows-mirror/1.0"}
    req = urllib.request.Request(SB + path, headers=h)
    for a in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 416:
                return []
            if a == 4:
                raise RuntimeError(f"{path}: {e.code} {e.read().decode()[:200]}")
            time.sleep(2 * (a + 1))
        except Exception as e:
            if a == 4:
                raise RuntimeError(f"{path}: {e}")
            time.sleep(2 * (a + 1))


def con():
    c = duckdb.connect(DB)
    c.execute("create table if not exists _mirror_state "
              "(tbl varchar primary key, watermark double, rows bigint, synced_at bigint)")
    return c


def ensure_table(c, tbl, sample):
    """Create the mirror table from a sample row's keys if it does not exist. Everything is stored
    as-is; DuckDB infers types on insert from JSON, and we keep JSON blobs (`extra`) as VARCHAR."""
    if c.execute("select 1 from information_schema.tables where table_name=?", [tbl]).fetchone():
        return
    cols = ", ".join(f'"{k}" {_ddl_type(v)}' for k, v in sample.items())
    c.execute(f'create table "{tbl}" ({cols})')


def _ddl_type(v):
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "bigint"
    if isinstance(v, float):
        return "double"
    if isinstance(v, (dict, list)):
        return "varchar"          # store nested JSON as text; analysis parses on demand
    return "varchar"


def _row(sample, r):
    out = {}
    for k in sample:
        v = r.get(k)
        out[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
    return out


def upsert(c, tbl, keys, rows):
    if not rows:
        return 0
    sample = rows[0]
    cols = list(sample)
    c.execute("create temp table _stage as select * from \"%s\" limit 0" % tbl)
    c.executemany(
        f'insert into _stage ({",".join(chr(34)+k+chr(34) for k in cols)}) '
        f'values ({",".join("?" for _ in cols)})',
        [[ _row(sample, r)[k] for k in cols ] for r in rows])
    keyexpr = " and ".join(f't."{k}"=s."{k}"' for k in keys)
    c.execute(f'delete from "{tbl}" t using _stage s where {keyexpr}')
    c.execute(f'insert into "{tbl}" select * from _stage')
    c.execute("drop table _stage")
    return len(rows)


def sync_table(c, tbl, clock, keys, seed=False, t_end=None):
    """Keyset pagination on (clock, tie-break key). Filtering `clock >= watermark` and tie-breaking
    on the first key column advances past a plateau of rows that share one clock value without deep
    OFFSET (which times out on the nano) and without the risk of an infinite loop. Upsert is
    idempotent, so the small overlap re-pulled at each watermark boundary is harmless."""
    tie = keys[0]
    wm = 0.0 if seed else (c.execute(
        "select watermark from _mirror_state where tbl=?", [tbl]).fetchone() or [0.0])[0] or 0.0
    total, hi, last_tie = 0, wm, ""
    while True:
        if t_end and time.time() > t_end:
            print(f"  {tbl}: time budget reached at {total} rows", flush=True)
            break
        q = (f"/{tbl}?order={clock}.asc,{tie}.asc&limit={PAGE}"
             f"&or=({clock}.gt.{hi},and({clock}.eq.{hi},{tie}.gt.{last_tie}))")
        rows = http(q)
        if not rows:
            break
        ensure_table(c, tbl, rows[0])
        upsert(c, tbl, keys, rows)
        total += len(rows)
        last = rows[-1]
        hi = float(last[clock]) if last.get(clock) is not None else hi
        last_tie = str(last.get(tie, ""))
        time.sleep(SLEEP)
        if len(rows) < PAGE:
            break
    n = c.execute(f'select count(*) from "{tbl}"').fetchone()[0]
    c.execute("insert or replace into _mirror_state values (?,?,?,?)",
              [tbl, hi, n, int(time.time())])
    print(f"  {tbl:20} +{total:>7} pulled, {n:>9,} in mirror, watermark={hi:.0f}", flush=True)
    return total


def sync_bars(c, seed=False, t_end=None):
    """trending_bars has no insert clock. Strategy: mirror keeps ALL history; the live table is a
    small rolling window once pruning is on, so pull it whole (paged, gentle) and upsert on
    (mint, ts). `--seed bars` does the same but is expected to be the large one-time crawl."""
    tbl, keys = "trending_bars", ["mint", "ts"]
    total, off = 0, 0
    # Page by (mint, ts) keyset so we never use deep OFFSET.
    last_mint, last_ts = "", -1
    while True:
        if t_end and time.time() > t_end:
            print(f"  {tbl}: time budget reached at {total} rows", flush=True)
            break
        q = (f"/{tbl}?order=mint.asc,ts.asc&limit={PAGE}"
             f"&or=(mint.gt.{last_mint},and(mint.eq.{last_mint},ts.gt.{last_ts}))")
        rows = http(q)
        if not rows:
            break
        ensure_table(c, tbl, rows[0])
        upsert(c, tbl, keys, rows)
        total += len(rows)
        last_mint, last_ts = rows[-1]["mint"], rows[-1]["ts"]
        time.sleep(SLEEP)
        if len(rows) < PAGE:
            break
    n = c.execute(f'select count(*) from "{tbl}"').fetchone()[0]
    c.execute("insert or replace into _mirror_state values (?,?,?,?)",
              [tbl, last_ts, n, int(time.time())])
    print(f"  {tbl:20} +{total:>7} pulled, {n:>9,} in mirror", flush=True)
    return total


def status():
    c = con()
    rows = c.execute("select tbl, rows, watermark, synced_at from _mirror_state order by tbl").fetchall()
    if not rows:
        print("mirror empty — run `python mirror.py` to seed it")
        return
    print(f"{'table':22}{'rows':>12}{'synced':>20}")
    for tbl, n, _wm, ts in rows:
        age = f"{(time.time()-ts)/60:.0f}m ago" if ts else "never"
        print(f"{tbl:22}{n:>12,}{age:>20}")


def main():
    if "--status" in sys.argv:
        status()
        return
    t_end = time.time() + MAX_SECONDS if MAX_SECONDS else None
    c = con()
    if "--seed" in sys.argv:
        tbl = sys.argv[sys.argv.index("--seed") + 1]
        print(f"SEED {tbl} -> {DB}", flush=True)
        if tbl == "trending_bars":
            sync_bars(c, seed=True, t_end=t_end)
        else:
            clock, keys = TABLES[tbl]
            sync_table(c, tbl, clock, keys, seed=True, t_end=t_end)
        return
    print(f"sync -> {DB}", flush=True)
    for tbl, (clock, keys) in TABLES.items():
        try:
            sync_table(c, tbl, clock, keys, t_end=t_end)
        except Exception as e:
            print(f"  {tbl}: FAILED {e}", flush=True)
    try:
        sync_bars(c, t_end=t_end)
    except Exception as e:
        print(f"  trending_bars: FAILED {e}", flush=True)
    print("done", flush=True)


if __name__ == "__main__":
    main()
