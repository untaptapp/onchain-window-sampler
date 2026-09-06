#!/usr/bin/env python3
"""SENTINEL for Robinhood path materialisation — the work itself moved to pg_cron.

`materialize_rh_paths()` outgrew PostgREST's 120s statement budget (57014 on every attempt once
trending_bars passed ~3M rows), and batching can't fix it: mints that never get an entry bar stay
in the todo forever, so a newest-first batch fills with them and deadlocks. Since 2026-09-06 the
function runs INSIDE the database as pg_cron job 'materialize-rh-paths' (*/20, with its own
`set statement_timeout='900s'` — a function-level SET cannot extend an already-armed timer).

This script only VERIFIES the cron is alive, per C0f: judge data recency, not run status. It fails
the workflow when paths are stale WHILE work is waiting — max(computed_at) old AND unfrozen recent
mints exist. A quiet board with nothing to do is not a failure.

Env: SUPABASE_URL, SUPABASE_KEY. STALE_MIN (default 120).
"""
import json, os, urllib.request

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
STALE_MIN = int(os.environ.get("STALE_MIN", "120"))


def q(path):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
         "User-Agent": "onchain-window-sampler/1.0"}
    with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h), timeout=120) as r:
        return json.load(r)


def main():
    import time
    now = time.time()
    rows = q("/trending_paths?source=eq.gmgn_rh&select=computed_at&order=computed_at.desc&limit=1")
    if not rows:
        raise SystemExit("no gmgn_rh rows in trending_paths at all — cron never ran")
    from datetime import datetime, timezone
    mx = datetime.fromisoformat(rows[0]["computed_at"].replace("Z", "+00:00")).timestamp()
    age_min = (now - mx) / 60
    print(f"gmgn_rh paths: newest computed_at {age_min:.0f} min ago", flush=True)
    if age_min <= STALE_MIN:
        return
    # Stale — but is there work waiting? A mint first seen 1–13h ago should have a path row by
    # now if it will ever get one (entry bar within tolerance arrives with the bar collector's
    # lag, well under an hour).
    lo = int((now - 13 * 3600) * 1000)
    hi = int((now - 3600) * 1000)
    recent = q(f"/trending_snapshots?source=eq.gmgn_rh&select=mint&captured_at=gte.{lo}"
               f"&captured_at=lte.{hi}&limit=1000")
    mints = sorted({r["mint"] for r in recent})
    if not mints:
        print("stale but no recent board mints — nothing to materialise, OK", flush=True)
        return
    have = set()
    for i in range(0, len(mints), 80):
        sel = ",".join(mints[i:i + 80])
        have |= {r["mint"] for r in q(f"/trending_paths?source=eq.gmgn_rh&select=mint&mint=in.({sel})")}
    missing = len(set(mints) - have)
    print(f"recent mints {len(mints)}, with path row {len(have)}, missing {missing}", flush=True)
    if missing > len(mints) * 0.5:
        raise SystemExit(f"paths {age_min:.0f} min stale with {missing}/{len(mints)} recent mints "
                         "unmaterialised — pg_cron job 'materialize-rh-paths' looks dead "
                         "(select * from cron.job_run_details order by start_time desc)")
    print("stale computed_at but recent mints are covered — OK", flush=True)


if __name__ == "__main__":
    main()
