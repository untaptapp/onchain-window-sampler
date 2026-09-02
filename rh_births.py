#!/usr/bin/env python3
"""TRUE BIRTHS for board tokens the launch scan never saw.

WHY THIS EXISTS
---------------
`rh_launches` is built by watching a fixed set of launchpad factories, forward from the day the
scan started. Two populations therefore have no usable birth:

  * tokens launched BEFORE the scan window (3.78 days deep as of 2026-09-02), and
  * tokens launched on a contract not in WATCH, or seen only via the shared-AMM `pair` extractor,
    whose timestamp is a pool-creation UPPER BOUND rather than a birth (A7d).

Together that is **1,830 of 4,199 board mints (43.6%)**. Every one of them is excluded from age
matching and from lead generation, so they contribute nothing to the case/control contrast.

They are also not a random 43.6%. A token is undated mostly BECAUSE it is old, so the undated set is
overwhelmingly the REVIVAL cohort — a token launched weeks ago that gets woken and trends again,
which is Track A's entire thesis. Probed before building this: 3 of 5 sampled undated board mints
were **26, 51 and 57 days old** at board entry. Measuring only the dated population made revivals
look like 2.3% of the board, and that number is an artifact of the scan depth, not a fact about the
chain. The front-run ceiling differs by cohort too: a fresh launch gives a median 5.6 minutes, an
old token gives hours, so leaving these undated silently tested only the fast strategy.

THE METHOD
----------
An ERC-20's birth is its first `Transfer` from the zero address. Filtering on
`topics=[TRANSFER, ZERO_TOPIC]` returns a handful of logs for the whole of chain history, so the
query is cheap even over the full block range and the 10,000-log cap is never in play — measured
1-10 RPC calls per mint, median ~2. RPC logs are permanent, so this is recoverable at any time; it
just has to be asked for.

Rows are written with `birth_kind='mint_event'`, which is authoritative (it IS the birth) and must
be treated exactly like 'launchpad' by anything that dates a token. `prune_rh_launches` already
refuses to delete any launch that reached the board, so inserting a token born 57 days ago is safe
even against a 21-day retention.

Usage:  python rh_births.py [--apply] [--limit N]
Env:    SUPABASE_URL, SUPABASE_KEY, RH_RPC, MAX_CALLS (default 30000)
"""
import os, sys, time

import rh_chain as C

MAX_CALLS = int(os.environ.get("MAX_CALLS", "30000"))
PLACEHOLDER = "0x" + "0" * 40          # factory/topic0 are NOT NULL but meaningless for a mint event


def board_mints():
    snaps = C.sb_all("/trending_snapshots?source=eq.gmgn_rh&select=mint,captured_at"
                     "&order=captured_at.asc,mint.asc")
    first = {}
    for r in snaps:
        first.setdefault(r["mint"].lower(), r["captured_at"] / 1000)
    return first


def existing():
    rows = C.sb_all("/rh_launches?select=mint,birth_kind,creator,symbol,tx_hash"
                    "&order=first_seen_at.asc,mint.asc")
    return {r["mint"].lower(): r for r in rows}


def find_birth(mint, head):
    """Block number of the token's first Transfer from 0x0, or None if it never mints one."""
    lg = C.get_logs({"address": mint, "topics": [C.TRANSFER, C.ZERO_TOPIC],
                     "fromBlock": hex(0), "toBlock": hex(head)})
    if not lg:
        return None
    return min(int(x["blockNumber"], 16) for x in lg)


def main():
    apply_ = "--apply" in sys.argv
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    first = board_mints()
    have = existing()
    need = [m for m in first if (have.get(m) or {}).get("birth_kind") != "launchpad"]
    need.sort(key=lambda m: first[m])
    if limit:
        need = need[:limit]
    print(f"board mints {len(first):,}; needing a true birth {len(need):,} "
          f"({sum(1 for m in need if m in have):,} update / {sum(1 for m in need if m not in have):,} insert)",
          flush=True)
    head, _t, _bt = C.refresh_head()
    rows, miss, fail = [], 0, 0
    t0 = time.time()
    for i, m in enumerate(need):
        if C.calls() >= MAX_CALLS:
            print(f"  call budget {MAX_CALLS} reached at {i}/{len(need)}", flush=True)
            break
        try:
            bn = find_birth(m, head)
        except Exception as ex:
            fail += 1
            if fail <= 3:
                print(f"    {m[:14]}.. {ex!r}", flush=True)
            continue
        if bn is None:
            miss += 1                      # no mint event visible: predeploy, proxy, or bridged in
            continue
        prev = have.get(m) or {}
        rows.append({
            "mint": m, "created_at": C.blk_to_ts(bn), "block_number": bn,
            "launchpad": None, "factory": PLACEHOLDER, "topic0": C.TRANSFER,
            "birth_kind": "mint_event",
            # Preserve what the launch scan already knew; a birth backfill must not erase it.
            "creator": prev.get("creator"), "tx_hash": prev.get("tx_hash"),
            "symbol": prev.get("symbol"),
            "first_seen_at": int(time.time()),
        })
        if apply_ and len(rows) >= 200:
            C.sb_write("/rh_launches?on_conflict=mint", rows)
            print(f"  .. {i + 1}/{len(need)} wrote {len(rows)}, {C.calls()} rpc, "
                  f"{time.time() - t0:.0f}s", flush=True)
            rows = []
        elif not apply_ and len(rows) >= 200:
            break
    if apply_ and rows:
        C.sb_write("/rh_launches?on_conflict=mint", rows)
    ages = sorted((first[r["mint"]] - r["created_at"]) / 3600 for r in rows) if rows else []
    print(f"\nresolved {len(rows):,} births; {miss} had no mint event; {fail} failed; "
          f"{C.calls()} rpc calls in {time.time() - t0:.0f}s", flush=True)
    if ages:
        print(f"  age at board entry (h): p25 {ages[len(ages)//4]:.1f}  p50 {ages[len(ages)//2]:.1f}"
              f"  p75 {ages[3*len(ages)//4]:.1f}  max {ages[-1]:.1f}")
    if not apply_:
        print("\ndry run — pass --apply to write")


if __name__ == "__main__":
    main()
