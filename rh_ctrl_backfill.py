#!/usr/bin/env python3
"""Backfill bars for the rh_tape CONTROL arm, around each control's OWN observation moment.

WHY THIS EXISTS (measured 2026-09-04): of thousands of control lead rows, the number with a
priceable forward return was 5, 3, 2, 0, 0, 0 across the six leads — the false-positive cost of
the revival front-run is UNMEASURABLE from current coverage, and the base-rate estimate (27.4%
of active revival moments reach the board within 60m) is survivorship-inflated by the same hole.
GeckoTerminal OHLCV is backfillable (B3), so fetching these windows NOW retroactively completes
both measurements.

WHY THIS IS NOT THE COLLECTOR'S CONTROL ARM. trending_bars.py samples controls AT RANDOM from the
population at risk, and its comment is right that sampling "most active controls" would bias that
pool. This script does something different: it completes price coverage for a FIXED list of mints
the matching design already selected into rh_tape. The list is closed, the selection happened at
match time, and draining it to zero adds no new selection. It fetches each control's window
around its matched as_of — the birth-anchored window the main collector would fetch is the wrong
window for these rows and is why they were never covered.

WHY A SEPARATE WORKFLOW: GeckoTerminal's keyless limit is per-IP (~5-6 successful req/min,
B-GT-RATE). A separate job runs on its own runner IP with its own budget, taking nothing from the
case-arm collectors the forward test depends on.

Env: SUPABASE_URL, SUPABASE_KEY, GT_NETWORK=robinhood (required), MAX_CALLS, RUN_SECONDS,
     MIN_WALLETS (default 5 — the activity floor below which a control could never be a
     plausible false positive of an activity-gated strategy).
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import trending_bars as TB

assert TB.GT_NETWORK == "robinhood", "set GT_NETWORK=robinhood — this backfill is chain-4663 only"
MIN_WALLETS = int(os.environ.get("MIN_WALLETS", "5"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "0"))
PRE_S, POST_S = 3600, 10800 + 900          # [as_of-1h, as_of+3h(+tolerance)] per tape row


def main():
    t_end = time.time() + RUN_SECONDS if RUN_SECONDS else None
    rows = TB.sb_all("/rh_tape?arm=eq.control&select=mint,as_of,n_wallets,lead_s"
                     "&order=as_of.asc,mint.asc")
    want = {}
    for r in rows:
        m = r["mint"]
        w = want.setdefault(m, [r["as_of"] - PRE_S, r["as_of"] + POST_S, 0, 0])
        w[0] = min(w[0], r["as_of"] - PRE_S)
        w[1] = max(w[1], r["as_of"] + POST_S)
        w[2] = max(w[2], r.get("n_wallets") or 0)
        w[3] = max(w[3], r.get("lead_s") or 0)
    want = {m: w for m, w in want.items() if w[2] >= MIN_WALLETS}
    print(f"{len(rows):,} control rows -> {len(want):,} mints at >= {MIN_WALLETS} wallets", flush=True)

    cov = {r["mint"]: (r["ts_from"], r["ts_to"], r["n_bars"]) for r in
           TB.sb_all("/trending_bar_cov?select=mint,ts_from,ts_to,n_bars&mint=like.0x*")}
    pools = {r["mint"]: r for r in
             TB.sb_all("/trending_pools?select=mint,ok,pool_address&mint=like.0x*")}

    todo = []
    for m, (lo, hi, w, ls) in want.items():
        c = cov.get(m)
        if c and c[0] is not None and c[0] <= lo + 180 and c[1] >= hi - 180:
            continue                                   # window already covered
        p = pools.get(m)
        if p is not None and not p.get("ok"):
            continue                                   # known unresolvable: no GT pool exists
        if p is not None and (p.get("last_fetch_to") or 0) >= hi - 180:
            continue                                   # already asked to the window end
        todo.append((m, lo, hi, w, ls))
    # LONG-LEAD CONTROLS FIRST. The revival front-run's false-positive cost lives in the
    # L>=900 comparators, and wallet-count ordering alone left them starved: after 2,823
    # fetched windows the priced control counts at L=900/1800/3600 were still 2/0/1. A control
    # matched at a long lead outranks any wallet count; within a tier, most active first.
    todo.sort(key=lambda x: (-(x[4] >= 900), -x[3]))
    print(f"{len(todo):,} mints still need their window", flush=True)

    # resolve missing pools in batches of 30 (1 GT call each)
    need_pool = [m for m, *_ in todo if m not in pools]
    for i in range(0, len(need_pool), 30):
        if TB.calls["n"] >= TB.MAX_CALLS or (t_end and time.time() >= t_end):
            break
        got = TB.resolve_pools_batch(need_pool[i:i + 30])
        if got:
            batch = []
            for m, p in got.items():
                p.setdefault("last_fetch_to", None)
                pools[m] = p
                batch.append(p)
            for j in range(0, len(batch), 100):
                TB.sb("POST", "/trending_pools?on_conflict=mint", batch[j:j + 100],
                      prefer="resolution=merge-duplicates,return=minimal")
    print(f"pool resolution done at {TB.calls['n']} calls", flush=True)

    done = failed = 0
    new_bars, new_cov = [], []
    def flush():
        for j in range(0, len(new_bars), 500):
            TB.sb("POST", "/trending_bars?on_conflict=mint,ts", new_bars[j:j + 500],
                  prefer="resolution=merge-duplicates,return=minimal")
        if new_cov:
            TB.sb("POST", "/trending_bar_cov?on_conflict=mint", new_cov,
                  prefer="resolution=merge-duplicates,return=minimal")
    for m, lo, hi, w, ls in todo:
        if TB.calls["n"] >= TB.MAX_CALLS or (t_end and time.time() >= t_end):
            print("  budget reached — stopping cleanly", flush=True)
            break
        p = pools.get(m)
        if not p or not p.get("ok") or not p.get("pool_address"):
            failed += 1
            continue
        bars, landed = TB.fetch_bars(p["pool_address"], lo, hi)
        if not landed:
            continue                                   # never landed — retry next run (B-THROTTLE)
        # Stamp how far we ASKED (landed answers only). Without this, a pool whose bars all sit
        # after the window returns empty-but-landed and requeues at top priority every run — a
        # permanent budget leak. With it, this run's answer retires the mint for good.
        TB.sb("PATCH", f"/trending_pools?mint=eq.{m}", {"last_fetch_to": int(hi)},
              prefer="return=minimal")
        for b in bars:
            new_bars.append({"mint": m, "ts": int(b[0]), "o": b[1], "h": b[2],
                             "l": b[3], "c": b[4], "vol": b[5]})
        c0 = cov.get(m)
        if bars:
            blo, bhi = int(bars[0][0]), int(bars[-1][0])
            merged = (min(blo, c0[0]) if c0 and c0[0] is not None else blo,
                      max(bhi, c0[1]) if c0 and c0[1] is not None else bhi,
                      (c0[2] or 0 if c0 else 0) + len(bars))
            cov[m] = merged
            new_cov.append({"mint": m, "ts_from": merged[0], "ts_to": merged[1],
                            "n_bars": merged[2]})
        done += 1
        if len(new_bars) >= 1000 or len(new_cov) >= 20:
            flush()
            print(f"  .. {done} windows, {len(new_bars)} bars flushed, {TB.calls['n']} calls "
                  f"({TB.calls['no_answer']} never landed)", flush=True)
            new_bars, new_cov = [], []
    flush()
    print(f"backfill pass done: {done} windows fetched, {failed} unresolvable, "
          f"{TB.calls['n']} GT calls ({TB.calls['no_answer']} never landed)", flush=True)


if __name__ == "__main__":
    main()
