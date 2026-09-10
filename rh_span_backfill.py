#!/usr/bin/env python3
"""Backfill minute bars across the FULL TRENDING LIFESPAN of re-trending Robinhood mints.

WHY (measured 2026-09-06): tokens that stay on the board are exactly the ones the collector
starves. The RH bars instance rotates ~9,155 case mints through ~60% of ~1,457 GT calls per
5-hour pass, so a perpetually-open window gets extended roughly once every two days — three
currently-trending example mints had bar coverage ending 60–172 HOURS before their latest
sighting, which excludes every late re-ignition (the GME-class pump) from any backtest.
GeckoTerminal serves minute OHLCV 100+ hours back (verified), so the lifespan is backfillable:
1,402 mints have sighting spans >6h, ~97k window-hours ≈ ~5,900 calls at ~16.6h/call.

Fetches [span start, last_sighting + POST_H] per mint in <=60h segments walked OLDEST-FIRST so
trending_bar_cov's single (ts_from, ts_to) range never claims an interior hole as covered.
Re-runnable on cron: the coverage credit makes each run resume where the last stopped, and a
mint that keeps trending keeps extending its own span — so this doubles as the ongoing
continuous-coverage mechanism for board-resident tokens.

Separate workflow = separate runner IP (B-GT-RATE): takes nothing from the case-arm collector
or the ctrl backfill.

Env: SUPABASE_URL, SUPABASE_KEY, GT_NETWORK=robinhood (required), MAX_CALLS, RUN_SECONDS,
     MIN_SPAN_H (default 6), MAX_SPAN_D (default 14 — cap per-mint cost, C0h).
"""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import trending_bars as TB

assert TB.GT_NETWORK == "robinhood", "set GT_NETWORK=robinhood — this backfill is chain-4663 only"
MIN_SPAN_H = float(os.environ.get("MIN_SPAN_H", "6"))
MAX_SPAN_D = float(os.environ.get("MAX_SPAN_D", "14"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "0"))
SEG_S = 60 * 3600                      # fetch_bars pages 4x1000 minute bars = 66h; stay inside
POST_S = int(TB.POST_H * 3600)


def main():
    t_end = time.time() + RUN_SECONDS if RUN_SECONDS else None
    now = time.time()
    snaps = TB.sb_all("/trending_snapshots?source=eq.gmgn_rh&select=mint,captured_at"
                      "&order=captured_at.asc,mint.asc")
    first, last = {}, {}
    for r in snaps:
        m, t = r["mint"], r["captured_at"] / 1000
        if m not in first: first[m] = t
        last[m] = max(last.get(m, 0), t)
    spans = {m: (first[m], last[m]) for m in first if last[m] - first[m] > MIN_SPAN_H * 3600}
    print(f"{len(first):,} RH board mints, {len(spans):,} with span > {MIN_SPAN_H:g}h", flush=True)

    cov = {r["mint"]: (r["ts_from"], r["ts_to"], r["n_bars"]) for r in
           TB.sb_all("/trending_bar_cov?select=mint,ts_from,ts_to,n_bars&mint=like.0x*")}
    pools = {r["mint"]: r for r in
             TB.sb_all("/trending_pools?select=mint,ok,pool_address,last_fetch_to&mint=like.0x*")}

    todo = []
    for m, (f, l) in spans.items():
        lo = max(f - 3600, l - MAX_SPAN_D * 86400)      # span cap bounds per-mint cost
        hi = min(l + POST_S, now - 120)
        c = cov.get(m)
        # resume AFTER what a contiguous cov range already covers; a gap in front of cov (lo
        # before ts_from) is fetched too, as its own segment, so the merged range stays honest
        start = lo
        if c and c[0] is not None and c[0] <= lo + 600:
            start = max(lo, c[1])
        # Credit the furthest point already REQUESTED (`last_fetch_to`, patched per segment
        # below), not only the furthest bar received. Coverage's ts_to is the last BAR, so a
        # token that stopped trading before its window end has an empty tail that a cov-only
        # start re-requests every run, forever — the same "deficit measured on bars alone never
        # shrinks" trap trending_bars.deficit() documents. Measured 2026-09-09: 658 mints
        # visited per 5h run, ~4 runs/day, and the finished-span backlog still GREW 44.2k ->
        # 57.3k window-hours in 1.3 days; the flip to inactive-first (09-08) could not help
        # because the demand was phantom.
        lf = (pools.get(m) or {}).get("last_fetch_to") or 0
        start = max(start, lf)
        if hi - start < 900:
            continue                                     # nothing meaningful missing
        p = pools.get(m)
        if p is not None and not p.get("ok"):
            continue                                     # no GT pool exists — unresolvable
        active = l > now - 24 * 3600
        todo.append((m, start, hi, active, hi - start))
    # INACTIVE (finished-span) mints first: they are the finite, historical study set the
    # full-coverage re-ignition re-run is waiting on. Active mints' windows grow ~24 wh/day each
    # (~11k wh/day across ~470 of them) and regrow forever — serving them first starved the
    # finished spans completely: total backlog measured GROWING 65.1k -> 70.2k wh over 22h
    # (2026-09-07) with 44.2k wh of finished spans untouched behind 28.3k of live tails. The
    # C0h lesson inverted: an uncapped, self-renewing tier at the queue head starves the finite
    # work. Within a tier, biggest gap first; active tails get the leftovers until a live
    # re-ignition monitor actually exists to need them.
    todo.sort(key=lambda x: (x[3], -x[4]))
    print(f"{len(todo):,} mints need span coverage "
          f"({sum(1 for t in todo if t[3])} active, "
          f"{sum(t[4] for t in todo)/3600:,.0f} window-hours)", flush=True)

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

    done_seg = done_mint = failed = 0
    new_bars, new_cov = [], []
    def flush():
        for j in range(0, len(new_bars), 500):
            TB.sb("POST", "/trending_bars?on_conflict=mint,ts", new_bars[j:j + 500],
                  prefer="resolution=merge-duplicates,return=minimal")
        if new_cov:
            TB.sb("POST", "/trending_bar_cov?on_conflict=mint", new_cov,
                  prefer="resolution=merge-duplicates,return=minimal")
    for m, start, hi, active, gap in todo:
        if TB.calls["n"] >= TB.MAX_CALLS or (t_end and time.time() >= t_end):
            print("  budget reached — stopping cleanly", flush=True)
            break
        p = pools.get(m)
        if not p or not p.get("ok") or not p.get("pool_address"):
            failed += 1
            continue
        seg_lo, advanced = start, False
        while seg_lo < hi:
            if TB.calls["n"] >= TB.MAX_CALLS or (t_end and time.time() >= t_end):
                break
            seg_hi = min(seg_lo + SEG_S, hi)
            bars, landed = TB.fetch_bars(p["pool_address"], seg_lo, seg_hi)
            if not landed:
                break                                    # rate-limited — resume next run
            TB.sb("PATCH", f"/trending_pools?mint=eq.{m}", {"last_fetch_to": int(seg_hi)},
                  prefer="return=minimal")
            for b in bars:
                new_bars.append({"mint": m, "ts": int(b[0]), "o": b[1], "h": b[2],
                                 "l": b[3], "c": b[4], "vol": b[5]})
            c0 = cov.get(m)
            blo = int(bars[0][0]) if bars else int(seg_lo)
            bhi = int(bars[-1][0]) if bars else int(seg_hi)
            merged = (min(blo, c0[0]) if c0 and c0[0] is not None else blo,
                      max(bhi, c0[1]) if c0 and c0[1] is not None else bhi,
                      ((c0[2] or 0) if c0 else 0) + len(bars))
            cov[m] = merged
            new_cov.append({"mint": m, "ts_from": merged[0], "ts_to": merged[1],
                            "n_bars": merged[2]})
            done_seg += 1
            advanced = True
            seg_lo = seg_hi
            if len(new_bars) >= 1500 or len(new_cov) >= 30:
                flush()
                print(f"  .. {done_mint} mints / {done_seg} segments, {len(new_bars)} bars "
                      f"flushed, {TB.calls['n']} calls ({TB.calls['no_answer']} never landed)",
                      flush=True)
                new_bars, new_cov = [], []
        if advanced:
            done_mint += 1
    flush()
    print(f"span backfill pass done: {done_mint} mints / {done_seg} segments, {failed} "
          f"unresolvable, {TB.calls['n']} GT calls ({TB.calls['no_answer']} never landed)",
          flush=True)


if __name__ == "__main__":
    main()
