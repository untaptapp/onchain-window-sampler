#!/usr/bin/env python3
"""Does the edge break out by VENUE — and is our venue label even right at the time we traded?

Two questions, one script.

  1. FEE CORRECTNESS. `backtest.fee()` reads `trending_pools.dex`, which is ONE row per mint,
     resolved once (recently). A token that traded on the pump.fun bonding curve when we entered
     and graduated to PumpSwap afterwards now carries a `pumpswap` label, so we charge it 0.25%/side
     instead of 1%/side. That is a point-in-time violation of exactly the kind the liquidity fix
     already corrected: it prices a past leg with a present-day fact. Worse, the SAME rate is
     applied to both legs, so a trade that entered on the curve and exited on the AMM — the single
     most common trajectory in this dataset — cannot be costed correctly by construction.

     `trending_quotes.route` is the point-in-time fix: Jupiter records the actually-routed venue at
     a timestamp, and 7% of quoted mints show more than one distinct route across their history.

  2. EDGE BY VENUE. Given a correct per-leg venue, does net edge differ by where the trade actually
     executes? Venue is not a free parameter — it is a *tradeable filter*, because we can refuse to
     enter a mint whose live route is the bonding curve.

Env: SUPABASE_URL, SUPABASE_KEY. SIZE_USD, ENTRY_TOL_MIN, LIQ_TOL_MIN as in backtest.py.
"""
import bisect, os, sys, statistics as st
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest as B
import strategy_grid as G

# Per-side fee by routed venue. The bonding curves charge ~1%; the AMMs ~0.25%. A multi-hop route
# pays at EVERY hop, so the route string is split and the fees summed — a 3-hop route through two
# AMMs and a curve is not a 0.25% trade.
VENUE_FEE = {
    "pump.fun": 0.010,          # bonding curve
    "meteora dbc": 0.010,       # dynamic bonding curve
    "launchlab": 0.010,
    "moonshot": 0.010,
    "boop.fun": 0.010,
    "bags": 0.010,
}
HOP_DEFAULT = 0.0025            # established AMMs


def venue_class(route):
    """'Pump.fun Amm,Meteora DLMM' -> ('amm', 0.005). Curve venues are named exactly; the graduated
    'Pump.fun Amm' is a normal AMM and must NOT match the curve key by prefix."""
    if not route:
        return None, None
    hops = [h.strip().lower() for h in route.split(",") if h.strip()]
    if not hops:
        return None, None
    f = sum(VENUE_FEE.get(h, HOP_DEFAULT) for h in hops)
    kind = "curve" if any(h in VENUE_FEE for h in hops) else "amm"
    if len(hops) > 1:
        kind += f"/{len(hops)}hop"
    return kind, f


def load_routes():
    """mint -> sorted [(ts, route)], the venue actually routed at that instant."""
    rows = B.sb_all("/trending_quotes?select=mint,quoted_at,route,ok&order=quoted_at.asc")
    by = defaultdict(list)
    for r in rows:
        if r.get("ok") and r.get("route"):
            by[r["mint"]].append((r["quoted_at"], r["route"]))
    for m in by:
        by[m].sort()
    print(f"routes: {len(rows):,} quotes -> {len(by):,} mints with >=1 routed quote", flush=True)
    return by


ROUTE_TOL_MIN = float(os.environ.get("ROUTE_TOL_MIN", "120"))


def route_at(rs, ts, stats=None):
    """Route in force at `ts`: most recent quote AT OR BEFORE it. Never a later one — that is the
    look-ahead the liquidity fix removed. Falls back forward only when nothing precedes, and counts
    it, because a graduation between the fallback and `ts` would flip the fee the wrong way."""
    if not rs:
        if stats is not None: stats["route_none"] += 1
        return None, "none"
    i = bisect.bisect_right([t for t, _ in rs], ts) - 1
    if i < 0:
        if stats is not None: stats["route_forward"] += 1
        return rs[0][1], "forward"        # earliest known; flagged, not silently trusted
    if (ts - rs[i][0]) / 60 > ROUTE_TOL_MIN:
        if stats is not None: stats["route_stale"] += 1
        return rs[i][1], "stale"
    return rs[i][1], "ok"


# A PRINT IS NOT A MARKET. Below this much traded volume across the holding window there is no
# counterparty to have traded against, and the "return" is a quote artifact rather than a result.
# Measured 2026-09-03: 245 Solana paths (3.7%) hold a window volume under $10, and that bucket
# carries a mean capped_net of +11,216 against a median of -0.30 -- one path returned
# +274,808,400% off three bars totalling $0.40, printing a low of 5.09e-12 and recovering the next
# bar. Every bucket at $1k+ behaves sanely (max 1.6-13.1).
#
# The floor is deliberately CONSERVATIVE -- it rejects only non-markets, not merely thin ones --
# because the right threshold depends on the notional being traded, which is an analysis-time
# choice, not a materialisation-time one. `win_vol` is stored on every path row so a screen can
# apply its own size-appropriate participation limit without re-materialising.
#
# Untradeable prints are scored ZERO, never dropped: dropping them selects on the outcome and is
# survivorship in a new costume.
MIN_WIN_USD = float(os.environ.get("MIN_WIN_USD", "10"))


def window_volume(bars, t0, xts):
    """USD traded across the holding window [t0, xts]. Bars carry vol at element 5.

    RAISES if the bars carry no volume element at all. Defaulting to 0.0 there would put every
    trade below the floor and silently zero the entire book — a plumbing bug presenting as a
    uniform, plausible-looking result. A missing vol on an INDIVIDUAL bar is a real gap in the
    feed and counts as 0; a missing element on EVERY bar is a wiring fault and must be loud."""
    if bars and not any(len(b) > 5 for b in bars):
        raise RuntimeError(
            "bars carry no volume element — backtest.load_entries must select `vol` and append it "
            "as element 5, or the notional floor zeroes every trade silently")
    return sum((b[5] if len(b) > 5 else 0.0) or 0.0 for b in bars if t0 <= b[0] <= xts)


def pit_net_return(e, rule, solat, routes, stats):
    """net return with PER-LEG, POINT-IN-TIME venue fees. Returns (net, entry_kind, exit_kind)."""
    g, xts, closed = B.simulate(e["bars"], rule, e.get("fetched_to"))
    if g is None or not closed:
        if g is not None: stats["open_position"] += 1
        return None, None, None
    if window_volume(e["bars"], e["t0"], xts) < MIN_WIN_USD:
        stats["untradeable_zeroed"] += 1
        return 0.0, "untradeable", "untradeable"
    a, b = solat(e["t0"]), solat(xts)
    r = ((1 + g) * (a / b) - 1) if (a and b) else g          # SOL-denominated
    r_in, q_in = route_at(routes.get(e["mint"]), e["t0"], stats)
    r_out, q_out = route_at(routes.get(e["mint"]), xts, stats)
    k_in, f_in = venue_class(r_in)
    k_out, f_out = venue_class(r_out)
    if f_in is None:
        f_in = B.fee(e["mint"]); k_in = "unquoted"; stats["fee_fallback_in"] += 1
    if f_out is None:
        f_out = B.fee(e["mint"]); k_out = "unquoted"; stats["fee_fallback_out"] += 1
    c_in = B.cost(e["liq"], stats=stats) + f_in
    c_out = B.cost(B.liq_at(e["liqser"], xts, stats=stats) or e["liq"], stats=stats) + f_out
    return r - c_in - c_out, k_in, k_out


def summarize(label, vals):
    if not vals:
        return f"  {label:28s}      n=0"
    n = len(vals)
    return (f"  {label:28s} n={n:4d}  mean {st.mean(vals)*100:+7.1f}%  "
            f"med {st.median(vals)*100:+7.1f}%  win {sum(v>0 for v in vals)/n*100:4.0f}%  "
            f"geo@10% {B.geo(vals, 0.10)*100:+6.2f}%")


def main():
    B.load_dex()
    solat = B.load_sol()
    routes = load_routes()
    ents = G.enrich(B.load_entries(solat))   # age_min + board features
    stats = defaultdict(int)

    # --- 1. how wrong is the static label, at the moment we actually entered? ------------------
    print("\n=== STATIC dex label vs POINT-IN-TIME route at entry ===")
    mism = Counter(); both = 0
    for e in ents:
        r_in, q = route_at(routes.get(e["mint"]), e["t0"])
        k_in, _ = venue_class(r_in)
        if k_in is None:
            continue
        both += 1
        static = B._DEX.get(e["mint"])
        static_kind = "curve" if static in ("pump-fun", "meteora-dbc", "moonshot",
                                            "raydium-launchlab") else "amm" if static else "unknown"
        pit_kind = k_in.split("/")[0]
        mism[(static_kind, pit_kind)] += 1
    print(f"  entries with a point-in-time route: {both}/{len(ents)}")
    for (s, p), c in mism.most_common():
        flag = "   <-- MISLABELLED" if s != p and "unknown" not in (s, p) else ""
        print(f"    static={s:8s} -> actual@entry={p:8s}  {c:5d}{flag}")

    # --- 2. did the venue CHANGE between entry and exit? ----------------------------------------
    # This is the case a single per-mint fee cannot represent: entering on the bonding curve at 1%
    # and exiting post-graduation at 0.25% is a different trade from either leg alone.
    print("\n=== venue at ENTRY -> venue at EXIT (capped rule) ===")
    pairs = Counter()
    for e in ents:
        n, ki, ko = pit_net_return(e, G.CAPPED, solat, routes, stats)
        if n is None or ki is None:
            continue
        pairs[(ki.split("/")[0], ko.split("/")[0])] += 1
    for (i, o), c in pairs.most_common():
        flag = "   <-- MIGRATED MID-TRADE" if i != o and "unquoted" not in (i, o) else ""
        print(f"    {i:9s} -> {o:9s}  {c:5d}{flag}")

    # --- 3. edge by venue, per track ------------------------------------------------------------
    for tname, sel, rule, rname in (
            ("TRACK A (age>=1d)", lambda e: (e.get("age_min") or 0) >= 1440, G.CAPPED, "capped"),
            ("TRACK B (age<1h)",  lambda e: e.get("age_min") is not None and 0 <= e["age_min"] < 60,
             G.RUNNER, "runner")):
        sub = [e for e in ents if sel(e)]
        print(f"\n=== {tname} — {rname} rule — net edge by ENTRY venue "
              f"(point-in-time, per-leg fees) ===")
        by = defaultdict(list); old_by = defaultdict(list)
        for e in sub:
            n, ki, ko = pit_net_return(e, rule, solat, routes, stats)
            if n is None:
                continue
            by[ki or "unquoted"].append(n)
            o = B.net_return(e, rule, solat)          # the STATIC-label number, for comparison
            if o is not None:
                old_by[ki or "unquoted"].append(o)
        for k in sorted(by, key=lambda k: -len(by[k])):
            print(summarize(k, by[k]))
            if old_by.get(k):
                d = (st.mean(by[k]) - st.mean(old_by[k])) * 100
                print(f"  {'':28s}   (static-label mean was {st.mean(old_by[k])*100:+.1f}%, "
                      f"fee correction {d:+.2f}pp)")
        allv = [v for vs in by.values() for v in vs]
        print(summarize("ALL", allv))

    print("\nfallback counters:", dict(stats))


if __name__ == "__main__":
    main()
