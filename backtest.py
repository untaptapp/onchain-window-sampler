#!/usr/bin/env python3
"""Minute-resolution backtest engine for the trending front-run thesis.

This is the file that turns collected data into a decision. It exists because every earlier
number had one of four defects, each of which is corrected here:

  1. COARSE EXITS. Snapshot feeds sample at 5/15/30 min, so a 20% trailing stop "fired" on 6% of
     paths and every exit statistic was a hold-return in disguise. Here every rule is simulated on
     MINUTE bars using intra-bar HIGH and LOW — the prices a stop or take-profit actually touches.
     Within a bar we assume the LOW is hit before the HIGH (the conservative ordering: stops fill
     before targets), so results are pessimistic rather than flattering.
  2. IGNORED EXECUTION COST. Returns are net of MEASURED Jupiter cost, applied per leg at the
     liquidity prevailing AT THAT MOMENT (liquidity drains as price falls, so a loser's exit is
     dearer than its entry).
  3. WRONG DENOMINATION. Feeds price tokens in USD, so a raw return bundles a SOL/USD bet we never
     took. Everything here is SOL-denominated — the P&L a SOL-funded trade actually realises.
  4. IN-SAMPLE SELECTION. ~20 filters were screened and the best reported, with no correction. Here
     every filter is scored on a TIME-SPLIT holdout, and the in-sample winner's holdout result is
     reported alongside a multiple-testing adjustment, so a filter has to survive being chosen.

Env: SUPABASE_URL, SUPABASE_KEY. SIZE_USD (default 500), SPLIT (default 0.5 = train fraction).
"""
import bisect, json, math, os, statistics as st, time, urllib.request, urllib.error
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
SIZE_USD = float(os.environ.get("SIZE_USD", "500"))
SPLIT = float(os.environ.get("SPLIT", "0.5"))
# Max minutes between the intended entry and the first bar we actually price at. Bars have gaps
# (an illiquid token simply doesn't trade every minute), and taking "the first bar at or after t0"
# regardless of distance silently priced 10% of entries more than an HOUR late — worst case 10.7h.
# A missing bar also means there was no trading, so there was no fill to be had: these are not
# tradeable entries and must be dropped, not repriced. The drop rate is reported, since excluding
# untradeable names is itself a survivorship choice.
ENTRY_TOL_MIN = float(os.environ.get("ENTRY_TOL_MIN", "5"))
_S = defaultdict(int)          # fallback counters — every silent path must be countable


def sb_all(path, page=1000, cap=800000):
    out = []
    while len(out) < cap:
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
             "Range-Unit": "items", "Range": f"{len(out)}-{len(out) + page - 1}"}
        try:
            with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h), timeout=120) as r:
                t = r.read(); chunk = json.loads(t) if t else []
        except urllib.error.HTTPError as e:
            if e.code == 416: break
            raise
        if not chunk: break
        out += chunk
        if len(chunk) < page: break
    if len(out) >= cap:
        print(f"!! sb_all cap reached ({cap}) for {path[:70]} — RESULT IS TRUNCATED, raise cap", flush=True)
    return out


# ---- execution cost: measured Jupiter curve, cost% = a * size_usd^b per TVL band ----------
BANDS = ((0, 15000), (15000, 50000), (50000, 200000), (200000, 1e6), (1e6, 9e12))
FIT = {0: (0.112, 0.77), 1: (0.937, 0.29), 2: (0.901, 0.17), 3: (0.168, 0.22), 4: (0.039, 0.87)}


def cost(tvl, usd=None, stats=None):
    usd = SIZE_USD if usd is None else usd
    if not tvl or tvl <= 0:
        # A missing TVL silently applied a 50% round-trip. It fires on 0% of the current sample —
        # precisely when to make a landmine visible, rather than after it starts firing.
        if stats is not None: stats["cost_fallback"] += 1
        return 0.50
    i = next((k for k, (lo, hi) in enumerate(BANDS) if lo <= tvl < hi), 4)
    a, b = FIT[i]
    return min(a * (usd ** b), 95.0) / 100.0


# ---- SOL/USD ------------------------------------------------------------------------------
def load_sol():
    s = [(r["ts"], r["price"]) for r in sb_all("/sol_usd_ref?select=ts,price&order=ts.asc")]
    def at(ts):
        if not s: return None
        i = bisect.bisect_left(s, (ts, -1)); c = []
        if i < len(s): c.append(s[i])
        if i > 0: c.append(s[i - 1])
        b = min(c, key=lambda z: abs(z[0] - ts))
        return b[1] if abs(b[0] - ts) <= 3600 else None
    return at


# ---- exit rules: all simulated on minute bars with intra-bar high/low ----------------------
def simulate(bars, rule):
    """bars: [(ts,o,h,l,c)] from entry. Returns (gross_return_usd_terms, exit_ts).
    Within a bar the LOW is assumed hit before the HIGH — stops fill before targets."""
    if len(bars) < 2:
        return None, None
    p0 = bars[0][4]
    if not p0 or p0 <= 0:
        return None, None
    kind = rule["kind"]
    peak = p0
    banked = 0.0
    frac_left = 1.0
    for (ts, o, h, l, c) in bars[1:]:
        if kind == "stop_tp":
            if l <= p0 * (1 - rule["sl"]):
                return -rule["sl"], ts
            if h >= p0 * (1 + rule["tp"]):
                return rule["tp"], ts
        elif kind == "trail":
            w = rule["w"]
            if l <= peak * (1 - w):
                return max(l, peak * (1 - w)) / p0 - 1, ts
            peak = max(peak, h)
        elif kind == "stop_trail":
            if l <= p0 * (1 - rule["sl"]):
                return -rule["sl"], ts
            if peak > p0 * (1 + rule["arm"]) and l <= peak * (1 - rule["w"]):
                return max(l, peak * (1 - rule["w"])) / p0 - 1, ts
            peak = max(peak, h)
        elif kind == "partial":
            # bank `frac` at +tp, trail the remainder; hard stop until the TP arms
            if frac_left == 1.0 and l <= p0 * (1 - rule["sl"]):
                return -rule["sl"], ts
            if frac_left == 1.0 and h >= p0 * (1 + rule["tp"]):
                banked = rule["frac"] * rule["tp"]
                frac_left = 1 - rule["frac"]
                peak = max(peak, p0 * (1 + rule["tp"]))
                continue
            if frac_left < 1.0:
                if l <= peak * (1 - rule["w"]):
                    return banked + frac_left * (max(l, peak * (1 - rule["w"])) / p0 - 1), ts
                peak = max(peak, h)
        elif kind == "time":
            if ts - bars[0][0] >= rule["mins"] * 60:
                return c / p0 - 1, ts
            if rule.get("sl") and l <= p0 * (1 - rule["sl"]):
                return -rule["sl"], ts
    last = bars[-1]
    r = last[4] / p0 - 1
    if kind == "partial" and frac_left < 1.0:
        r = banked + frac_left * r
    return r, last[0]


RULES = [
    ("hold 60m",                   {"kind": "time", "mins": 60}),
    ("hold 120m",                  {"kind": "time", "mins": 120}),
    ("hold 120m + 30% stop",       {"kind": "time", "mins": 120, "sl": 0.30}),
    ("stop30 / tp30",              {"kind": "stop_tp", "sl": 0.30, "tp": 0.30}),
    ("stop30 / tp50",              {"kind": "stop_tp", "sl": 0.30, "tp": 0.50}),
    ("stop30 / tp100",             {"kind": "stop_tp", "sl": 0.30, "tp": 1.00}),
    ("trail 15%",                  {"kind": "trail", "w": 0.15}),
    ("trail 30%",                  {"kind": "trail", "w": 0.30}),
    ("trail 50%",                  {"kind": "trail", "w": 0.50}),
    ("stop30 + trail30 armed+20",  {"kind": "stop_trail", "sl": 0.30, "w": 0.30, "arm": 0.20}),
    ("stop30 + trail40 armed+30",  {"kind": "stop_trail", "sl": 0.30, "w": 0.40, "arm": 0.30}),
    ("TP50%@+40, trail40, stop30", {"kind": "partial", "tp": 0.40, "frac": 0.5, "w": 0.40, "sl": 0.30}),
    ("TP50%@+80, trail40, stop30", {"kind": "partial", "tp": 0.80, "frac": 0.5, "w": 0.40, "sl": 0.30}),
]


def ex1(v):
    s = sorted(v)
    return st.mean(s[:-1]) if len(s) > 1 else (s[0] if s else None)


def geo(v, f):
    """Per-trade geometric (compounded) growth at fixed fraction f of bankroll."""
    if not v:
        return 0.0
    g = 0.0
    for x in v:
        if 1 + f * x <= 1e-9:
            return -1.0
        g += math.log(1 + f * x)
    return math.exp(g / len(v)) - 1


def boot_p(v, n=2000):
    """One-sided bootstrap: P(mean <= 0). Small n and fat tails make the t-test useless here."""
    if len(v) < 5:
        return None
    import random
    random.seed(11)
    hits = 0
    for _ in range(n):
        s = [v[random.randrange(len(v))] for _ in range(len(v))]
        if st.mean(s) <= 0:
            hits += 1
    return hits / n


def ff(v):
    try: return float(v)
    except (TypeError, ValueError): return None


def load_entries(solat):
    """One entry per (source, mint): board features at first sighting + minute bars after it."""
    bars_raw = sb_all("/trending_bars?select=mint,ts,o,h,l,c&order=ts.asc")
    bars = defaultdict(list)
    for b in bars_raw:
        bars[b["mint"]].append((b["ts"], b["o"], b["h"], b["l"], b["c"]))
    for m in bars:
        bars[m].sort()
    print(f"bars: {len(bars_raw):,} rows across {len(bars):,} mints", flush=True)
    ents = []
    dropped = defaultdict(int)
    for src in ("gmgn", "solanatracker", "geckoterminal"):
        rows = sb_all(f"/trending_snapshots?source=eq.{src}"
                      "&select=mint,captured_at,rank,price,market_cap,liquidity,extra&order=captured_at.asc")
        seen = {}
        liqser = defaultdict(list)
        for r in rows:
            liqser[r["mint"]].append((r["captured_at"] / 1000, r.get("liquidity")))
            if r["mint"] not in seen:
                seen[r["mint"]] = r
        for m, r in seen.items():
            bb = bars.get(m)
            if not bb:
                continue
            t0 = r["captured_at"] / 1000
            seg = [b for b in bb if b[0] >= t0 - 60]
            if len(seg) < 20:            # need a real path, not a stub
                continue
            if (seg[0][0] - t0) / 60 > ENTRY_TOL_MIN:
                dropped["stale_entry_bar"] += 1      # no bar near t0 => no fill was available
                continue
            ex = r.get("extra") or {}
            if src == "gmgn":
                b_, s_ = ex.get("buys"), ex.get("sells")
                f = {"buyskew": (b_ / (b_ + s_)) if (b_ and s_ is not None and b_ + s_ > 0) else None,
                     "p5m": ff(ex.get("price_change_percent5m")), "bundle": ff(ex.get("bundler_rate")),
                     "rug": ff(ex.get("rug_ratio")), "smart": ff(ex.get("smart_degen_count")),
                     "renown": ff(ex.get("renowned_count")), "top10": ff(ex.get("top_10_holder_rate"))}
            elif src == "solanatracker":
                pt = ex.get("pool_txns") or {}; b_, s_ = pt.get("buys"), pt.get("sells")
                evs = ex.get("events") or {}
                f = {"buyskew": (b_ / (b_ + s_)) if (b_ and s_ is not None and b_ + s_ > 0) else None,
                     "p5m": ff((evs.get("5m") or {}).get("priceChangePercentage")),
                     "bundle": None, "rug": None, "smart": None, "renown": None, "top10": None}
            else:
                t = (ex.get("txns") or {}).get("h1") or {}
                b_, s_ = t.get("buys"), t.get("sells")
                f = {"buyskew": (b_ / (b_ + s_)) if (b_ and s_ is not None and b_ + s_ > 0) else None,
                     "p5m": ff((ex.get("pchg") or {}).get("m5")),
                     "bundle": None, "rug": None, "smart": None, "renown": None, "top10": None}
            ls = sorted(liqser[m])
            ents.append({"src": src, "mint": m, "t0": t0, "bars": seg, "rank": r.get("rank"),
                         "mcap": r.get("market_cap"), "liq": r.get("liquidity"), "liqser": ls, **f})
    if dropped:
        tot = len(ents) + sum(dropped.values())
        print(f"dropped {sum(dropped.values())}/{tot} entries "
              f"({sum(dropped.values())/max(1,tot)*100:.1f}%): {dict(dropped)}", flush=True)
    return ents


LIQ_TOL_MIN = float(os.environ.get("LIQ_TOL_MIN", "45"))


def liq_at(ls, ts, stats=None):
    """Liquidity prevailing at `ts`, from the most recent observation AT OR BEFORE it.

    Replaces two defects. (a) Picking the *nearest* reading could select one from AFTER the exit —
    look-ahead, pricing a fill with liquidity we could not have known. (b) Nearest-without-tolerance
    costed 31% of exits with a reading >30 min away (14% >2h, worst 12h), defeating the point of
    point-in-time costing: liquidity drains exactly when price falls, so a stale reading
    systematically under-prices the exit leg of a loser.

    Falls back to the last known value when nothing is recent enough, and COUNTS it — an invisible
    fallback is how the first version of this survived."""
    prev = None
    for t, v in ls:
        if v is None or t > ts:
            continue
        if prev is None or t > prev[0]:
            prev = (t, v)
    if prev is None:
        if stats is not None: stats["liq_none"] += 1
        return None
    if (ts - prev[0]) / 60 > LIQ_TOL_MIN:
        if stats is not None: stats["liq_stale"] += 1
    return prev[1]


def gross_return(e, rule, solat):
    g, xts = simulate(e["bars"], rule)
    if g is None:
        return None
    a, b = solat(e["t0"]), solat(xts)
    return ((1 + g) * (a / b) - 1) if (a and b) else g


def net_return(e, rule, solat):
    g, xts = simulate(e["bars"], rule)
    if g is None:
        return None
    a, b = solat(e["t0"]), solat(xts)
    if not (a and b):
        _S["sol_missing"] += 1     # returning a USD number here mixes denominations in one average
    r = ((1 + g) * (a / b) - 1) if (a and b) else g      # SOL-denominated
    c_in = cost(e["liq"], stats=_S)
    c_out = cost(liq_at(e["liqser"], xts, stats=_S) or e["liq"], stats=_S)
    return r - c_in - c_out


FILTERS = [
    ("ALL",                       lambda e: True),
    ("buyskew>=0.6",              lambda e: (e.get("buyskew") or 0) >= 0.6),
    ("buyskew>=0.7",              lambda e: (e.get("buyskew") or 0) >= 0.7),
    ("liq>=$50k",                 lambda e: (e["liq"] or 0) >= 5e4),
    ("liq>=$200k",                lambda e: (e["liq"] or 0) >= 2e5),
    ("buyskew>=0.6 & liq>=$50k",  lambda e: (e.get("buyskew") or 0) >= 0.6 and (e["liq"] or 0) >= 5e4),
    ("buyskew>=0.7 & liq>=$50k",  lambda e: (e.get("buyskew") or 0) >= 0.7 and (e["liq"] or 0) >= 5e4),
    ("mcap<150k",                 lambda e: (e["mcap"] or 0) < 1.5e5),
    ("rank>15",                   lambda e: (e["rank"] or 0) > 15),
    ("not_spiked(p5m<=0)",        lambda e: e.get("p5m") is not None and e["p5m"] <= 0),
    ("rug<=0.1",                  lambda e: e.get("rug") is not None and e["rug"] <= 0.1),
    ("bundle<0.1",               lambda e: e.get("bundle") is not None and e["bundle"] < 0.1),
]


def main():
    solat = load_sol()
    ents = load_entries(solat)
    if os.environ.get("DEDUP", "1") == "1":
        by = {}
        for e in ents:
            if e["mint"] not in by or e["t0"] < by[e["mint"]]["t0"]:
                by[e["mint"]] = e
        if len(by) < len(ents):
            print(f"dedup: {len(ents)} feed-entries -> {len(by)} unique mints "
                  f"({len(ents)-len(by)} duplicate observations of the same token dropped); "
                  f"a mint is attributed to the feed that saw it FIRST")
        ents = list(by.values())
    if not ents:
        print("no entries with minute bars yet — run trending_bars.py first"); return
    ents.sort(key=lambda e: e["t0"])
    print(f"\n{len(ents)} entries with minute paths · size ${SIZE_USD:,.0f} · "
          f"SOL-denominated, net of measured cost")

    for src in ("gmgn", "solanatracker", "geckoterminal"):
        S = [e for e in ents if e["src"] == src]
        if len(S) < 20:
            print(f"\n=== {src}: only {len(S)} entries, skipping ==="); continue
        print(f"\n{'='*104}\n{src.upper()}  n={len(S)}\n{'='*104}")
        # split FIRST: the exit rule must be chosen without the holdout in scope, or the
        # "out-of-sample" number is contaminated by the selection that produced it
        S.sort(key=lambda e: e["t0"])
        uniq = sorted({e["t0"] for e in S})
        cut = (min(uniq, key=lambda t: abs(sum(1 for e in S if e["t0"] < t) - len(S) * SPLIT))
               if len(uniq) > 1 else uniq[0])
        TR = [e for e in S if e["t0"] < cut]; HO = [e for e in S if e["t0"] >= cut]
        print(f"  time split {time.strftime('%m-%d %H:%M', time.gmtime(cut))} "
              f"(train {len(TR)} / holdout {len(HO)})")
        # --- exit-rule sweep, reported on TRAIN (selection set) ---
        print(f"  {'exit rule':<30} {'n':>4} {'GROSS':>8} {'mean':>8} {'median':>8} {'geo@10%':>9} {'win%':>6} {'P(mean<=0)':>11}")
        best = None
        for nm, rule in RULES:
            v = [x for x in (net_return(e, rule, solat) for e in TR) if x is not None]
            if len(v) < 10: continue
            p = boot_p(v)
            gr = [x for x in (gross_return(e, rule, solat) for e in TR) if x is not None]
            print(f"  {nm:<30} {len(v):>4} {st.mean(gr)*100:+7.1f}% {st.mean(v)*100:+7.1f}% {st.median(v)*100:+7.1f}% "
                  f"{geo(v,0.10)*100:+8.2f}% {sum(x>0 for x in v)/len(v)*100:5.0f}% {p if p is not None else float('nan'):11.3f}")
            if best is None or st.mean(v) > best[1]:
                best = (nm, st.mean(v), rule)
        if not best: continue
        print(f"\n  best exit rule ON TRAIN: {best[0]}  -> now applied unchanged to the holdout")
        vho = [x for x in (net_return(e, best[2], solat) for e in HO) if x is not None]
        if len(vho) >= 5:
            print(f"    holdout with that rule: mean {st.mean(vho)*100:+.1f}%  median {st.median(vho)*100:+.1f}%  "
                  f"win {sum(x>0 for x in vho)/len(vho)*100:.0f}%  geo@f=10% {geo(vho,0.10)*100:+.2f}%/trade")
        # --- filter screen, TRAIN vs HOLDOUT, with the chosen exit ---
        rule = best[2]
        tr, ho = TR, HO
        print(f"\n  {'filter':<28} | {'TRAIN n':>8} {'mean':>8} {'ex1':>8} | {'HOLDOUT n':>10} {'mean':>8} {'ex1':>8} {'win%':>6}")
        rows = []
        for nm, pred in FILTERS:
            a = [x for x in (net_return(e, rule, solat) for e in tr if pred(e)) if x is not None]
            b = [x for x in (net_return(e, rule, solat) for e in ho if pred(e)) if x is not None]
            if len(a) < 4 or len(b) < 4:
                continue
            rows.append((nm, a, b))
            print(f"  {nm:<28} | {len(a):>8} {st.mean(a)*100:+7.1f}% {ex1(a)*100:+7.1f}% | "
                  f"{len(b):>10} {st.mean(b)*100:+7.1f}% {ex1(b)*100:+7.1f}% {sum(x>0 for x in b)/len(b)*100:5.0f}%")
        if rows:
            win = max(rows, key=lambda r: st.mean(r[1]))
            k = len(rows)
            p_raw = boot_p(win[2])
            print(f"\n  IN-SAMPLE WINNER: {win[0]}  (train {st.mean(win[1])*100:+.1f}%)")
            print(f"    -> HOLDOUT      : {st.mean(win[2])*100:+.1f}%  ex-top1 {ex1(win[2])*100:+.1f}%  n={len(win[2])}")
            if p_raw is not None:
                print(f"    -> holdout P(mean<=0) = {p_raw:.3f};  Bonferroni over {k} filters: "
                      f"{min(1.0, p_raw*k):.3f} {'(SURVIVES)' if p_raw*k < 0.05 else '(NOT significant)'}")


if __name__ == "__main__":
    main()
    print(f"\nfallbacks fired: {dict(_S) or 'none'}  "
          f"(liq_stale = exit costed with liquidity older than {LIQ_TOL_MIN:.0f} min)")
