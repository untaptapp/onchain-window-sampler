#!/usr/bin/env python3
"""Two-track strategy grid — the systematic slice evaluation.

TWO TRACKS, JUDGED ON DIFFERENT METRICS
---------------------------------------
Age at first trending is bimodal: ~70% of cases are <1h old, but median outcome improves
monotonically with age (-37% fresh -> -5% for >30d). The two populations are different businesses
and must not be scored the same way:

  TRACK A - REVIVAL (age >= 1d).  Judge on MEDIAN and GEOMETRIC growth. A tradeable, compoundable
            edge. Controls are available (GeckoTerminal indexes established tokens), and `burst`
            separates cases from controls at 0.90 AUC here.
  TRACK B - LAUNCH  (age < 1h).   Judge on the TAIL: P(>2x), P(>5x), max, and mean. A negative
            median is EXPECTED and is not a defect — this is the convex 90/10 book. Its problem is
            selectivity: sifting rugs and failed launches out of a huge population.

Every number is SOL-denominated, net of measured Jupiter impact PLUS venue fees (pump.fun's
bonding curve charges ~1%/side vs ~0.25% on the AMMs), exits simulated on minute bars with
intra-bar high/low, and split train/holdout with the rule chosen on train only.

MULTIPLE TESTING: this screens many filters across two tracks and several rules. The holdout column
is the only one that means anything, and even it is optimistic because the filter set was chosen
after looking at earlier results. Treat a slice as a HYPOTHESIS to pre-register, never a finding.

Env: SUPABASE_URL, SUPABASE_KEY. SIZE_USD (100), SPLIT (0.5), MIN_N (12).
"""
import os, sys, statistics as st
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest as B

MIN_N = int(os.environ.get("MIN_N", "12"))
SPLIT = float(os.environ.get("SPLIT", "0.5"))
GRAD = {"pumpswap", "raydium", "raydium-clmm", "meteora", "meteora-damm-v2", "orca", "bags-fm"}
CAPPED = {"kind": "partial", "tp": 0.80, "frac": 0.5, "w": 0.40, "sl": 0.30}
RUNNER = {"kind": "stop_trail", "sl": 0.35, "w": 0.50, "arm": 1.00}


def ff(v):
    try: return float(v)
    except (TypeError, ValueError): return None


def enrich(ents):
    """Attach board features, age and venue to each entry."""
    rows = B.sb_all("/trending_snapshots?source=eq.gmgn"
                    "&select=mint,captured_at,rank,market_cap,liquidity,extra&order=captured_at.asc")
    seen = {}
    for r in rows:
        if r["mint"] not in seen: seen[r["mint"]] = r
    for e in ents:
        r = seen.get(e["mint"])
        x = (r or {}).get("extra") or {}
        ct = x.get("open_timestamp") or x.get("creation_timestamp")
        e["age_min"] = ((r["captured_at"] / 1000 - float(ct)) / 60
                        if (r and ct and 0 <= (r["captured_at"] / 1000 - float(ct)) < 60 * 60 * 24 * 400) else None)
        e["dex"] = B._DEX.get(e["mint"])
        e["grad"] = e["dex"] in GRAD
        b, s = x.get("buys"), x.get("sells")
        e["buyskew"] = (b / (b + s)) if (b and s is not None and b + s > 0) else None
        e["p5m"] = ff(x.get("price_change_percent5m"))
        e["bundle"] = ff(x.get("bundler_rate"))
        e["rug"] = ff(x.get("rug_ratio"))
        e["smart"] = ff(x.get("smart_degen_count"))
        e["renown"] = ff(x.get("renowned_count"))
        e["sniper"] = ff(x.get("sniper_count"))
        e["top10"] = ff(x.get("top_10_holder_rate"))
        e["holders"] = ff(x.get("holder_count"))
        e["wash"] = x.get("is_wash_trading")
        e["rank_"] = (r or {}).get("rank")
    return ents


FILTERS = [
    ("(none)",            lambda e: True),
    ("graduated",         lambda e: e["grad"]),
    ("buyskew>=0.6",      lambda e: (e.get("buyskew") or 0) >= 0.6),
    ("buyskew>=0.7",      lambda e: (e.get("buyskew") or 0) >= 0.7),
    ("bundle<0.1",        lambda e: e.get("bundle") is not None and e["bundle"] < 0.1),
    ("bundle>=0.3",       lambda e: e.get("bundle") is not None and e["bundle"] >= 0.3),
    ("rug<=0.1",          lambda e: e.get("rug") is not None and e["rug"] <= 0.1),
    ("smart_degen>=50",   lambda e: (e.get("smart") or 0) >= 50),
    ("renowned>=1",       lambda e: (e.get("renown") or 0) >= 1),
    ("sniper<=5",         lambda e: e.get("sniper") is not None and e["sniper"] <= 5),
    ("top10<=0.3",        lambda e: e.get("top10") is not None and e["top10"] <= 0.3),
    ("not_wash",          lambda e: e.get("wash") is False),
    ("not_spiked(p5m<=0)",lambda e: e.get("p5m") is not None and e["p5m"] <= 0),
    ("rank>15",           lambda e: (e.get("rank_") or 0) > 15),
    ("rank<=15",          lambda e: (e.get("rank_") or 99) <= 15),
    ("mcap<50k",          lambda e: (e.get("mcap") or 0) < 5e4),
    ("mcap 50-500k",      lambda e: 5e4 <= (e.get("mcap") or 0) < 5e5),
    ("mcap>=500k",        lambda e: (e.get("mcap") or 0) >= 5e5),
    ("liq>=50k",          lambda e: (e.get("liq") or 0) >= 5e4),
    ("holders>=500",      lambda e: (e.get("holders") or 0) >= 500),
]


def metrics(v):
    s = sorted(v)
    return {"n": len(v), "mean": st.mean(v), "median": st.median(v),
            "win": sum(x > 0 for x in v) / len(v), "geo": B.geo(v, 0.10),
            "p2x": sum(1 for x in v if x >= 1.0) / len(v),
            "p5x": sum(1 for x in v if x >= 4.0) / len(v),
            "max": max(v), "ex1": st.mean(s[:-1]) if len(s) > 1 else s[0]}


def run_track(name, ents, rule, rule_name, sort_key, cols):
    ents = sorted(ents, key=lambda e: e["t0"])
    if len(ents) < MIN_N * 2:
        print(f"\n### {name}: only {len(ents)} entries — skipping\n"); return
    uniq = sorted({e["t0"] for e in ents})
    cut = (min(uniq, key=lambda t: abs(sum(1 for e in ents if e["t0"] < t) - len(ents) * SPLIT))
           if len(uniq) > 1 else uniq[0])
    print(f"\n### {name} · rule={rule_name} · n={len(ents)} "
          f"(train {sum(1 for e in ents if e['t0'] < cut)} / holdout {sum(1 for e in ents if e['t0'] >= cut)})")
    hdr = f"  {'filter':<22}" + "".join(f"{c:>9}" for c in cols) + f"{'HOLDOUT':>10}{'n_ho':>6}"
    print(hdr)
    out = []
    for fn, pred in FILTERS:
        sub = [e for e in ents if pred(e)]
        v = [x for x in (B.net_return(e, rule, B._SOLAT) for e in sub) if x is not None]
        ho = [x for x in (B.net_return(e, rule, B._SOLAT) for e in sub if e["t0"] >= cut) if x is not None]
        if len(v) < MIN_N: continue
        m = metrics(v)
        m["ho"] = st.mean(ho) if len(ho) >= 5 else None
        m["n_ho"] = len(ho); m["name"] = fn
        out.append(m)
    for m in sorted(out, key=lambda z: -(z[sort_key] if z[sort_key] is not None else -9)):
        cells = "".join(
            (f"{m[c]*100:+8.1f}%" if c in ("mean","median","ex1","max","ho") else
             f"{m[c]*100:8.0f}%" if c in ("win","p2x","p5x") else
             f"{m[c]*100:+8.2f}%" if c == "geo" else f"{m[c]:>9}")
            for c in cols)
        hoc = f"{m['ho']*100:+9.1f}%" if m["ho"] is not None else "        --"
        print(f"  {m['name']:<22}{cells}{hoc}{m['n_ho']:>6}")
    print(f"  ({len(out)} filters screened — the holdout column is the only meaningful one, and it is")
    print(f"   still optimistic because the filter set was chosen after seeing earlier results)")


def main():
    B._SOLAT = B.load_sol()
    B.load_dex()
    ents = enrich(B.load_entries(B._SOLAT))
    A = [e for e in ents if (e.get("age_min") or -1) >= 1440]
    Bt = [e for e in ents if 0 <= (e.get("age_min") or -1) < 60]
    print(f"\n{'='*104}\nTRACK A — REVIVAL (age >= 1 day) · judged on MEDIAN + GEOMETRIC growth\n{'='*104}")
    run_track("TRACK A", A, CAPPED, "capped", "geo", ["n","mean","median","win","geo"])
    run_track("TRACK A", A, RUNNER, "runner", "geo", ["n","mean","median","win","geo"])
    print(f"\n{'='*104}\nTRACK B — LAUNCH (age < 1h) · judged on the TAIL (negative median EXPECTED)\n{'='*104}")
    run_track("TRACK B", Bt, RUNNER, "runner", "mean", ["n","mean","p2x","p5x","max"])
    run_track("TRACK B", Bt, CAPPED, "capped", "mean", ["n","mean","p2x","p5x","max"])


if __name__ == "__main__":
    main()
