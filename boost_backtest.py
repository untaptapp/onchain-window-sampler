#!/usr/bin/env python3
"""Read-only analysis of the paid-signal study tables: the five bundle questions + a horizon backtest.

Reads boost_events / boost_trades / boost_bars / boost_followups (PostgREST, keyset-paged) and prints:
  Q1 bundle presence after the signal (same-slot identical-size clusters; wallet counts)
  Q2 latency: payment → first buy / first cluster (block-time resolution), and OUR feed lag (live only)
  Q3 wallet recurrence across events in the first 60 s
  Q4 their config: size modes, Jito tips, priority fee, router programs, jitodontfront markers
  Q5 beatability proxies: first-buy lag vs our lag, follow-through flow (sol_in_60) and failed txs
  BT  horizon returns from GeckoTerminal bars: entry = close of the first FULL minute after payment
      (D-FILL), exits at fixed horizons (D18), chain-native quote (D14), gross AND with a flat
      round-trip cost parameter (a MODEL, labelled as such — D15), per slice, with train/holdout
      by payment time per population (D4/D6). n is distinct events (D5).
Every statistic prints its n. Nothing here selects a rule; it reports distributions.

Env: SUPABASE_URL, SUPABASE_KEY; COST_RT=0.03 (flat round-trip cost model), FRAC=0.10 (bankroll
fraction for geometric growth), MIN_N=20.
"""
import os, sys, json, math, statistics, collections, urllib.request, urllib.error, urllib.parse

SB_URL = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
SB_KEY = os.environ["SUPABASE_KEY"]
COST_RT = float(os.environ.get("COST_RT", "0.03"))
FRAC = float(os.environ.get("FRAC", "0.10"))
MIN_N = int(os.environ.get("MIN_N", "20"))
HORIZONS_M = [1, 5, 15, 60, 180]
HORIZONS_H = [6, 24, 48]


def sb_get(path):
    req = urllib.request.Request(SB_URL + path, headers={"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def keyset(table, select, key, extra=""):
    out, last = [], None
    while True:
        k0 = key.split(",")[0]
        order = ",".join(f"{k}.asc" for k in key.split(","))
        q = f"/{table}?select={select}&order={order}&limit=1000{extra}" + (f"&{k0}=gt.{urllib.parse.quote(str(last))}" if last is not None else "")
        rows = sb_get(q)
        out += rows
        if len(rows) < 1000:
            return out
        last = rows[-1][k0]
        if len(key.split(",")) > 1:
            # composite key: pull the remainder of the last event_id in full, then continue strictly after it
            tail = sb_get(f"/{table}?select={select}&order={order}&{k0}=eq.{urllib.parse.quote(str(last))}&limit=5000")
            out = [r for r in out if r[k0] != last] + tail


def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def fmt(x, d=2):
    return "-" if x is None else (f"{x:.{d}f}" if isinstance(x, float) else str(x))


def dist(name, xs, d=2):
    xs = [x for x in xs if x is not None]
    print(f"  {name:44s} n {len(xs):5d}  p10 {fmt(q(xs,.1),d)}  p50 {fmt(q(xs,.5),d)}  p90 {fmt(q(xs,.9),d)}  mean {fmt(statistics.mean(xs) if xs else None,d)}")


def main():
    ev = keyset("boost_events", "*", "event_id")
    print("NOTE kind=profile: paymentTimestamp is the PAYMENT, publication is a later manual approval (minutes-hours),")
    print("     so profile latencies are not trigger latencies; only boosts publish at payment. Treat profile as a covariate.")
    print(f"events {len(ev)}  by population/kind/chain:")
    for k, n in sorted(collections.Counter((e['population'] or 'live', e['kind'], e['chain']) for e in ev).items()):
        print(f"   {k}: {n}")
    print("tape_status:", dict(collections.Counter(e["tape_status"] for e in ev)), "| bars_status:", dict(collections.Counter(e.get("bars_status") for e in ev)))
    tape = [e for e in ev if e["tape_status"] == "done"]
    if not tape:
        print("\nno completed tapes yet")
    else:
        print(f"\n== Q1 bundle presence (tapes n={len(tape)}; Solana only)")
        for name, f in (("any buy in 60 s", lambda e: (e["buys_60"] or 0) > 0), (">=5 wallets in 60 s", lambda e: (e["wallets_60"] or 0) >= 5),
                        (">=20 wallets in 60 s", lambda e: (e["wallets_60"] or 0) >= 20), ("identical-size same-slot cluster in 60 s", lambda e: (e["clusters_60"] or 0) > 0),
                        ("any Jito-tipped buy in 60 s", lambda e: (e["tipped_60"] or 0) > 0), ("any jitodontfront marker in 60 s", lambda e: (e["marker_60"] or 0) > 0)):
            for kind in ("boost", "profile"):
                g = [e for e in tape if e["kind"] == kind]
                if g:
                    print(f"  {name:44s} {kind:8s} {100*sum(1 for e in g if f(e))/len(g):5.1f}%  (n {len(g)})")
        print("\n== Q1b activity LIFT: buys in the first 60 s vs the per-minute rate in the 120 s before payment (busy tokens look 'fast' without any trigger)")
        for kind in ("boost", "profile"):
            g = [e for e in tape if e["kind"] == kind]
            quiet = [e for e in g if (e["pre_trades"] or 0) == 0]
            print(f"  {kind:8s} n {len(g):4d}  quiet-before (0 pre trades) {len(quiet):4d}; of those with >=5 buys in 60 s: {sum(1 for e in quiet if (e['buys_60'] or 0) >= 5):4d}  "
                  f"| busy-before median buys_60/(pre/2): {fmt(q([(e['buys_60'] or 0) / max(0.5, (e['pre_trades'] or 0) / 2) for e in g if (e['pre_trades'] or 0) > 0], .5), 2)}")
        print("\n== Q2 latency (s after paymentTimestamp; block time is 1-s resolution)")
        for kind in ("boost", "profile"):
            g = [e for e in tape if e["kind"] == kind and (e["buys_60"] or 0) > 0]
            dist(f"{kind}: first buy lag", [e["first_buy_lag_s"] for e in g])
            dist(f"{kind}: first cluster lag", [e["first_cluster_lag_s"] for e in g])
        live = [e for e in ev if e["channel"] == "ws"]
        dist("our feed lag, ws channel (ms)", [e["our_lag_ms"] for e in live], 0)
        dist("our feed lag, rest channel (ms)", [e["our_lag_ms"] for e in ev if e["channel"] == "rest"], 0)
        g = [e for e in live if e["tape_status"] == "done" and e["first_buy_lag_s"] is not None]
        if g:
            ahead = sum(1 for e in g if e["our_lag_ms"] / 1000 < e["first_buy_lag_s"])
            print(f"  live events where our ws lag < first on-chain buy lag: {ahead}/{len(g)} ({100*ahead/len(g):.0f}%) — a necessary, not sufficient, condition")
        print("\n== Q3/Q4 wallets and config in the first 60 s after a BOOST")
        pay = {e["event_id"]: e["payment_ts"] / 1000 for e in tape if e["kind"] == "boost"}   # boosts only: a profile's payment is not its publication
        first60 = []
        for eid, p0 in pay.items():   # one small windowed call per boost event instead of paging the whole table
            first60 += sb_get(f"/boost_trades?select=event_id,slot,ts,wallet,side,sol_amt,tip,fee,programs,marker,source"
                              f"&event_id=eq.{urllib.parse.quote(eid)}&side=eq.buy&ts=gte.{int(p0)}&ts=lte.{int(p0)+60}&limit=2000")
        print(f"  first-60s buy legs {len(first60)} across {len({l['event_id'] for l in first60})} events, {len({l['wallet'] for l in first60})} wallets")
        per_w = collections.defaultdict(list)
        for l in first60:
            per_w[l["wallet"]].append(l)
        rec = sorted(((len({l['event_id'] for l in ls}), w) for w, ls in per_w.items()), reverse=True)
        print(f"  wallets in >=2 events: {sum(1 for n,_ in rec if n>=2)}, >=5: {sum(1 for n,_ in rec if n>=5)}, >=10: {sum(1 for n,_ in rec if n>=10)}")
        for n, w in rec[:12]:
            ls = per_w[w]
            print(f"   {w[:12]} events {n:3d}  lag p50 {fmt(q([l['ts']-pay[l['event_id']] for l in ls],.5),1)}s  size p50 {fmt(q([l['sol_amt'] for l in ls],.5),3)}  tip p50 {fmt(q([l['tip'] for l in ls],.5),5)}  fee p50 {fmt(q([l['fee'] for l in ls],.5),6)}  marker {100*sum(1 for l in ls if l['marker'])/len(ls):.0f}%  progs {collections.Counter(p for l in ls for p in (l['programs'] or [])).most_common(1)}")
        dist("buy size (SOL)", [l["sol_amt"] for l in first60], 3)
        print(f"  tipped share {100*sum(1 for l in first60 if l['tip']>0)/max(1,len(first60)):.1f}%; tip p50 among tipped {fmt(q([l['tip'] for l in first60 if l['tip']>0],.5),5)}; fee p50 {fmt(q([l['fee'] for l in first60],.5),6)} p90 {fmt(q([l['fee'] for l in first60],.9),6)}; marker share {100*sum(1 for l in first60 if l['marker'])/max(1,len(first60)):.1f}%")
        print("  top programs:", collections.Counter(p for l in first60 for p in (l["programs"] or [])).most_common(6))
        print("  top sources:", collections.Counter(l["source"] for l in first60).most_common(5))
        print("\n== Q5 follow-through after the first buy (boost tapes with >=1 buy)")
        g = [e for e in tape if e["kind"] == "boost" and (e["buys_60"] or 0) > 0]
        dist("SOL bought in first 60 s", [e["sol_in_60"] for e in g])
        dist("SOL sold in first 60 s", [e["sol_out_60"] for e in g])
        dist("failed txs in window", [e["n_failed"] for e in g], 0)
        dist("price_60 / price_pre", [e["price_60"] / e["price_pre"] for e in g if e.get("price_60") and e.get("price_pre")], 3)
        dist("price_180 / price_pre", [e["price_180"] / e["price_pre"] for e in g if e.get("price_180") and e.get("price_pre")], 3)

    # ---------------------------------------------------------------- backtest on bars
    withbars = [e for e in ev if e.get("bars_status") == "done"]
    print(f"\n== BT horizon returns on GeckoTerminal bars (events with bars: {len(withbars)})")
    if not withbars:
        print("  no bars yet (boost_bars.py drains the queue ~150 events/h)")
        return
    bars = keyset("boost_bars", "event_id,res,ts,o,h,l,c,v", "event_id,res,ts")
    by = collections.defaultdict(lambda: {"m": [], "h": []})
    for b in bars:
        by[b["event_id"]][b["res"]].append(b)
    rets = []   # (event, horizon_label, gross_ret)
    for e in withbars:
        m = sorted(by[e["event_id"]]["m"], key=lambda b: b["ts"]); h = sorted(by[e["event_id"]]["h"], key=lambda b: b["ts"])
        pay = e["payment_ts"] / 1000
        entry_bar = next((b for b in m if b["ts"] >= math.ceil(pay / 60) * 60 + 60), None)   # first FULL minute after payment, priced at its close
        if not entry_bar or not entry_bar["c"] or not (entry_bar["v"] or 0) > 0:
            continue
        px0, t0 = entry_bar["c"], entry_bar["ts"] + 60
        for hm in HORIZONS_M:
            bar = next((b for b in reversed(m) if b["ts"] + 60 <= t0 + hm * 60), None)
            if bar and bar["ts"] + 60 >= t0 + hm * 60 - 180 and bar["c"]:
                rets.append((e, f"{hm}m", bar["c"] / px0 - 1))
        for hh in HORIZONS_H:
            bar = next((b for b in reversed(h) if b["ts"] + 3600 <= t0 + hh * 3600), None)
            if bar and bar["ts"] + 3600 >= t0 + hh * 3600 - 7200 and bar["c"]:
                rets.append((e, f"{hh}h", bar["c"] / px0 - 1))

    def slice_of(e, key):
        if key == "kind": return e["kind"]
        if key == "chain": return e["chain"]
        if key == "population": return e["population"] or "live"
        if key == "amount": return "profile" if e["kind"] == "profile" else (f"boost{e['amount']}" if e["amount"] in (10, 30, 50, 100, 500) else f"boost{e['amount']}")
        if key == "mcap": v = e.get("mcap_usd"); return "unknown" if v is None else ("<10k" if v < 1e4 else "<50k" if v < 5e4 else "<250k" if v < 2.5e5 else ">=250k")
        if key == "pair_age": v = e.get("pair_age_s"); return "unknown" if v is None else ("<1h" if v < 3600 else "<24h" if v < 86400 else ">=24h")
        if key == "cluster": return "unknown" if e["tape_status"] != "done" else ("cluster" if (e["clusters_60"] or 0) > 0 else "no_cluster")
        if key == "dex": return e.get("dex_id") or "unknown"

    def report(rows, label):
        xs = [r for _, _, r in rows]
        if len(xs) < MIN_N:
            return
        net = [(1 + x) * (1 - COST_RT) - 1 for x in xs]
        geo = sum(math.log(max(1e-9, 1 + FRAC * x)) for x in net) / len(net)
        top1 = max(xs)
        ex1 = (sum(xs) - top1) / (len(xs) - 1) if len(xs) > 1 else None
        print(f"  {label:46s} n {len(xs):4d} mean {100*statistics.mean(xs):+7.1f}%  ex-top1 {100*ex1:+6.1f}%  median {100*statistics.median(xs):+6.1f}%  win {100*sum(1 for x in xs if x>0)/len(xs):4.0f}%  net-mean {100*statistics.mean(net):+6.1f}%  geo@{int(FRAC*100)}% {100*geo:+6.2f}%/trade")

    print(f"  entry = close of first full minute after payment; exits at horizon close; cost model flat {COST_RT*100:.1f}% round trip; n = distinct events")
    for hz in [f"{m}m" for m in HORIZONS_M] + [f"{h}h" for h in HORIZONS_H]:
        report([r for r in rets if r[1] == hz], f"ALL  hold {hz}")
    for key in ("kind", "amount", "chain", "population", "mcap", "pair_age", "cluster", "dex"):
        print(f"  -- by {key}")
        for hz in ("5m", "60m", "24h"):
            for sl in sorted({slice_of(e, key) for e, _, _ in rets}):
                report([r for r in rets if r[1] == hz and slice_of(r[0], key) == sl], f"{key}={sl} hold {hz}")
    # train / holdout by payment time per population (D4/D6): report both halves so a reader sees drift
    print("  -- time halves per population (first half / second half of payment_ts; no selection performed)")
    for pop in sorted({(e["population"] or "live") for e, _, _ in rets}):
        ts = sorted({e["payment_ts"] for e, _, _ in rets if (e["population"] or "live") == pop})
        if len(ts) < 2 * MIN_N:
            continue
        cut = ts[len(ts) // 2]
        for hz in ("5m", "60m", "24h"):
            report([r for r in rets if r[1] == hz and (r[0]["population"] or "live") == pop and r[0]["payment_ts"] < cut], f"{pop} {hz} FIRST half")
            report([r for r in rets if r[1] == hz and (r[0]["population"] or "live") == pop and r[0]["payment_ts"] >= cut], f"{pop} {hz} SECOND half")


if __name__ == "__main__":
    main()
