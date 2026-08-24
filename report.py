#!/usr/bin/env python3
"""Read completed events + their samples from Supabase and summarise the windowed
microstructure: participant count in the first 60s, same-second participants, and
the price path a fast participant filling +N s after the event could realise,
exiting within 60s. Broken out per source and per capitalisation bucket.

Env: SUPABASE_URL, SUPABASE_KEY
"""
import json, os, statistics as st, urllib.request, urllib.error
from collections import defaultdict

SB  = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
FILL = [1, 2, 3, 5]
EXIT = [10, 20, 30, 45, 60]
CAP_BUCKETS = [("<=250k", 2.5e5), ("<=1M", 1e6), ("<=5M", 5e6), (">5M", float("inf"))]


def sb(path):
    req = urllib.request.Request(SB + path, headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def price_at(tr, t):
    c = [x for x in tr if x["ts"] <= t];  return c[-1]["price"] if c else None
def price_from(tr, t):
    c = [x for x in tr if x["ts"] >= t];  return c[0]["price"] if c else None


def metrics(ev, tr):
    tr = sorted(tr, key=lambda x: x["ts"]); et = ev["event_ts"]
    win = [x for x in tr if et <= x["ts"] <= et + 60]
    buyers = {x["actor"] for x in win if x["side"] == "buy"}
    same = {x["actor"] for x in win if x["side"] == "buy" and x["ts"] <= et + 1}
    r = {"participants": len(buyers), "same_sec": len(same),
         "net": sum(x["notional"] for x in win if x["side"] == "buy")
                - sum(x["notional"] for x in win if x["side"] == "sell")}
    for df in FILL:
        fp = price_from(tr, et + df) or price_at(tr, et + df)
        if not fp: continue
        for xs in EXIT:
            xp = price_at(tr, et + xs)
            if xp: r[f"f{df}_x{xs}"] = xp / fp - 1
        peak = [x["price"] for x in tr if et + df <= x["ts"] <= et + 60]
        if peak: r[f"f{df}_mfe"] = max(peak) / fp - 1
    return r


def agg(rows, keys):
    for k in keys:
        xs = [r[k] for r in rows if k in r]
        if not xs: continue
        xs2 = sorted(xs)
        print(f"  {k:<10} n={len(xs):<3} med={st.median(xs)*100:+6.2f}% mean={sum(xs)/len(xs)*100:+6.2f}% "
              f"%pos={100*sum(1 for x in xs if x>0)/len(xs):>3.0f}% p25={xs2[len(xs2)//4]*100:+6.2f}%")


def main():
    events = sb("/events?status=eq.done&select=id,label,asset,symbol,event_ts,ref_cap")
    if not events:
        print("no completed events yet — let the sampler run."); return
    R = []
    for ev in events:
        tr = sb(f"/samples?event_id=eq.{ev['id']}&select=ts,side,actor,price,notional")
        if tr: R.append({"ev": ev, **metrics(ev, tr)})
    print(f"=== {len(R)} completed events ===\n")
    part = [r["participants"] for r in R]; same = [r["same_sec"] for r in R]
    print(f"PARTICIPANT WAVE in 60s: distinct median {st.median(part):.0f} (max {max(part)}); "
          f"same-second median {st.median(same):.0f} (max {max(same)})")
    print(f"net-inflow in [event,+60s]: {sum(1 for r in R if r['net']>0)}/{len(R)}\n")
    print("REALISABLE RETURN (fN = fill N s after event; xM = exit at M s; mfe = best exit <=60s)")
    agg(R, [f"f{d}_x{x}" for d in FILL for x in (10, 30, 60)] + [f"f{d}_mfe" for d in FILL])

    print("\nBY CAPITALISATION BUCKET (fill +2s, exit +30s):")
    lo = 0
    for name, hi in CAP_BUCKETS:
        sub = [r for r in R if r["ev"].get("ref_cap") is not None and lo < r["ev"]["ref_cap"] <= hi]
        lo = hi
        xs = [r["f2_x30"] for r in sub if "f2_x30" in r]
        if xs:
            print(f"  {name:<8} n={len(xs):<3} med={st.median(xs)*100:+6.2f}% "
                  f"%pos={100*sum(1 for x in xs if x>0)/len(xs):>3.0f}%")

    print("\nBY SOURCE (fill +2s, best exit <=60s):")
    byk = defaultdict(list)
    for r in R: byk[r["ev"]["label"]].append(r)
    print(f"{'label':<16}{'n':>3}{'medPart':>9}{'medMFE':>9}{'medX30':>9}{'%posX30':>9}")
    for h, rs in sorted(byk.items(), key=lambda kv: -len(kv[1])):
        x30 = [r["f2_x30"] for r in rs if "f2_x30" in r]
        mfe = [r["f2_mfe"] for r in rs if "f2_mfe" in r]
        prt = [r["participants"] for r in rs]
        if not x30: continue
        print(f"{str(h):<16}{len(rs):>3}{st.median(prt):>9.0f}{st.median(mfe)*100:>8.1f}%"
              f"{st.median(x30)*100:>8.1f}%{100*sum(1 for x in x30 if x>0)/len(x30):>8.0f}%")


if __name__ == "__main__":
    main()
