#!/usr/bin/env python3
"""Point-in-time EXECUTION COST collector for trending mints (Jupiter routed quotes).

WHY THIS EXISTS
---------------
`trending_snapshots.liquidity` is pool TVL. TVL is a *weak* proxy for what a trade actually
costs: measured Spearman(TVL, routed impact) = -0.41, the relationship is non-monotonic, and it
misses trap pools outright (a $356k-TVL Meteora pool quoted 100% price impact on a $106 order).
Modelling impact as constant-product `size/(TVL/2+size)` understates real cost by ~6x median,
because real cost is dominated by a FIXED FLOOR (fees + routing + spread), not the AMM curve:
a 50x larger order raises cost only ~2.7x.

So execution cost must be QUOTED, not modelled. And it is a POINT-IN-TIME quantity that moves
with price — liquidity drains ~11% exactly when price falls, so the sell leg of a losing trade
is systematically more expensive than the buy leg. A single "latest" quote therefore cannot
represent the cost at the moment we model an entry or an exit; we need a quote TIME SERIES
aligned with the price path.

Jupiter has no historical quote API, so **any interval we don't sample is permanently
un-costable**. That is the whole argument for running this continuously.

WHAT IT DOES
------------
For every mint seen on any trending feed, periodically quote a SIZE LADDER against Jupiter and
append to `trending_quotes` (mint, quoted_at, size_sol, price_impact_pct, route, ...). Cadence
tiers by age: dense while the token is young (that's when entries and most exits happen), then
decaying. Also records the board `liquidity`/`price` at quote time so the cost-vs-TVL curve can
be recalibrated later.

Reads `trending_snapshots` (never writes it) and writes only `trending_quotes`, so the collector
feeds and the analysis rollup are untouched.

Source: Jupiter lite-api /swap/v1/quote — free, keyless, the actual executable routed price.

Env: SUPABASE_URL, SUPABASE_KEY.
     RUN_SECONDS (default 20000 ~5.5h), PASS_INTERVAL (default 600 =10 min),
     MAX_QUOTES (Jupiter calls per pass, default 1200), SLEEP (default 0.3),
     SIZES_SOL  — full ladder for tokens younger than LADDER_FULL_H (default
                  '0.25,1,2.5,5,10,25,50' ~= $26..$5,300 at SOL $106),
     SIZES_SOL_SPARSE — reduced ladder for older tokens (default '1,5,25'), so the call
                  budget concentrates where entries and exits actually happen,
     SKIP_TVL_MULT (default 1.5) — don't spend a call quoting a clip larger than 1.5x the
                  whole pool; the answer is known ("untradeable") and carries no curve info.
"""
import json, os, time, urllib.request, urllib.error
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
JUP = os.environ.get("JUP_BASE", "https://lite-api.jup.ag").rstrip("/")
SOL = "So11111111111111111111111111111111111111112"
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "600"))
MAX_QUOTES = int(os.environ.get("MAX_QUOTES", "1200"))
SLEEP = float(os.environ.get("SLEEP", "0.3"))
SIZES = [float(x) for x in os.environ.get("SIZES_SOL", "0.25,1,2.5,5,10,25,50").split(",")]
SIZES_SPARSE = [float(x) for x in os.environ.get("SIZES_SOL_SPARSE", "1,5,25").split(",")]
LADDER_FULL_H = float(os.environ.get("LADDER_FULL_H", "12"))
SKIP_TVL_MULT = float(os.environ.get("SKIP_TVL_MULT", "1.5"))
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                t = r.read()
                return r.status, (json.loads(t) if t else None)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(1.5 * (a + 1)); continue
            return e.code, e.read().decode()[:200]
        except Exception:
            time.sleep(1.5 * (a + 1))
    return 0, None


def sol_usd():
    """SOL price from a 1-SOL -> USDC quote; used to skip clips larger than the pool itself."""
    try:
        u = f"{JUP}/swap/v1/quote?inputMint={SOL}&outputMint={USDC}&amount=1000000000&slippageBps=100"
        with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=15) as r:
            q = json.loads(r.read())
        if q.get("swapUsdValue"):
            return float(q["swapUsdValue"])
        return int(q["outAmount"]) / 1e6
    except Exception:
        return 150.0


def cadence(age_h):
    """seconds between quote passes, by hours since first trending sighting.
    Dense while young: entries happen at t=0 and most exits inside the first hours."""
    if age_h < 2:   return 0             # every pass (~10 min)
    if age_h < 12:  return 30 * 60
    if age_h < 48:  return 4 * 3600
    if age_h < 336: return 24 * 3600     # ~daily for 2 weeks
    return 7 * 24 * 3600                 # weekly thereafter (dormant coins can wake)


def quote(mint, size_sol):
    """One Jupiter routed quote. Returns a row dict, or an ok=False row if unroutable."""
    lam = int(size_sol * 1_000_000_000)
    url = f"{JUP}/swap/v1/quote?inputMint={SOL}&outputMint={mint}&amount={lam}&slippageBps=500"
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=20) as r:
                q = json.loads(r.read())
            if q.get("priceImpactPct") is None:
                return {"ok": False}
            rp = q.get("routePlan") or []
            return {"ok": True,
                    "size_usd": float(q["swapUsdValue"]) if q.get("swapUsdValue") else None,
                    "price_impact_pct": float(q["priceImpactPct"]) * 100.0,
                    "out_amount": q.get("outAmount"),
                    "route": ",".join(s.get("swapInfo", {}).get("label", "") for s in rp)[:200],
                    "n_hops": len(rp)}
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2.0 * (a + 1)); continue
            return {"ok": False}
        except Exception:
            time.sleep(1.0 * (a + 1))
    return {"ok": False}


def universe():
    """Every mint ever seen on a trending feed, with its first sighting + latest board state."""
    rows = []
    for src in ("gmgn", "solanatracker", "geckoterminal", "fomoscan"):
        st, body = sb("GET", f"/trending_snapshots?source=eq.{src}"
                             "&select=mint,captured_at,liquidity,price"
                             "&order=captured_at.desc&limit=20000")
        if isinstance(body, list):
            rows += body
    first, latest = {}, {}
    for r in rows:
        m, t = r["mint"], r["captured_at"] / 1000.0
        if m not in first or t < first[m]:
            first[m] = t
        if m not in latest or t > latest[m]["_t"]:
            latest[m] = {"_t": t, "liquidity": r.get("liquidity"), "price": r.get("price")}
    return first, latest


def last_quoted():
    st, body = sb("GET", "/trending_quotes?select=mint,quoted_at&order=quoted_at.desc&limit=40000")
    out = {}
    if isinstance(body, list):
        for r in body:
            m = r["mint"]
            if m not in out or r["quoted_at"] > out[m]:
                out[m] = r["quoted_at"]
    return out


def one_pass():
    first, latest = universe()
    if not first:
        print("no mints in trending_snapshots yet", flush=True)
        return "ok", 0
    lastq = last_quoted()
    now = time.time()
    due = []
    for m, t0 in first.items():
        age_h = (now - t0) / 3600.0
        lq = lastq.get(m)
        if lq is None or (now - lq) >= cadence(age_h):
            # youngest first — those are the ones at/near entry
            due.append((age_h, m))
    due.sort()
    spend = 0
    deadline = time.time() + PASS_INTERVAL * 0.8      # never overrun the pass interval
    px = sol_usd()
    rows, unroutable, skipped, n_mints = [], 0, 0, 0
    for age_h, m in due:
        if spend >= MAX_QUOTES or time.time() >= deadline:
            break
        ts = int(time.time())
        meta = latest.get(m) or {}
        tvl = meta.get("liquidity")
        # full ladder while the token is young (that is where entries and most exits sit),
        # sparse ladder afterwards, so the budget buys resolution where we actually model
        ladder = SIZES if age_h < LADDER_FULL_H else SIZES_SPARSE
        n_mints += 1
        for sz in ladder:
            if tvl and (sz * px) > SKIP_TVL_MULT * tvl:
                skipped += 1
                continue
            q = quote(m, sz); spend += 1
            time.sleep(SLEEP)
            ok = bool(q.get("ok"))
            if not ok:
                unroutable += 1
            # every row carries the SAME keys — PostgREST rejects a batch with ragged rows
            rows.append({"mint": m, "quoted_at": ts, "size_sol": sz, "ok": ok,
                         "size_usd": q.get("size_usd"), "price_impact_pct": q.get("price_impact_pct"),
                         "out_amount": q.get("out_amount"), "route": q.get("route"),
                         "n_hops": q.get("n_hops"),
                         "liquidity": meta.get("liquidity"), "price": meta.get("price")})
    if not rows:
        print("nothing due this pass", flush=True)
        return "ok", 0
    wrote = 0
    for i in range(0, len(rows), 500):
        st, _ = sb("POST", "/trending_quotes?on_conflict=mint,quoted_at,size_sol", rows[i:i + 500],
                   prefer="resolution=merge-duplicates,return=minimal")
        if st and 200 <= st < 300:
            wrote += len(rows[i:i + 500])
        else:
            print(f"write failed status={st}", flush=True)
            return "fail", wrote
    print(f"pass: {n_mints}/{len(due)} mints quoted, {wrote} rows written, "
          f"{unroutable} unroutable, {skipped} skipped (clip>pool), {spend} jupiter calls", flush=True)
    return "ok", wrote


def main():
    end = time.time() + RUN_SECONDS
    n = 0; fails = 0
    while True:
        try:
            outcome, _ = one_pass(); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True); outcome = "fail"
        fails = fails + 1 if outcome == "fail" else 0
        if fails >= 6:
            print(f"{fails} consecutive failures — exiting to avoid a silent zombie run.", flush=True)
            break
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print(f"done {n} passes", flush=True)


if __name__ == "__main__":
    main()
