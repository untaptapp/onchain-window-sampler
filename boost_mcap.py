#!/usr/bin/env python3
"""Does a boost at low market cap predict a run to $20k? — touch rates, tradeable returns, control.

Owner's theory (from the kontract launch): "a token boosted while below $5k mcap has a high
probability of reaching at least $20k". kontract is the example the theory came from, so it is
EXCLUDED from every statistic and printed separately (selection on the outcome otherwise).

ARMS
  boost   : boost_events kind='boost', chain='solana', with GeckoTerminal bars (boost_bars, quote-token
            priced). Market cap at time t = bar_close(t) * supply * quoteUSD(t), where supply/quote come
            from the token's DexScreener pairs (fdv / priceUsd; quote SOL -> SOL/USD hourly from GT;
            USDC/USDT -> 1). Pools DexScreener no longer lists: pump.fun mints (…pump) assume 1e9 supply,
            SOL quote; anything else is dropped and COUNTED.
  control : pump.fun launch mints in trending_bars (USD minute bars, supply 1e9) never seen in
            boost_events, at the FIRST minute their close-mcap is inside the same bucket. Outcomes use
            only horizons the collector actually covered (trending_bar_cov.ts_to >= t+H) — D18.
            Contamination: a control token may have been boosted outside our scan (~0.8% base rate).

STATE AT SIGNAL (boost arm): pre_mcap = last minute close at or before paymentTimestamp (bars start
30 min before). No trade in those 30 min -> the price is unchanged on an AMM, but we still label it
`pre_src=none` and exclude it (we cannot see a liquidity pull that leaves no swap, D-LIQ).

OUTCOMES (both arms): max CLOSE mcap within H (a close is a price you could have sold at; the
`touch_high` variant uses bar highs and is shown alongside, D-PRINT), for H in 15m/1h/3h/6h/24h.
A horizon counts only if it had elapsed when the bars were fetched (boost: P+H <= row created_at, or
hour bars reach P+H; control: bar coverage reaches t+H).

TRADEABLE (boost arm): entry at the close of the first FULL minute starting after P+L, L in
{0, 60, 120} s (our public-feed lag is ~53 s p50, so L=60 is the honest case; D-FILL). Exits:
take-profit at an absolute mcap ($20k) or a multiple (2x), time stop at H; stop-loss variants; same-bar
ordering pessimistic (stop before target). Flat round-trip cost COST_RT (a model, labelled; D15).
Reported per pre-mcap bucket with n, mean, ex-top1, median, win, geo@FRAC, and Wilson CIs on rates.

Env: PG_DSN (direct Postgres; local analysis tool), COST_RT=0.03, FRAC=0.10, CONTROL_N=4000
"""
import os, sys, math, json, time, statistics, collections, urllib.request, urllib.error, random
import psycopg

PG_DSN = os.environ["PG_DSN"]
COST_RT = float(os.environ.get("COST_RT", "0.03"))
FRAC = float(os.environ.get("FRAC", "0.10"))
CONTROL_N = int(os.environ.get("CONTROL_N", "4000"))
EXCLUDE = {"kntrct9U7DfJqb9n3uPmAWvC5Bd3STfjs5oqt1zokcc"}
BUCKETS = [(0, 5e3, "<5k"), (5e3, 1e4, "5-10k"), (1e4, 3e4, "10-30k"), (3e4, 1e5, "30-100k"), (1e5, 1e6, "100k-1M"), (1e6, 1e12, ">1M")]
HZ = [("15m", 900), ("1h", 3600), ("3h", 10800), ("6h", 21600), ("24h", 86400)]
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def bucket(m):
    for lo, hi, n in BUCKETS:
        if lo <= m < hi:
            return n
    return None


AGEB = [(0, 600, "<10m"), (600, 3600, "10-60m"), (3600, 21600, "1-6h"), (21600, 1e12, ">6h")]


def ageband(a):
    if a is None or a < 0:
        return "unk"
    for lo, hi, n in AGEB:
        if lo <= a < hi:
            return n
    return "unk"


def wilson(k, n, z=1.96):
    if n == 0:
        return (None, None)
    p = k / n; d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d; h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (c - h, c + h)


def pct(x):
    return "-" if x is None else f"{100*x:5.1f}%"


def get(url, tries=8):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=40) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and i < tries - 1:
                time.sleep(20 * (i + 1)); continue
            raise


# ---------------------------------------------------------------- SOL/USD hourly (GeckoTerminal, SOL/USDC pool)
def sol_usd_series():
    cache = os.environ.get("SOL_CACHE", "sol_usd_hourly.json")
    if os.path.exists(cache) and time.time() - os.path.getmtime(cache) < 3 * 3600:
        return {int(k): v for k, v in json.load(open(cache)).items()}
    out = _sol_usd_fetch()
    json.dump(out, open(cache, "w"))
    return out


def _sol_usd_fetch():
    pool = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"   # Raydium SOL/USDC
    out, before = {}, int(time.time())
    for _ in range(2):
        d = get(f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/hour?aggregate=1&limit=1000&before_timestamp={before}")
        lst = d["data"]["attributes"]["ohlcv_list"]
        for r in lst:
            out[r[0] // 3600 * 3600] = r[4]
        before = lst[-1][0]
        time.sleep(3)
    return out


def sol_at(series, t):
    h = int(t) // 3600 * 3600
    for k in range(0, 48):
        if h - 3600 * k in series:
            return series[h - 3600 * k]
    return None


# ---------------------------------------------------------------- DexScreener supply/quote per token
def ds_meta(tokens):
    meta = {}   # token -> {pool: (supply, quote_kind)}
    for i in range(0, len(tokens), 30):
        try:
            d = get("https://api.dexscreener.com/latest/dex/tokens/" + ",".join(tokens[i:i + 30]))
        except Exception:
            time.sleep(2); continue
        for p in d.get("pairs") or []:
            if p.get("chainId") != "solana":
                continue
            t = p["baseToken"]["address"]
            try:
                supply = float(p["fdv"]) / float(p["priceUsd"])
            except (TypeError, ValueError, KeyError, ZeroDivisionError):
                continue
            q = (p.get("quoteToken") or {}).get("symbol", "").upper()
            kind = "SOL" if q in ("SOL", "WSOL") else ("USD" if q in ("USDC", "USDT", "USD1") else "OTHER")
            meta.setdefault(t, {})[p["pairAddress"]] = (supply, kind)
        time.sleep(0.3)
    return meta


def main():
    t_start = time.time()
    with psycopg.connect(PG_DSN, connect_timeout=20) as c:
        c.execute("set statement_timeout='300s'")
        ev = c.execute("""select event_id, token, payment_ts, created_at, channel, population, pair_age_s, mcap_usd, tape_status,
                                 buys_60, wallets_60, sol_in_60, clusters_60, tipped_60, marker_60, dex_id, amount, n_prior_boosts, profile_paid_ts
                          from boost_events where kind='boost' and chain='solana' and bars_status='done'""").fetchall()
        cols = ["event_id", "token", "payment_ts", "created_at", "channel", "population", "pair_age_s", "mcap_snap", "tape_status",
                "buys_60", "wallets_60", "sol_in_60", "clusters_60", "tipped_60", "marker_60", "dex_id", "amount", "n_prior_boosts", "profile_paid_ts"]
        ev = [dict(zip(cols, r)) for r in ev]
        bars = collections.defaultdict(lambda: {"m": [], "h": [], "pool": None})
        for eid, pool, res, ts, o, h, l, cl, v in c.execute(
                "select b.event_id, b.pool, b.res, b.ts, b.o, b.h, b.l, b.c, b.v from boost_bars b join boost_events e on e.event_id=b.event_id "
                "where e.kind='boost' and e.chain='solana' order by b.event_id, b.res, b.ts"):
            bars[eid][res].append((ts, o, h, l, cl, v)); bars[eid]["pool"] = pool
        trades = collections.defaultdict(list)
        for eid, ts, slot, wallet, side, sol_amt, tip, marker in c.execute(
                "select t.event_id, t.ts, t.slot, t.wallet, t.side, t.sol_amt, t.tip, t.marker from boost_trades t join boost_events e on e.event_id=t.event_id "
                "where e.kind='boost' and e.chain='solana'"):
            trades[eid].append((ts, slot, wallet, side, sol_amt or 0, tip or 0, marker))
        boosted_tokens = {r[0] for r in c.execute("select distinct token from boost_events")}
        # control: pump.fun launches with minute bars, not in boost_events
        ctrl_mints = [r[0] for r in c.execute(
            "select p.mint, p.created_at from pump_launches p join trending_bar_cov b on b.mint=p.mint order by random() limit %s", (CONTROL_N,))]
        ctrl_mints = [m for m in ctrl_mints if m not in boosted_tokens]
        ctrl_birth = dict(c.execute("select mint, created_at from pump_launches where mint = any(%s)", (ctrl_mints,)).fetchall())
        ctrl_cov = dict(c.execute("select mint, ts_to from trending_bar_cov where mint = any(%s)", (ctrl_mints,)).fetchall())
        cb = collections.defaultdict(list)
        for m, ts, o, h, l, cl, v in c.execute("select mint, ts, o, h, l, c, vol from trending_bars where mint = any(%s) order by mint, ts", (ctrl_mints,)):
            cb[m].append((ts, o, h, l, cl, v))
    print(f"loaded {len(ev)} boost events with bars, {len(ctrl_mints)} control mints ({len(cb)} with bars) in {time.time()-t_start:.0f}s")

    sol = sol_usd_series()
    meta = ds_meta(sorted({e["token"] for e in ev}))
    drop = collections.Counter()

    # ------------------------------------------------------------ boost arm: per-event state + outcomes
    rows = []
    for e in ev:
        if e["token"] in EXCLUDE:
            continue
        b = bars[e["event_id"]]; P = e["payment_ts"] / 1000
        pm = meta.get(e["token"], {}).get(b["pool"])
        if pm is None:
            if e["token"].endswith("pump"):
                pm = (1e9, "SOL")
            else:
                drop["no_supply_quote"] += 1; continue
        supply, qk = pm
        if qk == "OTHER":
            drop["quote_other"] += 1; continue

        def mc(price, t):
            q = 1.0 if qk == "USD" else sol_at(sol, t)
            return None if (q is None or price is None) else price * supply * q

        m = sorted(b["m"]); hbars = sorted(b["h"])
        pre = [x for x in m if x[0] + 60 <= P + 1 and x[0] >= P - 1800]   # minute bars that CLOSED by P
        pre_src = "minute"
        if not pre and e["token"].endswith("pump") and (e.get("dex_id") or "").startswith("pumpfun"):
            # bonding curve: liquidity cannot be withdrawn, so the last trade price IS the price (no D-LIQ risk)
            pre = [x for x in hbars if x[0] + 3600 <= P + 1]
            pre_src = "hour_curve"
        if not pre:
            drop["pre_src_none"] += 1; continue
        pre_mc = mc(pre[-1][4], P)
        if not pre_mc:
            drop["pre_mc_none"] += 1; continue
        fetched = e["created_at"] or 0
        last_h = hbars[-1][0] + 3600 if hbars else 0
        age = e["pair_age_s"]
        out = {"pre_mc": pre_mc, "bucket": bucket(pre_mc), "P": P, "e": e, "pre_src": pre_src, "age": age, "ageb": ageband(age)}
        for hn, H in HZ:
            if not (P + H <= fetched or last_h >= P + H):
                out[hn] = None; continue
            seg = [x for x in m if P < x[0] and x[0] + 60 <= P + H] + [x for x in hbars if x[0] >= P + 10800 and x[0] + 3600 <= P + H]
            out[hn] = (max([mc(x[4], x[0]) for x in seg] or [pre_mc]), max([mc(x[2], x[0]) for x in seg] or [pre_mc]))
        out["m"] = m; out["h"] = hbars; out["mc"] = mc
        rows.append(out)
    print("dropped:", dict(drop), "| kept", len(rows))

    # ------------------------------------------------------------ control arm
    ctrl = []
    for mnt, bs in cb.items():
        if len(bs) < 3:
            continue
        cov = ctrl_cov.get(mnt) or bs[-1][0]
        birth = ctrl_birth.get(mnt)
        if birth is None:
            continue
        for lo, hi, name in BUCKETS[:4]:
          for alo, ahi, aname in AGEB:
            first = next((x for x in bs if lo <= x[4] * 1e9 < hi and alo <= x[0] + 60 - birth < ahi), None)
            if not first:
                continue
            t = first[0] + 60
            o = {"pre_mc": first[4] * 1e9, "bucket": name, "age": t - birth, "ageb": aname}
            for hn, H in HZ:
                if cov < t + H or H > 10800:   # >3h control coverage exists only for tokens that later trended (D-COND)
                    o[hn] = None; continue
                seg = [x for x in bs if t < x[0] + 60 <= t + H]
                o[hn] = (max([x[4] * 1e9 for x in seg] or [o["pre_mc"]]), max([x[2] * 1e9 for x in seg] or [o["pre_mc"]]))
            ctrl.append(o)

    # ------------------------------------------------------------ report 1: touch rates by bucket
    print(f"\n== TOUCH RATES — reach target mcap within H (max CLOSE; [touch by HIGH]); Wilson 95% CI; kontract excluded")
    for target_name, f in (("abs $20k", lambda o, v: v >= 2e4), ("2x pre", lambda o, v: v >= 2 * o["pre_mc"]), ("5x pre", lambda o, v: v >= 5 * o["pre_mc"])):
        print(f" -- target {target_name}")
        for _, _, bn in BUCKETS:
            for hn, _ in HZ:
                for arm, data in (("BOOST", rows), ("ctrl ", ctrl)):
                    g = [o for o in data if o["bucket"] == bn and o.get(hn) is not None]
                    if len(g) < 5:
                        continue
                    k = sum(1 for o in g if f(o, o[hn][0])); kh = sum(1 for o in g if f(o, o[hn][1]))
                    lo_, hi_ = wilson(k, len(g))
                    print(f"   {bn:8s} {hn:4s} {arm} n {len(g):5d}  hit {pct(k/len(g))}  CI [{pct(lo_)},{pct(hi_)}]  [high {pct(kh/len(g))}]")
    print("\n== AGE-MATCHED touch rates (control restricted to the same age band; horizons <=3h)")
    for target_name, f in (("abs $20k", lambda o, v: v >= 2e4), ("2x pre", lambda o, v: v >= 2 * o["pre_mc"])):
        for _, _, bn in BUCKETS[:4]:
            for _, _, an in AGEB:
                for hn in ("1h", "3h"):
                    gb = [o for o in rows if o["bucket"] == bn and o["ageb"] == an and o.get(hn) is not None]
                    gc = [o for o in ctrl if o["bucket"] == bn and o["ageb"] == an and o.get(hn) is not None]
                    if len(gb) < 3:
                        continue
                    kb = sum(1 for o in gb if f(o, o[hn][0])); kc = sum(1 for o in gc if f(o, o[hn][0]))
                    lb, hb_ = wilson(kb, len(gb)); lc, hc = wilson(kc, len(gc)) if gc else (None, None)
                    print(f"   {target_name:8s} {bn:8s} age {an:6s} {hn:3s}  BOOST {kb:3d}/{len(gb):3d} {pct(kb/len(gb))} [{pct(lb)},{pct(hb_)}]   "
                          f"CTRL {kc:4d}/{len(gc):4d} {pct(kc/len(gc) if gc else None)} [{pct(lc)},{pct(hc)}]")
    ages = {bn: (statistics.median([o["e"]["pair_age_s"] for o in rows if o["bucket"] == bn and o["e"]["pair_age_s"] is not None] or [float('nan')]),
                 statistics.median([o["age"] for o in ctrl if o["bucket"] == bn] or [float('nan')])) for _, _, bn in BUCKETS[:4]}
    print("  median age at signal (s): boost / control:", {k: (round(a), round(b)) if a == a and b == b else (a, b) for k, (a, b) in ages.items()})

    # ------------------------------------------------------------ report 2: tradeable
    print(f"\n== TRADEABLE — entry at close of first full minute after P+L; cost model {COST_RT*100:.0f}% RT; geo at {FRAC*100:.0f}% of bankroll")

    def sim(o, L, tp, sl, H):
        m, hb, mc = o["m"], o["h"], o["mc"]; P = o["P"]
        start = math.ceil((P + L) / 60) * 60
        eb = next((x for x in m if x[0] >= start), None)
        if eb is None or eb[0] > start + 600:
            return None                                  # no trade within 10 min of the intended entry: no fill (D11)
        px0 = eb[4]; t0 = eb[0] + 60
        if o["e"]["created_at"] and t0 + H > o["e"]["created_at"] and (not hb or hb[-1][0] + 3600 < t0 + H):
            return None                                  # horizon not elapsed at fetch (D18)
        path = [x for x in m if x[0] >= t0 and x[0] + 60 <= t0 + H] + [x for x in hb if x[0] >= P + 10800 and x[0] + 3600 <= t0 + H]
        tgt = None
        if tp is not None:
            tgt = (tp / mc(1.0, t0)) if tp > 50 else px0 * tp   # absolute USD mcap -> price, or multiple
        for x in path:
            if sl is not None and x[3] <= px0 * (1 - sl):
                return (1 - sl) - 1
            if tgt is not None and tgt > px0 and x[2] >= tgt:
                return tgt / px0 - 1
        last = path[-1][4] if path else px0
        return last / px0 - 1

    def stats(xs):
        net = [(1 + x) * (1 - COST_RT) - 1 for x in xs]
        geo = sum(math.log(max(1e-9, 1 + FRAC * x)) for x in net) / len(net)
        ex1 = (sum(net) - max(net)) / (len(net) - 1) if len(net) > 1 else float('nan')
        return (f"n {len(xs):4d} net mean {100*statistics.mean(net):+7.1f}%  ex-top1 {100*ex1:+6.1f}%  median {100*statistics.median(net):+6.1f}%  "
                f"win {100*sum(1 for x in net if x > 0)/len(net):3.0f}%  geo {100*geo:+6.2f}%/trade")

    for bn in ("<5k", "5-10k", "10-30k", "30-100k", "ALL"):
        g = rows if bn == "ALL" else [o for o in rows if o["bucket"] == bn]
        if len(g) < 5:
            print(f"  {bn:8s} n {len(g)} — too few"); continue
        print(f"  -- pre-mcap {bn} (events {len(g)})")
        for L in (0, 60, 120):
            for name, tp, sl, H in (("hold 1h", None, None, 3600), ("hold 24h", None, None, 86400), ("TP $20k / 24h", 2e4, None, 86400),
                                    ("TP 2x / 6h", 2.0, None, 21600), ("TP 2x SL 50% / 6h", 2.0, 0.5, 21600), ("TP 3x SL 50% / 24h", 3.0, 0.5, 86400)):
                xs = [r for r in (sim(o, L, tp, sl, H) for o in g) if r is not None]
                if len(xs) >= 5:
                    print(f"   L={L:3d}s {name:20s} {stats(xs)}")
    # time halves on the headline cell
    g = sorted([o for o in rows if o["bucket"] in ("<5k", "5-10k")], key=lambda o: o["P"])
    if len(g) >= 10:
        half = len(g) // 2
        for nm, part in (("first half", g[:half]), ("second half", g[half:])):
            xs = [r for r in (sim(o, 60, 2e4, None, 86400) for o in part) if r is not None]
            if xs:
                print(f"  <10k TP $20k/24h L=60 {nm:12s} {stats(xs)}")

    # ------------------------------------------------------------ profiles: what the first 30 s of tape looks like, and what follows
    print("\n== PROFILES — tape in the first 30 s after payment (taped boosts only); outcome from entry at L=60 s (after the profile is observable)")
    def prof(o):
        tr = [x for x in trades.get(o["e"]["event_id"], []) if 0 <= x[0] - o["P"] <= 30]
        buys = [x for x in tr if x[3] == "buy" and x[4] >= 0.01]
        w = len({x[2] for x in buys}); sol_in = sum(x[4] for x in buys); sol_out = sum(x[4] for x in tr if x[3] == "sell")
        slots = collections.Counter(x[1] for x in buys)
        same_slot = max(slots.values()) if slots else 0
        name = "quiet" if w == 0 else ("trickle 1-4" if w < 5 else ("crowd 5-19" if w < 20 else "wave 20+"))
        return name, dict(w=w, sol_in=sol_in, sol_out=sol_out, same_slot=same_slot, tipped=sum(1 for x in buys if x[5] > 0))
    taped = [o for o in rows if o["e"]["tape_status"] == "done"]
    print(f"  taped boosts with bars and pre-mcap: {len(taped)}")
    groups = collections.defaultdict(list)
    for o in taped:
        n, feat = prof(o); o["prof"] = n; o["feat"] = feat; groups[n].append(o)
    for n in ("quiet", "trickle 1-4", "crowd 5-19", "wave 20+"):
        g = groups.get(n, [])
        if not g:
            print(f"   {n:12s} n   0"); continue
        h1 = [o for o in g if o.get("1h") is not None]
        k20 = sum(1 for o in h1 if o["1h"][0] >= 2e4); k2x = sum(1 for o in h1 if o["1h"][0] >= 2 * o["pre_mc"])
        xs = [r for r in (sim(o, 60, None, None, 3600) for o in g) if r is not None]
        xs2 = [r for r in (sim(o, 60, 2.0, 0.5, 21600) for o in g) if r is not None]
        print(f"   {n:12s} n {len(g):3d}  pre-mcap p50 ${statistics.median(o['pre_mc'] for o in g):,.0f}  reach $20k/1h {k20}/{len(h1)}  2x/1h {k2x}/{len(h1)}")
        if len(xs) >= 3:
            print(f"      hold 1h from L=60   {stats(xs)}")
        if len(xs2) >= 3:
            print(f"      TP2x SL50 6h L=60   {stats(xs2)}")

    # ------------------------------------------------------------ filter search with a time holdout (D4/D6/D7)
    print("\n== FILTER SEARCH — every slice x rule at L=60 s; choose on the EARLIER half (by payment time), report the frozen choice on the LATER half")
    RULES = [("hold 15m", None, None, 900), ("hold 1h", None, None, 3600), ("hold 6h", None, None, 21600),
             ("TP 2x / 6h", 2.0, None, 21600), ("TP 2x SL 50% / 6h", 2.0, 0.5, 21600), ("TP $20k / 24h", 2e4, None, 86400),
             ("TP 3x SL 50% / 24h", 3.0, 0.5, 86400)]
    def feats(o):
        e = o["e"]
        f = {"bucket": o["bucket"], "age": o["ageb"], "tier": f"x{e['amount']}",
             "venue": "curve" if (e.get("dex_id") or "").startswith("pumpfun") else "amm",
             "prior_boost": "yes" if (e.get("n_prior_boosts") or 0) > 0 else "no",
             "profile_before": "yes" if (e.get("profile_paid_ts") and e["profile_paid_ts"] <= e["payment_ts"]) else "no"}
        if o.get("prof"):
            f["tape"] = o["prof"]
        return f
    for o in rows:
        o["f"] = feats(o)
        o["res"] = {rn: sim(o, 60, tp, sl, H) for rn, tp, sl, H in RULES}
    srt = sorted(rows, key=lambda o: o["P"])
    cut = srt[len(srt) // 2]["P"] if srt else 0
    train = [o for o in srt if o["P"] < cut]; test = [o for o in srt if o["P"] >= cut]
    slices = [("all", None)] + sorted({(k, v) for o in rows for k, v in o["f"].items()})
    def cell(data, sl_, rn):
        g = data if sl_[0] == "all" else [o for o in data if o["f"].get(sl_[0]) == sl_[1]]
        xs = [o["res"][rn] for o in g if o["res"][rn] is not None]
        return xs
    def geo(xs):
        net = [(1 + x) * (1 - COST_RT) - 1 for x in xs]
        return sum(math.log(max(1e-9, 1 + FRAC * x)) for x in net) / len(net)
    MIN_CELL = int(os.environ.get("MIN_CELL", "20"))
    cands = []
    for sl_ in slices:
        for rn, *_ in RULES:
            xs = cell(train, sl_, rn)
            if len(xs) >= MIN_CELL:
                cands.append((geo(xs), sl_, rn, len(xs)))
    cands.sort(reverse=True)
    print(f"  events: train {len(train)} / holdout {len(test)}; candidate cells with n>={MIN_CELL} on train: {len(cands)}")
    for g_, sl_, rn, n_ in cands[:8]:
        ht = cell(test, sl_, rn)
        print(f"   TRAIN geo {100*g_:+6.2f}%/trade n {n_:3d}  {sl_[0]}={sl_[1]:<12s} {rn:20s} | HOLDOUT " + (stats(ht) if len(ht) >= 3 else f"n {len(ht)} (too few)"))
    if cands:
        g_, sl_, rn, n_ = cands[0]
        ht = cell(test, sl_, rn)
        print(f"  FROZEN CHOICE {sl_[0]}={sl_[1]} / {rn}: holdout " + (stats(ht) if ht else "n 0"))
        print(f"  (tried {len(cands)} cells; with that many tries the best TRAIN cell is expected to look good by chance — only the holdout counts)")

    # ------------------------------------------------------------ kontract, reported separately
    print("\n== kontract (excluded above): pre $3.6k at the boost (tape), +40 s $13k, +5 min $24k peak, +40 min $18k — the hypothesis source")


if __name__ == "__main__":
    main()
