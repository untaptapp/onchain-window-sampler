#!/usr/bin/env python3
"""Retrospective DexScreener paid-signal events (boosts + Enhanced Token Info) with their on-chain tape.

WHY: the live sampler (boost_tape.py) accrues ~100–200 Solana boosts/day; the questions "how often is
a bundle present after the signal, how fast, is it the same wallets, what is their config" want an n in
the hundreds NOW. DexScreener's `orders/v1/{chain}/{token}` returns every paid order with its
`paymentTimestamp` for ANY token, so the event time is recoverable after the fact; Helius serves the
tape for any past window. What is NOT recoverable is our own feed latency (`seen_at`) and the
point-in-time DexScreener snapshot — backfill rows carry channel='backfill', seen_at=payment_ts,
our_lag_ms=NULL and NULL snapshot prices; the price path comes from boost_bars.py (GeckoTerminal).

POPULATIONS (the `population` column — never pool them without saying so):
  pump_launch : random sample of pump_launches (the unbiased launch firehose; retention 5 days)
  trending    : trending_pools mints (board-selected: SURVIVORSHIP-biased toward tokens that worked)
  rh_launch   : rh_launches tokens (Robinhood Chain; events + bars only, no tape decoder)

TAPE (Solana): RPC getSignaturesForAddress on the pair, paged back from the head until the window is
reached (cap SIG_PAGES, recorded as tape_status='too_deep' when hit), then Helius /v0/transactions
(parsed, 100 per call) on the in-window signatures → the same tx_to_legs()/summarise() as the live
collector, so live and backfill rows are comparable. Failed txs are counted from the signature index.

Env: SUPABASE_URL, SUPABASE_KEY, HELIUS_KEY; SAMPLE_PUMP=3000, SAMPLE_TRENDING=1500, SAMPLE_RH=1000,
     SIG_PAGES=30, HELIUS_BUDGET=6000, ORDERS_RPM=55, RUN_SECONDS=20000, POPULATIONS=pump_launch,trending,rh_launch
"""
import os, sys, time, json, random, math, collections, urllib.parse
os.environ.setdefault("SINK", "supabase")
import boost_tape as bt   # reuses http_json, sb, Sink, tx_to_legs, summarise, Event, templates, TIP_ACCOUNTS

SAMPLE = {"pump_launch": int(os.environ.get("SAMPLE_PUMP", "3000")),
          "trending": int(os.environ.get("SAMPLE_TRENDING", "1500")),
          "rh_launch": int(os.environ.get("SAMPLE_RH", "1000"))}
POPULATIONS = [p for p in os.environ.get("POPULATIONS", "pump_launch,trending,rh_launch").split(",") if p]
SIG_PAGES = int(os.environ.get("SIG_PAGES", "30"))
TAPE_MAX_AGE_D = float(os.environ.get("TAPE_MAX_AGE_D", "7"))   # older events: recorded, no tape (each too_deep burns SIG_PAGES calls)
ORDERS_RPM = int(os.environ.get("ORDERS_RPM", "55"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
CHAIN_OF = {"pump_launch": "solana", "trending": "solana", "rh_launch": "robinhood"}
T_END = time.time() + RUN_SECONDS
log = bt.log


def sb_keyset(table, select, extra, key="mint", cap=400000):
    """Keyset paging on `key` (A-OFFSET); raises if a page fails (A-SHORT)."""
    out, last = [], None
    while True:
        q = f"/{table}?select={select}&order={key}.asc&limit=1000{extra}" + (f"&{key}=gt.{urllib.parse.quote(last)}" if last else "")
        st, rows = bt.sb("GET", q)
        if st != 200:
            raise RuntimeError(f"page failed {table} {st} {rows}")
        out += rows
        if len(rows) < 1000:
            break
        last = rows[-1][key]
        if len(out) >= cap:
            log(f"WARNING: {table} read hit cap {cap} — population truncated")
            break
    return out


PG_DSN = os.environ.get("PG_DSN")   # local only: direct Postgres lets us TABLESAMPLE instead of paging 350k rows


def pg_sample(sql):
    import psycopg
    with psycopg.connect(PG_DSN, connect_timeout=20) as c:
        c.execute("set statement_timeout='120s'")
        return c.execute(sql).fetchall()


def population(name):
    if PG_DSN:
        n = SAMPLE[name] * 2
        if name == "pump_launch":
            lo = int(time.time()) - 5 * 86400
            return pg_sample(f"select mint, created_at, bonding_curve from pump_launches where created_at >= {lo} order by random() limit {n}")
        if name == "trending":
            return pg_sample(f"select mint, null, pool_address from trending_pools tablesample system (2) where ok order by random() limit {n}")
        if name == "rh_launch":
            return pg_sample(f"select mint, first_seen_at, null from rh_launches tablesample system (1) order by random() limit {n}")
    if name == "pump_launch":
        lo = int(time.time()) - 5 * 86400
        rows = sb_keyset("pump_launches", "mint,created_at,bonding_curve", f"&created_at=gte.{lo}")
        return [(r["mint"], r["created_at"], r["bonding_curve"]) for r in rows]
    if name == "trending":
        rows = sb_keyset("trending_pools", "mint,pool_address", "&ok=is.true")
        return [(r["mint"], None, r["pool_address"]) for r in rows]
    if name == "rh_launch":
        rows = sb_keyset("rh_launches", "mint,first_seen_at", "")
        return [(r["mint"], r["first_seen_at"], None) for r in rows]
    raise ValueError(name)


def known_tokens():
    """Tokens already scanned (any kind/population) so re-runs skip them."""
    st, rows = bt.sb("GET", "/boost_events?select=token&channel=eq.backfill&order=token.asc&limit=1000")
    seen, last = set(), None
    while st == 200 and rows:
        seen |= {r["token"] for r in rows}
        if len(rows) < 1000:
            break
        last = rows[-1]["token"]
        st, rows = bt.sb("GET", f"/boost_events?select=token&channel=eq.backfill&order=token.asc&limit=1000&token=gt.{last}")
    return seen


def sig_window(pair, lo_s, hi_s):
    """Signatures (with err flags) in [lo_s, hi_s] by paging the RPC index back from the head."""
    before, sigs, pages, reached = None, [], 0, False
    while pages < SIG_PAGES and bt.helius_calls < bt.HELIUS_BUDGET:
        body = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                "params": [pair, {"limit": 1000, **({"before": before} if before else {})}]}
        st, d = bt.http_json(f"https://mainnet.helius-rpc.com/?api-key={bt.HELIUS_KEY}", method="POST", body=body, timeout=60)
        bt.helius_calls += 1
        pages += 1
        if st != 200 or not isinstance(d, dict) or "result" not in d:
            bt.STATS["helius_fail"] += 1
            return None, False
        res = d["result"]
        if not res:
            reached = True
            break
        for s_ in res:
            t = s_.get("blockTime") or 0
            if lo_s <= t <= hi_s:
                sigs.append(s_)
        before = res[-1]["signature"]
        if (res[-1].get("blockTime") or 0) < lo_s:
            reached = True
            break
    return sigs, reached


def parse_sigs(sigs):
    out = []
    for i in range(0, len(sigs), 100):
        if bt.helius_calls >= bt.HELIUS_BUDGET:
            return out, False
        st, d = bt.http_json(f"https://api.helius.xyz/v0/transactions?api-key={bt.HELIUS_KEY}", method="POST",
                             body={"transactions": sigs[i:i + 100]}, timeout=90)
        bt.helius_calls += 1
        if st != 200 or not isinstance(d, list):
            bt.STATS["helius_fail"] += 1
            return out, False
        out += d
    return out, True


def tape_for(ev):
    lo, hi = ev.payment_ts / 1000 - bt.PRE_S, ev.payment_ts / 1000 + bt.POST_S
    sigs, reached = sig_window(ev.pair, lo, hi)
    if sigs is None:
        ev.row.update(tape_status="failed", helius_calls=bt.helius_calls)
        return []
    if not reached:
        ev.row.update(tape_status="too_deep", helius_calls=bt.helius_calls)
        bt.STATS["tape_too_deep"] += 1
        return []
    ok_sigs = [s["signature"] for s in sigs if s.get("err") is None]
    n_failed = sum(1 for s in sigs if s.get("err") is not None)
    txs, complete = parse_sigs(ok_sigs)
    legs = []
    for t in txs:
        legs += bt.tx_to_legs(t, ev.token, ev.event_id, ev.chain)
    bt.summarise(ev, legs, len(sigs), n_failed, complete)
    return legs


def scan(pop, items, known):
    chain = CHAIN_OF[pop]
    random.seed(20261005)
    items = [it for it in items if it[0] not in known]
    if len(items) > SAMPLE[pop]:
        items = random.sample(items, SAMPLE[pop])
    log(f"{pop}: scanning {len(items)} tokens on {chain} (orders at {ORDERS_RPM}/min ≈ {len(items) / ORDERS_RPM:.0f} min)")
    n_ev, n_tok_hit = 0, 0
    for i, (token, born, fallback_pool) in enumerate(items):
        if time.time() > T_END:
            log("RUN_SECONDS reached mid-scan")
            break
        t0 = time.time()
        o = bt.ds_orders(chain, token)
        if o is None:
            continue
        events = [("boost", b["paymentTimestamp"], b.get("amount")) for b in o.get("boosts", [])]
        events += [("profile", x["paymentTimestamp"], None) for x in o.get("orders", []) if x.get("type") == "tokenProfile" and x.get("status") == "approved"]
        if events:
            n_tok_hit += 1
            pairs = bt.ds_pairs(chain, [token])
            p = pairs.get(token) or {}
            prof = [x for x in o.get("orders", []) if x.get("type") == "tokenProfile"]
            pair_src = "dexscreener"
            if not p.get("pairAddress") and fallback_pool:
                p = dict(p, pairAddress=fallback_pool, dexId=(p.get("dexId") or "pumpfun-curve" if pop == "pump_launch" else "gt-pool"))
                pair_src = "fallback"
                bt.STATS["pair_fallback"] += 1
            for kind, pay, amount in events:
                eid = (f"{token}:{pay}" if kind == "boost" else f"{token}:p{pay}") if chain == "solana" else \
                      (f"{chain}:{token}:{pay}" if kind == "boost" else f"{chain}:{token}:p{pay}")
                row = bt.event_row_template()
                row.update(event_id=eid, chain=chain, token=token, amount=amount, channel="backfill", seen_at=pay,
                           payment_ts=pay, our_lag_ms=None, pair_address=p.get("pairAddress"), dex_id=p.get("dexId"),
                           pair_created_at=p.get("pairCreatedAt"),
                           pair_age_s=(pay - p["pairCreatedAt"]) / 1000 if p.get("pairCreatedAt") else None,
                           profile_status=(prof[-1]["status"] if prof else None),
                           profile_paid_ts=(max(x["paymentTimestamp"] for x in prof) if prof else None),
                           n_prior_boosts=sum(1 for b in o.get("boosts", []) if b["paymentTimestamp"] < pay),
                           tape_status=("pending" if (chain in bt.TAPE_CHAINS and p.get("pairAddress")) else
                                        ("no_pair" if chain in bt.TAPE_CHAINS else "no_tape_chain")),
                           created_at=int(time.time()))
                row["kind"] = kind
                row["population"] = pop
                ev = bt.Event(event_id=eid, chain=chain, token=token, amount=amount, total=None, channel="backfill",
                              seen_at=pay / 1000, payment_ts=pay, pair=p.get("pairAddress"), dex=p.get("dexId"),
                              tape_status=row["tape_status"], followups_done=set(), row=row)
                if ev.tape_status == "pending" and time.time() - pay / 1000 > TAPE_MAX_AGE_D * 86400:
                    ev.tape_status = "too_old"
                    row["tape_status"] = "too_old"
                legs = tape_for(ev) if ev.tape_status == "pending" else []
                bt.SINK_OBJ.write("boost_trades", legs, "event_id,sig,wallet,side")
                bt.SINK_OBJ.write("boost_events", [row], "event_id")
                n_ev += 1
                r = row
                log(f"{pop} {kind} {token[:10]} pay {pay} tape {r['tape_status']} first_buy_lag {r['first_buy_lag_s']} "
                    f"buys60 {r['buys_60']} clusters {r['clusters_60']} tipped {r['tipped_60']} marker {r['marker_60']} helius {bt.helius_calls}")
        if i % 100 == 0:
            log(f"{pop} progress {i}/{len(items)} tokens, hits {n_tok_hit}, events {n_ev}, helius {bt.helius_calls}/{bt.HELIUS_BUDGET}")
        time.sleep(max(0.0, 60.0 / ORDERS_RPM - (time.time() - t0)))
    log(f"{pop} DONE tokens {len(items)} hits {n_tok_hit} events {n_ev}")


if __name__ == "__main__":
    known = known_tokens()
    log(f"known backfilled tokens {len(known)}")
    for pop in POPULATIONS:
        items = population(pop)
        log(f"{pop}: population {len(items)}")
        scan(pop, items, known)
    log("END", dict(bt.STATS), "helius", bt.helius_calls)
    if bt.STATS["write_fail"]:
        raise SystemExit("failed writes")
