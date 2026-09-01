#!/usr/bin/env python3
"""WALLET-LEVEL TAPE for Robinhood Chain — buyer breadth, concentration, and wash structure.

WHY
---
Everything we currently screen on describes a token's PRICE (GMGN board fields, OHLCV bars).
Nothing sees WHO is buying. The winner profile found on 2026-09-01 — a token trading below its own
ATH with real volume already flowing before the board picks it up — is a volume story, and volume
is the one signal a deployer is paid to manufacture: Bankr routes 59% of trading fees to the
creating wallet. A profile that cannot separate 300 real buyers from one wallet cycling 300 times
will preferentially buy the manufactured kind, because manufactured volume looks better on every
price-derived metric. Transfer logs are the only place that distinction lives.

POINT-IN-TIME BY CONSTRUCTION
-----------------------------
Every row covers [as_of - WINDOW_S, as_of) and reads nothing after `as_of`, so it is admissible as
a feature for a decision made AT `as_of`. Case rows use as_of = the token's FIRST board sighting —
the instant a front-running model must fire before. A feature measured after t0 predicts the
outcome for mechanical reasons and is not tradeable (D-POSTOBS): board persistence correlates with
returns at rho=+0.30 on Solana, yet entering on the 3rd sighting instead of the 1st turned +24.3%
into -8.5%.

CONTROLS ARE SAMPLED FROM THE RISK SET (E1, E10)
------------------------------------------------
Control tasks are drawn from `rh_launches` and evaluated at an `as_of` drawn from the CASE
timestamp distribution, so both arms are measured at comparable moments with identical feature
quality (E2). Whether a control is *eligible* at that instant (i.e. it had not yet trended) is left
to analysis — this file never filters on having trended, because doing so would make the control
pool mean "never trended" rather than "had not trended yet".

Env: SUPABASE_URL, SUPABASE_KEY.
     WINDOW_S (default 21600 = 6h), MAX_CALLS (default 2000), MAX_TOKENS (default 400),
     CONTROL_FRAC (default 0.4), LOG_CAP (default 12000), PASS_INTERVAL (600), RUN_SECONDS (18000)
"""
import os, random, time

import rh_chain as C

WINDOW_S = int(os.environ.get("WINDOW_S", "21600"))
MAX_CALLS = int(os.environ.get("MAX_CALLS", "2000"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "400"))
CONTROL_FRAC = float(os.environ.get("CONTROL_FRAC", "0.4"))
LOG_CAP = int(os.environ.get("LOG_CAP", "12000"))
# How far back to look for prior holders when the token's birth block is unknown.
PRIOR_LOOKBACK_S = int(os.environ.get("PRIOR_LOOKBACK_S", "604800"))   # 7 days
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "600"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
ZERO = "0x" + "0" * 40


def decode(logs):
    """(from, to, value) per Transfer. ERC-721 Transfers carry the id as a 4th TOPIC and an empty
    data field — decoding those as a value would invent enormous fake volume, so they are skipped
    rather than coerced."""
    out = []
    for lg in logs:
        tps = lg.get("topics") or []
        if len(tps) != 3:
            continue
        d = (lg.get("data") or "0x")[2:]
        if not d:
            continue
        try:
            v = int(d[:64], 16)
        except ValueError:
            continue
        out.append((C.topic_addr(tps[1]), C.topic_addr(tps[2]), v,
                    int(lg["blockNumber"], 16)))
    return out


def infer_pool(tr):
    """The venue is the highest-degree counterparty.

    Deriving it rather than reading `trending_pools` is deliberate: controls have no pool row (they
    have never trended), and using a per-mint label resolved TODAY to classify a past trade is
    look-ahead (D20). Degree share is stored so analysis can drop tokens where the inference is
    weak instead of silently trusting it.
    """
    if not tr:
        return None, 0.0
    deg = {}
    for a, b, _v, _bn in tr:
        for x in (a, b):
            if x != ZERO:
                deg[x] = deg.get(x, 0) + 1
    if not deg:
        return None, 0.0
    pool, d = max(deg.items(), key=lambda kv: kv[1])
    return pool, d / max(len(tr), 1)


def features(mint, as_of, window_s, tr, truncated, creator, pool, pool_share, prior):
    """`prior` = wallets that already held/traded this token BEFORE the window (for new_wallet_rate)."""
    buys, sells = {}, {}
    buy_n = sell_n = 0
    vol = 0.0
    circ_v = 0.0
    tot_v = 0.0
    creator_v = 0.0
    wallets = set()
    edges = set()
    self_loops = 0
    for a, b, v, _bn in tr:
        tot_v += v
        vol += v
        if a != ZERO:
            wallets.add(a)
        if b != ZERO:
            wallets.add(b)
        if creator and creator in (a, b):
            creator_v += v
        if pool and a == pool and b != ZERO:            # pool -> wallet == a BUY
            buys[b] = buys.get(b, 0) + v
            buy_n += 1
        elif pool and b == pool and a != ZERO:          # wallet -> pool == a SELL
            sells[a] = sells.get(a, 0) + v
            sell_n += 1
        elif a != ZERO and b != ZERO:
            circ_v += v                                  # wallet <-> wallet, bypassing the venue
            if (b, a) in edges:
                self_loops += 1
            edges.add((a, b))
    wallets.discard(pool)
    tb = sum(buys.values()) or 1.0
    top = sorted(buys.values(), reverse=True)
    hhi = sum((x / tb) ** 2 for x in buys.values()) if buys else None
    both = set(buys) & set(sells)
    fresh = [w for w in buys if w not in prior] if buys else []
    return {
        "mint": mint, "as_of": int(as_of), "window_s": int(window_s),
        "n_logs": len(tr), "truncated": bool(truncated),
        "pool": pool, "pool_degree_share": round(pool_share, 4),
        "n_wallets": len(wallets), "n_buyers": len(buys), "n_sellers": len(sells),
        "buy_count": buy_n, "sell_count": sell_n,
        "top1_buy_share": (top[0] / tb) if top else None,
        "top5_buy_share": (sum(top[:5]) / tb) if top else None,
        "buyer_hhi": hhi,
        "round_trip_rate": (len(both) / len(buys)) if buys else None,
        "circular_rate": (circ_v / tot_v) if tot_v else None,
        "self_loop_n": self_loops,
        "creator_share": (creator_v / tot_v) if (tot_v and creator) else None,
        "new_wallet_rate": (len(fresh) / len(buys)) if buys else None,
        "net_buy_ratio": (buy_n / (buy_n + sell_n)) if (buy_n + sell_n) else None,
        "vol_tokens": float(vol),
        "computed_at": int(time.time()),
    }


def measure(mint, as_of, window_s, creator=None, born_block=None):
    """One token, one window. Returns a feature row or None when the window is unreadable."""
    b_lo, b_hi = C.ts_to_blk(as_of - window_s), C.ts_to_blk(as_of)
    logs = C.get_logs({"address": mint, "topics": [C.TRANSFER],
                       "fromBlock": hex(max(0, b_lo)), "toBlock": hex(max(0, b_hi))})
    tr = decode(logs)
    # A token that did not trade in the window is an OBSERVATION, not a missing row.
    #
    # The first version returned None here, and it silently emptied the control arm: measured
    # 2026-09-01, rh_tape held 106 case rows and ZERO control rows, because a random launch usually
    # has no transfers in a 6h window and every one of them was dropped. That is the worst possible
    # bias for this study — it keeps only the ACTIVE controls, so cases and controls end up looking
    # alike on exactly the volume/breadth features the winner profile keys on, and the arm that is
    # supposed to prove discrimination quietly proves nothing (E2: both arms need identical feature
    # quality; E4: report drop-out rather than hiding it).
    #
    # Zero-filling is only safe because `None` here means the RPC SUCCEEDED and returned no logs.
    # A failed query raises out of get_logs above and never reaches this line, so we can never
    # record "no trading" when what actually happened was "we could not ask" (F5).
    # `features()` already yields real zeros for the counts and NULL for ratios that are undefined
    # without trades, which is exactly the right shape for a silent token.
    truncated = len(logs) > LOG_CAP
    pool, share = infer_pool(tr)
    # Wallets active BEFORE the window, so "new wallet" means new to this token rather than merely
    # new to our sample — it is what separates organic buyer growth from one cohort recycling.
    #
    # The first version scanned from block 0. On a chain at 860k blocks/day that range always fails,
    # the exception was swallowed, `prior` came back empty, and EVERY buyer scored as new:
    # new_wallet_rate was 1.000 on 4 of 4 rows — a dead feature wearing a plausible value (D17).
    # A rejected request is evidence about the request, not about the world (F5). So: bound the
    # lookback to the token's own birth when we know it, cap it otherwise, and record whether the
    # scan actually succeeded. When it did not, the feature is NULL, never a default.
    prior, prior_ok = set(), False
    floor = born_block if born_block else b_lo - int(PRIOR_LOOKBACK_S / max(C.block_time()[2], 1e-9))
    floor = max(0, min(floor, b_lo - 1))
    if b_lo - 1 >= floor:
        try:
            pre = C.get_logs({"address": mint, "topics": [C.TRANSFER],
                              "fromBlock": hex(floor), "toBlock": hex(b_lo - 1)})
            for a, b, _v, _bn in decode(pre):
                prior.add(a)
                prior.add(b)
            prior_ok = True
        except Exception:
            prior_ok = False
    else:
        prior_ok = True                 # window starts at birth: nobody held it before, legitimately
    row = features(mint, as_of, window_s, tr, truncated, creator, pool, share, prior)
    row["prior_ok"] = prior_ok
    if not prior_ok:
        row["new_wallet_rate"] = None
    return row


def build_queue():
    """Case tasks from the board (as_of = first sighting); control tasks from rh_launches at an
    as_of drawn from the CASE timestamp distribution — incidence-density sampling, so both arms
    are measured at comparable moments (E1/E2)."""
    done = {(r["mint"], r["as_of"]) for r in
            C.sb_all(f"/rh_tape?window_s=eq.{WINDOW_S}&select=mint,as_of")}
    snaps = C.sb_all("/trending_snapshots?source=eq.gmgn_rh&select=mint,captured_at"
                     "&order=captured_at.asc")
    first = {}
    for r in snaps:
        first.setdefault(r["mint"].lower(), r["captured_at"] / 1000)
    now = time.time()
    cases = [(m, int(t), "case") for m, t in first.items()
             if (m, int(t)) not in done and t < now - 300]
    cases.sort(key=lambda x: -x[1])                       # newest board entries first
    n_ctrl = int(MAX_TOKENS * CONTROL_FRAC)
    n_case = MAX_TOKENS - n_ctrl
    cases = cases[:n_case]
    controls = []
    if n_ctrl > 0 and first:
        launches = C.sb_all("/rh_launches?select=mint,created_at,creator")
        case_ts = [int(t) for t in first.values()]
        pool_ = [r for r in launches if r["mint"].lower() not in first]
        random.shuffle(pool_)
        for r in pool_:
            if len(controls) >= n_ctrl:
                break
            t = random.choice(case_ts)
            # a control must EXIST at the moment it is measured, and have a full window of history
            if r["created_at"] > t - WINDOW_S:
                continue
            if (r["mint"], t) in done:
                continue
            controls.append((r["mint"].lower(), t, "control"))
    # INTERLEAVE, don't append — the same defect trending_bars.py already carries a comment about.
    # `cases + controls` puts every control behind 240 cases, so a pass that runs out of calls or
    # wall-clock never reaches one: measured, rh_tape held 106 case rows and 0 control rows. Round
    # robin so both arms advance in roughly CONTROL_FRAC proportion on every pass.
    out, i, j = [], 0, 0
    step = (1 - CONTROL_FRAC) / CONTROL_FRAC if 0 < CONTROL_FRAC < 1 else None
    if step is None:
        return controls if CONTROL_FRAC >= 1 else cases
    acc = 0.0
    while i < len(cases) or j < len(controls):
        if i < len(cases) and (acc < step or j >= len(controls)):
            out.append(cases[i]); i += 1; acc += 1
        elif j < len(controls):
            out.append(controls[j]); j += 1; acc = 0.0
        else:
            break
    return out


CREATOR, BORN = {}, {}


def load_launch_meta():
    """Deployer and birth block per known launch. The deployer drives creator_share (the
    fee-farming question) and the birth block bounds the prior-holder scan."""
    CREATOR.clear(); BORN.clear()
    for r in C.sb_all("/rh_launches?select=mint,creator,block_number"):
        m = r["mint"].lower()
        if r.get("creator"):
            CREATOR[m] = r["creator"].lower()
        if r.get("block_number"):
            BORN[m] = int(r["block_number"])


def one_pass():
    C.refresh_head()
    load_launch_meta()
    q = build_queue()
    if not q:
        print("  queue empty", flush=True)
        return 0
    rows, skipped = [], 0
    for mint, as_of, arm in q:
        if C.calls() >= MAX_CALLS:
            break
        try:
            f = measure(mint, as_of, WINDOW_S, creator=CREATOR.get(mint),
                        born_block=BORN.get(mint))
        except Exception as ex:
            skipped += 1
            if skipped <= 3:
                print(f"    {mint[:14]}.. {ex!r}", flush=True)
            continue
        if not f:
            skipped += 1
            continue
        f["arm"] = arm
        rows.append(f)
        if len(rows) >= 100:                              # C3: flush on the same cadence as work
            C.sb_write("/rh_tape?on_conflict=mint,as_of,window_s", rows)
            print(f"    .. {len(rows)} rows flushed, {C.calls()} rpc calls", flush=True)
            rows = []
    wrote = C.sb_write("/rh_tape?on_conflict=mint,as_of,window_s", rows)
    ncase = sum(1 for x in q if x[2] == "case")
    print(f"  pass: queue {len(q)} ({ncase} case / {len(q) - ncase} control), "
          f"{wrote} final rows, {skipped} unreadable, {C.calls()} rpc, {C.throttled()} x429",
          flush=True)
    return wrote


def main():
    end = time.time() + RUN_SECONDS
    fails = 0
    while True:
        try:
            one_pass()
            fails = 0
        except Exception as ex:
            fails += 1
            print(f"pass error ({fails}): {ex!r}", flush=True)
            if fails >= 5:
                raise SystemExit(f"{fails} consecutive pass failures — refusing to zombie")
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print("done", flush=True)


if __name__ == "__main__":
    main()
