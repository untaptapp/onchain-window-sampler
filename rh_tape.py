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
# Controls are matched to cases on BIRTH TIME as well as age; this is the half-width of the birth
# bracket a candidate control must fall inside. Launch supply is ~21k/day (~15/min), so +/-30 min
# offers ~900 candidates per case — deep enough that matching almost never fails.
BIRTH_TOL_S = int(os.environ.get("BIRTH_TOL_S", "1800"))
# Buyer addresses retained per row, largest by volume first. Within-token aggregates cannot express
# "the same cohort bought the last 40 launches", which is where sybil/farm signal actually lives.
TOP_BUYERS_K = int(os.environ.get("TOP_BUYERS_K", "20"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "600"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
ZERO = "0x" + "0" * 40
# A transfer to a burn sink is not a purchase. Counting one as a buy inflates top1_buy_share and
# buyer_hhi — the two features the wash-trading screen leans on — and puts a non-wallet at the head
# of top_buyers. ZERO was already excluded; the 0x…dead convention was not, and it showed up as the
# top "buyer" in live output.
BURN = {ZERO, "0x" + "0" * 36 + "dead", "0x" + "d" * 40}


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
        if a not in BURN:
            wallets.add(a)
        if b not in BURN:
            wallets.add(b)
        if creator and creator in (a, b):
            creator_v += v
        if pool and a == pool and b not in BURN:        # pool -> wallet == a BUY
            buys[b] = buys.get(b, 0) + v
            buy_n += 1
        elif pool and b == pool and a not in BURN:      # wallet -> pool == a SELL
            sells[a] = sells.get(a, 0) + v
            sell_n += 1
        elif a not in BURN and b not in BURN:
            circ_v += v                                  # wallet <-> wallet, bypassing the venue
            if (b, a) in edges:
                self_loops += 1
            edges.add((a, b))
    wallets.discard(pool)
    tb = sum(buys.values()) or 1.0
    top = sorted(buys.values(), reverse=True)
    top_addrs = [w for w, _v in sorted(buys.items(), key=lambda kv: -kv[1])[:TOP_BUYERS_K]]
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
        "top_buyers": top_addrs,
        "computed_at": int(time.time()),
    }


def measure(mint, as_of, window_s, creator=None, born_block=None, born_ts=None):
    """One token, its life up to `as_of`, capped at `window_s`.

    WINDOW_S IS A CAP, NOT A FIXED LOOKBACK. The original fixed 6h window silently made the two arms
    incomparable: 92% of board-sighted tokens are under half an hour old when they are sighted, so
    their "6h of pre-entry history" was overwhelmingly time before the token existed, while controls
    were required to be >=6h old and were therefore measured a full day after launch, when nearly
    every memecoin is already dead (measured: cases median age -0.01h, controls 27.1h; activity
    12.0% at 6-24h decaying to 0% past 72h). Clamping the window to the token's own birth, recording
    `exposure_s`, and matching controls on age is what makes a case/control contrast mean anything.
    """
    b_lo, b_hi = C.ts_to_blk(as_of - window_s), C.ts_to_blk(as_of)
    if born_block:
        b_lo = max(b_lo, int(born_block))     # nothing exists before birth; do not pay to scan it
    if b_hi < b_lo:
        # `as_of` precedes the recorded birth, so the clamp inverted the range. eth_getLogs answers
        # an inverted range with an empty list, which this function would then record as a
        # legitimately SILENT token — a wrong fact about the world manufactured by a wrong fact
        # about our own metadata (F5). Measured: 0.6% of launchpad-dated board mints are sighted
        # before their creation event fires. Refuse the clamp and mark the birth unknown, so the
        # row reports NULL age/exposure instead of a fabricated zero.
        b_lo = C.ts_to_blk(as_of - window_s)
        born_ts = None
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
    # age_s is THE confounder in this design and exposure_s is what makes count features
    # comparable. Both are stored rather than re-derived, so no downstream query can forget them.
    if born_ts is not None:
        row["age_s"] = int(as_of - born_ts)
        row["exposure_s"] = int(min(window_s, max(0, as_of - born_ts)))
    else:
        row["age_s"] = None
        row["exposure_s"] = None
    if not prior_ok:
        row["new_wallet_rate"] = None
    return row


def build_queue():
    """Case tasks from the board (as_of = first sighting); control tasks MATCHED to them on both
    birth time and token AGE.

    WHY MATCHING ON CALENDAR TIME ALONE WAS NOT ENOUGH
    --------------------------------------------------
    The previous version drew each control's `as_of` from the case timestamp distribution — correct
    incidence-density sampling on the calendar — and then required `created_at <= t - WINDOW_S` so
    the control had a full window of history. That second rule, applied to controls ONLY, silently
    destroyed the design: it forces every control to be at least WINDOW_S (6h) old, while 97% of
    cases are YOUNGER than the window. Measured 2026-09-02 over 3,524 joined rows:

        age at measurement   n      cases  controls  active
        <0.5h              1819     1818        1     54.3%
        0.5-2h               86       86        0    100.0%
        2-6h                 21       21        0    100.0%
        6-24h               715       38      677     12.0%
        24-72h              864       20      844      5.2%

    There is no age band holding a usable number of BOTH arms — complete separation on the strongest
    confounder. Any model fitted on that learns "is this token minutes old", which is the sampling
    rule, not a signal (E1). It also explains the "95% silent control arm": controls were not mostly
    stillborn, they were mostly measured a day late (controls-only activity 7.7% at 6-24h -> 3.2% at
    24-72h -> 0% past 72h).

    THE MATCH
    ---------
    For a case born at `b` and sighted at `t` (age `a = t - b`), draw a control born within
    BIRTH_TOL_S of `b` and measure it at `born + a`. Both arms then share an age AND a calendar
    moment, so `exposure_s` is equal by construction and the contrast is about the token rather than
    about the clock. Launch supply is ~15/min, so the risk set at any (b, a) is deep.

    A silent matched control is a REAL observation ("launched alongside it, never traded, never
    trended") and is kept. Only mistimed silence was ever the problem.
    """
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
    unmatched = 0
    if n_ctrl > 0 and cases:
        import bisect
        # Draw controls from BORN_TS, which load_launch_meta already restricted to LAUNCHPAD births.
        # Reading rh_launches again here would re-admit pool-derived rows, whose created_at is only
        # an upper bound: the control would be matched on a fake birth and then refused a date by
        # measure(), which is how 87% of the first live batch came back with a NULL age. One source
        # of truth for "when was this token born", used by both the match and the measurement.
        pool_ = sorted(((b, m) for m, b in BORN_TS.items() if m not in first), key=lambda x: x[0])
        births = [p[0] for p in pool_]
        used = set()
        for m, t, _arm in cases:
            if len(controls) >= n_ctrl:
                break
            b = BORN_TS.get(m)
            if b is None:                      # case predates the launch scan: cannot be matched
                unmatched += 1
                continue
            age = t - b
            if age < 0:
                # Sighted before its own creation event: the birth is wrong for this mint, so any
                # match built on it is wrong too. max(0, age) would have hidden that by silently
                # pairing it with a newborn control.
                unmatched += 1
                continue
            lo = bisect.bisect_left(births, b - BIRTH_TOL_S)
            hi = bisect.bisect_right(births, b + BIRTH_TOL_S)
            cand = pool_[lo:hi]
            if not cand:
                unmatched += 1
                continue
            random.shuffle(cand)
            for cb, cm in cand:
                if cm in used:
                    continue
                t_c = int(cb + age)
                # The window must have fully elapsed, or we would measure a partial one and record
                # it as complete — the same defect that made un-exited trades look like results.
                if t_c > now - 300:
                    continue
                if (cm, t_c) in done:
                    continue
                used.add(cm)
                controls.append((cm, t_c, "control"))
                break
            else:
                unmatched += 1
    if unmatched:
        print(f"  {unmatched} cases could not be age-matched (no candidate in the birth bracket)",
              flush=True)
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


CREATOR, BORN, BORN_TS = {}, {}, {}


def load_launch_meta():
    """Deployer, birth block and birth TIME per known launch. The deployer drives creator_share
    (the fee-farming question), the birth block bounds the prior-holder scan and clamps the
    measurement window, and the birth time is what controls are age-matched on."""
    CREATOR.clear(); BORN.clear(); BORN_TS.clear()
    for r in C.sb_all("/rh_launches?select=mint,creator,block_number,created_at,birth_kind"):
        m = r["mint"].lower()
        if r.get("creator"):
            CREATOR[m] = r["creator"].lower()
        # ONLY a launchpad row dates a birth. A `pair`-sourced row records when a pool appeared on
        # a shared AMM, which is an upper bound: 71.7% of amm_shared board mints were sighted BEFORE
        # it, median 14.4h before. Trusting it would (a) clamp the measurement window to well after
        # the token started trading, cutting off the real activity, and (b) hand build_queue a
        # negative age to match on. Leaving these absent makes both failures impossible: the window
        # falls back to the bounded lookback, and the case is reported as unmatchable rather than
        # matched wrongly.
        if r.get("birth_kind") != "launchpad":
            continue
        if r.get("block_number"):
            BORN[m] = int(r["block_number"])
        if r.get("created_at"):
            BORN_TS[m] = int(r["created_at"])


def one_pass():
    C.reset_calls()          # MAX_CALLS is a PER-PASS budget, not a lifetime one
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
                        born_block=BORN.get(mint), born_ts=BORN_TS.get(mint))
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
