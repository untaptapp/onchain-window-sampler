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
# Front-run leads, seconds. A lead row measures a token at `board_sighting - L`, which is the only
# way to tell prediction from description. Bounded by the token's age at board entry, which on this
# chain is p25 2.9 min / p50 6.6 min / p75 12.4 min -- so the grid is seconds-to-minutes. Each lead
# is scored on the cases old enough to admit it, which is a DIFFERENT subpopulation per lead.
# The grid spans SECONDS to an HOUR because there are two different strategies here with two
# different ceilings. 90.0% of board entrants are fresh launches (<30 min old, median 5.6 min), and
# for those the lead can only ever be minutes. But the REVIVAL cohort -- a token woken from the dead,
# which is Track A's whole thesis -- is 2.3% of dated entrants at >=6h old (p50 14.0h, max 3.2d), and
# for those a 15/30/60-minute lead is entirely feasible. A single short grid would have silently
# tested only the fresh strategy. Each L is filtered per token by `age - L >= MIN_LEAD_EXPOSURE_S`,
# so long leads simply do not apply to young tokens rather than producing garbage rows.
LEADS = [int(x) for x in
         os.environ.get("LEADS", "60,120,300,900,1800,3600").split(",") if x.strip()]
# A lead row still needs a window to measure. Stepping back to within a few seconds of birth leaves
# nothing but the mint Transfer, which is not an observation about trading.
MIN_LEAD_EXPOSURE_S = int(os.environ.get("MIN_LEAD_EXPOSURE_S", "60"))
# Share of the per-pass CASE budget reserved for lead rows. Board-entry rows cannot be starved
# either -- they are the anchor every lead is measured relative to -- so this is a split, not a
# priority.
LEAD_FRAC = float(os.environ.get("LEAD_FRAC", "0.5"))
# Rows per flush. This was 100, chosen when a token cost ~3 RPC calls and a couple of seconds. A
# token now costs ~20 calls and 20-47s (bracketed ts_to_blk, the wallet probes, 429 backoff), so 100
# rows is ~78 MINUTES between writes -- and a pass interrupted before its first flush loses
# everything and looks, from outside, exactly like a collector doing nothing. I cancelled and
# redispatched this job four times on that mistaken reading, resetting the clock each time. Flush
# cadence must track the cost of the work, not a number fixed when the work was cheap (C3).
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY", "20"))
# WALLET EXPERIENCE — "new to the CHAIN", not "new to this token".
#
# `new_wallet_rate` is degenerate by construction: it asks whether a buyer held THIS token before,
# and at launch nobody did, so it pins at 1.000 on 85% of cases. The discriminating question is
# whether the buyer is a freshly created wallet, which is the sybil-ladder fingerprint (S24).
#
# The obvious implementation -- eth_getTransactionCount(wallet, block_at_as_of) -- is IMPOSSIBLE
# here: this RPC is not an archive node. Measured 2026-09-02, state resolves ~1,000 blocks back
# (~100 s) and anything deeper returns {'code': -32000, 'message': 'metadata is not found'}. Nonce
# at 'latest' is not a substitute: a wallet that transacted 500 times AFTER the event would read as
# experienced at the moment it bought, which is a post-entry observable (D-POSTOBS).
#
# LOGS are permanent, so ask the log index instead: did this wallet receive any ERC-20 transfer
# before the window opened? Two tiers, because most wallets resolve on the first.
# The lookback is BOUNDED and the feature is defined by it: "did this wallet receive an ERC-20
# transfer in the LOOKBACK before the window opened". The first version fell back to scanning from
# block 0 whenever the recent slice was empty -- which is precisely the case we care about, a wallet
# with no recent activity -- and that scan is unbounded: it times out on the node and get_logs
# bisects, so one probe could run for minutes. Deployed, it wrote ZERO rows in 70 minutes and
# starved the lead backfill, with a green workflow throughout. A bounded window answers the same
# question at fixed cost; a wallet with nothing in 7 days before buying is the signal either way.
# K=3, not 6. Each probe costs ~2.8s / 2.8 calls even bounded, so K multiplies directly into the
# per-token cost and therefore into how long the 4,872-task LEAD backfill takes -- and the front-run
# question the leads answer is worth more right now than a wider buyer sample. Raise it once the
# lead backlog is drained.
WALLET_PROBE_K = int(os.environ.get("WALLET_PROBE_K", "3"))     # 0 disables the probe entirely
WALLET_LOOKBACK_BLOCKS = int(os.environ.get("WALLET_LOOKBACK_BLOCKS", "6000000"))   # ~7 days
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


def wallet_prior(w, b_lo):
    """Did wallet `w` receive any ERC-20 transfer strictly BEFORE block b_lo?

    True/False, or None when we could not find out — never a default. "No prior activity" is the
    interesting case (a wallet created to buy this launch), so defaulting an unanswerable query to
    either value would manufacture the very signal being tested (F5).
    """
    if b_lo <= 0 or WALLET_PROBE_K <= 0:
        return None
    topic = "0x" + "00" * 12 + w[2:]
    lo = max(0, b_lo - WALLET_LOOKBACK_BLOCKS)
    if lo >= b_lo:
        return None
    try:
        return bool(C.get_logs({"topics": [C.TRANSFER, None, topic],
                                "fromBlock": hex(lo), "toBlock": hex(b_lo - 1)}))
    except Exception:
        return None


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
    # Probe the largest buyers by volume. Sampling the top K keeps the cost bounded;
    # `buyer_probe_n` records how many actually answered, so a partial probe can never be read as
    # a whole-cohort measurement.
    probed = []
    for w in (row.get("top_buyers") or [])[:WALLET_PROBE_K]:
        if C.calls() >= MAX_CALLS:
            break
        v = wallet_prior(w, b_lo)
        if v is not None:
            probed.append(v)
    row["buyer_probe_n"] = len(probed)
    row["buyer_prior_rate"] = (sum(probed) / len(probed)) if probed else None
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
    # Both of these are append-only, so read incrementally and keep the maps between passes.
    hw = _META_HW["done"]
    for r in C.sb_all(f"/rh_tape?window_s=eq.{WINDOW_S}&select=mint,as_of,computed_at"
                      f"&order=computed_at.asc,mint.asc,as_of.asc"
                      + (f"&computed_at=gte.{hw}" if hw else "")):
        _DONE.add((r["mint"], r["as_of"]))
        _META_HW["done"] = max(_META_HW["done"], int(r.get("computed_at") or 0))
    # captured_at is a bigint of MILLISECONDS. Keeping the high-water mark as a float rendered it
    # as "1788349257495.0" in the filter and PostgREST rejected the whole page with 22P02 -- loudly,
    # because sb_all raises, which is the only reason this was a one-line fix and not a silent
    # short read. Keep the mark in the column's own type and unit.
    hws = int(_META_HW["snap"])
    for r in C.sb_all("/trending_snapshots?source=eq.gmgn_rh&select=mint,captured_at"
                      "&order=captured_at.asc,mint.asc"
                      + (f"&captured_at=gte.{hws}" if hws else "")):
        _FIRST.setdefault(r["mint"].lower(), r["captured_at"] / 1000)
        _META_HW["snap"] = max(int(_META_HW["snap"]), int(r["captured_at"]))
    done, first = _DONE, _FIRST
    now = time.time()
    cases = [(m, int(t), "case", 0) for m, t in first.items()
             if (m, int(t)) not in done and t < now - 300]
    cases.sort(key=lambda x: -x[1])                       # newest board entries first

    # LEAD ROWS — the actual front-running test.
    #
    # A row measured at as_of = first board sighting cannot distinguish "the tape PREDICTS board
    # entry" from "the tape DESCRIBES it": the token is on the board largely because it is trading,
    # and GMGN's inclusion criteria are undocumented. The only way to tell is to measure the same
    # token at as_of - L and ask whether the separation survives a tradeable lead (D-POSTOBS).
    #
    # The lead is bounded by the token's own age at board entry, and on this chain that is SHORT:
    # measured over 2,262 dated board mints, age at entry is p25 2.9 min, p50 6.6 min, p75 12.4 min.
    # A 15-minute lead is physically possible for only 16.9% of cases. So the grid is seconds-to-
    # minutes, not hours -- and each lead is evaluated on a DIFFERENT subpopulation (the cases old
    # enough to admit it), which is a composition shift that must be reported with n, never pooled.
    leads = []
    for m, t, _a, _l in list(cases) + [(m, int(t), "case", 0) for m, t in first.items()]:
        b = BORN_TS.get(m)
        if b is None:
            continue
        age = int(t) - b
        for L in LEADS:
            if age - L < MIN_LEAD_EXPOSURE_S:     # no window left after stepping back
                continue
            a_l = int(t) - L
            if (m, a_l) in done or a_l > now - 300:
                continue
            leads.append((m, a_l, "case", L))
    seen_l = set()
    leads = [x for x in leads if not (x[:2] in seen_l or seen_l.add(x[:2]))]
    leads.sort(key=lambda x: -x[1])
    _lead_supply[:] = [len(leads), len(cases)]
    # RESERVE a share of the case budget for leads instead of ordering entries first.
    #
    # "Entries first, leads with the leftovers" starved the leads completely: the entry backlog sits
    # at ~239 against n_case=240, so leads got ONE slot per pass, and the backlog does not drain
    # because new board mints arrive about as fast as the collector measures them (56 rows/hour
    # measured). An unbounded-priority queue starving the other class of work is exactly C0h, and it
    # is invisible from outside -- the workflow is green, rows are being written, and the new data
    # type simply never appears. Each class gets a floor; whichever is short donates to the other.
    n_ctrl = int(MAX_TOKENS * CONTROL_FRAC)
    n_case = MAX_TOKENS - n_ctrl
    n_lead = int(n_case * LEAD_FRAC)
    n_entry = n_case - n_lead
    take_e = cases[:n_entry]
    take_l = leads[:n_lead + (n_entry - len(take_e))]          # unused entry slots go to leads
    take_e = cases[:n_case - len(take_l)]                      # and unused lead slots come back
    # INTERLEAVE entry and lead tasks, do not concatenate. A quota reserves SLOTS; it does not
    # reserve WORK. `take_e + take_l` puts every lead behind 120 entries, so a pass that exhausts
    # MAX_CALLS partway through still never reaches one -- the same starvation the quota was added
    # to fix, and the identical defect this file already documents for cases vs controls.
    mixed, i, j = [], 0, 0
    while i < len(take_e) or j < len(take_l):
        if i < len(take_e):
            mixed.append(take_e[i]); i += 1
        if j < len(take_l):
            mixed.append(take_l[j]); j += 1
    cases = mixed
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
        for m, t, _arm, lead in cases:
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
                # Same point in the control's own life, so a lead row on the case side is compared
                # against a control observed the same distance before ITS matched moment.
                controls.append((cm, t_c, "control", lead))
                break
            else:
                unmatched += 1
    if unmatched:
        print(f"  {unmatched} cases could not be age-matched (no candidate in the birth bracket)",
              flush=True)
    if _lead_supply:
        print(f"  lead supply: {_lead_supply[0]:,} tasks outstanding across L={LEADS} "
              f"(entry backlog {_lead_supply[1]:,})", flush=True)
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
# High-water marks for the incremental caches below. Every one of these tables only ever GAINS rows,
# so re-reading them whole on every pass is pure waste that grows with the study: measured
# 2026-09-02, one pass spent ~121 HTTP round trips (81 pages of rh_launches + 33 of snapshots + 7 of
# rh_tape) before doing any work, and rh_launches alone adds ~8 pages/day forever (A6). Keep the
# maps across passes and fetch only what is new.
_META_HW = {"launch": 0, "snap": 0.0, "done": 0}
_FIRST, _DONE = {}, set()
_lead_supply = []          # [outstanding lead tasks, outstanding board-entry tasks] for logging


def load_launch_meta():
    """Deployer, birth block and birth TIME per known launch. The deployer drives creator_share
    (the fee-farming question), the birth block bounds the prior-holder scan and clamps the
    measurement window, and the birth time is what controls are age-matched on."""
    # Incremental: only rows first seen since the last pass. `creator` is backfilled onto existing
    # rows by rh_universe, so re-read a small trailing overlap rather than assuming append-only
    # content -- the overlap is cheap and a missed creator only weakens creator_share, never
    # corrupts an identity.
    hw = _META_HW["launch"]
    flt = f"&first_seen_at=gte.{hw - 3600}" if hw else ""
    got = 0
    for r in C.sb_all("/rh_launches?select=mint,creator,block_number,created_at,birth_kind,"
                      f"first_seen_at&order=first_seen_at.asc,mint.asc{flt}"):
        got += 1
        _META_HW["launch"] = max(_META_HW["launch"], int(r.get("first_seen_at") or 0))
        m = r["mint"].lower()
        if r.get("creator"):
            CREATOR[m] = r["creator"].lower()
        # A mint whose birth_kind is not 'launchpad' must be REMOVED if it was cached earlier as
        # one: rh_universe can relabel a row, and a stale launchpad birth would silently re-admit
        # the pool-timestamp bug this cache is meant to keep out.
        if r.get("birth_kind") != "launchpad":
            BORN.pop(m, None); BORN_TS.pop(m, None)
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
    rows, skipped, done_n, t_pass = [], 0, 0, time.time()
    for mint, as_of, arm, lead in q:
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
        # 0 = measured AT board entry; >0 = measured that many seconds BEFORE it. Pooling the two
        # would mix a description of board inclusion with a prediction of it.
        f["lead_s"] = int(lead)
        rows.append(f)
        done_n += 1
        if done_n % 10 == 0:
            print(f"    .. {done_n}/{len(q)} measured, {C.calls()} rpc, {C.throttled()} x429, "
                  f"{time.time() - t_pass:.0f}s", flush=True)
        if len(rows) >= FLUSH_EVERY:                      # C3: flush on the same cadence as work
            C.sb_write("/rh_tape?on_conflict=mint,as_of,window_s", rows)
            print(f"    .. {len(rows)} rows flushed, {C.calls()} rpc calls", flush=True)
            rows = []
    wrote = C.sb_write("/rh_tape?on_conflict=mint,as_of,window_s", rows)
    ncase = sum(1 for x in q if x[2] == "case")
    nlead = sum(1 for x in q if x[3])
    print(f"  pass: queue {len(q)} ({ncase} case / {len(q) - ncase} control, {nlead} lead), "
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
