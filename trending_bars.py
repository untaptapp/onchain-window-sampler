#!/usr/bin/env python3
"""Minute-resolution price paths for every trending mint — the backtest substrate.

WHY THIS EXISTS
---------------
The board feeds sample at 5 / 15 / 30 minutes. That is far too coarse to evaluate an EXIT rule:
measured on snapshot data a 20% trailing stop "fired" on only 6% of paths — not because the
drawdowns were absent, but because a 30-minute gap cannot see them. Every exit statistic computed
from snapshots is therefore a hold-return wearing a trailing-stop label, and MFE is quantised to
whenever we happened to look.

Minute bars carry HIGH and LOW — the intra-bar extremes a stop actually hits — so MFE/MAE,
time-to-peak, stops, trails and take-profits all become genuinely measurable.

Unlike Jupiter quotes (live-only, so our history is permanently un-costable), GeckoTerminal's
OHLCV endpoint supports `before_timestamp` paging, so this **backfills the entire history we have
already collected**. That is what makes a real backtest possible on existing data.

WHAT IT DOES
------------
1. Resolves each mint's deepest pool once, cached in `trending_pools`.
2. Pulls minute OHLCV covering [first trending sighting - PRE_MIN, +POST_H hours] into
   `trending_bars`, paging backwards when one 1000-bar page doesn't reach far enough.
3. Prioritises mints with the LEAST coverage, so a budgeted run always makes progress and the
   job is resumable across runs.

PRE_MIN defaults to 360 (6h BEFORE first sighting) on purpose. "First sighting" is not the
trending-entry event — a mint appears on GMGN a median +106 min after the 5-min feed sees it
(p10 -215, p90 +274), because each board has its own inclusion criteria and its own poll clock.
Collecting deep pre-history lets us (a) locate the objective volume onset from the bars themselves
rather than trusting any board's clock, and (b) backtest entering BEFORE the board lists a token —
which is the actual thesis. Because GeckoTerminal OHLCV is backfillable, this needs no streaming
infrastructure: the pre-trend counterfactual is answerable from history we can still fetch.

Covers BOTH arms of the prediction study: mints that reached a trending board (cases) and a random
sample of eligible mints from `candidate_universe` that did not (controls). Collecting bars only for
cases would break the study twice over — the model could learn "has bars" = case, and we could never
test whether a flagged token that never trends is profitable anyway.

Free + keyless. Reads trending_snapshots / candidate_universe, writes only trending_pools / trending_bars.

Env: SUPABASE_URL, SUPABASE_KEY. MAX_CALLS (default 900), SLEEP (default 2.1 -> ~28 req/min),
     PRE_MIN (default 360 = 6h before first sighting), POST_H (default 3 — bars are the largest
     storage cost in the project and the exit study never reads past ~2h),
     RUN_SECONDS (default 0 = single pass),
     MIN_OBS (default 1) — collect every board sighting; see the note at its definition for why
     a higher value is an outcome-correlated filter, not a budget saving.
"""
import json, os, time, urllib.request, urllib.error
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
MAX_CALLS = int(os.environ.get("MAX_CALLS", "900"))
SLEEP = float(os.environ.get("SLEEP", "2.1"))
PRE_MIN = int(os.environ.get("PRE_MIN", "360"))
# 3h, not 12h. Minute bars are the single largest storage cost in the project (425 B/row); at the
# observed case-arrival rate, 12h of post-entry bars per mint grows the DB by ~360-1,280 MB PER DAY,
# which blows a 500 MB budget in under a day. The exit study never reads past ~2h (MFE peaks around
# 52 min), so the extra 9h was pure cost. PRE_MIN stays at 6h — that is the pre-trend window the
# front-run counterfactual actually needs.
# POST_H was 3 on a 500 MB free-tier storage argument, and that decision cost a retracted finding:
# the CAPPED exit rule chosen later could not resolve inside 3h, so 60.5% of Track A trades were
# still open at the horizon and were being marked to market and scored as closed. On the Pro plan
# (8 GB) that constraint is gone, so the window is now set by what the RULES need, not by the disk.
# 6h covers Track B comfortably (93.6% of its trades close within 3h).
POST_H = float(os.environ.get("POST_H", "6"))
# Tokens at least a day old at first sighting (the "Track A" revival track) get a LONGER post
# window still, because that is the track whose rule needs the room. Track A is 21% of mints
# (771 of 3,659) — the share is 21% and not 82% only because GMGN reports open_timestamp: 0 for
# unknown, which coalesce() turns into a 1970 creation date; see trending_mint_age.
LONG_POST_H = float(os.environ.get("LONG_POST_H", "24"))
LONG_AGE_S = 86400
# Max hours of missing window any single mint may claim when competing for the call budget.
DEFICIT_CAP_H = float(os.environ.get("DEFICIT_CAP_H", "6"))
# Which snapshot sources are Solana. See the allowlist note in main().
SOL_SOURCES = os.environ.get("SOL_SOURCES", "gmgn,solanatracker,geckoterminal,fomoscan")
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "0"))
# MIN_OBS = 1: collect EVERY board sighting. This was 3 as a call-budget measure ("a mint seen
# once has no path to model"), and that was wrong twice over. A mint seen once has exactly the same
# price path as any other; what it has less of is BOARD data, and the only board row the analysis
# reads is the one at t0. Worse, "seen 3+ times" is "stayed on the trending board for ~45+ minutes"
# — information from AFTER the entry, and strongly correlated with the outcome it is used to
# select on: mean capped_net runs -18.3% at 3 sightings to +7.9% at 26+ (rho=+0.22), runner_net
# -29.2% to +22.4% (rho=+0.30). So a budget knob was silently choosing a winning sub-population,
# and its strength moved with our own polling reliability (37% of Track-A-qualifying mints reached
# nobs>=3 pre-freeze, only 13% post-freeze, because of an 11.4h polling hole). Batch resolution
# (resolve_pools_batch, 30 mints per call) removes the budget argument that justified it.
# Any subsetting that IS forced by budget must use a rule that cannot see the future — the queue
# order below is by seconds-of-missing-window and sighting recency, never by board persistence.
MIN_OBS = int(os.environ.get("MIN_OBS", "1"))
# Share of the call budget spent on CONTROL mints drawn from candidate_universe. Without this,
# minute bars exist only for tokens that reached a board, and the prediction study breaks two ways:
# (1) cases would have richer features than controls, so a model could trivially learn "has bars"
#     = case; and (2) we could never test the second success criterion — whether a flagged token
# that NEVER trends is still profitable — because we would have no price path for it.
UNIVERSE_FRAC = float(os.environ.get("UNIVERSE_FRAC", "0.4"))

# Retention is GLOBAL and must run from exactly one collector instance — see the call site.
PRUNE = os.environ.get("PRUNE", "1") not in ("0", "false", "")

# Skip the batch-resolve phase while this many already-resolved mints still have no bars. Resolution
# is 30 mints/call and fetching is 1 mint/call, so without this the resolved pile grows every pass
# and the front-loaded resolve phase steals budget and wall time from the fetch loop.
RESOLVE_SKIP_ABOVE = int(os.environ.get("RESOLVE_SKIP_ABOVE", "1500"))

# Mints first sighted within this many hours are ALWAYS resolved, however deep the fetch backlog is.
# Resolution gates fetching, so deferring it for fresh sightings makes them permanently unscoreable.
RESOLVE_ALWAYS_H = float(os.environ.get("RESOLVE_ALWAYS_H", "12"))

# The scoring horizon, in seconds. MUST match backtest.HORIZON_H — the collector prioritises the
# window the analysis actually scores, so if they disagree the collector optimises for the wrong one.
HORIZON_S = float(os.environ.get("HORIZON_H", "3")) * 3600
# The GeckoTerminal network. Robinhood Chain (id 4663) is served under `robinhood` — verified live
# 2026-09-01: /networks/robinhood/tokens/multi/ returned 30/30 board mints with pools
# (uniswap-v4-robinhood, pons-v2-dex, bankr-robinhood), and /pools/<addr>/ohlcv/minute returned
# 1,000 minute bars current to the minute. So one collector serves both chains; only this and
# SOL_SOURCES change. Shared tables need no migration: base58 and 0x addresses cannot collide.
GT_NETWORK = os.environ.get("GT_NETWORK", "solana")
GT = f"https://api.geckoterminal.com/api/v2/networks/{GT_NETWORK}"

# FAIL CLOSED on a network/source mismatch. Pointing the Solana network at gmgn_rh (or the reverse)
# would spend the whole call budget resolving addresses that can never resolve, and cache every
# failure as ok=false — poisoning trending_pools for the chain that WOULD have worked. An address
# shape check is the cheapest possible guard: EVM mints start 0x, Solana mints are base58.
EVM_NETWORKS = {"robinhood"}


def wrong_chain(mint):
    is_evm = mint.startswith("0x")
    return is_evm != (GT_NETWORK in EVM_NETWORKS)
UA = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                t = r.read()
                return r.status, (json.loads(t) if t else None)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(1.5 * (a + 1)); continue
            return e.code, e.read().decode()[:200]
        except Exception:
            time.sleep(1.5 * (a + 1))
    return 0, None


def sb_all(path, page=1000, cap=600000):
    """PostgREST caps every response at 1000 rows and truncates SILENTLY — always page."""
    out = []
    while len(out) < cap:
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
             "Range-Unit": "items", "Range": f"{len(out)}-{len(out) + page - 1}"}
        chunk = None
        TRIES = 5
        for a in range(TRIES):
            try:
                with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h), timeout=90) as r:
                    t = r.read(); chunk = json.loads(t) if t else []
                break
            except urllib.error.HTTPError as e:
                if e.code == 416:                      # range past the end — a real empty answer
                    chunk = []; break
                # FAIL LOUD. This used to set `chunk = []` and break, so a failed page returned a
                # SHORT list that read as a complete one. That is the 1000-row silent-truncation
                # trap wearing a different hat, and it cost a whole collection day: the snapshots
                # query 500'd on its first page under the old 8s statement timeout, `first` came
                # back empty, and the run printed "universe 0 mints" and spent six hours fetching
                # control-arm bars while the case arm — the forward test itself — got nothing.
                if e.code < 500 or a == TRIES - 1:
                    raise
                time.sleep(1.5 * (a + 1))
            except Exception:
                if a == TRIES - 1:
                    raise
                time.sleep(1.5 * (a + 1))
        if chunk is None:
            raise RuntimeError(f"sb_all: page at offset {len(out)} never returned for {path[:70]}")
        if not chunk:
            break
        out += chunk
        if len(chunk) < page:
            break
    if len(out) >= cap:
        # Hitting the cap silently truncates exactly like the PostgREST 1000-row limit did.
        # Shout rather than return a short read that looks complete.
        print(f"!! sb_all cap reached ({cap}) for {path[:70]} — RESULT IS TRUNCATED, raise cap", flush=True)
    return out


calls = {"n": 0}


def gt(url):
    if calls["n"] >= MAX_CALLS:
        return None
    calls["n"] += 1
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
                out = json.loads(r.read())
            time.sleep(SLEEP)
            return out
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(6 * (a + 1)); continue
            time.sleep(SLEEP)
            return None
        except Exception:
            time.sleep(2)
    return None


def resolve_pool(mint):
    j = gt(f"{GT}/tokens/{mint}/pools")
    if not j or not j.get("data"):
        return {"mint": mint, "ok": False, "resolved_at": int(time.time()),
                "pool_address": None, "dex": None, "reserve_usd": None, "n_pools": 0}
    best = max(j["data"], key=lambda d: float(d["attributes"].get("reserve_in_usd") or 0))
    a = best["attributes"]
    dex = ((best.get("relationships") or {}).get("dex") or {}).get("data") or {}
    return {"mint": mint, "ok": True, "resolved_at": int(time.time()),
            "pool_address": a.get("address"), "dex": dex.get("id"),
            "reserve_usd": float(a.get("reserve_in_usd") or 0), "n_pools": len(j["data"])}


def resolve_pools_batch(mints):
    """Resolve up to 30 mints in ONE GeckoTerminal call.

    `/tokens/{mint}/pools` costs one call PER MINT, and pool resolution is the gate on everything
    downstream: no pool -> no bars -> never scored. That per-mint cost is what forced MIN_OBS=3,
    and MIN_OBS=3 is an outcome-correlated filter (see D-COND) because "seen 3+ times" means "stayed
    on the board ~45+ minutes", which predicts the return. `/tokens/multi/` takes 30 addresses and
    `include=top_pools` returns each token's deepest pool inline, so resolution gets ~30x cheaper
    and the budget argument for MIN_OBS disappears.

    Returns {mint: record}. Mints GeckoTerminal does not know are recorded ok=False so the attempt
    is never re-paid."""
    now_i = int(time.time())
    def miss(m):
        return {"mint": m, "ok": False, "resolved_at": now_i, "pool_address": None,
                "dex": None, "reserve_usd": None, "n_pools": 0, "last_fetch_to": None}
    j = gt(f"{GT}/tokens/multi/{','.join(mints)}?include=top_pools")
    if not j:
        return {}                                   # budget exhausted or API down — retry next pass
    by_id = {}
    for o in (j.get("included") or []):
        if o.get("type") != "pool":
            continue
        a = o.get("attributes") or {}
        by_id[o["id"]] = (a.get("address"),
                          float(a.get("reserve_in_usd") or 0),
                          ((o.get("relationships") or {}).get("dex") or {}).get("data", {}).get("id"))
    out = {}
    # GT echoes the address it was given, but index case-insensitively too so a future EVM chain
    # (which GT DOES lowercase) cannot silently drop every resolution on the floor.
    want = {m.lower(): m for m in mints}
    for t in (j.get("data") or []):
        a = t.get("attributes") or {}
        got = a.get("address") or ""
        m = want.get(got.lower())
        if not m:
            continue
        ids = [x["id"] for x in ((t.get("relationships") or {}).get("top_pools") or {}).get("data", [])]
        cand = [by_id[i] for i in ids if i in by_id]
        if not cand:
            out[m] = miss(m); continue
        addr, res, dex = max(cand, key=lambda c: c[1])
        out[m] = {"mint": m, "ok": bool(addr), "resolved_at": now_i, "pool_address": addr,
                  "dex": dex, "reserve_usd": res,
                  # NOTE: this endpoint returns only the token's TOP pools, so n_pools is a count of
                  # those, not of every pool as the per-mint endpoint gave. Nothing reads it.
                  "n_pools": len(cand), "last_fetch_to": None}
    for m in mints:
        out.setdefault(m, miss(m))
    return out


def fetch_bars(pool, need_from, need_to):
    """Minute bars covering [need_from, need_to], paging backwards until we reach need_from."""
    got, before = {}, None
    for _ in range(4):
        u = f"{GT}/pools/{pool}/ohlcv/minute?aggregate=1&limit=1000"
        if before:
            u += f"&before_timestamp={before}"
        j = gt(u)
        if not j:
            break
        lst = ((j.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        if not lst:
            break
        for b in lst:
            if b[0] and b[4] and need_from - 60 <= b[0] <= need_to + 60:
                got[int(b[0])] = b
        oldest = min(int(b[0]) for b in lst)
        if oldest <= need_from or calls["n"] >= MAX_CALLS:
            break
        before = oldest
    return [got[k] for k in sorted(got)]


def main():
    t_end = time.time() + RUN_SECONDS if RUN_SECONDS else None
    while True:
        # SOLANA sources only, as an ALLOWLIST. trending_snapshots is now multi-chain
        # (source='gmgn_rh' carries Robinhood Chain, chain id 4663, EVM addresses), and this
        # collector resolves pools against GeckoTerminal's `solana` network. Without the filter it
        # would spend the GT call budget resolving 0x… addresses that can never resolve, and cache
        # each failure as ok=false in trending_pools. An allowlist rather than a denylist so the
        # next chain we add fails closed — invisible to this collector until deliberately included.
        snaps = sb_all(f"/trending_snapshots?source=in.({SOL_SOURCES})"
                       "&select=mint,captured_at&order=captured_at.asc")
        first, last, nobs = {}, {}, defaultdict(int)
        for r in snaps:
            m, t = r["mint"], r["captured_at"] / 1000
            nobs[m] += 1
            if m not in first or t < first[m]:
                first[m] = t
            if m not in last or t > last[m]:
                last[m] = t
        # a mint seen once has no path to model — never spend GT calls on it
        first = {m: t for m, t in first.items() if nobs[m] >= MIN_OBS}
        # Drop anything whose address shape does not match GT_NETWORK, and SHOUT if that is most of
        # the universe — that means SOL_SOURCES and GT_NETWORK disagree and the run would be a
        # budget bonfire.
        bad = [m for m in first if wrong_chain(m)]
        if bad:
            print(f"  !! {len(bad)}/{len(first)} mints have the wrong address shape for "
                  f"GT_NETWORK={GT_NETWORK} — dropping them", flush=True)
            if len(bad) > len(first) * 0.5:
                raise RuntimeError(f"GT_NETWORK={GT_NETWORK} vs SOL_SOURCES={SOL_SOURCES}: "
                                   f"{len(bad)}/{len(first)} mints are the wrong chain — refusing")
            for m in bad:
                del first[m]
        # An empty case arm is never legitimate — there are thousands of Solana board sightings on
        # record — so it can only mean the snapshot read came back short. Refuse to spend the call
        # budget on the control arm alone, which is what a silent short read bought last time.
        if not first:
            raise RuntimeError(f"case arm EMPTY after MIN_OBS={MIN_OBS} on {len(snaps)} snapshot "
                               f"rows — refusing to run the control arm on its own")
        # Which mints get the long post window. Read from the trending_mint_age VIEW, which is the
        # single definition shared with prune_trending_bars — if the collector and the retention job
        # disagreed, one would fetch bars the other immediately deletes, burning GT calls forever.
        long_post = {r["mint"] for r in sb_all("/trending_mint_age?select=mint,age_s"
                                               f"&age_s=gte.{LONG_AGE_S}")}
        print(f"long post window ({LONG_POST_H}h): {len(long_post)} mints; "
              f"{POST_H}h for the rest", flush=True)
        pools = {p["mint"]: p for p in
                 sb_all("/trending_pools?select=mint,pool_address,ok,last_fetch_to")}
        # Coverage from a server-side aggregate view. Deriving per-mint min/max/count by scanning
        # every bar row client-side is O(all bars) on EVERY pass — it already timed out at 122k
        # rows and would only get worse as the table grows.
        have = defaultdict(lambda: [None, None, 0])
        # Coverage comes from the maintained TABLE, not the trending_bar_coverage VIEW.
        # The view is `group by mint` over the whole bars table: at 900k rows it took ~82 seconds
        # regardless of how few mints were requested (the aggregate runs before the filter), which
        # is above Cloudflare's ~100s gateway limit — so this one call, made once per pass, is what
        # stopped the collector from running at all. The table is ~4.6k rows and reads in ~1s.
        # It is kept current below: every mint this pass writes bars for gets its row updated.
        for r in sb_all("/trending_bar_cov?select=mint,ts_from,ts_to,n_bars"):
            have[r["mint"]] = [r["ts_from"], r["ts_to"], r["n_bars"]]
        now = time.time()
        # CONTROL arm: sample mints seen in the candidate universe that never reached a board.
        # Sampled at random (not by rank) so the control pool stays an unbiased draw from the
        # population at risk — selecting "most active controls" would bias the comparison.
        controls = {}
        if UNIVERSE_FRAC > 0:
            # PRIMARY control source: the pump.fun launch firehose. candidate_universe failed its
            # coverage gate (3.7-9.4%), while 84.3% of trending mints are pump.fun tokens, so
            # pump_launches is the population at risk. Sampled at RANDOM — choosing the most active
            # launches would bias the case/control comparison it exists to support.
            # Only launches 1-24h old: younger than that and GeckoTerminal has no pool/bars yet.
            now_s = time.time()
            # AGE-MATCHED to the cases, which is what risk-set sampling requires. Measured from
            # GMGN's own open_timestamp (n=1751, unbiased by our collection window), age at first
            # trending sighting is p25 3 min / MEDIAN 10 min / p75 38.8h — 59.3% of cases trend
            # within 15 minutes of launch. An earlier version sampled controls at 6-24h to dodge
            # GeckoTerminal's indexing lag; that made controls 36-144x older than the median case
            # and broke the age covariate while fixing observability. Cover the whole case range and
            # let the analysis do the matching.
            lo, hi = int(now_s - 24*3600), int(now_s - 5*60)
            # Do NOT exclude mints that later trended. Risk-set sampling requires a token be usable
            # as a control at timestamps BEFORE it became a case — excluding "future cases" uses
            # information unavailable at sampling time and biases the comparison, because the
            # control pool then means "never trended" rather than "had not trended YET". Whether a
            # mint is an eligible control at a given moment is an ANALYSIS-time decision
            # (eligible iff first_trend > t), not a collection-time filter.
            for r in sb_all("/pump_launches?select=mint,created_at"
                            f"&created_at=gte.{lo}&created_at=lte.{hi}&order=created_at.asc"):
                m = r.get("mint")
                if m:
                    controls[m] = r["created_at"]
            # paginate the WHOLE universe: `limit=1000` is silently capped by PostgREST and covers
            # only the last few sweeps, so the control pool collapsed to a few dozen mints drawn
            # from one moment — not a sample of the population at risk across the study period.
            # secondary pool: still valid, just size-biased toward mega-caps and brand-new pools
            uni = sb_all("/candidate_universe?select=mint,captured_at,liquidity"
                         "&order=captured_at.asc")
            seen_board = set(first)
            for r in uni:
                m = r.get("mint")
                if not m or m in seen_board or m in controls:
                    continue
                if (r.get("liquidity") or 0) < 10000:      # the pre-registered eligibility floor
                    continue
                controls[m] = r["captured_at"]
            import random as _r
            _r.seed(int(now) // 3600)                       # stable within the hour, varies across
            keys = list(controls)
            _r.shuffle(keys)
            controls = {k: controls[k] for k in keys}
        # Least-covered first, so a budgeted run always makes progress and is resumable — but among
        # the mints with NO bars at all, newest sighting first.
        #
        # Sorting on n_bars alone made every zero-bar mint tie, and Python's stable sort then kept
        # them in `first`'s insertion order, which is OLDEST first. With a standing backlog of ~1,600
        # uncovered mints, brand-new sightings sorted to the BACK of that queue every pass, and pool
        # resolution ran a measured 583.9 minutes (9.7h) behind first sighting: 0% of mints seen in
        # the last 6h had any bars, against ~90% by 9h. The pipeline was keeping pace but never
        # catching up, so the freshest data — the data a forward test is made of — was always the
        # last to arrive. GeckoTerminal serves full history, so the backlog loses nothing by waiting;
        # a fresh sighting waiting is a fresh sighting we cannot act on.
        def window_end(m, t0):
            """End of the window we must collect for `m`.

            Anchored on the mint's LAST trending sighting, not its first. A token can trend, go
            quiet, and trend again — which is precisely Track A's "revival" thesis — and anchoring
            on the first sighting alone stops collecting at first+POST_H, so the second episode gets
            NO bars. Measured: 322 of 4,558 mints had an episode more than 24h after their first
            sighting. prune_trending_bars already RETAINS to max(anchors)+post_h, so the collector
            was leaving a window the pruner keeps permanently unfilled — collector and pruner must
            agree on the window (guardrail C0c) or one of them is doing nothing useful."""
            span = (LONG_POST_H if m in long_post else POST_H) * 3600
            return min(now, max(t0, last.get(m, t0)) + span)

        def deficit(m, t0):
            """Seconds of the NEEDED window not yet collected. This is the scheduling key."""
            need_from = t0 - PRE_MIN * 60
            need_to = window_end(m, t0)
            cov = have[m]
            # Credit the furthest point we have ALREADY REQUESTED, not merely the furthest bar we
            # received. A token that stopped trading returns no bars past its final trade, so its
            # deficit measured on bars alone never shrinks — it stays at the head of the queue and is
            # re-fetched every pass, forever, for nothing. Measured: 651 of 795 open windows belonged
            # to mints with no board sighting in 2h+, i.e. ~82% of all extension work was spent on
            # tokens that can never produce another bar.
            tried = (pools.get(m) or {}).get("last_fetch_to") or 0
            if cov[0] is None:
                d = max(0, need_to - max(need_from, tried))
            else:
                d = max(0, need_to - max(cov[1], tried)) + max(0, cov[0] - need_from)
            # CAP the deficit. Anchoring on the last sighting recovers a median +5.5h of window and
            # a p90 of +33.6h — but an uncapped 33h tail gap outranks a brand-new mint that needs
            # only its first 3h, which is precisely the data the pre-registered forward test
            # consumes. Capping bounds how much any one mint can dominate; the tail still gets
            # collected, just not ahead of everything else.
            return min(d, DEFICIT_CAP_H * 3600)

        # MOST-INCOMPLETE FIRST, measured in seconds of missing window — not in bar COUNT.
        #
        # Sorting on n_bars ascending (the previous key) starved exactly the mints that matter most.
        # A mint whose window is still OPEN needs re-fetching every pass to extend it, but by then it
        # already has hundreds of bars, so it sorted behind every thin mint in the table and a finite
        # call budget never reached it. Measured consequence: the newest bar in the whole table sat
        # frozen at 22:02Z for 4.9 hours while the collector ran, because new mints were being
        # covered and no existing window was ever extended. Coverage looked fine (91.6%); recency
        # was dead. Raising POST_H 3h->6h and LONG_POST_H 12h->24h made it worse, since windows now
        # stay open far longer and therefore need far more extension passes.
        #
        # Seconds-of-missing-window handles both cases in one number: a brand-new mint is missing its
        # entire window, an open window is missing only what has elapsed since the last fetch, and
        # whichever is further from complete goes first.
        # Ties (everything at the cap) break on OLDEST-DATA-FIRST, which is a fair round-robin:
        # whoever has gone longest without a successful fetch or an attempt goes next.
        def flush_cov(rows):
            for i in range(0, len(rows), 500):
                st, _ = sb("POST", "/trending_bar_cov?on_conflict=mint", rows[i:i + 500],
                           prefer="resolution=merge-duplicates,return=minimal")
                if st >= 300:
                    print(f"!! trending_bar_cov write FAILED {st}", flush=True)

        def flush_pools(rows, attempts):
            """Write pools in batches that are uniform BY CONSTRUCTION, not by convention.

            Splitting resolutions from attempts is not enough. A pool resolved this iteration is
            held by reference in `rows`, and the fetch loop later mutates it with `last_fetch_to`
            — but only when the pool is ok and a fetch was attempted. A not-ok pool never reaches
            that line, so `rows` ends up holding 7-key and 8-key objects, PostgREST rejects the
            whole batch with PGRST102 "All object keys must match", and every resolution in it is
            silently discarded. Measured on run 33369090482: three 400s, 511 lost resolutions in
            one pass. Those mints then have no pool, so they get no bars, so they are never scored
            — which is why post-freeze coverage sat at 8% of board-qualifying mints while the
            board itself was perfectly healthy.

            Grouping by key-set makes the shape irrelevant: whatever mix arrives, each shape goes
            out in its own request."""
            from collections import defaultdict as _dd
            for batch, path in ((rows, "resolutions"), (attempts, "attempts")):
                shapes = _dd(list)
                for r in batch:
                    shapes[tuple(sorted(r))].append(r)
                if len(shapes) > 1:
                    print(f"   note: {path} batch had {len(shapes)} key-shapes, "
                          f"sent separately", flush=True)
                for grp in shapes.values():
                    for i in range(0, len(grp), 500):
                        st, body = sb("POST", "/trending_pools?on_conflict=mint", grp[i:i + 500],
                                      prefer="resolution=merge-duplicates,return=minimal")
                        if st >= 300:
                            print(f"!! trending_pools {path} write FAILED {st} "
                                  f"({len(grp[i:i+500])} rows): {body}", flush=True)

        def staleness(m):
            return max(have[m][1] or 0, (pools.get(m) or {}).get("last_fetch_to") or 0)

        def horizon_gap(m, t0):
            """Seconds of the SCORING window [t0, t0+HORIZON_H] that have already elapsed in the
            world but that we have not requested. A mint with a positive gap is ONE FETCH away from
            being a scored trade; a mint with zero is not, however incomplete it looks."""
            need = t0 + HORIZON_S
            if now < need:
                return 0                     # horizon has not elapsed — those bars do not exist yet
            return max(0, need - max((pools.get(m) or {}).get("last_fetch_to") or 0, t0))

        # Ties (everything at the deficit cap) break on oldest-data-first, then NEWEST SIGHTING
        # first. That last term used to be implicit — Python's stable sort left tied mints in
        # `first`'s insertion order, which is captured_at ASCENDING, i.e. oldest first. With a
        # never-touched pile of ~5,000 mints (which is what MIN_OBS=1 creates), that silently put
        # every brand-new sighting behind the entire historical backlog. The backlog is
        # backfillable from GeckoTerminal at any time; a fresh sighting is what the forward test
        # is made of, so it goes first and the backlog drains with whatever budget is left.
        # SCOREABLE-IF-FETCHED FIRST, then most-incomplete.
        #
        # Seconds-of-missing-window alone starved exactly the entries the forward test is made of.
        # A fresh mint is fetched once, minutes after its first sighting, covering ~0-5 minutes past
        # entry — and that sets last_fetch_to to a RECENT time, so `staleness` immediately sorts it
        # behind the entire backlog. The newest-first rule only breaks ties among NEVER-touched
        # mints, so a mint that has been touched once is never re-visited while it is young, and its
        # window is never extended to t0+HORIZON. Measured 2026-09-01: of gmgn mints first seen 3-6h
        # ago, 72% had a resolved pool and 0% had bars through t0+3h; 6-12h ago, 6%.
        #
        # horizon_gap makes the distinction the deficit cannot: a mint whose scoring window has
        # elapsed but is unfetched is one call from being an evaluable trade. Everything else —
        # backfill, open-window extension beyond the horizon, the pre-window — is genuinely less
        # urgent, because GeckoTerminal serves it whenever we get to it.
        def sort_key(kv):
            m, t0 = kv
            if horizon_gap(m, t0) > 0:
                return (0, -t0, 0.0, 0.0)          # newest first among the scoreable
            return (1, -deficit(m, t0), staleness(m), -t0)

        todo = sorted(first.items(), key=sort_key)
        n_urgent = sum(1 for kv in first.items() if horizon_gap(*kv) > 0)
        print(f"  {n_urgent} mints are one fetch from being scoreable (horizon elapsed, "
              f"not yet requested) — those go first", flush=True)
        ctrl_todo = list(controls.items())
        # INTERLEAVE, don't append. Concatenating controls after every case made them structurally
        # unreachable: with hundreds of cases queued ahead of them, no finite call budget ever
        # reached position len(cases)+1, so the control arm sat at zero bars indefinitely. Round-robin
        # so both arms advance every pass in roughly UNIVERSE_FRAC proportion.
        merged, ci, cj = [], 0, 0
        step = (1 - UNIVERSE_FRAC) / UNIVERSE_FRAC if 0 < UNIVERSE_FRAC < 1 else None
        if step is None:
            merged = ctrl_todo if UNIVERSE_FRAC >= 1 else todo
        else:
            acc = 0.0
            while ci < len(todo) or cj < len(ctrl_todo):
                if ci < len(todo) and (acc < step or cj >= len(ctrl_todo)):
                    merged.append(todo[ci]); ci += 1; acc += 1
                elif cj < len(ctrl_todo):
                    merged.append(ctrl_todo[cj]); cj += 1; acc = 0.0
                else:
                    break
        todo = merged
        print(f"  control arm: {len(controls)} eligible universe mints, {len(ctrl_todo)} interleaved "
              f"at {UNIVERSE_FRAC:.0%} of the budget", flush=True)

        # PRE-RESOLVE POOLS IN BATCHES OF 30, before the per-mint loop.
        #
        # This is what makes MIN_OBS=1 affordable. Resolution used to cost one GT call per mint, so
        # covering every board sighting was ~1,900 calls/day on resolution alone and the budget said
        # no — which is how a call-budget setting (MIN_OBS=3) ended up acting as an
        # outcome-correlated sample filter. /tokens/multi/ takes 30 at a time, so the same coverage
        # costs ~64 calls/day and the filter is no longer needed.
        unres = [m for m, _ in todo if m not in pools]
        # DON'T resolve ahead of what bar-fetching can consume.
        #
        # Resolution is ~30 mints per call; a bar fetch is 1 mint per call. So the resolve phase
        # outruns the fetch loop 30:1 by construction, and it runs FIRST, before any bars. Measured
        # 2026-09-01: Solana had 10,658 resolved pools against 4,840 with bars — a 5,818-mint
        # backlog already queued — while each pass still resolved another ~3,600 and fetched ~950.
        # Under 429 backoff (~37s/call) that is ~75 minutes of every pass with the bar table
        # completely idle, and ~25% of the call budget spent enlarging a pile nothing can consume.
        #
        # A resolved pool is permanent, so this is not wasted work — it is work done far too early,
        # at the cost of the only thing that produces scored trades. Skip resolution entirely while
        # the fetch loop already has a deep enough queue.
        unfetched = sum(1 for m in first
                        if (pools.get(m) or {}).get("ok") and (have.get(m) or [None])[0] is None)
        if unres and unfetched > RESOLVE_SKIP_ABOVE:
            # NEVER skip a FRESH sighting. A mint cannot be fetched before it is resolved, so a
            # blanket skip blocks exactly the entries the horizon-priority ordering exists to serve.
            # Measured: with the blanket guard, pool coverage for mints first seen 3-6h ago fell
            # from 72% to 12% in 90 minutes, so the freshest cohort could never become scoreable at
            # all. Backfill can wait for the queue to drain; today's board cannot.
            fresh = [m for m in unres if now - first[m] <= RESOLVE_ALWAYS_H * 3600]
            print(f"  pool resolution: {unfetched} resolved mints still have no bars "
                  f"(> {RESOLVE_SKIP_ABOVE}) — deferring {len(unres) - len(fresh)} backfill "
                  f"resolutions, still resolving {len(fresh)} fresh (<{RESOLVE_ALWAYS_H}h) mints",
                  flush=True)
            unres = fresh
        if unres:
            cap = int(os.environ.get("RESOLVE_CALLS", "40")) * 30
            # Flush every FLUSH_EVERY records, NOT at the end of the loop. 120 calls is several
            # minutes of wall time, and holding every resolution until then is the same "an
            # interrupted run loses what it just paid for" failure this is supposed to avoid —
            # it also makes the run look completely idle from the database side while it works.
            queued, got_ok, nres, ncalls = [], 0, 0, 0
            FLUSH_EVERY = 300
            for i in range(0, min(len(unres), cap), 30):
                if calls["n"] >= MAX_CALLS or (t_end and time.time() >= t_end):
                    break
                recs = resolve_pools_batch(unres[i:i + 30])
                ncalls += 1
                for m, rec in recs.items():
                    pools[m] = rec
                    queued.append(rec)
                    got_ok += 1 if rec["ok"] else 0
                nres += len(recs)
                if len(queued) >= FLUSH_EVERY:
                    flush_pools(queued, []); queued.clear()
                    print(f"  .. batch-resolved {nres} pools ({got_ok} ok) in {ncalls} calls",
                          flush=True)
            if queued:
                flush_pools(queued, []); queued.clear()
            print(f"  batch-resolved {nres} pools ({got_ok} ok) in {ncalls} calls; "
                  f"{len(unres) - nres} still unresolved", flush=True)
        print(f"universe {len(first)} mints · {len(pools)} pools cached · "
              f"{sum(1 for m in first if have[m][2])} with bars · budget {MAX_CALLS}", flush=True)
        # Attempt records are kept in their OWN batch. PostgREST rejects a bulk upsert whose objects
        # have different key sets — "PGRST102: All object keys must match" — so mixing a full
        # resolve_pool record with a two-key attempt record 400s the ENTIRE batch. That failure was
        # silent (the pools write ignores its status), and it took the newly resolved pools down with
        # it, so every resolution was lost and re-paid for on the next pass.
        new_pools, new_attempts, new_bars, new_cov, done = [], [], [], [], 0
        for mint, t0 in todo:
            if calls["n"] >= MAX_CALLS:
                break
            # The RUN_SECONDS budget must be honoured HERE, not only between passes. A pass is
            # thousands of rate-limited calls and now runs far longer than it used to, so checking
            # only at pass end overshot the workflow's `timeout-minutes: 340` every single time:
            # six consecutive runs were killed at exactly 340 minutes mid-pass. GitHub records a
            # timeout as "cancelled", not "failure", so the workflow list looked healthy while the
            # collector was being killed on every run and the final flush and prune never ran.
            if t_end and time.time() >= t_end:
                print(f"  run budget reached ({RUN_SECONDS}s) — stopping cleanly mid-pass", flush=True)
                break
            p = pools.get(mint)
            newly = False
            if p is None:
                p = resolve_pool(mint)
                # Give every fresh resolution the same key set the fetch loop may later add, so the
                # batch stays uniform whether or not this mint reaches the fetch. Safe to write NULL
                # here precisely because this mint had no row: it cannot clobber an existing mark.
                p.setdefault("last_fetch_to", None)
                new_pools.append(p)
                pools[mint] = p
                newly = True
            if not p.get("ok") or not p.get("pool_address"):
                continue
            need_from = t0 - PRE_MIN * 60
            need_to = window_end(mint, t0)
            cov = have[mint]
            # already covered (allow a 3-bar edge tolerance)
            if cov[0] is not None and cov[0] <= need_from + 180 and cov[1] >= need_to - 180:
                continue
            bars = fetch_bars(p["pool_address"], need_from, need_to)
            if bars:
                # Keep trending_bar_cov current for this mint, merging with what we already had so
                # a partial re-fetch never narrows the recorded window.
                lo_new = min(int(b[0]) for b in bars); hi_new = max(int(b[0]) for b in bars)
                cov0, cov1, covn = have[mint]
                have[mint] = [min(lo_new, cov0) if cov0 is not None else lo_new,
                              max(hi_new, cov1) if cov1 is not None else hi_new,
                              (covn or 0) + len(bars)]
                new_cov.append({"mint": mint, "ts_from": have[mint][0], "ts_to": have[mint][1],
                                "n_bars": have[mint][2]})
            # Remember how far we asked, so a dead token's deficit collapses after ONE attempt
            # instead of re-queuing at the head of the list on every pass.
            # A pool resolved THIS iteration is already in new_pools by reference, so mutating it
            # is enough — appending again would put the same mint twice in one upsert batch, which
            # Postgres rejects ("ON CONFLICT DO UPDATE cannot affect row a second time").
            p["last_fetch_to"] = int(need_to)
            if not newly:
                new_attempts.append({"mint": mint, "last_fetch_to": int(need_to)})
            for b in bars:
                new_bars.append({"mint": mint, "ts": int(b[0]), "o": b[1], "h": b[2],
                                 "l": b[3], "c": b[4], "vol": b[5]})
            done += 1
            if len(new_bars) >= 2000:
                for i in range(0, len(new_bars), 500):
                    sb("POST", "/trending_bars?on_conflict=mint,ts", new_bars[i:i + 500],
                       prefer="resolution=merge-duplicates,return=minimal")
                # flush pools on the SAME cadence — writing them only at end-of-pass meant an
                # interrupted run lost every resolution and re-paid for it on the next pass
                flush_pools(new_pools, new_attempts); flush_cov(new_cov)
                print(f"  .. {done} mints, {len(new_bars)} bars + {len(new_pools)} pools flushed, "
                      f"{calls['n']} calls", flush=True)
                new_bars, new_pools, new_attempts, new_cov = [], [], [], []
        flush_pools(new_pools, new_attempts); flush_cov(new_cov)
        for i in range(0, len(new_bars), 500):
            sb("POST", "/trending_bars?on_conflict=mint,ts", new_bars[i:i + 500],
               prefer="resolution=merge-duplicates,return=minimal")
        # Retention, server-side. Minute bars are the largest storage cost in the project; without
        # this the table grows by hundreds of MB/day and exhausts the 500 MB budget in under a day.
        # Runs as an RPC so the delete happens IN the database — pulling bars client-side to filter
        # them would burn the 5 GB monthly egress budget (one full read is already ~88 MB).
        # Only ONE collector instance may prune. Both the Solana and Robinhood instances run this
        # loop, and prune_trending_bars is a full-table delete on a 1 GB instance — running it from
        # two jobs concurrently is pure contention for no benefit, and the pruner is global (it
        # anchors on trending_snapshots with no source filter), so the Solana instance already
        # covers Robinhood's rows. PRUNE=0 on the secondary instance.
        if PRUNE:
            st, pruned = sb("POST", "/rpc/prune_trending_bars", {})
        else:
            st, pruned = 200, 0
        # The other two retention jobs run here too, on the same cadence, because this is the only
        # collector that already owns a server-side cleanup step. Measured at the time they were
        # added: `extra` was 35 MB of trending_snapshots' 41 MB and only the FIRST row per
        # (source, mint) is ever read, so nulling the rest took the table 55 MB -> 19 MB and its
        # growth 27 -> ~6 MB/day. Both are idempotent and cheap when there is nothing to do.
        # prune_snapshot_extra() is deliberately NOT called any more. It nulls `extra` on every
        # snapshot but a mint's first, which on the free tier saved 21 MB/day — but `extra` is the
        # point-in-time board record (rank, buy/sell counts, holder_count, bundler_rate as they
        # MOVE), and destroying it forecloses any study of how a token's features evolve after it
        # is first seen. The function stays deployed as a lever if storage ever binds again.
        st_u, uni = sb("POST", "/rpc/prune_candidate_universe", {}) if PRUNE else (200, 0)
        print(f"pass done: {done} mints filled, {len(new_pools)} pools resolved, "
              f"{calls['n']} GT calls, pruned {pruned if st == 200 else f'FAILED({st})'} bars, "
              f"dropped {uni if st_u == 200 else f'FAILED({st_u})'} universe rows",
              flush=True)
        if not t_end or time.time() >= t_end:
            break
        calls["n"] = 0
        time.sleep(30)


if __name__ == "__main__":
    main()
