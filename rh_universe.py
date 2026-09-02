#!/usr/bin/env python3
"""ROBINHOOD CHAIN LAUNCH FIREHOSE — the population at risk for the trending-prediction study.

WHAT THIS UNBLOCKS
------------------
Predicting "which token is about to trend" needs controls drawn from the tokens that looked like
plausible candidates AT THE SAME INSTANT and did not trend (risk-set / incidence-density sampling).
Without them, a model compares trenders against random tokens and scores ~0.99 AUC by learning
"is this token alive at all" — zero discrimination among the candidates production must choose
between (E1). `trending_bars_rh` currently runs with UNIVERSE_FRAC=0 for exactly this reason: there
was no Robinhood control pool to draw from. This collector is that pool.

HOW
---
Each launchpad factory emits one creation event per token. `eth_getLogs` filtered by contract
address enumerates the entire population for a handful of calls, and RPC logs are PERMANENT, so
unlike the board feed this is fully backfillable — a gap here is recoverable, which is why it can
safely run second.

Measured 2026-09-01, and the reason this is not a chain-wide scan: Transfer-from-zero across the
whole chain is 153,717 logs/hour (78% of it a single high-frequency contract) to surface ~2,000
tokens that matter. Filtering by factory address gets the same tokens for ~0.1% of the log volume.

RECALL IS THE WHOLE POINT (E3)
------------------------------
A token that later trends but was never recorded here can never be caught by any model, and an
unwatched launchpad is a HOLE IN THE CONTROL ARM, not merely missing rows. Run
`python rh_universe.py --audit` to measure coverage against the board population before trusting
any number built on this table; it prints, per launchpad, how many board mints are present.

ELIGIBILITY IS DECIDED AT ANALYSIS TIME (E10)
---------------------------------------------
Every launch is stored, INCLUDING tokens that go on to trend. A mint is a valid control at time t
iff its first board sighting is after t. Filtering "has ever trended" here would make the control
pool mean "never trended" rather than "had not trended YET", biasing every case/control comparison.
There is deliberately no eligibility filter in this file.

Env: SUPABASE_URL, SUPABASE_KEY.
     BACKFILL_DAYS (default 3; first run only, 0 = start at head)
     CHUNK_BLOCKS (default 200000 ~5.6h), MAX_CALLS (default 1500), CREATOR_CALLS (default 150)
     PASS_INTERVAL (default 600), RUN_SECONDS (default 18000), RH_WATCH (JSON override)
"""
import json, os, sys, time

import rh_chain as C

# (launchpad, factory contract, creation topic0, EXTRACTOR)
#
# Discovered empirically 2026-09-01 by taking known board tokens per launchpad, finding the first
# Transfer of the token, and reading which contract in that transaction's receipt emitted an event
# carrying the new token's address. The method rediscovered Long's already-known factory/topic
# unaided, which is why the rest are trusted.
#
# THE EXTRACTOR IS NOT OPTIONAL, AND GUESSING IT IS A SILENT DATA-POISONING BUG.
# The first version of this file took "the first topic that looks like an address", which is right
# for a launchpad's TokenCreated (topic[1] = the new token) and WRONG for an AMM's pool-creation
# event, where topic[1] is the POOL and topics[2..3] are the two currencies. Measured: 1,890 of
# 4,279 rows (44%) were pool addresses stored as mints — every one a plausible-looking hex string
# that would have entered the control arm as a token that never trades. Layouts:
#   topic1 — launchpad TokenCreated(token indexed, ...). Verified: pons_v2 topic[1] = "Fina".
#   pair   — AMM Initialize(poolId indexed, currency0 indexed, currency1 indexed). Verified:
#            0x8366a39c… topic[1] symbol()=None (a pool), topic[2]="LIGMA", topic[3]="FIG".
#            The new token is whichever side is NOT the recurring quote asset; `pair` decides that
#            from the data (a quote asset appears in many pools, a memecoin in one or two) rather
#            than from a hard-coded list, so a new quote asset does not silently break it.
#
# `0x8366a39c…` is NOT a launchpad — it is a shared AMM whose pool-creation event fires for tokens
# from several launchpads (seen for both longxyz and pools_trade). It is watched as `amm_shared`
# because a token cannot trend without a pool, so it catches launchpads we have not enumerated.
WATCH_DEFAULT = [
    ("pons_v2",     "0xe33e9e479df8802cb0866d5d05258bec4cf62948",
     "0xdcacba5e347ae7abd91cb519eb877af8fa7774e347b85dd3ddcd24a2ba8cdf37", "topic1"),
    ("longxyz",     "0x22e99278308b393ea1260859b181ad7e78f5eeed",
     "0xadc6f1f726f7c710f77ec06adc75f3bb964e5be19581b072c67f7b9b4039267b", "topic1"),
    ("pools_trade", "0x0000ffffbe8efe702c8703ae3477ff5de3d319c0",
     "0x2e2b3f61b70d2d131b2a807371103cc98d51adcaa5e9a8f9c32658ad8426e74e", "topic1"),
    ("noxa",        "0x0f2730c4b0c279c8c7e3e5f9b7032eb7d42d06c0",
     "0x2ed5a8749a7e3a68a074750cc77850912a0708dc62ab7ea42b0c3e5beb36f017", "topic1"),
    ("amm_shared",  "0x8366a39cc670b4001a1121b8f6a443a643e40951",
     "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438", "pair"),
    # Added 2026-09-02 after `--audit` showed both under 40% covered. Each yields 5/5 ERC-20 tokens
    # at topic[1] over a 15-minute window, at 864/day (o1_rwa) and 5,856/day (bankr).
    #
    # o1_rwa was a REAL hole and adding it worked: coverage 37% -> 54.7% within one pass, still
    # climbing. It emits the same launches from two contracts (a factory and a registry); both are
    # watched because dedup is by mint, so the overlap costs one getLogs per chunk and nothing else.
    #
    # bankr was NOT a real hole, and the audit misled me about it. Measured 2026-09-02 over a live
    # 15-minute window: bankr emits 56 tokens and **100% of them are also emitted by longxyz**, so
    # bankr is a front-end on the Long launchpad rather than an independent one. `scan()` walks
    # WATCH in order and skips a mint already claimed (`mint in out`), so longxyz — earlier in this
    # list — has been collecting these all along under its own label. The audit groups by GMGN's
    # board `launchpad` field, so the same mint reads as "bankr" on the board and "longxyz" here:
    # what looked like a coverage hole was a LABEL MISMATCH. Adding the entry closed nothing
    # (coverage 38.5% -> 35.7% on n=14, i.e. noise). It is kept because it is cheap and it is the
    # evidence, but note the consequence: `launchpad` in rh_launches CANNOT distinguish bankr from
    # longxyz, so never group an analysis by it and expect the board's split.
    #
    # The lesson generalises: a per-launchpad coverage number compares OUR label to THEIRS, and a
    # low cell means the labels disagree OR the mints are missing. Check which before adding a
    # watch entry — a mint already collected under another name needs no new contract.
    ("o1_rwa",      "0x6a95911db04219674323aa0137c3377523c0e29f",
     "0xca4da5ec8448afb7e0c9e8b124653a2a4146cfd2f5a8f9778f93cf206e0a5bc0", "topic1"),
    ("o1_rwa",      "0xe64ac4113848bbc1a6dde1a6d1da96720a36f297",
     "0x207384e895174175cc774fe7f7457b37c382f27ebf53d37d5257b862f80eaf9c", "topic1"),
    ("bankr",       "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544",
     "0x5be4f748347693e0500df872d81f7d96bce1b98e6f5adff0cfddfe3e9e415f20", "topic1"),
    # NOT added: pons (0xcaf681a6…). The audit flags it at 5.6%, but the factory emits ZERO logs in
    # a 15-minute window — it is a retired launchpad, so its uncovered board mints are a backfill
    # DEPTH gap (they predate our scan), not a watch hole. Watching a dead contract would close
    # nothing while making the audit look fixed.
]
WATCH = [tuple(x) for x in json.loads(os.environ["RH_WATCH"])] if os.environ.get("RH_WATCH") \
    else WATCH_DEFAULT
# An address appearing as a pool side in at least this many distinct pools is a QUOTE asset
# (WETH / HOOD / USDG / VIRTUAL and friends), never the newly launched token.
QUOTE_MIN_POOLS = int(os.environ.get("QUOTE_MIN_POOLS", "4"))

BACKFILL_DAYS = float(os.environ.get("BACKFILL_DAYS", "3"))
CHUNK_BLOCKS = int(os.environ.get("CHUNK_BLOCKS", "200000"))
MAX_CALLS = int(os.environ.get("MAX_CALLS", "1500"))
CREATOR_CALLS = int(os.environ.get("CREATOR_CALLS", "150"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "600"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))

_NOT_TOKEN = {t[1].lower() for t in WATCH} | {"0x" + "0" * 40}


def _plausible(a):
    return a not in _NOT_TOKEN and int(a, 16) > 0xffff


def extract(log, how, quotes):
    """Pull the NEW TOKEN out of a creation log, per the watch entry's declared layout."""
    tps = log.get("topics") or []
    if how == "topic1":
        if len(tps) < 2:
            return None
        a = C.topic_addr(tps[1])
        return a if _plausible(a) else None
    if how == "pair":
        if len(tps) < 4:
            return None
        c0, c1 = C.topic_addr(tps[2]), C.topic_addr(tps[3])
        cand = [a for a in (c0, c1) if _plausible(a) and a not in quotes]
        # Both sides unknown means we cannot say which is the launch — skip rather than guess.
        return cand[0] if len(cand) == 1 else None
    raise ValueError(f"unknown extractor {how!r}")


def quote_assets(logs_by_watch):
    """Identify quote assets from the data: an address that shows up as a pool side in many
    distinct pools is the numeraire, not the launch. Derived per pass so a newly popular quote
    asset is picked up automatically instead of silently being recorded as a launch."""
    import collections
    seen = collections.Counter()
    for (_, _, _, how), logs in logs_by_watch.items():
        if how != "pair":
            continue
        for lg in logs:
            tps = lg.get("topics") or []
            if len(tps) >= 4:
                for tp in (tps[2], tps[3]):
                    seen[C.topic_addr(tp)] += 1
    return {a for a, n in seen.items() if n >= QUOTE_MIN_POOLS}


def is_erc20(addr):
    """decimals() is the cheapest reliable ERC-20 probe: a pool or a random contract reverts."""
    try:
        r = C.rpc("eth_call", [{"to": addr, "data": "0x313ce567"}, "latest"])
    except Exception:
        return False
    return bool(r) and r != "0x" and int(r, 16) <= 36


def selftest(sample=4, window_s=900):
    """Refuse to run a watch entry whose extractor does not yield ERC-20 tokens.

    This exists because the extractor bug above was SILENT: it produced well-formed hex addresses
    at the right rate, and only an on-chain symbol() call revealed 44% of them were pools. A
    layout assumption is a data-integrity assumption, so it gets checked at startup, on real logs,
    every run — not reasoned about once and trusted forever.
    """
    # A 15-minute window, not an hour. The gate exists to catch a WRONG EXTRACTOR, and four
    # sampled tokens prove that as well as six; what an hour of logs across five factories buys is
    # 4x the data to move and, under this node's flakiness, up to ~3 minutes of backoff per call.
    # Measured 2026-09-01: startup sat in selftest for 12+ minutes before any launch was written,
    # which is a verification gate costing more than the thing it verifies.
    latest, _, bt = C.refresh_head()
    lo = latest - int(window_s / bt)
    raw = {}
    for w in WATCH:
        lp, fac, topic, how = w
        raw[w] = C.get_logs({"address": fac, "topics": [topic],
                             "fromBlock": hex(lo), "toBlock": hex(latest)})
    quotes = quote_assets(raw)
    print(f"  selftest: {len(quotes)} quote assets inferred from pool sides", flush=True)
    bad = []
    for w in WATCH:
        lp, fac, topic, how = w
        toks = [t for t in (extract(l, how, quotes) for l in raw[w]) if t]
        if not toks:
            print(f"  selftest {lp:12} no logs in the window — cannot verify", flush=True)
            continue
        probe = toks[:sample]
        ok = sum(1 for t in probe if is_erc20(t))
        print(f"  selftest {lp:12} {len(raw[w]):5} logs -> {len(toks):5} tokens, "
              f"ERC-20 {ok}/{len(probe)}", flush=True)
        if ok < len(probe) * 0.5:
            bad.append(f"{lp}({how}) {ok}/{len(probe)} ERC-20")
    if bad:
        raise SystemExit("EXTRACTOR SELFTEST FAILED — refusing to write pool addresses as mints: "
                         + "; ".join(bad))
    return quotes


def decode_symbol(data_hex):
    """Best-effort ASCII symbol out of the event payload. Purely cosmetic — never used as a key,
    because a wrong guess here must not be able to corrupt an identity.

    Strict on purpose: the loose version returned things like 'q/:1jMӃu' from what was actually a
    uint256, and a junk string is worse than NULL — it looks like a decoded value. Require the word
    to be plain printable ASCII and overwhelmingly alphanumeric, else give up and leave it null;
    the real symbol is one `symbol()` call away for any token the analysis cares about.
    """
    d = data_hex[2:] if data_hex.startswith("0x") else data_hex
    for i in range(0, len(d), 64):
        w = d[i:i + 64]
        try:
            s = bytes.fromhex(w).decode("ascii", "ignore").strip("\x00").strip()
        except ValueError:
            continue
        if (2 <= len(s) <= 16 and s.isprintable() and s.isascii()
                and sum(c.isalnum() or c in " ._-" for c in s) >= len(s) - 1
                and any(c.isalpha() for c in s)):
            return s
    return None


def read_bookmark():
    rows = C.sb_all("/rh_scan_state?scanner=eq.launches&select=last_block")
    return int(rows[0]["last_block"]) if rows else None


def write_bookmark(block):
    C.sb_write("/rh_scan_state?on_conflict=scanner",
               [{"scanner": "launches", "last_block": int(block), "updated_at": int(time.time())}])


def scan(lo, hi, budget, quotes):
    """Scan [lo, hi] across every watched factory. Returns (rows, calls_used, reached_block)."""
    out, reached = {}, lo - 1
    b = lo
    # Launchpad entries are processed before `pair` entries so that a token seen via its own
    # launchpad keeps that label rather than being relabelled by the pool event that follows it.
    order = sorted(WATCH, key=lambda w: w[3] == "pair")
    while b <= hi:
        top = min(b + CHUNK_BLOCKS - 1, hi)
        raw = {}
        for w in order:
            lp, fac, topic, how = w
            if C.calls() >= budget:
                return list(out.values()), C.calls(), reached
            raw[w] = C.get_logs({"address": fac, "topics": [topic],
                                 "fromBlock": hex(b), "toBlock": hex(top)})
        # Refresh the quote set from this chunk's own pool sides, unioned with what we knew.
        quotes = quotes | quote_assets(raw)
        for w in order:
            lp, fac, topic, how = w
            for lg in raw.get(w, []):
                mint = extract(lg, how, quotes)
                if not mint or mint in out:
                    continue
                bn = int(lg["blockNumber"], 16)
                out[mint] = {"mint": mint, "created_at": C.blk_to_ts(bn), "block_number": bn,
                             "launchpad": lp, "factory": fac, "topic0": topic,
                             # A `pair` entry watches POOL creation, which can be many hours after
                             # the token was minted — measured, 71.7% of amm_shared board mints were
                             # on the board BEFORE their recorded "birth", median 14.4h before. So
                             # created_at from a pair event is an UPPER BOUND, not a birth, and any
                             # age computed from it is wrong. Record which kind it is.
                             "birth_kind": "pool" if how == "pair" else "launchpad",
                             "creator": None, "tx_hash": lg.get("transactionHash"),
                             "symbol": decode_symbol(lg.get("data") or "0x"),
                             "first_seen_at": int(time.time())}
        reached = top
        b = top + 1
    return list(out.values()), C.calls(), reached


def fill_creators(rows, budget):
    """Resolve tx.from for a bounded number of rows. The deployer is the axis of the fee-farming
    and sybil questions, but one call per launch does not scale to a firehose — so tx_hash is
    always stored and `creator` is backfilled newest-first within a call budget."""
    n = 0
    for r in sorted(rows, key=lambda x: -x["created_at"]):
        if n >= budget or C.calls() >= MAX_CALLS:
            break
        if not r.get("tx_hash"):
            continue
        try:
            tx = C.rpc("eth_getTransactionByHash", [r["tx_hash"]])
        except Exception:
            continue
        if tx:
            r["creator"] = (tx.get("from") or "").lower() or None
            n += 1
    return n


def one_pass(quotes):
    C.reset_calls()          # MAX_CALLS is a PER-PASS budget, not a lifetime one
    latest, _, bt = C.refresh_head()
    mark = read_bookmark()
    if mark is None:
        mark = latest - int(BACKFILL_DAYS * 86400 / bt) if BACKFILL_DAYS > 0 else latest - 1
        print(f"  no bookmark — starting {BACKFILL_DAYS}d back at block {mark:,}", flush=True)
    if mark >= latest:
        print(f"  bookmark {mark:,} is at head {latest:,} — nothing to scan", flush=True)
        return 0
    rows, used, reached = scan(mark + 1, latest, MAX_CALLS, quotes)
    # WRITE THE LAUNCHES FIRST. `fill_creators` is up to CREATOR_CALLS sequential
    # eth_getTransactionByHash calls — 400 of them at 0.25s pacing is 100s at best, and minutes
    # once this node starts backing off. Doing it before the write put the ESSENTIAL data (the
    # population at risk) behind an OPTIONAL enrichment: measured 2026-09-01, rh_launches sat
    # unchanged for 51 minutes while the collector looked busy and healthy. `creator` is a
    # nice-to-have for the fee-farming question; a missing launch row is a hole in the control arm.
    # A pool-creation row must never OVERWRITE a launchpad row for the same mint. Both events fire
    # for the same token, typically hours apart and therefore in different passes, so with plain
    # merge-duplicates the later pool event would replace a correct birth with a much later one.
    # Launchpad rows merge (they are authoritative and creator enrichment must be able to update
    # them); pool rows only ever fill a gap.
    lp_rows = [r for r in rows if r["birth_kind"] == "launchpad"]
    pool_rows = [r for r in rows if r["birth_kind"] == "pool"]
    wrote = C.sb_write("/rh_launches?on_conflict=mint", lp_rows)
    wrote += C.sb_write("/rh_launches?on_conflict=mint", pool_rows,
                        prefer="resolution=ignore-duplicates,return=minimal")
    # Only advance the bookmark over the range actually scanned, and only AFTER the write lands —
    # C3: an interrupted pass must be re-doable, never silently skipped.
    if reached >= mark:
        write_bookmark(reached)
    # Enrichment second, in its own write. tx_hash is already stored, so a creator missed here is
    # recoverable on any later pass; a launch missed is not recoverable until the next scan.
    got = fill_creators(rows, CREATOR_CALLS)
    if got:
        # Only launchpad rows are re-sent with merge: re-merging a pool row here would undo the
        # protection above and clobber the authoritative birth.
        rows = lp_rows
        # Re-send the FULL rows, not {mint, creator}. PostgREST's merge-duplicates still attempts
        # the INSERT, and NOT NULL on created_at/block_number/factory/topic0/first_seen_at is
        # checked before conflict resolution — a partial payload would 400 the whole batch.
        C.sb_write("/rh_launches?on_conflict=mint", [r for r in rows if r.get("creator")])
    span_h = (reached - mark) * bt / 3600
    print(f"  scanned blocks {mark + 1:,}..{reached:,} ({span_h:.1f}h): {len(rows)} launches, "
          f"{wrote} written, {got} creators resolved, {C.calls()} rpc calls, "
          f"{C.throttled()} x429, head {latest:,}", flush=True)
    return len(rows)


def audit():
    """E3 recall ceiling: what share of the board population does WATCH actually contain?

    Scored ONLY over board mints whose creation time falls inside the block range actually
    scanned. Without that window the number is dominated by how far back the backfill has run —
    a board token created before the scan started is a gap in COVERAGE, not a hole in WATCH, and
    conflating the two makes a working watchlist look broken (and, worse, could make a broken one
    look fine once the backfill is deep).
    """
    import collections
    snaps = C.sb_all("/trending_snapshots?source=eq.gmgn_rh&select=mint,captured_at,extra"
                     "&order=captured_at.asc,mint.asc")
    first = {}
    for r in snaps:
        first.setdefault(r["mint"].lower(), r)
    launches = C.sb_all("/rh_launches?select=mint,created_at")
    have = {r["mint"].lower() for r in launches}
    if not launches:
        raise SystemExit("rh_launches is empty — run the collector before auditing")
    lo, hi = min(r["created_at"] for r in launches), max(r["created_at"] for r in launches)
    print(f"rh_launches holds {len(have):,} launches spanning "
          f"{time.strftime('%m-%d %H:%M', time.gmtime(lo))} .. "
          f"{time.strftime('%m-%d %H:%M', time.gmtime(hi))} ({(hi - lo) / 86400:.2f}d)\n")
    by = collections.defaultdict(lambda: [0, 0])
    out_of_window = 0
    for m, r in first.items():
        e = r.get("extra") or {}
        born = next((x for x in (e.get("creation_timestamp") or None,
                                 e.get("open_timestamp") or None) if x), None)
        if born is None or not (lo <= born <= hi):
            out_of_window += 1
            continue
        lp = e.get("launchpad") or "(none)"
        by[lp][0] += 1
        by[lp][1] += 1 if m in have else 0
    tot = sum(v[0] for v in by.values())
    hit = sum(v[1] for v in by.values())
    print(f"board mints born inside the scanned window: {tot:,} "
          f"({out_of_window:,} born outside it, not scoreable)")
    print(f"  of those, present in rh_launches: {hit:,} ({hit / max(tot, 1):.1%})"
          f"   <-- RECALL CEILING\n")
    print(f"{'launchpad':16}{'board':>8}{'covered':>9}{'pct':>8}")
    for lp, (n, k) in sorted(by.items(), key=lambda kv: -kv[1][0]):
        flag = "   <-- UNWATCHED HOLE" if n >= 20 and k / n < 0.5 else ""
        print(f"{lp:16}{n:8}{k:9}{k / n:8.1%}{flag}")


def backfill(days):
    """Scan BACKWARDS over a fixed window without touching the forward bookmark.

    The normal loop only ever moves forward from `rh_scan_state`, so once it has run there is no
    way to reach older blocks — and the control arm needs history that predates the collector.
    RPC logs are permanent, so this is always recoverable; it just has to be asked for explicitly
    rather than by deleting the bookmark, which would make the live collector rescan and stall.
    """
    latest, _, bt = C.refresh_head()
    lo = latest - int(days * 86400 / bt)
    quotes = selftest()
    print(f"backfill: blocks {lo:,}..{latest:,} ({days}d)", flush=True)
    step = CHUNK_BLOCKS * 4
    total = 0
    for b in range(lo, latest, step):
        top = min(b + step - 1, latest)
        rows, _, _ = scan(b, top, MAX_CALLS + C.calls(), quotes)
        total += C.sb_write("/rh_launches?on_conflict=mint", rows)
        print(f"  blocks {b:,}..{top:,}: +{len(rows)} launches (total written {total:,}), "
              f"{C.calls()} rpc, {C.throttled()} x429", flush=True)
    print(f"backfill done: {total:,} rows written", flush=True)


def main():
    if "--audit" in sys.argv:
        audit()
        return
    if "--backfill" in sys.argv:
        i = sys.argv.index("--backfill")
        backfill(float(sys.argv[i + 1]) if len(sys.argv) > i + 1 else BACKFILL_DAYS)
        return
    # The selftest must be INSIDE the retry loop. It calls eth_getLogs, and the public RPC serves
    # sustained "internal server errror" bursts that outlast rpc()'s seven retries — so running it
    # once at startup made a transient node condition permanently fatal: a 5-hour collector died
    # 30 seconds in, having verified nothing and collected nothing.
    #
    # Distinguish the two failures, because they deserve opposite responses. "The extractor yields
    # non-ERC-20s" is a DATA-INTEGRITY verdict and must still abort (SystemExit, raised inside
    # selftest and re-raised here untouched). "We could not reach the node to check" is not a
    # verdict at all — it is an absent measurement (F5), so it retries like any other pass failure.
    quotes = None
    end = time.time() + RUN_SECONDS
    fails = 0
    while True:
        try:
            if quotes is None:
                quotes = selftest()
            one_pass(quotes)
            fails = 0
        except SystemExit:
            raise                      # extractor verdict — never retried away
        except Exception as ex:
            fails += 1
            print(f"pass error ({fails}): {ex!r}", flush=True)
            # C-EXIT0: every abnormal exit path RAISES. A collector that gives up quietly and
            # exits 0 is indistinguishable from one that finished, and nothing restarts it.
            if fails >= 5:
                raise SystemExit(f"{fails} consecutive pass failures — refusing to zombie")
        if time.time() >= end:
            break
        # Back off FAST after a failure, not a full pass interval: the node's blips clear in
        # seconds, and waiting 10 minutes to retry turns a 30-second outage into 30 minutes of
        # lost collection.
        time.sleep(min(60 * max(fails, 1), PASS_INTERVAL) if fails else PASS_INTERVAL)
    print("done", flush=True)


if __name__ == "__main__":
    main()
