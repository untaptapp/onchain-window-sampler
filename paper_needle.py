#!/usr/bin/env python3
"""PAPER BOOK for the needle range-order strategy, replayed against the chain's own swap record.

The frozen spec (research/needle-lp-forward-test.md) is scored on GT minute bars. This book
re-scores the SAME rule at swap resolution from the chain record (`shadow-tape/prices/v2/…`, see
DATA SOURCE below): a resting bid 30%/50% below a reference price fills when a sell swap carries
the pool's price through the level; the position exits as a taker on the last swap inside the
next 10 minutes. It runs hourly, is idempotent on (pool, uni, mode, tier, fill_ts), and writes
`paper_needle_fills`. It EVALUATES nothing — the read stays with the frozen spec; this is the
resolution check (guardrail 5) and the execution-realism check the bar read cannot give.

Two universes, two reference modes, two tiers, all recorded side by side:
  uni  'board'  = frozen universe: hookless static-fee (<=3%) v4 pool whose base token has a
                  gmgn_rh board sighting before the fill (rh_pool_fees join, paged);
       'other'  = every other hookless <=3% v4 pool the tape has seen (shadow_pool registry).
  mode 'spec'   = reference is the previous minute's last price (bar-faithful, continuous
                  re-centering — not implementable, ~$0.52 gas per re-center after 09-29);
       'impl'   = reference re-centers only when price drifts >15% (>=60 s apart), and every
                  re-center is counted so gas can be charged at the read.
Prices are quote-per-base in raw units (decimals cancel in every ratio), so stock-quoted pools
(WIWI/MU, FATCOIN/LLY…) score without any stock USD feed; the tape's `usd` is carried only for
reporting and the capacity cap.

DATA SOURCE (rebuilt 2026-09-13, guardrail D-PRICE). Swaps come from the CHAIN RECORD written by
tape_prices.py — every V3/V4 Swap log, with the event's own post-swap `sqrtPriceX96` and the block
timestamp — not from the shadow tape. The first book (rows archived, never analyse:
`paper_needle_fills_void_20260912`) had three defects, each measured: it reconstructed post-swap
prices as avg²/prev_end, which drifts without bound (~97% phantom fills); it read a tape holding
~26–50% of the chain's swaps with ARRIVAL timestamps; and it labelled V4 buys as sells (V3 amounts
are the pool's delta, V4 PoolManager amounts the swapper's). Every pass now asserts, before writing:
the record is contiguous over the pass window; every swap's execution price lies inside
[pre, post] ± fee (≤2% violations); buy/sell labels agree with the chain price direction (≤1%).
The tape is joined only for `usd` (verified on amounts), else USD is estimated from the hour's
tape rate for that quote token (`usd_src`). A fill requires the TRUE post-swap price at or below
the level on a SELL; `cross_kind` = 'avg' when even the swap's average execution price is below
the level (a deep blow-through), else 'end'. Exits are valued at the exit swap's true post-swap
price (the pool mid), with the fee and slip charged by the net formulas.

Exit per spec: last swap in (fill, fill+12 min]; if it is earlier than fill+8 min the exit is
'thin' and the trade scores at the −90% FLOOR (exit_px is still stored, so the thin-price
alternative is recomputable); no swap at all is 'floor'. net_taker = (1+g)(1−f)²(1−slip)−1 as
registered; net_maker credits the pool fee on entry instead of paying it. slip = 1.5% flat.

Write-once semantics (2026-09-10 fix): a fill is emitted only once its exit window is on the tape
(fill_ts <= record_end − 13 min) AND it lies past the FRONTIER `rh_state.paper_needle_final_ts` of
the previous pass; the LOOKBACK_H hours before the frontier are loaded as WARM-UP only (they seed
minute closes, the impl reference and — from the rows already written — the 10-min refractory
chain). The first version re-emitted a 2-hour overlap each run, and because the refractory chain
and the impl reference restarted at the window edge, 40% of the overlap rows were never
reproduced and stayed as orphans. Nothing is rewritten now; a pool's fills are one path.

Every fill carries `swaps_prev_h` (swaps in the pool in the 60 min before the fill — the
activity gate of the 2026-09-10 spec amendment, >=30, is applied in the SUMMARY and at the read,
never in the universe), `px_missing` (a swap in its [fill−60 min, fill+12 min] window had no chain
price — excluded from the gated summary) and `tape_gap` (always false since the chain record: kept
for the archived rows' schema). Self-polls every PASS_INTERVAL inside RUN_SECONDS; the cron
is a restart heartbeat (GitHub dropped 4 of 5 hourly slots on 2026-09-10).

Env: SUPABASE_URL, SUPABASE_KEY, SLIP (0.015), LOOKBACK_H (2, warm-up hours before the
frontier), TAPE_START (2026-09-09/23), RUN_SECONDS (18000), PASS_INTERVAL (1200).
REPLAY=1 rebuilds from the frontier with a SIMULATED clock advancing REPLAY_STEP_S (3600) per
pass — the universe ('other' = trailing-24h top pools) is chosen as of each simulated hour, never
with hindsight — until the simulated clock reaches the real tape end.
"""
import gzip, io, json, os, sys, time, urllib.request, datetime as dt
from bisect import bisect_left, bisect_right
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rh_chain as C
import backtest as BT

ZERO = "0x" + "0" * 40
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
STOCKS = {"0x117cc2133c37b721f49de2a7a74833232b3b4c0c", "0xd5f3879160bc7c32ebb4dc785f8a4f505888de68",
          "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec", "0x322f0929c4625ed5bad873c95208d54e1c003b2d",
          "0xaf3d76f1834a1d425780943c99ea8a608f8a93f9", "0x1b0e319c6a659f002271b69db8a7df2f911c153e",
          "0xff080c8ce2e5feadaca0da81314ae59d232d4afd", "0xccee82fe024c36fa15e1005ede3e9e4787e23d09",
          "0xc9a981fee1f9dec688bb123ccdecc63d0debfc4e", "0x92fd66527192e3e61d4ddd13322aa222de86f9b5",
          "0x05a3d1cd21d0c88145e82600e62e7e496e0f222b", "0x1d11f0496982706c5e14a514d4e79f2e6bde4516"}
QUOTES = {ZERO, WETH, USDG} | STOCKS
SLIP = float(os.environ.get("SLIP", "0.015"))
LOOKBACK_H = int(os.environ.get("LOOKBACK_H", "2"))
TAPE_START = os.environ.get("TAPE_START", "2026-09-09/23")
FEE_MAX = 30000
TIERS = (0.3, 0.5)
REFRACTORY = 600
DRIFT = 0.15
BUCKET = "shadow-tape"
GATE_SWAPS = 30          # activity gate (spec amendment 2026-09-10): swaps in the pool in the prior hour
VOL_H = 24               # trailing window for choosing an 'other' token's top pool (stable across passes)
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "1200"))
BLOCKS_PER_S = 9.0       # fallback for mapping a gap's block range to time (heartbeat: ~536 blocks/min)
Q96 = 2 ** 96
MAX_INVARIANT_BAD = 0.02 # share of swaps whose execution price |aq/ab| lies outside [pre, post] ± fee on the chain
                         # record (measured 0.02%; a wrong price decode or orientation puts it near 100%)
REPLAY = os.environ.get("REPLAY") == "1"
REPLAY_STEP_S = int(os.environ.get("REPLAY_STEP_S", "3600"))


def sbj(path):
    st, body = C.sb("GET", path)
    if not (200 <= st < 300):
        raise RuntimeError(f"supabase GET {path[:70]} -> {st}: {str(body)[:120]}")
    return body or []


def storage(method, path, body=None):
    h = {"apikey": C.KEY, "Authorization": f"Bearer {C.KEY}", "Content-Type": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(C.SB.replace("/rest/v1", "") + path, data=data, headers=h, method=method)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return None
            time.sleep(2 * (a + 1))
        except Exception:
            time.sleep(2 * (a + 1))
    raise RuntimeError(f"storage {method} {path} never landed")


def hour_files(h):
    names = []
    lst = storage("POST", f"/storage/v1/object/list/{BUCKET}", {"prefix": f"swaps/{h}", "limit": 1000})
    for f in json.loads(lst or b"[]"):
        if f["name"].endswith(".jsonl.gz"):
            names.append(f"swaps/{h}/{f['name']}")
    # legacy single-file layout
    if storage("HEAD", f"/storage/v1/object/{BUCKET}/swaps/{h}.jsonl.gz") is not None:
        names.append(f"swaps/{h}.jsonl.gz")
    return sorted(set(names))


def load_hour(h, keep):
    rows, seen = [], set()
    for n in hour_files(h):
        raw = storage("GET", f"/storage/v1/object/{BUCKET}/{n}")
        if not raw:
            continue
        for line in gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode().splitlines():
            r = json.loads(line)
            if r.get("kind") not in ("v4", "v3") or r["key"] not in keep:
                continue
            k = (r["tx"], r["li"])
            if k in seen:
                continue
            seen.add(k)
            rows.append(r)
    return rows


import tape_prices as TP

CHAIN_CHUNK = TP.CHUNK
_marks = {}


class PricesNotReady(RuntimeError):
    pass


def chain_markers():
    """{chunk_start: marker} for the chain swap record (tape_prices.py). A closed chunk's marker never
    changes, so only unseen or still-open chunks are re-read."""
    names, off = [], 0
    while True:
        lst = json.loads(TP.storage("POST", f"/storage/v1/object/list/{BUCKET}",
                                    json.dumps({"prefix": TP.PREFIX + "/", "limit": 1000, "offset": off,
                                                "sortBy": {"column": "name", "order": "asc"}}).encode()))
        names += [x["name"] for x in lst if x["name"].endswith(".done.json")]
        if len(lst) < 1000:
            break
        off += 1000
    for n in names:
        st = int(n.split(".")[0])
        m = _marks.get(st)
        if m is None or m["to"] < st + CHAIN_CHUNK - 1:
            raw = TP.storage("GET", f"/storage/v1/object/{BUCKET}/{TP.PREFIX}/{n}")
            if raw:
                _marks[st] = json.loads(raw)
    return _marks


def load_chain(t_from, t_to, uni):
    """Every V3/V4 swap of universe pools with block time in [t_from, t_to], from the chain record.
    Returns (rows, covered_to_ts). Raises PricesNotReady unless the record covers t_from..t_to
    contiguously — a hole in the middle would be a silent half-tape, the defect this replaces."""
    mk = chain_markers()
    starts = sorted(mk)
    if not starts:
        raise PricesNotReady("chain swap record is empty: run tape_prices.py")
    sel = [st for st in starts if mk[st]["ts_max"] is not None and mk[st]["ts_max"] >= t_from]
    if not sel or mk[starts[0]]["ts_min"] > t_from:
        raise PricesNotReady(f"chain record does not reach back to {hour_of(t_from)}")
    # contiguity over the chunks this pass needs: from the chunk holding t_from until one starts after t_to
    i0 = starts.index(sel[0])
    chain = [starts[i0]]
    for st in starts[i0 + 1:]:
        if mk[st]["ts_min"] is not None and mk[st]["ts_min"] > t_to:
            break
        if mk[st]["from"] != mk[chain[-1]]["to"] + 1:
            break                                    # a hole: the record is only usable up to here
        chain.append(st)
    covered_to = mk[chain[-1]]["ts_max"]
    if covered_to < min(t_to, time.time() - 1800):
        raise PricesNotReady(f"chain record is contiguous only to {dt.datetime.fromtimestamp(covered_to, dt.timezone.utc):%m-%d %H:%M} UTC "
                             f"(block {mk[chain[-1]]['to']}), pass needs {dt.datetime.fromtimestamp(t_to, dt.timezone.utc):%m-%d %H:%M}")
    rows = []
    for st in chain:
        m = mk[st]
        if m["ts_min"] is not None and m["ts_min"] > t_to:
            break
        raw = TP.storage("GET", f"/storage/v1/object/{BUCKET}/{TP.PREFIX}/{st:09d}.jsonl.gz")
        if raw is None:
            raise RuntimeError(f"chain record chunk {st}: marker without data")
        n = 0
        for line in gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode().splitlines():
            if not line:
                continue
            n += 1
            r = json.loads(line)
            if r["key"] in uni and t_from <= r["ts"] <= t_to:
                rows.append(r)
        if n != m["rows"]:
            raise RuntimeError(f"chain record chunk {st}: {n} rows, marker says {m['rows']}")
    return rows, min(covered_to, t_to)


def attach_usd(rows, uni, t_from, t_to):
    """USD size per swap: the tape's own sizing where the tape has the swap (joined on (blk, li) and
    VERIFIED on amounts), else estimated from the same hour's tape USD-per-raw-quote-unit for that quote
    token (`usd_src` = 'est'), else None. USD is used only for reporting and the capacity cap."""
    tape, rate = {}, defaultdict(list)
    for h in hours_between(hour_of(t_from), hour_of(t_to + 3600)):
        for r in load_hour(h, uni):
            if r.get("usd") is None:
                continue
            tape[(r["blk"], r["li"])] = r
            m = uni[r["key"]]
            aq = int(r["a0"]) if m["qidx"] == 0 else int(r["a1"])
            if aq:
                rate[(m["quote"], hour_of(r["ts"]))].append(r["usd"] / abs(aq))
    med = {k: sorted(v)[len(v) // 2] for k, v in rate.items() if len(v) >= 5}
    st = {"tape": 0, "est": 0, "none": 0}
    for r in rows:
        t = tape.get((r["blk"], r["li"]))
        if t is not None:
            if str(t["a0"]) != str(r["a0"]) or str(t["a1"]) != str(r["a1"]):
                raise RuntimeError(f"tape/chain join mismatch at blk {r['blk']} li {r['li']}")
            r["usd"], r["usd_src"] = t["usd"], "tape"
            st["tape"] += 1
            continue
        m = uni[r["key"]]
        aq = int(r["a0"]) if m["qidx"] == 0 else int(r["a1"])
        k = (m["quote"], hour_of(r["ts"]))
        if k in med and aq:
            r["usd"], r["usd_src"] = round(med[k] * abs(aq), 4), "est"
            st["est"] += 1
        else:
            r["usd"], r["usd_src"] = None, None
            st["none"] += 1
    return st


def hours_between(a, b):
    ta = dt.datetime.strptime(a, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc)
    tb = dt.datetime.strptime(b, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc)
    while ta <= tb:
        yield ta.strftime("%Y-%m-%d/%H")
        ta += dt.timedelta(hours=1)


def hour_of(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d/%H")


_UCACHE = {}
UCACHE_FILE = os.environ.get("UCACHE_FILE")    # local replays only: persist static pool metadata across restarts
if UCACHE_FILE and os.path.exists(UCACHE_FILE):
    import pickle
    _UCACHE.update({k: v for k, v in pickle.load(open(UCACHE_FILE, "rb")).items() if k in ("deep", "gated", "v3", "asked")})


def build_universe(now):
    """pool key -> {base, quote, qidx, fee_ppm, uni, first_seen, kind}.

    ONE POOL PER TOKEN — the token's DEEPEST pool — exactly what the frozen spec and the bar
    census score (GT bars are per deepest pool). The first replay (2026-09-10) used EVERY
    hookless pool of every board token (9,838 pools) and was dominated by dust pools where a
    $20 swap is a 30% move: fills crossed by a single swap's average price won 6%, fills where
    price walked through the level won 58% — the universe, not the rule, was the artifact.
      'board'  = gmgn_rh board mint (14-day lookback, sighting before the fill) whose
                 trending_pools.pool_address is a hookless static-fee (<=3%) v4 pool;
      'board_v3' = same but the deepest pool is a 20-byte (ramses v3) address — outside the
                 frozen spec (v4 only), scored for information;
      'other'  = non-board base tokens: their highest-volume hookless <=3% v4 pool over the
                 trailing VOL_H hours (shadow_venue_hour), one per token."""
    # First board sighting per RH mint from the pg_cron-refreshed MV (guardrail A-VIEW): the 14-day
    # snapshot keyset read took ~5 s/page (~25 min) under IO load, every hour. A sighting AFTER `now`
    # never counts (replay look-ahead). RH mints are the 0x-prefixed rows (the MV also holds Solana).
    # Staleness: the MV refreshes every 20 min, so a token boarded minutes ago is still 'other'.
    if "firsts" not in _UCACHE or not REPLAY:
        _UCACHE["firsts"] = C.sb_all("/trending_mint_age_mv?select=mint,first_seen&mint=like.0x*&order=mint.asc")
        if not _UCACHE["firsts"]:
            raise RuntimeError("trending_mint_age_mv returned no RH mints")
    first = {r["mint"].lower(): float(r["first_seen"]) for r in _UCACHE["firsts"]
             if r["first_seen"] is not None and float(r["first_seen"]) <= now}
    mints = sorted(first)
    # pool metadata is static per id: memoize across passes, fetch only ids not seen before
    deep_c, gated_c, v3_c, asked = (_UCACHE.setdefault(k, {} if k != "asked" else set()) for k in ("deep", "gated", "v3", "asked"))
    new = [m for m in mints if ("m", m) not in asked]
    for i in range(0, len(new), 80):
        q = ",".join(new[i:i + 80])
        for p in C.sb_all(f"/trending_pools?select=mint,pool_address,dex&ok=eq.true&pool_address=not.is.null"
                          f"&mint=in.({q})&order=mint.asc"):
            deep_c[p["mint"].lower()] = (p["pool_address"].lower(), p.get("dex"))
    # live: a mint with no resolved pool yet is asked again next pass (resolution lags new board mints by hours)
    asked.update(("m", m) for m in new if REPLAY or m in deep_c)
    deepest = {m: deep_c[m] for m in mints if m in deep_c}
    uni = {}
    v4_new = sorted({a for a, _ in deepest.values() if len(a) == 66 and ("p", a) not in asked})
    for i in range(0, len(v4_new), 80):
        q = ",".join(v4_new[i:i + 80])
        for p in C.sb_all(f"/rh_pool_fees?select=pool_id,currency0,currency1,fee_ppm"
                          f"&hooks=eq.{ZERO}&fee_dynamic=eq.false&fee_ppm=lte.{FEE_MAX}"
                          f"&pool_id=in.({q})&order=pool_id.asc"):
            gated_c[p["pool_id"].lower()] = p
    v3_new = sorted({a for a, _ in deepest.values() if len(a) == 42 and ("p", a) not in asked})
    for i in range(0, len(v3_new), 80):
        q = ",".join(v3_new[i:i + 80])
        for p in C.sb_all(f"/shadow_pool?select=key,token0,token1,fee_ppm&kind=eq.v3&fee_ppm=lte.{FEE_MAX}"
                          f"&key=in.({q})&order=key.asc"):
            v3_c[p["key"].lower()] = p
    asked.update(("p", a) for a in v4_new + v3_new)
    gated, v3meta = gated_c, v3_c
    for m, (addr, dex) in deepest.items():
        if addr in gated:
            p = gated[addr]; c0, c1 = p["currency0"].lower(), p["currency1"].lower(); kind, tag = "v4", "board"
        elif addr in v3meta:
            p = v3meta[addr]; c0, c1 = (p["token0"] or "").lower(), (p["token1"] or "").lower(); kind, tag = "v3", "board_v3"
        else:
            continue
        if m == c0 and c1 in QUOTES:
            uni[addr] = {"base": m, "quote": c1, "qidx": 1, "fee_ppm": p["fee_ppm"], "uni": tag, "first_seen": first[m], "kind": kind}
        elif m == c1 and c0 in QUOTES:
            uni[addr] = {"base": m, "quote": c0, "qidx": 0, "fee_ppm": p["fee_ppm"], "uni": tag, "first_seen": first[m], "kind": kind}
    n_board = len(uni)
    # 'other': highest-volume gated v4 pool per non-board base over the trailing VOL_H hours
    vol = defaultdict(float)
    # replay: only COMPLETED hours (a shadow_venue_hour row holds the whole hour, i.e. up to 59 min of the future)
    vh_hours = (hours_between(hour_of(now - VOL_H * 3600), hour_of(now - 3600)) if REPLAY
                else hours_between(hour_of(now - (VOL_H - 1) * 3600), hour_of(now)))
    for h in vh_hours:
        iso = dt.datetime.strptime(h, "%Y-%m-%d/%H").strftime("%Y-%m-%dT%H:00:00Z")   # '+00:00' decodes to a space in a URL
        ck = "vh:" + h
        if ck not in _UCACHE or not REPLAY or h >= hour_of(now):       # the current hour is still accruing
            _UCACHE[ck] = C.sb_all(f"/shadow_venue_hour?select=venue,usd_vol&kind=eq.v4&delta_s=eq.3&hour=eq.{iso}&order=venue.asc")
        for r in _UCACHE[ck]:
            vol[r["venue"].lower()] += r["usd_vol"] or 0
    best = {}
    if "v4pools" not in _UCACHE or not REPLAY:
        _UCACHE["v4pools"] = C.sb_all(f"/shadow_pool?select=key,token0,token1,fee_ppm,hooks&kind=eq.v4"
                                      f"&hooks=eq.{ZERO}&fee_ppm=lte.{FEE_MAX}&order=key.asc")
    for p in _UCACHE["v4pools"]:
        k = p["key"].lower()
        if k in uni or k not in vol:
            continue
        c0, c1 = (p["token0"] or "").lower(), (p["token1"] or "").lower()
        if c1 in QUOTES and c0 not in QUOTES:
            base, quote, qidx = c0, c1, 1
        elif c0 in QUOTES and c1 not in QUOTES:
            base, quote, qidx = c1, c0, 0
        else:
            continue
        if base in first:
            continue
        if base not in best or vol[k] > vol[best[base][0]]:
            best[base] = (k, {"base": base, "quote": quote, "qidx": qidx, "fee_ppm": p["fee_ppm"], "uni": "other", "first_seen": 0, "kind": "v4"})
    for base, (k, meta) in best.items():
        uni[k] = meta
    if UCACHE_FILE:
        import pickle
        pickle.dump({k: _UCACHE[k] for k in ("deep", "gated", "v3", "asked")}, open(UCACHE_FILE + ".partial", "wb"))
        os.replace(UCACHE_FILE + ".partial", UCACHE_FILE)
    print(f"universe: {n_board} deepest-pool board tokens "
          f"({sum(1 for u in uni.values() if u['uni']=='board')} v4 in-spec, "
          f"{sum(1 for u in uni.values() if u['uni']=='board_v3')} v3) + {len(best)} other tokens' top pools", flush=True)
    return uni


INV = {"n": 0, "bad": 0}
SIDE = {"n": 0, "bad": 0}   # swaps whose buy/sell label disagrees with the direction the chain price moved


def simulate(rows, uni, max_ts, final_prev=0.0, seed_last=None, gaps=()):
    """rows: tape swaps of universe pools (any order), including LOOKBACK_H warm-up hours.
    Emits only fills with final_prev < fill_ts <= max_ts − 780 (exit window on the tape);
    everything earlier is warm-up. seed_last: {(pool, mode, tier): last written fill_ts}."""
    seed_last = seed_last or {}
    by_pool = defaultdict(list)
    for r in rows:
        by_pool[r["key"]].append(r)
    out = []
    for key, sw in by_pool.items():
        sw.sort(key=lambda r: (r["blk"], r["li"]))
        meta = uni[key]
        qidx = meta["qidx"]
        px = []          # (ts, blk, avg, end, side, usd, usd_src) — end = TRUE post-swap price (sqrtPriceX96), ts = block time
        missing = []     # ts of swaps with no chain price: fills near them are flagged, never estimated
        prev_end = prev_px = None
        fee = meta["fee_ppm"] / 1e6
        for r in sw:
            a0, a1 = int(r["a0"]), int(r["a1"])
            aq, ab = (a0, a1) if qidx == 0 else (a1, a0)
            if aq == 0 or ab == 0:
                continue
            if not r.get("sp") or int(r["sp"]) == 0:
                missing.append(r["ts"])
                prev_end = prev_px = None
                continue
            p1per0 = (int(r["sp"]) / Q96) ** 2
            end = 1 / p1per0 if qidx == 0 else p1per0        # quote per base, raw units
            avg = abs(aq) / abs(ab)
            if prev_end is not None:                          # physical check: execution price inside [pre, post] ± fee
                lo, hi = min(prev_end, end), max(prev_end, end)
                tol = fee + 0.003
                INV["n"] += 1
                if not (lo * (1 - tol) / (1 + tol) <= avg <= hi * (1 + tol) / (1 - tol)):
                    INV["bad"] += 1
            prev_end = end
            # Amount signs differ by family (measured 2026-09-13 on 48,351 chain price moves, 100% separation):
            # V3 Swap amounts are the POOL's delta (quote in > 0 = a buy, price up); V4 PoolManager amounts are
            # the SWAPPER's delta (quote received > 0 = a sell, price down). The first book used the V3 rule for
            # both, so every V4 pool's buys and sells were swapped.
            side = ("buy" if aq > 0 else "sell") if r.get("kind", meta["kind"]) == "v3" else ("buy" if aq < 0 else "sell")
            if prev_px is not None and end != prev_px:
                SIDE["n"] += 1
                SIDE["bad"] += (end > prev_px) != (side == "buy")
            prev_px = end
            px.append((r["ts"], r["blk"], avg, end, side, r.get("usd"), r.get("usd_src")))
        if len(px) < 2:
            continue
        tss = [p[0] for p in px]
        fills = []
        for mode in ("spec", "impl"):
            ref, ref_minute, last_ts_seen, last_rc, n_rc = None, None, None, 0, 0
            last_fill = {t: seed_last.get((key, mode, t), -1e12) for t in TIERS}
            minute_close = {}
            for ts, blk, avg, end, side, usd, usd_src in px:
                m = int(ts // 60)
                if mode == "spec":
                    # reference = last price of the most recent EARLIER minute that traded,
                    # valid only if that trade is <= 5 min old (spec: prior close at most 5 min older)
                    if ref_minute is None or m != ref_minute:
                        prior = [(mm, c) for mm, c in minute_close.items() if mm < m]
                        if prior:
                            mm, (cts, c) = max(prior)
                            ref = c if ts - cts <= 300 else None
                        ref_minute = m
                    minute_close[m] = (ts, end)
                else:
                    if ref is None:
                        ref, last_rc = end, ts
                    elif abs(end / ref - 1) > DRIFT and ts - last_rc >= 60:
                        ref, last_rc, n_rc = end, ts, n_rc + 1
                if ref is None or side != "sell":
                    continue
                for tier in TIERS:
                    level = ref * (1 - tier)
                    if ts - last_fill[tier] <= REFRACTORY:
                        continue
                    if end <= level:
                        last_fill[tier] = ts
                        if mode == "impl":
                            ref, last_rc = end, ts   # re-arm below the new price after a fill
                        if ts <= final_prev:
                            n_rc = 0
                            continue                 # warm-up: state only, already written
                        if meta["uni"] == "board" and ts < meta["first_seen"]:
                            uni_tag = "other"         # not yet boarded at fill time
                        else:
                            uni_tag = meta["uni"]
                        fills.append({"pool": key, "kind": meta.get("kind", "v4"), "base": meta["base"], "quote": meta["quote"],
                                      "fee_ppm": meta["fee_ppm"], "uni": uni_tag, "mode": mode, "tier": tier,
                                      "fill_ts": ts, "fill_blk": blk, "ref_px": ref, "fill_px": level,
                                      "cross_kind": "avg" if avg <= level else "end",
                                      "swap_usd": usd, "n_recenters": n_rc if mode == "impl" else None,
                                      "swaps_prev_h": bisect_left(tss, ts) - bisect_left(tss, ts - 3600),
                                      "tape_gap": any(g0 <= ts + 720 and g1 >= ts - 3600 for g0, g1 in gaps),
                                      "px_missing": any(ts - 3600 <= t <= ts + 720 for t in missing),
                                      "px_src": "chain", "usd_src": usd_src})
                        n_rc = 0
        # exits
        f = meta["fee_ppm"] / 1e6
        for fl in fills:
            cut = fl["fill_ts"] + 720
            if cut > max_ts - 60:
                continue                                  # not finalizable this run
            after = [p for p in px if fl["fill_ts"] < p[0] <= cut]
            if after:
                ets, eblk, eavg, eend, _, _, _ = after[-1]
                fl["exit_ts"], fl["exit_px"] = ets, eend       # pool mid after the exit swap
                fl["exit_kind"] = "swap" if ets >= fl["fill_ts"] + 480 else "thin"
            else:
                fl["exit_ts"], fl["exit_px"], fl["exit_kind"] = None, None, "floor"
            gross = (fl["exit_px"] / fl["fill_px"] - 1) if fl["exit_kind"] == "swap" else -0.90
            fl["gross"] = round(gross, 6)
            fl["net_taker"] = round((1 + gross) * (1 - f) ** 2 * (1 - SLIP) - 1, 6)
            fl["net_maker"] = round((1 + gross) * (1 + f) * (1 - f) * (1 - SLIP) - 1, 6)
            fl["created_at"] = int(time.time())
            out.append(fl)
    return out


def summ_short(rs):
    import math
    if len(rs) < 5:
        return f"n={len(rs)}"
    s = sorted(rs); n = len(s)
    geo = 100 * (math.exp(sum(math.log(max(1 + 0.25 * r, 1e-9)) for r in s) / n) - 1)
    return f"n={n} win {100*sum(1 for r in s if r>0)/n:.0f}% med {100*s[n//2]:+.1f}% geo25 {geo:+.2f}%"


def summarize(fills, gated=False):
    """gated=True: only fills with swaps_prev_h >= GATE_SWAPS and no tape gap (the spec amendment's
    activity gate — the tape-equivalent of 'a GT bar exists'). Both tables print every pass."""
    import math
    def geo(rs, frac):
        return 100 * (math.exp(sum(math.log(max(1 + frac * r, 1e-9)) for r in rs) / len(rs)) - 1)
    if gated:
        fills = [f for f in fills if (f.get("swaps_prev_h") or 0) >= GATE_SWAPS and not f.get("tape_gap") and not f.get("px_missing")]
    if not fills:
        print(f"\n[{'GATED' if gated else 'ALL'}] no fills"); return
    days = max((max(f["fill_ts"] for f in fills) - min(f["fill_ts"] for f in fills)) / 86400, 1e-9)
    print(f"\n[{'GATED >=%d swaps prior hour, no tape gap' % GATE_SWAPS if gated else 'ALL (ungated)'}]")
    print(f"{'uni':<11} {'mode':<5} {'tier':<4} {'n':>5} {'floor%':>6} {'win%':>5} {'med':>7} {'mean':>7} "
          f"{'geo10':>6} {'geo25':>6} {'$/day@100':>9} {'rc/fill':>7}")
    for uset, lab in ((("board",), "board"), (("board_v3",), "board_v3"), (("board", "other"), "board+other")):
        for mode in ("spec", "impl"):
            for tier in TIERS:
                g = [x for x in fills if x["uni"] in uset and x["mode"] == mode and x["tier"] == tier]
                if not g:
                    continue
                rs = [x["net_taker"] for x in g]
                s = sorted(rs); n = len(s)
                rc = sum(x["n_recenters"] or 0 for x in g) / n if mode == "impl" else 0
                alt = [x["exit_px"] / x["fill_px"] - 1 if (x["exit_kind"] == "thin" and x["exit_px"]) else x["gross"] for x in g]
                print(f"{lab:<11} {mode:<5} {tier:<4} {n:>5} {100*sum(1 for x in g if x['exit_kind']!='swap')/n:>6.0f} "
                      f"{100*sum(1 for r in s if r>0)/n:>5.0f} {100*s[n//2]:>7.1f} {100*sum(s)/n:>7.1f} "
                      f"{geo(s,0.10):>6.2f} {geo(s,0.25):>6.2f} {100*sum(s)/days:>9.0f} {rc:>7.1f}"
                      f"   | end-cross only: {summ_short([x['net_taker'] for x in g if x['cross_kind']=='end'])}"
                      f" | thin@price gross med {100*sorted(alt)[n//2]:+.1f}%")


def run_pass(uni_cache, now=None):
    now = now or time.time()
    st = sbj("/rh_state?key=eq.paper_needle_final_ts&select=val")
    final_prev = float(st[0]["val"]) if st else 0.0
    start = TAPE_START
    if final_prev > 0:
        start = max(TAPE_START, hour_of(final_prev - LOOKBACK_H * 3600))
    end = hour_of(now)
    hours = list(hours_between(start, end))
    # universe: rebuilt at most once per hour (board snapshots are a 100k-row read)
    if uni_cache.get("hour") != end:
        uni_cache["uni"], uni_cache["hour"] = build_universe(now), end
    uni = uni_cache["uni"]
    if not REPLAY:
        TP.run()                      # extend the chain record to head − SAFETY before reading it
    t_from = (final_prev - LOOKBACK_H * 3600) if final_prev > 0 else \
        dt.datetime.strptime(TAPE_START, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc).timestamp()
    rows, covered_to = load_chain(t_from, now, uni)
    us = attach_usd(rows, uni, t_from, covered_to)
    print(f"  chain swaps {len(rows)} in {len({r['key'] for r in rows})} pools, {hour_of(t_from)} -> "
          f"{dt.datetime.fromtimestamp(covered_to, dt.timezone.utc):%m-%d %H:%M} UTC | usd: {us['tape']} tape, "
          f"{us['est']} estimated, {us['none']} none", flush=True)
    if not rows:
        print("no swaps in range — nothing to do", flush=True); return
    max_ts = covered_to
    frontier = max_ts - 780
    if frontier <= final_prev:
        print(f"record end {hour_of(max_ts)} not past frontier — nothing to finalize", flush=True); return
    # seed the 10-min refractory chain from what is already written (write-once: never re-derive)
    seed = {}
    for r in C.sb_all(f"/paper_needle_fills?select=pool,mode,tier,fill_ts&fill_ts=gt.{final_prev - REFRACTORY}"
                      f"&fill_ts=lte.{final_prev}&order=fill_ts.asc,pool.asc,mode.asc,tier.asc"):
        k = (r["pool"], r["mode"], float(r["tier"]))
        seed[k] = max(seed.get(k, 0), r["fill_ts"])
    gaps = ()                         # the chain record is contiguous over the pass (load_chain raises otherwise): no tape gaps
    INV["n"] = INV["bad"] = SIDE["n"] = SIDE["bad"] = 0
    fills = simulate(rows, uni, max_ts, final_prev, seed, gaps)
    sbad = SIDE["bad"] / max(1, SIDE["n"])
    print(f"  side check: {SIDE['bad']} of {SIDE['n']} price moves disagree with the buy/sell label ({100*sbad:.3f}%)", flush=True)
    if SIDE["n"] > 1000 and sbad > 0.01:
        raise RuntimeError(f"buy/sell labels disagree with price direction on {100*sbad:.1f}% of swaps — sign convention wrong; nothing written")
    bad = INV["bad"] / max(1, INV["n"])
    print(f"  price invariant: {INV['bad']} of {INV['n']} priced swaps outside [pre, post] ± fee ({100*bad:.2f}%)", flush=True)
    if INV["n"] > 1000 and bad > MAX_INVARIANT_BAD:
        raise RuntimeError(f"price invariant failed on {100*bad:.1f}% of swaps — the price join or orientation is wrong; nothing written")
    if fills:
        C.sb_write("/paper_needle_fills?on_conflict=pool,uni,mode,tier,fill_ts", fills,
                   prefer="resolution=merge-duplicates,return=minimal")
    C.sb("POST", "/rh_state?on_conflict=key", [{"key": "paper_needle_final_ts", "val": f"{frontier:.3f}"}],
         prefer="resolution=merge-duplicates,return=minimal")
    print(f"pass done: {len(rows)} swaps in {len({r['key'] for r in rows})} pools, {len(fills)} fills finalized "
          f"(tape to {hour_of(max_ts)}, frontier {final_prev:.0f} -> {frontier:.0f}, "
          f"{sum(1 for f in fills if f['px_missing'])} price-missing)", flush=True)
    summarize(fills)
    summarize(fills, gated=True)


def main():
    t_end = time.time() + RUN_SECONDS
    uni_cache = {}
    if REPLAY:
        st = sbj("/rh_state?key=eq.paper_needle_final_ts&select=val")
        final_prev = float(st[0]["val"]) if st else 0.0
        clock = (final_prev if final_prev > 0 else
                 dt.datetime.strptime(TAPE_START, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc).timestamp()) + REPLAY_STEP_S
        while clock < time.time():
            print(f"\n=== REPLAY pass, simulated now {dt.datetime.fromtimestamp(clock, dt.timezone.utc):%Y-%m-%d %H:%M} UTC", flush=True)
            try:
                run_pass(uni_cache, now=clock)
            except PricesNotReady as e:
                print(f"  waiting for the price sidecar: {e}", flush=True)
                time.sleep(300)
                continue
            clock += REPLAY_STEP_S
        print("replay reached the present; continuing live", flush=True)
    while True:
        t0 = time.time()
        run_pass(uni_cache)
        if time.time() + PASS_INTERVAL > t_end:
            break
        time.sleep(max(0, PASS_INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    main()
