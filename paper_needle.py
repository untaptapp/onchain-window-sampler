#!/usr/bin/env python3
"""PAPER BOOK for the needle range-order strategy, replayed against the shadow swap tape.

The frozen spec (research/needle-lp-forward-test.md) is scored on GT minute bars. This book
re-scores the SAME rule at swap resolution from `shadow-tape/swaps/…` (every decoded swap on the
chain, ~230k/hour): a resting bid 30%/50% below a reference price fills when a sell swap carries
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
reporting. The post-swap price is estimated as avg²/prev_end (constant-liquidity identity:
average execution price is the geometric mean of start and end); `cross_kind` records whether
the level was crossed by that estimate or by the swap's average price itself (conservative).

Exit per spec: last swap in (fill, fill+12 min]; if it is earlier than fill+8 min the exit is
'thin' and the trade scores at the −90% FLOOR (exit_px is still stored, so the thin-price
alternative is recomputable); no swap at all is 'floor'. net_taker = (1+g)(1−f)²(1−slip)−1 as
registered; net_maker credits the pool fee on entry instead of paying it. slip = 1.5% flat.

Env: SUPABASE_URL, SUPABASE_KEY, SLIP (0.015), LOOKBACK_H (2, hours re-processed behind the
bookmark), TAPE_START (2026-09-09/23).
"""
import gzip, io, json, os, sys, time, urllib.request, datetime as dt
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


def hours_between(a, b):
    ta = dt.datetime.strptime(a, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc)
    tb = dt.datetime.strptime(b, "%Y-%m-%d/%H").replace(tzinfo=dt.timezone.utc)
    while ta <= tb:
        yield ta.strftime("%Y-%m-%d/%H")
        ta += dt.timedelta(hours=1)


def hour_of(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d/%H")


def build_universe(now, hours):
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
      'other'  = non-board base tokens: their highest-volume hookless <=3% v4 pool over this
                 run's hours (shadow_venue_hour), one per token."""
    snaps = BT.sb_keyset(f"source=eq.gmgn_rh&captured_at=gte.{int((now - 14 * 86400) * 1000)}",
                         "mint,captured_at")
    first = {}
    for r in snaps:
        m, t = r["mint"].lower(), r["captured_at"] / 1000
        if m not in first or t < first[m]:
            first[m] = t
    mints = sorted(first)
    deepest = {}
    for i in range(0, len(mints), 80):
        q = ",".join(mints[i:i + 80])
        for p in C.sb_all(f"/trending_pools?select=mint,pool_address,dex&ok=eq.true&pool_address=not.is.null"
                          f"&mint=in.({q})&order=mint.asc"):
            deepest[p["mint"].lower()] = (p["pool_address"].lower(), p.get("dex"))
    uni = {}
    v4_ids = sorted({a for a, _ in deepest.values() if len(a) == 66})
    gated = {}
    for i in range(0, len(v4_ids), 80):
        q = ",".join(v4_ids[i:i + 80])
        for p in C.sb_all(f"/rh_pool_fees?select=pool_id,currency0,currency1,fee_ppm"
                          f"&hooks=eq.{ZERO}&fee_dynamic=eq.false&fee_ppm=lte.{FEE_MAX}"
                          f"&pool_id=in.({q})&order=pool_id.asc"):
            gated[p["pool_id"].lower()] = p
    v3_ids = sorted({a for a, _ in deepest.values() if len(a) == 42})
    v3meta = {}
    for i in range(0, len(v3_ids), 80):
        q = ",".join(v3_ids[i:i + 80])
        for p in C.sb_all(f"/shadow_pool?select=key,token0,token1,fee_ppm&kind=eq.v3&fee_ppm=lte.{FEE_MAX}"
                          f"&key=in.({q})&order=key.asc"):
            v3meta[p["key"].lower()] = p
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
    # 'other': highest-volume gated v4 pool per non-board base over this run's hours
    vol = defaultdict(float)
    for h in hours:
        iso = dt.datetime.strptime(h, "%Y-%m-%d/%H").strftime("%Y-%m-%dT%H:00:00Z")   # '+00:00' decodes to a space in a URL
        for r in C.sb_all(f"/shadow_venue_hour?select=venue,usd_vol&kind=eq.v4&delta_s=eq.3&hour=eq.{iso}&order=venue.asc"):
            vol[r["venue"].lower()] += r["usd_vol"] or 0
    best = {}
    for p in C.sb_all(f"/shadow_pool?select=key,token0,token1,fee_ppm,hooks&kind=eq.v4"
                      f"&hooks=eq.{ZERO}&fee_ppm=lte.{FEE_MAX}&order=key.asc"):
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
    print(f"universe: {n_board} deepest-pool board tokens "
          f"({sum(1 for u in uni.values() if u['uni']=='board')} v4 in-spec, "
          f"{sum(1 for u in uni.values() if u['uni']=='board_v3')} v3) + {len(best)} other tokens' top pools", flush=True)
    return uni


def simulate(rows, uni, max_ts):
    """rows: tape swaps of universe pools (any order). Returns finalized fill rows."""
    by_pool = defaultdict(list)
    for r in rows:
        by_pool[r["key"]].append(r)
    out = []
    for key, sw in by_pool.items():
        sw.sort(key=lambda r: (r["blk"], r["li"]))
        meta = uni[key]
        qidx = meta["qidx"]
        px = []          # (ts, blk, avg, end, side, usd)
        prev_end = None
        for r in sw:
            a0, a1 = int(r["a0"]), int(r["a1"])
            aq, ab = (a0, a1) if qidx == 0 else (a1, a0)
            if aq == 0 or ab == 0:
                continue
            avg = abs(aq) / abs(ab)
            end = avg if prev_end is None else min(max(avg * avg / prev_end, avg / 4), avg * 4)
            prev_end = end
            px.append((r["ts"], r["blk"], avg, end, "buy" if aq > 0 else "sell", r.get("usd")))
        if len(px) < 2:
            continue
        fills = []
        for mode in ("spec", "impl"):
            ref, ref_minute, last_ts_seen, last_rc, n_rc = None, None, None, 0, 0
            last_fill = {t: -1e12 for t in TIERS}
            minute_close = {}
            for ts, blk, avg, end, side, usd in px:
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
                    if end <= level or avg <= level:
                        last_fill[tier] = ts
                        if meta["uni"] == "board" and ts < meta["first_seen"]:
                            uni_tag = "other"         # not yet boarded at fill time
                        else:
                            uni_tag = meta["uni"]
                        fills.append({"pool": key, "kind": meta.get("kind", "v4"), "base": meta["base"], "quote": meta["quote"],
                                      "fee_ppm": meta["fee_ppm"], "uni": uni_tag, "mode": mode, "tier": tier,
                                      "fill_ts": ts, "fill_blk": blk, "ref_px": ref, "fill_px": level,
                                      "cross_kind": "avg" if avg <= level else "end",
                                      "swap_usd": usd, "n_recenters": n_rc if mode == "impl" else None})
                        n_rc = 0
                        if mode == "impl":
                            ref, last_rc = end, ts   # re-arm below the new price after a fill
        # exits
        f = meta["fee_ppm"] / 1e6
        for fl in fills:
            cut = fl["fill_ts"] + 720
            if cut > max_ts - 60:
                continue                                  # not finalizable this run
            after = [p for p in px if fl["fill_ts"] < p[0] <= cut]
            if after:
                ets, eblk, eavg, eend, _, _ = after[-1]
                fl["exit_ts"], fl["exit_px"] = ets, eavg
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


def summarize(fills):
    import math
    def geo(rs, frac):
        return 100 * (math.exp(sum(math.log(max(1 + frac * r, 1e-9)) for r in rs) / len(rs)) - 1)
    days = max((max(f["fill_ts"] for f in fills) - min(f["fill_ts"] for f in fills)) / 86400, 1e-9) if fills else 1
    print(f"\n{'uni':<11} {'mode':<5} {'tier':<4} {'n':>5} {'floor%':>6} {'win%':>5} {'med':>7} {'mean':>7} "
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


def main():
    now = time.time()
    bm = sbj("/rh_state?key=eq.paper_needle_hour&select=val")
    start = TAPE_START
    if bm:
        t = dt.datetime.strptime(bm[0]["val"], "%Y-%m-%d/%H") - dt.timedelta(hours=LOOKBACK_H)
        start = max(TAPE_START, t.strftime("%Y-%m-%d/%H"))
    end = hour_of(now)
    hours = list(hours_between(start, end))
    uni = build_universe(now, hours)
    rows = []
    for h in hours:
        hr = load_hour(h, uni)
        rows += hr
        print(f"  {h}: {len(hr)} universe swaps", flush=True)
    if not rows:
        print("no tape rows in range — nothing to do"); return
    max_ts = max(r["ts"] for r in rows)
    fills = simulate(rows, uni, max_ts)
    if fills:
        C.sb_write("/paper_needle_fills?on_conflict=pool,uni,mode,tier,fill_ts", fills,
                   prefer="resolution=merge-duplicates,return=minimal")
    last_final = max((f["fill_ts"] for f in fills), default=None)
    bookmark = hour_of(min(max_ts - 900, last_final or max_ts))
    C.sb("POST", "/rh_state?on_conflict=key", [{"key": "paper_needle_hour", "val": bookmark}],
         prefer="resolution=merge-duplicates,return=minimal")
    print(f"pass done: {len(rows)} swaps in {len({r['key'] for r in rows})} pools, {len(fills)} fills finalized "
          f"(tape to {hour_of(max_ts)}, bookmark {bookmark})", flush=True)
    summarize(fills)


if __name__ == "__main__":
    main()
