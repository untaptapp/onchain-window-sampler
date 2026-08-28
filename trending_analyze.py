#!/usr/bin/env python3
"""Scheduled analysis/rollup for the trending front-run study.

Turns raw `trending_snapshots` (the collected price paths) into an analysis-ready layer,
so the winning-subset / exit study is ALWAYS CURRENT without manual script runs:
  1) trending_outcomes      — one row per (source,mint) entry: entry features + static-horizon
                              forward returns + MFE / time-to-MFE / MAE (from the snapshot series).
  2) trending_strategy_stats — per-filter n / mean / median / ex-top1 / %win, per run (time-series),
                              i.e. the auto-updating winning-subset hunt. ex-top1 = mean after
                              dropping the single best trade (the lottery-robustness check).

Reads snapshots only (never writes them). Resolution = each source's snapshot cadence (GeckoTerminal
5-min is finest → coarser sources understate MFE). Env: SUPABASE_URL, SUPABASE_KEY.
"""
import bisect, json, os, time, urllib.request, urllib.error, statistics as st
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
HZ = {"ret_15m": 15, "ret_30m": 30, "ret_1h": 60, "ret_2h": 120, "ret_4h": 240, "ret_6h": 360}


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


def sb_all(path, page=1000, cap=400000):
    """Fetch EVERY row for `path`, paginating with Range headers.

    PostgREST caps a single response at 1000 rows regardless of any `limit=` in the query, and
    it truncates SILENTLY — a `limit=300000` returns 1000 rows with a 200 status. Combined with
    `order=...asc` that quietly served the OLDEST 1000 rows and hid everything collected since,
    so every rollup was computed on a stale slice of the data. Always read through this helper.
    """
    out = []
    while len(out) < cap:
        lo = len(out); hi = lo + page - 1
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
             "Range-Unit": "items", "Range": f"{lo}-{hi}"}
        req = urllib.request.Request(SB + path, headers=h)
        chunk = None
        for a in range(4):
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    t = r.read(); chunk = json.loads(t) if t else []
                break
            except urllib.error.HTTPError as e:
                if e.code == 416:
                    chunk = []; break
                if e.code in (429, 500, 502, 503):
                    time.sleep(1.5 * (a + 1)); continue
                chunk = []; break
            except Exception:
                time.sleep(1.5 * (a + 1))
        if not chunk:
            break
        out += chunk
        if len(chunk) < page:
            break
    return out


def ff(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def mean(v):
    return sum(v) / len(v) if v else None


def buyskew(ex, source):
    """net-buying proxy from a source's entry features."""
    if source == "solanatracker":
        pt = (ex.get("pool_txns") or {})
        b, s = pt.get("buys"), pt.get("sells")
    elif source == "gmgn":
        b, s = ex.get("buys"), ex.get("sells")
    elif source == "geckoterminal":
        t = (ex.get("txns") or {}).get("h1") or {}
        b, s = t.get("buys"), t.get("sells")
    else:
        return None
    return (b / (b + s)) if (b and s is not None and (b + s) > 0) else None


def p5m(ex, source):
    if source == "solanatracker":
        return ff((((ex.get("events") or {}).get("5m")) or {}).get("priceChangePercentage"))
    if source == "gmgn":
        return ff(ex.get("price_change_percent5m"))
    if source == "geckoterminal":
        return ff((ex.get("pchg") or {}).get("m5"))
    return None


# per-source filter set for the strategy-stats rollup (feature accessors return None if N/A)
def filters_for(source):
    f = [("ALL", lambda o: True),
         ("mcap<150k", lambda o: o["entry_mcap"] and o["entry_mcap"] < 1.5e5),
         ("rank>15", lambda o: o["entry_rank"] and o["entry_rank"] > 15),
         ("buyskew>=0.6", lambda o: (buyskew(o["entry_extra"], source) or 0) >= 0.6),
         ("not_spiked(p5m<=0)", lambda o: (lambda v: v is not None and v <= 0)(p5m(o["entry_extra"], source)))]
    if source == "gmgn":
        f += [("bundle<0.1", lambda o: (lambda v: v is not None and v < 0.1)(ff(o["entry_extra"].get("bundler_rate")))),
              ("bundle>=0.3", lambda o: (lambda v: v is not None and v >= 0.3)(ff(o["entry_extra"].get("bundler_rate")))),
              ("rug<=0.1", lambda o: (lambda v: v is not None and v <= 0.1)(ff(o["entry_extra"].get("rug_ratio")))),
              ("smart_degen>=50", lambda o: (ff(o["entry_extra"].get("smart_degen_count")) or 0) >= 50)]
    return f


# ---- SOL/USD reference ------------------------------------------------------------------
# Every board feed stores a token's price in USD, so a raw USD return silently bundles the
# token bet with a SOL/USD bet we never intended to take. A trade here is funded in SOL and
# settled in SOL, so SOL-denominated P&L is the real P&L. SOL moved -2.98% across an early
# 18.5h window (5.26% peak-to-trough) — worth ~1.8pp on median returns, i.e. roughly half the
# median trade — and it is a systematic bias, not noise. Unlike Jupiter quotes, SOL/USD IS
# backfillable, so we keep a reference series and carry both denominations.
SOL_POOL = os.environ.get("SOL_POOL", "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE")  # SOL/USDC
GT_UA = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}


def refresh_sol_ref():
    """Pull recent SOL/USD 5-min bars into sol_usd_ref (upsert), then return the full series.
    The table accumulates beyond GeckoTerminal's ~3-day OHLCV window."""
    try:
        u = (f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{SOL_POOL}"
             "/ohlcv/minute?aggregate=5&limit=1000")
        with urllib.request.urlopen(urllib.request.Request(u, headers=GT_UA), timeout=30) as r:
            bars = json.loads(r.read())["data"]["attributes"]["ohlcv_list"]
        rows = [{"ts": int(b[0]), "price": float(b[4])} for b in bars if b[4]]
        for i in range(0, len(rows), 500):
            sb("POST", "/sol_usd_ref?on_conflict=ts", rows[i:i + 500],
               prefer="resolution=merge-duplicates,return=minimal")
        print(f"sol_usd_ref: refreshed {len(rows)} bars", flush=True)
    except Exception as ex:
        print("sol ref refresh failed (using stored):", repr(ex), flush=True)
    got = sb_all("/sol_usd_ref?select=ts,price&order=ts.asc")
    series = [(r["ts"], r["price"]) for r in got]
    print(f"sol_usd_ref: {len(series)} bars in table", flush=True)
    return series


def sol_at(series, ts):
    """SOL/USD nearest `ts`. None if the series doesn't cover that moment — better a missing
    conversion than one done at the wrong exchange rate."""
    if not series:
        return None
    i = bisect.bisect_left(series, (ts, -1))
    cand = []
    if i < len(series): cand.append(series[i])
    if i > 0: cand.append(series[i - 1])
    if not cand:
        return None
    best = min(cand, key=lambda z: abs(z[0] - ts))
    return best[1] if abs(best[0] - ts) <= 3600 else None


QUOTE_SIZE_SOL = float(os.environ.get("QUOTE_SIZE_SOL", "1"))
QUOTE_TOL_SEC = int(os.environ.get("QUOTE_TOL_SEC", "5400"))   # 90 min


def load_quotes():
    """mint -> sorted [(ts, price_impact_pct)] at the reference size, for point-in-time costing."""
    rows = sb_all("/trending_quotes?select=mint,quoted_at,size_sol,price_impact_pct,ok"
                  f"&size_sol=eq.{QUOTE_SIZE_SOL}&ok=is.true&order=quoted_at.asc")
    q = defaultdict(list)
    if True:
        for r in rows:
            if r.get("price_impact_pct") is not None:
                q[r["mint"]].append((r["quoted_at"], r["price_impact_pct"]))
    return q


def nearest_quote(quotes, mint, ts):
    """Quoted impact closest in time to `ts`, or None if we never sampled near that moment.
    Deliberately returns None rather than reaching for a far-away quote: a cost from three hours
    later is not the cost you would have paid."""
    if not quotes:
        return None
    arr = quotes.get(mint)
    if not arr:
        return None
    best = min(arr, key=lambda z: abs(z[0] - ts))
    return best[1] if abs(best[0] - ts) <= QUOTE_TOL_SEC else None


def build_outcomes(source, quotes=None, solref=None):
    rows = sb_all(f"/trending_snapshots?source=eq.{source}"
                  "&select=mint,captured_at,rank,price,market_cap,liquidity,extra"
                  "&order=captured_at.asc")
    if not rows:
        return []
    traj = defaultdict(list)
    for r in rows:
        if r.get("price") and r["price"] > 0:
            traj[r["mint"]].append(r)
    outs = []
    for m, pts in traj.items():
        pts.sort(key=lambda z: z["captured_at"])
        if len(pts) < 2:
            continue
        t0 = pts[0]["captured_at"] / 1000
        p0 = pts[0]["price"]
        rec = {"source": source, "mint": m, "entry_ts": int(t0), "entry_rank": pts[0].get("rank"),
               "entry_price": p0, "entry_mcap": pts[0].get("market_cap"),
               "entry_extra": pts[0].get("extra") or {}, "n_obs": len(pts),
               "span_min": int((pts[-1]["captured_at"] / 1000 - t0) / 60),
               "last_ret": pts[-1]["price"] / p0 - 1}
        for col, h in HZ.items():
            cand = [x["price"] for x in pts if x["captured_at"] / 1000 <= t0 + h * 60]
            rec[col] = (cand[-1] / p0 - 1) if len(cand) >= 2 else None
        series = [((x["captured_at"] / 1000 - t0) / 60, x["price"] / p0 - 1) for x in pts]
        peak = max(series, key=lambda z: z[1])
        rec["mfe_pct"] = peak[1]; rec["mfe_min"] = int(peak[0])
        rec["mae_pct"] = min(v for _, v in series)
        # POINT-IN-TIME liquidity. Execution cost is not a property of the token, it is a property
        # of the MOMENT — liquidity drains ~11% exactly when price falls, so the sell leg of a
        # losing trade is dearer than the buy leg. Carrying only "latest" liquidity would misprice
        # every modelled entry and exit, so record it at each point we actually model.
        peak_pt = max(pts, key=lambda x: x["price"])
        liqs = [x.get("liquidity") for x in pts if x.get("liquidity")]
        rec["entry_liq"] = pts[0].get("liquidity")
        rec["mfe_liq"] = peak_pt.get("liquidity")
        rec["last_liq"] = pts[-1].get("liquidity")
        rec["min_liq"] = min(liqs) if liqs else None
        # quoted execution cost (Jupiter) nearest the entry and nearest the exit, when we have it.
        # Quotes only exist from the moment trending_quotes.py started — history is un-costable.
        qe = nearest_quote(quotes, m, t0)
        qx = nearest_quote(quotes, m, pts[-1]["captured_at"] / 1000)
        # SOL-denominated view: a return in SOL is the P&L actually realised by a SOL-funded
        # trade. Derivable from any USD return as (1+r_usd) * (sol_entry/sol_exit) - 1, so we
        # store the rate at each modelled point rather than a column per horizon.
        se = sol_at(solref, t0)
        sm = sol_at(solref, peak_pt["captured_at"] / 1000)
        sl = sol_at(solref, pts[-1]["captured_at"] / 1000)
        rec["sol_usd_entry"] = se; rec["sol_usd_mfe"] = sm; rec["sol_usd_last"] = sl
        rec["mfe_pct_sol"] = ((1 + rec["mfe_pct"]) * (se / sm) - 1) if (se and sm) else None
        rec["last_ret_sol"] = ((1 + rec["last_ret"]) * (se / sl) - 1) if (se and sl) else None
        rec["q_entry_imp"] = qe; rec["q_exit_imp"] = qx
        # keys must be present on EVERY row — PostgREST rejects a ragged batch
        rec["net_mfe"] = rec["net_last"] = None
        if qe is not None and qx is not None:
            rt = (qe + qx) / 100.0
            rec["net_mfe"] = rec["mfe_pct"] - rt
            rec["net_last"] = rec["last_ret"] - rt
        rec["updated_at"] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        outs.append(rec)
    return outs


def rollup_stats(source, outs, run_ts):
    stats = []
    hz_map = {"mfe": "mfe_pct", "1h": "ret_1h", "2h": "ret_2h", "last": "last_ret",
              "net_mfe": "net_mfe", "net_last": "net_last",
              "mfe_sol": "mfe_pct_sol", "last_sol": "last_ret_sol"}
    for fname, pred in filters_for(source):
        sub = [o for o in outs if pred(o)]
        for hname, col in hz_map.items():
            vals = [o[col] for o in sub if o.get(col) is not None]
            if len(vals) < 5:
                continue
            srt = sorted(vals)
            stats.append({"run_ts": run_ts, "source": source, "filter_name": fname, "horizon": hname,
                          "n": len(vals), "mean_ret": mean(vals), "median_ret": st.median(vals),
                          "extop1_ret": mean(srt[:-1]), "pct_win": sum(v > 0 for v in vals) / len(vals)})
    return stats


def main():
    run_ts = int(time.time())
    quotes = load_quotes()
    solref = refresh_sol_ref()
    print(f"loaded quotes for {len(quotes)} mints @ {QUOTE_SIZE_SOL} SOL", flush=True)
    all_stats = []
    for source in ("geckoterminal", "solanatracker", "gmgn", "fomoscan"):
        outs = build_outcomes(source, quotes, solref)
        if not outs:
            continue
        # upsert outcomes in chunks
        for i in range(0, len(outs), 500):
            sb("POST", "/trending_outcomes?on_conflict=source,mint", outs[i:i + 500],
               prefer="resolution=merge-duplicates,return=minimal")
        s = rollup_stats(source, outs, run_ts)
        all_stats += s
        print(f"{source}: {len(outs)} outcomes, {len(s)} stat rows", flush=True)
    if all_stats:
        for i in range(0, len(all_stats), 500):
            sb("POST", "/trending_strategy_stats?on_conflict=run_ts,source,filter_name,horizon",
               all_stats[i:i + 500], prefer="resolution=merge-duplicates,return=minimal")
    print(f"done · run_ts={run_ts} · {len(all_stats)} stat rows", flush=True)


if __name__ == "__main__":
    main()
