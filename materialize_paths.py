#!/usr/bin/env python3
"""Compute bar-resolution path outcomes into `trending_paths`, so raw bars become droppable.

Minute bars are the largest storage cost in the project (~59 MB/day, unbounded in mint count) and
the 500 MB free tier is ~3 days from exhaustion. One row here is ~250 B and stands in for ~45 kB of
bars. Everything is computed at MINUTE resolution using intra-bar high/low, so unlike
trending_outcomes — which reads 5/15/30-min snapshots — the exit statistics are real and not
hold-returns wearing a stop-loss label.

Stores a GRID of horizon returns and the full MFE/MAE shape alongside the two frozen rules, so a
future rule can still be approximated after its bars are gone. That is deliberate: POST_H=3 was a
storage decision made for a fast rule that silently made the slower rule chosen later unevaluable
for 60.5% of Track A, and this table is the same kind of decision. Bar retention stays OFF unless
someone sets KEEP_DAYS.

Env: SUPABASE_URL, SUPABASE_KEY. HORIZON_H (default 3), KEEP_DAYS (default 0 = never drop bars).
"""
import os, sys, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest as B
import strategy_grid as G
import venue_edge as V

KEEP_DAYS = int(os.environ.get("KEEP_DAYS", "0"))


def sb(method, path, payload=None, prefer=None):
    """Write helper — backtest.py is read-only and has no equivalent."""
    import json, urllib.request, urllib.error
    h = {"apikey": B.KEY, "Authorization": f"Bearer {B.KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(B.SB + path, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            t = r.read()
            return r.status, (json.loads(t) if t else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]
HORIZONS = [("ret_15m", 15), ("ret_30m", 30), ("ret_1h", 60), ("ret_2h", 120),
            ("ret_3h", 180), ("ret_6h", 360), ("ret_12h", 720)]


def shape(bars, solat, t0, mark=None):
    """MFE/MAE and static-horizon returns, all SOL-denominated.

    `mark` is how far collection has actually got for THIS mint (trending_pools.last_fetch_to,
    falling back to the corpus high-water). A horizon counts as reached when the CORPUS has passed
    it -- NOT when this token's own bars do. The latter drops every token that stopped trading
    before the horizon, and a token stops trading mostly because it died, so that population is
    selected on the outcome: only 62.2% of Solana paths had a ret_3h and 37.8% had span_min < 180.
    A token that stopped trading exits at its last traded price, which is a real outcome -- and an
    optimistic one, since you would in fact be stuck in something with no bid. Only an UNFINISHED
    horizon is NULL. materialize_rh.sql has always done it this way; Solana did not, which made the
    two chains' returns non-comparable as well as biased."""
    p0 = bars[0][4]
    if not p0 or p0 <= 0:
        return {}
    a0 = solat(t0)
    out, hi, lo, hi_t, lo_t = {}, p0, p0, 0, 0
    for (ts, o, h, l, c, *_v) in bars[1:]:
        if h > hi: hi, hi_t = h, ts
        if l < lo: lo, lo_t = l, ts
    def sol(r, ts):
        b = solat(ts)
        return ((1 + r) * (a0 / b) - 1) if (a0 and b) else r
    out["mfe_pct"] = sol(hi / p0 - 1, hi_t or t0)
    out["mfe_min"] = int((hi_t - t0) / 60) if hi_t else 0
    out["mae_pct"] = sol(lo / p0 - 1, lo_t or t0)
    out["mae_min"] = int((lo_t - t0) / 60) if lo_t else 0
    reached = mark if mark is not None else (bars[-1][0] if bars else None)
    for name, mins in HORIZONS:
        cut = t0 + mins * 60
        seg = [b for b in bars if b[0] <= cut]
        # Report the horizon when COLLECTION has passed it, marking a token that went quiet at its
        # last traded price. Gating on this token's own last bar instead is what selected the
        # population on the outcome; gating on nothing at all would be the censoring bug, which is
        # why `reached` still has to clear the cut.
        out[name] = (sol(seg[-1][4] / p0 - 1, seg[-1][0])
                     if (len(seg) > 1 and reached is not None and reached >= cut) else None)
    return out


def main():
    solat = B.load_sol()
    B.load_dex()
    routes = V.load_routes()
    ents = G.enrich(B.load_entries(solat))
    stats = defaultdict(int)
    rows = []
    for e in ents:
        r = {"source": e["src"], "mint": e["mint"], "entry_ts": int(e["t0"]),
             "entry_price": e["bars"][0][4], "age_s": (e.get("age_min") or 0) * 60 or None,
             "n_bars": len(e["bars"]), "last_bar_ts": int(e["bars"][-1][0]),
             "span_min": int((e["bars"][-1][0] - e["t0"]) / 60),
             "horizon_h": B.HORIZON_H, "liq_entry": e.get("liq")}
        r.update(shape(e["bars"], solat, e["t0"], e.get("fetched_to") or B._HW))
        # Traded volume across the FULL horizon, stored so a screen can apply its own
        # size-appropriate participation limit without re-materialising. The floor applied inside
        # pit_net_return is deliberately conservative (non-markets only); this column is the raw
        # fact a stricter filter needs.
        r["win_vol"] = V.window_volume(e["bars"], e["t0"], e["t0"] + B.HORIZON_H * 3600)
        for nm, rule in (("capped", G.CAPPED), ("runner", G.RUNNER)):
            g, xts, closed = B.simulate(e["bars"], rule, e.get("fetched_to"))
            r[f"{nm}_closed"] = bool(closed)
            if g is None:
                r[f"{nm}_net"], r[f"{nm}_exit_ts"] = None, None
                continue
            net, ki, ko = V.pit_net_return(e, rule, solat, routes, stats)
            r[f"{nm}_net"], r[f"{nm}_exit_ts"] = net, int(xts)
            if nm == "capped":
                r["entry_venue"], r["exit_venue"] = ki, ko
                r["liq_exit"] = B.liq_at(e["liqser"], xts, stats=stats)
        rows.append(r)
    print(f"computed {len(rows)} paths; fallbacks {dict(stats)}", flush=True)
    for i in range(0, len(rows), 200):
        st, _ = sb("POST", "/trending_paths?on_conflict=source,mint", rows[i:i + 200],
                     prefer="resolution=merge-duplicates,return=minimal")
        if st >= 300:
            print(f"!! write failed {st} at {i}", flush=True)
            raise SystemExit(1)
    print(f"wrote {len(rows)} rows to trending_paths", flush=True)
    st, dropped = sb("POST", "/rpc/prune_bars_materialised", {"keep_days": KEEP_DAYS})
    print(f"bar retention: KEEP_DAYS={KEEP_DAYS} -> "
          f"{dropped if st == 200 else f'FAILED({st})'} bars dropped"
          f"{' (retention OFF)' if KEEP_DAYS <= 0 else ''}", flush=True)


if __name__ == "__main__":
    main()
