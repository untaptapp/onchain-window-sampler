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
     PRE_MIN (default 30), POST_H (default 12), RUN_SECONDS (default 0 = single pass),
     MIN_OBS (default 3) — a mint seen once has no path to model, so it is skipped.
"""
import json, os, time, urllib.request, urllib.error
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
MAX_CALLS = int(os.environ.get("MAX_CALLS", "900"))
SLEEP = float(os.environ.get("SLEEP", "2.1"))
PRE_MIN = int(os.environ.get("PRE_MIN", "360"))
POST_H = float(os.environ.get("POST_H", "12"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "0"))
MIN_OBS = int(os.environ.get("MIN_OBS", "3"))
# Share of the call budget spent on CONTROL mints drawn from candidate_universe. Without this,
# minute bars exist only for tokens that reached a board, and the prediction study breaks two ways:
# (1) cases would have richer features than controls, so a model could trivially learn "has bars"
#     = case; and (2) we could never test the second success criterion — whether a flagged token
# that NEVER trends is still profitable — because we would have no price path for it.
UNIVERSE_FRAC = float(os.environ.get("UNIVERSE_FRAC", "0.4"))
GT = "https://api.geckoterminal.com/api/v2/networks/solana"
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
        for a in range(4):
            try:
                with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h), timeout=90) as r:
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
        snaps = sb_all("/trending_snapshots?select=mint,captured_at&order=captured_at.asc")
        first, nobs = {}, defaultdict(int)
        for r in snaps:
            m, t = r["mint"], r["captured_at"] / 1000
            nobs[m] += 1
            if m not in first or t < first[m]:
                first[m] = t
        # a mint seen once has no path to model — never spend GT calls on it
        first = {m: t for m, t in first.items() if nobs[m] >= MIN_OBS}
        pools = {p["mint"]: p for p in sb_all("/trending_pools?select=mint,pool_address,ok")}
        # Coverage from a server-side aggregate view. Deriving per-mint min/max/count by scanning
        # every bar row client-side is O(all bars) on EVERY pass — it already timed out at 122k
        # rows and would only get worse as the table grows.
        have = defaultdict(lambda: [None, None, 0])
        for r in sb_all("/trending_bar_coverage?select=mint,ts_from,ts_to,n_bars"):
            have[r["mint"]] = [r["ts_from"], r["ts_to"], r["n_bars"]]
        now = time.time()
        # CONTROL arm: sample mints seen in the candidate universe that never reached a board.
        # Sampled at random (not by rank) so the control pool stays an unbiased draw from the
        # population at risk — selecting "most active controls" would bias the comparison.
        controls = {}
        if UNIVERSE_FRAC > 0:
            # paginate the WHOLE universe: `limit=1000` is silently capped by PostgREST and covers
            # only the last few sweeps, so the control pool collapsed to a few dozen mints drawn
            # from one moment — not a sample of the population at risk across the study period.
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
        # least-covered first, so a budgeted run always makes progress and is resumable
        todo = sorted(first.items(), key=lambda kv: have[kv[0]][2])
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
        print(f"universe {len(first)} mints · {len(pools)} pools cached · "
              f"{sum(1 for m in first if have[m][2])} with bars · budget {MAX_CALLS}", flush=True)
        new_pools, new_bars, done = [], [], 0
        for mint, t0 in todo:
            if calls["n"] >= MAX_CALLS:
                break
            p = pools.get(mint)
            if p is None:
                p = resolve_pool(mint)
                new_pools.append(p)
                pools[mint] = p
            if not p.get("ok") or not p.get("pool_address"):
                continue
            need_from = t0 - PRE_MIN * 60
            need_to = min(now, t0 + POST_H * 3600)
            cov = have[mint]
            # already covered (allow a 3-bar edge tolerance)
            if cov[0] is not None and cov[0] <= need_from + 180 and cov[1] >= need_to - 180:
                continue
            bars = fetch_bars(p["pool_address"], need_from, need_to)
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
                for i in range(0, len(new_pools), 500):
                    sb("POST", "/trending_pools?on_conflict=mint", new_pools[i:i + 500],
                       prefer="resolution=merge-duplicates,return=minimal")
                print(f"  .. {done} mints, {len(new_bars)} bars + {len(new_pools)} pools flushed, "
                      f"{calls['n']} calls", flush=True)
                new_bars, new_pools = [], []
        for i in range(0, len(new_pools), 500):
            sb("POST", "/trending_pools?on_conflict=mint", new_pools[i:i + 500],
               prefer="resolution=merge-duplicates,return=minimal")
        for i in range(0, len(new_bars), 500):
            sb("POST", "/trending_bars?on_conflict=mint,ts", new_bars[i:i + 500],
               prefer="resolution=merge-duplicates,return=minimal")
        print(f"pass done: {done} mints filled, {len(new_pools)} pools resolved, "
              f"{calls['n']} GT calls", flush=True)
        if not t_end or time.time() >= t_end:
            break
        calls["n"] = 0
        time.sleep(30)


if __name__ == "__main__":
    main()
