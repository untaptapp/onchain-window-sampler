#!/usr/bin/env python3
"""On-demand CONTROL backfill via Solana Tracker — the missing arm of the prediction study.

WHY SOLANA TRACKER, AND WHY ON DEMAND
-------------------------------------
Cases trend at a median age of 10 minutes (GMGN open_timestamp, n=1751). At that age GeckoTerminal
has indexed only ~12% of launches, while Solana Tracker's /chart returns real minute bars for 100%
of them — so ST is the ONLY source that can observe controls in the band where most cases live.

It is metered (1 credit/call, measured — the counter LAGS ~90s, so a naive before/after read
understates consumption by ~30%). But ST serves ARBITRARY HISTORICAL WINDOWS (verified 5 days back),
so control paths never need continuous polling: fetch them once, on demand, only for the controls a
study actually uses. That turns an open-ended drain into a bounded one-off cost.

TWO STAGES, BECAUSE CHARTING BLIND IS 14x TOO EXPENSIVE
------------------------------------------------------
Measured on 729 backfilled controls: the median pump.fun launch produces only 4 minute-bars and just
**2%** reach the >=25 bars the feature set needs. Charting at random therefore burns ~50 credits per
usable control. So:

  1. SCREEN  — POST /tokens/multi, 20 launches per call, ONE credit per batch. Returns
               txns{buys,sells,total,volume}, liquidity and multi-window events. Results are stored
               on pump_launches (screened_at / screen_txns / screen_vol / screen_liq) so eligibility
               is auditable and never re-paid for.
  2. CHART   — /chart only for launches passing the pre-registered activity bar, 1 credit each.

That is ~14x more efficient. It also makes the eligibility filter EXPLICIT rather than letting a
bar-count threshold silently do the same filtering after the fact. The honest risk set is
"launches that sustained real trading activity", not "all launches" — 98% of launches trade for a
few minutes and die, and rejecting those is trivial, not the discrimination a detector needs. The
SAME bar must be applied to the case arm when the comparison is run.

Within the eligible set, controls are taken at RANDOM — picking the most active would bias the very
comparison this arm exists to support.

Writes into `trending_bars` (same schema as the GeckoTerminal path), so every downstream analysis
works unchanged and cases/controls share one substrate.

Env: SUPABASE_URL, SUPABASE_KEY, SOLANA_TRACKER_KEY.
     MAX_CALLS (default 150) — chart calls only; screening is budgeted separately and is ~1/20th
     the cost. CREDIT_FLOOR (default 1200), PRE_MIN_H (default 2), POST_H (default 6),
     SCREEN_MIN_TXNS (default 30) — the pre-registered activity bar,
     SLEEP (default 0.45 -> ~2.2 req/s, under the 3 req/s free-tier limit).
"""
import json, os, random, time, urllib.request, urllib.error

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
STK = os.environ["SOLANA_TRACKER_KEY"]
ST_H = {"x-api-key": STK, "User-Agent": "Mozilla/5.0"}
MAX_CALLS = int(os.environ.get("MAX_CALLS", "150"))
CREDIT_FLOOR = int(os.environ.get("CREDIT_FLOOR", "1200"))
PRE_MIN_H = float(os.environ.get("PRE_MIN_H", "2"))
POST_H = float(os.environ.get("POST_H", "6"))
SLEEP = float(os.environ.get("SLEEP", "0.45"))
SCREEN_MIN_TXNS = int(os.environ.get("SCREEN_MIN_TXNS", "30"))
SCREEN_BATCH = 20


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


def sb_all(path, page=1000, cap=300000):
    """PostgREST caps every response at 1000 rows and truncates SILENTLY — always page."""
    out = []
    while len(out) < cap:
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
             "Range-Unit": "items", "Range": f"{len(out)}-{len(out)+page-1}"}
        try:
            with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h), timeout=90) as r:
                t = r.read(); chunk = json.loads(t) if t else []
        except urllib.error.HTTPError as e:
            if e.code == 416: break
            raise
        if not chunk: break
        out += chunk
        if len(chunk) < page: break
    if len(out) >= cap:
        print(f"!! sb_all cap reached ({cap}) — TRUNCATED", flush=True)
    return out


def credits():
    for _ in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://data.solanatracker.io/credits", headers=ST_H), timeout=20) as x:
                return json.loads(x.read()).get("credits")
        except Exception:
            time.sleep(2)
    return None


def chart(mint, fr, to):
    u = (f"https://data.solanatracker.io/chart/{mint}?type=1m"
         f"&time_from={int(fr)}&time_to={int(to)}")
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers=ST_H), timeout=30) as x:
                j = json.loads(x.read())
            return j.get("oclhv") or j.get("ohlcv") or []
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (a + 1)); continue
            return None
        except Exception:
            time.sleep(1.5)
    return None


def screen(mints):
    """One credit per 20 mints. Returns {mint: (txns_total, volume, liquidity_usd)}."""
    url = "https://data.solanatracker.io/tokens/multi"
    body = json.dumps({"tokens": mints}).encode()
    h = dict(ST_H, **{"Content-Type": "application/json"})
    j = None
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h, data=body,
                                                               method="POST"), timeout=45) as x:
                j = json.loads(x.read())
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (a + 1)); continue
            return {}
        except Exception:
            time.sleep(1.5)
    if j is None:
        return {}
    tok = j.get("tokens") if isinstance(j, dict) else j
    if isinstance(tok, dict):
        entries = list(tok.items())
    elif isinstance(tok, list):
        entries = [(((v.get("token") or {}).get("mint")) or (v.get("pools") or [{}])[0].get("tokenAddress"), v)
                   for v in tok if isinstance(v, dict)]
    else:
        return {}
    out = {}
    for k, v in entries:
        if not isinstance(v, dict):
            continue
        pools = v.get("pools") or []
        p0 = pools[0] if pools else {}
        t = p0.get("txns") or {}
        mint = k or p0.get("tokenAddress")
        if not mint:
            continue
        out[mint] = (t.get("total") or 0, t.get("volume") or 0.0,
                     ((p0.get("liquidity") or {}).get("usd")) or 0.0)
    return out


def main():
    c0 = credits()
    print(f"Solana Tracker credits at start: {c0}", flush=True)
    if c0 is None:
        print("cannot read credits — refusing to spend blind."); return
    if c0 <= CREDIT_FLOOR:
        print(f"at/below the floor ({CREDIT_FLOOR}) — stopping before spending."); return
    budget = min(MAX_CALLS, max(0, c0 - CREDIT_FLOOR))
    print(f"budget this run: {budget} calls (floor {CREDIT_FLOOR})", flush=True)

    cased = {r["mint"] for r in sb_all("/trending_snapshots?select=mint")}
    have = {r["mint"] for r in sb_all("/trending_bar_coverage?select=mint")}
    now = time.time()
    lo, hi = int(now - 24 * 3600), int(now - PRE_MIN_H * 3600)
    pool = [r for r in sb_all("/pump_launches?select=mint,created_at,screened_at,screen_txns"
                              f"&created_at=gte.{lo}&created_at=lte.{hi}&order=created_at.asc")
            if r["mint"] not in cased and r["mint"] not in have]
    random.seed(int(now) // 3600)
    random.shuffle(pool)                      # RANDOM, not most-active — else the arm is biased

    # ---- STAGE 1: screen, 1 credit per 20 ----
    unscreened = [r for r in pool if not r.get("screened_at")]
    screen_batches = int(os.environ.get("SCREEN_BATCHES", "40"))
    screened, spend_screen = {}, 0
    for i in range(0, min(len(unscreened), screen_batches * SCREEN_BATCH), SCREEN_BATCH):
        chunk = [r["mint"] for r in unscreened[i:i + SCREEN_BATCH]]
        res = screen(chunk); spend_screen += 1
        time.sleep(SLEEP)
        # created_at MUST be included: PostgREST upsert is INSERT..ON CONFLICT, so the insert half
        # still has to satisfy NOT NULL even when the row already exists. Omitting it made every
        # screen write fail silently while the run reported success.
        cmap = {r["mint"]: r["created_at"] for r in unscreened[i:i + SCREEN_BATCH]}
        st_, _ = sb("POST", "/pump_launches?on_conflict=mint",
                    [{"mint": m, "created_at": cmap[m], "screened_at": int(time.time()),
                      "screen_txns": res.get(m, (0, 0.0, 0.0))[0],
                      "screen_vol": res.get(m, (0, 0.0, 0.0))[1],
                      "screen_liq": res.get(m, (0, 0.0, 0.0))[2]} for m in chunk],
                    prefer="resolution=merge-duplicates,return=minimal")
        if not (st_ and 200 <= st_ < 300):
            print(f"!! screen write FAILED status={st_} — aborting rather than re-paying next run",
                  flush=True)
            return
        screened.update(res)
    active = {m: v for m, v in screened.items() if v[0] >= SCREEN_MIN_TXNS}
    print(f"screened {len(screened)} launches for {spend_screen} credits -> {len(active)} passed "
          f"the >={SCREEN_MIN_TXNS}-txn bar ({len(active)/max(1,len(screened))*100:.0f}%)", flush=True)

    # ---- STAGE 2: chart ONLY the active ones ----
    bym = {r["mint"]: r["created_at"] for r in pool}
    already = [r for r in pool if (r.get("screen_txns") or 0) >= SCREEN_MIN_TXNS]
    cand = ([{"mint": m, "created_at": bym[m]} for m in active if m in bym]
            + [r for r in already if r["mint"] not in active])
    random.shuffle(cand)
    cand = cand[:max(0, budget - spend_screen)]
    print(f"charting {len(cand)} ACTIVE controls", flush=True)

    rows, done, empty = [], 0, 0
    for r in cand:
        m, t0 = r["mint"], r["created_at"]
        o = chart(m, t0 - 60, t0 + POST_H * 3600)
        time.sleep(SLEEP)
        done += 1
        if not o:
            empty += 1; continue
        for b in o:
            if b.get("time") and b.get("close"):
                rows.append({"mint": m, "ts": int(b["time"]), "o": b.get("open"),
                             "h": b.get("high"), "l": b.get("low"), "c": b.get("close"),
                             "vol": b.get("volume")})
        if len(rows) >= 2000:
            for i in range(0, len(rows), 500):
                sb("POST", "/trending_bars?on_conflict=mint,ts", rows[i:i+500],
                   prefer="resolution=merge-duplicates,return=minimal")
            print(f"  .. {done}/{len(cand)} controls, {len(rows)} bars flushed", flush=True)
            rows = []
    for i in range(0, len(rows), 500):
        sb("POST", "/trending_bars?on_conflict=mint,ts", rows[i:i+500],
           prefer="resolution=merge-duplicates,return=minimal")
    print("waiting 90s for the credit counter to settle (it lags)...", flush=True)
    time.sleep(90)
    c1 = credits()
    print(f"done: {done} controls fetched ({empty} returned no bars)", flush=True)
    print(f"credits {c0} -> {c1}  (spent {c0-c1 if c1 is not None else '?'})", flush=True)


if __name__ == "__main__":
    main()
