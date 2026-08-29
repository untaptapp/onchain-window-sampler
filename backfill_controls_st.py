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

WHAT IT SELECTS
---------------
Launches that had NOT trended as of their sampling moment, aged so that a full pre-event profile is
computable: the Stage-1 feature set needs a 60-minute baseline plus the lead, so a control must be at
least PRE_MIN_H old. Sampled at RANDOM within that band — picking the most active launches would
bias the very comparison this arm exists to support.

Writes into `trending_bars` (same schema as the GeckoTerminal path), so every downstream analysis
works unchanged and cases/controls share one substrate.

Env: SUPABASE_URL, SUPABASE_KEY, SOLANA_TRACKER_KEY.
     MAX_CALLS (default 150), CREDIT_FLOOR (default 1200), PRE_MIN_H (default 2), POST_H (default 6),
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
    cand = [r for r in sb_all("/pump_launches?select=mint,created_at"
                              f"&created_at=gte.{lo}&created_at=lte.{hi}&order=created_at.asc")
            if r["mint"] not in cased and r["mint"] not in have]
    random.seed(int(now) // 3600)
    random.shuffle(cand)                      # RANDOM, not most-active — else the arm is biased
    cand = cand[:budget]
    print(f"eligible controls without bars: {len(cand)} queued", flush=True)

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
