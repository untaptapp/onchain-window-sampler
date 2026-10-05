#!/usr/bin/env python3
"""Price path per paid-signal event from GeckoTerminal OHLCV → boost_bars (minute −30m..+3h, hour ..+48h).

WHY: the backtest needs a path for every event — live AND backfilled — and DexScreener only serves
the present. GeckoTerminal candles are backfillable (B3) and per-POOL (B2): the pool is the event's
DexScreener pair when GT knows it, else GT's top pool for the token. Candles are priced in the quote
TOKEN (SOL/ETH/BNB), not USD, so returns are chain-native (D14); USD conversion is an analysis choice.

Budget: keyless GT sustains ~5–6 req/min (B-GT-RATE); 2–3 calls per event → ~150 events/hour.
Self-poll: picks the oldest events with bars_status null whose +3h minute window has ELAPSED (an
unfinished window is not a path, D18), oldest first, and marks each event done/no_pool/failed.

Env: SUPABASE_URL, SUPABASE_KEY, RUN_SECONDS=18000, SLEEP=11, HOUR_H=48, MIN_PRE=30, MIN_POST=180
"""
import os, time, json, datetime, urllib.request, urllib.error, urllib.parse

SB_URL = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
SB_KEY = os.environ["SUPABASE_KEY"]
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
SLEEP = float(os.environ.get("SLEEP", "11"))
HOUR_H = int(os.environ.get("HOUR_H", "48"))
MIN_PRE = int(os.environ.get("MIN_PRE", "30"))
MIN_POST = int(os.environ.get("MIN_POST", "180"))
ORDER = os.environ.get("ORDER", "asc")   # a second drain from another IP runs desc so the two never collide
NETWORK = {"solana": "solana", "ethereum": "eth", "base": "base", "bsc": "bsc", "robinhood": "robinhood", "arc": "arc"}
GT = "https://api.geckoterminal.com/api/v2"
STATS = {"events": 0, "done": 0, "no_pool": 0, "failed": 0, "gt_calls": 0, "gt_429": 0, "rows": 0}


def log(*a):
    print(datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


def sb(method, path, body=None, prefer=None):
    h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    req = urllib.request.Request(SB_URL + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            t = r.read().decode()
            return r.status, (json.loads(t) if t else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:300]


def gt(path):
    """One GT call; None on failure. 429 → back off; never record an empty result for a rejected request (B-THROTTLE)."""
    for i in range(4):
        STATS["gt_calls"] += 1
        req = urllib.request.Request(GT + path, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                STATS["gt_429"] += 1
                time.sleep(20 * (i + 1))
                continue
            if e.code == 404:
                return {"_404": True}
            if e.code == 401:                      # public API serves only the last 180 days
                return {"_401": True}
            return None
        except Exception:
            time.sleep(5)
    STATS["gt_throttled"] = STATS.get("gt_throttled", 0) + 1
    return {"_429": True}                          # a throttled request is NOT an observation (B-THROTTLE)


def ohlcv(net, pool, tf, before_s, limit, lo_s):
    rows, before = [], before_s
    for _ in range(3):
        d = gt(f"/networks/{net}/pools/{pool}/ohlcv/{tf}?aggregate=1&before_timestamp={before}&limit={limit}&currency=token")
        if d is None:
            return None
        if d.get("_429"):
            return "429"
        if d.get("_401"):
            return "401"
        if d.get("_404"):
            return "404"
        lst = (d.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
        rows += [r for r in lst if r[0] >= lo_s]
        if not lst or lst[-1][0] <= lo_s or len(lst) < limit:
            break
        before = lst[-1][0]
        time.sleep(SLEEP)
    return rows


def resolve_pool(net, token):
    d = gt(f"/networks/{net}/tokens/{token}/pools?page=1")
    if not d or d.get("_404"):
        return None
    pools = d.get("data") or []
    if not pools:
        return None
    pools.sort(key=lambda p: -float((p.get("attributes") or {}).get("reserve_in_usd") or 0))
    return pools[0]["attributes"]["address"]


def process(ev):
    net = NETWORK.get(ev["chain"])
    if not net:
        return "no_pool", []
    pay = ev["payment_ts"] / 1000
    pool = ev.get("pair_address")
    tried_resolve = False
    for attempt in range(2):
        if not pool:
            pool = resolve_pool(net, ev["token"]); tried_resolve = True
            time.sleep(SLEEP)
            if not pool:
                return "no_pool", []
        m = ohlcv(net, pool, "minute", int(pay + MIN_POST * 60), 1000, int(pay - MIN_PRE * 60))
        if m == "404" and not tried_resolve:
            pool = None
            continue
        if m == "429":
            return None, []                        # leave bars_status NULL: retry later, never record a throttle
        if m == "401":
            return "too_old", []
        if m is None or m == "404":
            return "failed" if m is None else "no_pool", []
        time.sleep(SLEEP)
        h = ohlcv(net, pool, "hour", int(pay + HOUR_H * 3600), 1000, int(pay - 3600))
        if h is None or h == "404":
            h = []
        rows = [dict(event_id=ev["event_id"], pool=pool, res="m", ts=r[0], o=r[1], h=r[2], l=r[3], c=r[4], v=r[5]) for r in m]
        rows += [dict(event_id=ev["event_id"], pool=pool, res="h", ts=r[0], o=r[1], h=r[2], l=r[3], c=r[4], v=r[5]) for r in h]
        return "done", rows
    return "no_pool", []


def main():
    end = time.time() + RUN_SECONDS
    while time.time() < end:
        cutoff = int((time.time() - MIN_POST * 60 - 300) * 1000)
        st, evs = sb("GET", f"/boost_events?select=event_id,chain,token,payment_ts,pair_address&bars_status=is.null"
                            f"&payment_ts=lte.{cutoff}&order=payment_ts.{ORDER}&limit=50")
        if st != 200:
            raise RuntimeError(f"queue read failed {st} {evs}")
        if not evs:
            log("queue empty", STATS)
            time.sleep(120)
            continue
        for ev in evs:
            if time.time() >= end:
                break
            status, rows = process(ev)
            STATS["events"] += 1
            if status is None:
                log(f"{ev['chain']} {ev['token'][:10]} throttled; backing off 120 s, event left for retry")
                time.sleep(120)
                continue
            STATS[status] = STATS.get(status, 0) + 1
            if rows:
                for i in range(0, len(rows), 500):
                    st2, body = sb("POST", "/boost_bars?on_conflict=event_id,res,ts", rows[i:i + 500], prefer="resolution=merge-duplicates,return=minimal")
                    if st2 not in (200, 201, 204):
                        log("bars write failed", st2, body)
                        status = "failed"
                STATS["rows"] += len(rows)
            st3, body = sb("PATCH", f"/boost_events?event_id=eq.{urllib.parse.quote(ev['event_id'])}", {"bars_status": status}, prefer="return=minimal")
            if st3 not in (200, 204):
                log("status write failed", st3, body)
            log(f"{ev['chain']} {ev['token'][:10]} -> {status} rows {len(rows)} gt {STATS['gt_calls']} 429s {STATS['gt_429']}")
            time.sleep(SLEEP)
    log("END", STATS)


if __name__ == "__main__":
    main()
