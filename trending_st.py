#!/usr/bin/env python3
"""Solana Tracker trending poller — richer / earlier momentum event source.

Polls /tokens/trending/{ST_TF} (default 5m) and appends to `trending_snapshots` with
source='solanatracker'. Far richer than the GeckoTerminal board: per token we capture
(in `extra`) multi-window price-change (events 1m..24h), pool txns (buys/sells/volume),
holder count, token age, venue, and risk sniper count. A SHORT-window trending list
catches momentum EARLIER than GeckoTerminal's slow top board. See research/trending-data-sources.md.

Free tier is 2,500 req/mo (~83/day), so we poll modestly (ONE call per pass, default 30 min
→ ~1,440/mo + startup credit checks). Budget-aware and FAIL-LOUD (won't silently stall the
way the fomoscan poller did): checks /credits at startup and exits cleanly on 402/429.

Self-poll pattern like the other collectors (cron = restart heartbeat; concurrency keeps one job).
Independent of the sampler — never touches events/samples.

Env: SUPABASE_URL, SUPABASE_KEY (service_role), SOLANA_TRACKER_KEY.
     RUN_SECONDS (default 20000 ≈5.5h), PASS_INTERVAL (default 1800 =30 min), ST_TF (default 5m).
"""
import json, os, time, urllib.request, urllib.error

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
STKEY = os.environ["SOLANA_TRACKER_KEY"]
BASE = os.environ.get("ST_BASE", "https://data.solanatracker.io").rstrip("/")
TF = os.environ.get("ST_TF", "5m")
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "1800"))


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                t = r.read()
                return r.status, (json.loads(t) if t else None)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(1.5 * (a + 1)); continue
            return e.code, e.read().decode()[:200]
        except Exception:
            time.sleep(1.5 * (a + 1))
    return 0, None


def st_get(path):
    """Parsed JSON on success, sentinel 'QUOTA' on 402/429, or None on other failure."""
    req = urllib.request.Request(BASE + path, headers={"x-api-key": STKEY, "Accept": "application/json"})
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            if e.code in (402, 429):
                print(f"ST quota/rate {e.code}: {body}", flush=True); return "QUOTA"
            if e.code in (500, 502, 503):
                time.sleep(2 * (a + 1)); continue
            print("ST http", e.code, body, flush=True); return None
        except Exception:
            time.sleep(2 * (a + 1))
    return None


def credits():
    d = st_get("/credits")
    if isinstance(d, dict):
        return d.get("credits", d.get("remaining"))
    return None


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def one_pass():
    d = st_get(f"/tokens/trending/{TF}")
    if d == "QUOTA":
        return "quota", 0
    if not isinstance(d, list) or not d:
        print("no board", flush=True); return "fail", 0
    cap = int(time.time() * 1000); polled = int(time.time())
    seen = set(); rows = []; rank = 0
    for it in d:
        tok = it.get("token") or {}
        mint = tok.get("mint")
        if not mint or mint in seen:
            continue
        seen.add(mint); rank += 1
        pool = (it.get("pools") or [{}])[0]
        px = pool.get("price") or {}; mc = pool.get("marketCap") or {}
        liq = pool.get("liquidity") or {}; ptx = pool.get("txns") or {}
        rows.append({
            "mint": mint, "captured_at": cap, "polled_at": polled, "source": "solanatracker",
            "rank": rank, "handle": tok.get("symbol"), "label": tok.get("name"),
            "volume": _f(ptx.get("volume24h")) or _f(ptx.get("volume")),
            "market_cap": _f(mc.get("usd")), "price": _f(px.get("usd")),
            "liquidity": _f(liq.get("usd")),
            "extra": {"tf": TF, "market": pool.get("market"),
                      "created": (tok.get("creation") or {}).get("created_time"),
                      "holders": it.get("holders"), "buys": it.get("buys"),
                      "sells": it.get("sells"), "txns": it.get("txns"),
                      "pool_txns": ptx, "events": it.get("events"),
                      "snipers": ((it.get("risk") or {}).get("snipers") or {}).get("count")},
        })
    if not rows:
        return "fail", 0
    st, _ = sb("POST", "/trending_snapshots?on_conflict=mint,captured_at,source",
               rows, prefer="resolution=merge-duplicates,return=minimal")
    ok = st in (200, 201, 204)
    print(f"pass cap={cap} rows={len(rows)} write={st}{'' if ok else ' FAIL'}", flush=True)
    return ("wrote" if ok else "fail"), len(rows)


def main():
    rem = credits()
    print(f"startup: solanatracker credits={rem}", flush=True)
    if rem == 0:
        print("CREDITS EXHAUSTED at startup — exiting (cron re-checks; top up the plan).", flush=True)
        return
    end = time.time() + RUN_SECONDS
    n = 0; fails = 0
    while True:
        try:
            outcome, _got = one_pass(); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True); outcome = "fail"
        if outcome == "quota":
            print("QUOTA/RATE exhausted mid-run — exiting so the run status reflects it.", flush=True)
            break
        fails = fails + 1 if outcome == "fail" else 0
        if fails >= 6:
            print(f"{fails} consecutive failures — exiting to avoid a silent zombie run.", flush=True)
            break
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print(f"done {n} passes", flush=True)


if __name__ == "__main__":
    main()
