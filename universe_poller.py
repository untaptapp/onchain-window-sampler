#!/usr/bin/env python3
"""The CANDIDATE UNIVERSE poller — the control group for the trending-prediction study.

WHY THIS IS THE LONG POLE
-------------------------
Predicting "which token is about to trend" needs controls drawn from the tokens that looked like
plausible candidates AT THE SAME INSTANT and did not trend — risk-set (incidence-density) sampling.
Comparing trending tokens against RANDOM Solana tokens is invalid: trending tokens are selected on
liquidity / volume / age while random tokens are overwhelmingly dead, so a model trained that way
scores ~0.99 AUC by learning "is this token alive at all" and offers zero discrimination among the
candidates production must actually choose between. That is the single most likely way the study
fails, and no amount of modelling fixes it after the fact.

This table also sets the RECALL CEILING: a token that later trends but was never in the universe
can never be caught. Coverage is therefore a blocking gate before any modelling
(research/prediction-methodology.md §2).

Nothing downstream can start until controls are accumulating, so this runs first and continuously.

WHAT IT DOES
------------
Sweeps GeckoTerminal's Solana pool listings across several sort orders plus `new_pools`
(20 pools/page, free and keyless) and appends a point-in-time snapshot of every pool seen to
`candidate_universe`. Deliberately stores the FULL sweep with no eligibility filter applied —
filters belong in analysis, where they can be varied and pre-registered; discarding rows at
collection time would silently fix a choice we have not yet justified.

Env: SUPABASE_URL, SUPABASE_KEY.
     PAGES (default 8 per sort), SLEEP (default 2.2 -> ~27 req/min), PASS_INTERVAL (default 300),
     RUN_SECONDS (default 20000 ~5.5h).
"""
import json, os, time, urllib.request, urllib.error

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
GT = "https://api.geckoterminal.com/api/v2/networks/solana"
UA = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}
PAGES = int(os.environ.get("PAGES", "8"))
SLEEP = float(os.environ.get("SLEEP", "2.2"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "300"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))

SWEEPS = [("h24_vol", "/pools?sort=h24_volume_usd_desc&page={p}"),
          ("h1_vol",  "/pools?sort=h1_volume_usd_desc&page={p}"),
          ("new",     "/new_pools?page={p}")]


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


def gt(path):
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(GT + path, headers=UA), timeout=30) as r:
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


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def row_of(d, via, ts):
    a = d.get("attributes") or {}
    rel = d.get("relationships") or {}
    base = ((rel.get("base_token") or {}).get("data") or {}).get("id") or ""
    mint = base.split("solana_")[-1] if base else None
    vol = a.get("volume_usd") or {}
    txn = a.get("transactions") or {}
    pch = a.get("price_change_percentage") or {}
    created = a.get("pool_created_at")
    ct = None
    if created:
        try:
            import datetime as dt
            ct = int(dt.datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp())
        except Exception:
            ct = None
    m5 = txn.get("m5") or {}; h1 = txn.get("h1") or {}
    return {"pool_address": a.get("address"), "captured_at": ts, "mint": mint,
            "symbol": (a.get("name") or "").split(" /")[0][:40] or None,
            "price_usd": _f(a.get("base_token_price_usd")),
            "liquidity": _f(a.get("reserve_in_usd")),
            "mcap": _f(a.get("market_cap_usd")), "fdv": _f(a.get("fdv_usd")),
            "vol_m5": _f(vol.get("m5")), "vol_h1": _f(vol.get("h1")), "vol_h24": _f(vol.get("h24")),
            "txn_m5_buys": _i(m5.get("buys")), "txn_m5_sells": _i(m5.get("sells")),
            "txn_h1_buys": _i(h1.get("buys")), "txn_h1_sells": _i(h1.get("sells")),
            "pchg_m5": _f(pch.get("m5")), "pchg_h1": _f(pch.get("h1")),
            "pool_created": ct, "via": via}


def one_pass():
    ts = int(time.time())
    seen, rows = set(), []
    for via, tmpl in SWEEPS:
        for p in range(1, PAGES + 1):
            j = gt(tmpl.format(p=p))
            if not j or not j.get("data"):
                break
            for d in j["data"]:
                r = row_of(d, via, ts)
                if r["pool_address"] and r["pool_address"] not in seen:
                    seen.add(r["pool_address"]); rows.append(r)
    if not rows:
        print("no pools returned", flush=True)
        return "fail", 0
    wrote = 0
    for i in range(0, len(rows), 500):
        st, _ = sb("POST", "/candidate_universe?on_conflict=pool_address,captured_at",
                   rows[i:i + 500], prefer="resolution=merge-duplicates,return=minimal")
        if st and 200 <= st < 300:
            wrote += len(rows[i:i + 500])
        else:
            print(f"write failed status={st}", flush=True)
            return "fail", wrote
    print(f"pass: {len(rows)} distinct pools captured, {wrote} rows written", flush=True)
    return "ok", wrote


def main():
    end = time.time() + RUN_SECONDS
    n = 0; fails = 0
    while True:
        try:
            outcome, _ = one_pass(); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True); outcome = "fail"
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
