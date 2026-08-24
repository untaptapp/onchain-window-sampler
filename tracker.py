#!/usr/bin/env python3
"""Long-horizon price tracker.

For every asset seen in `events`, periodically record its % change since the
FIRST recorded entry, plus running ATH/ATL. Writes only to `token_tracks` — it
reads `events` but never touches `events`/`samples`, so the windowed metrics are
untouched. Meant to run on its own slow schedule (~30 min); horizon precision is
hours/days, so schedule jitter is irrelevant.

Purpose: quantify what fraction of KOL-entered tokens become long-term winners,
and which ones — separate from the fast in/out strategy.

Env: SUPABASE_URL, SUPABASE_KEY.  MAX_DAYS (stop tracking after), MAX_CALLS (per run).
"""
import calendar, json, os, time, urllib.request, urllib.error

SB  = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
MAX_DAYS  = int(os.environ.get("MAX_DAYS", "14"))
MAX_CALLS = int(os.environ.get("MAX_CALLS", "120"))   # GeckoTerminal price fetches per run


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer: h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                t = r.read();  return r.status, (json.loads(t) if t else None)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503): time.sleep(1.5*(a+1)); continue
            return e.code, e.read().decode()[:200]
        except Exception:
            time.sleep(1.5*(a+1))
    return 0, None


def price_now(asset):
    try:
        req = urllib.request.Request(
            f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{asset}",
            headers={"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            j = json.loads(r.read())
        return float(j["data"]["attributes"]["price_usd"])
    except Exception:
        return None


def main():
    # earliest entry per asset from events
    st, evs = sb("GET", "/events?status=eq.done&select=asset,symbol,event_ts,ref_price,id&order=event_ts.asc")
    if st != 200 or not isinstance(evs, list):
        print("cannot read events:", st, evs); return
    first = {}
    for e in evs:
        a = e["asset"]
        if a and a not in first and e.get("ref_price"):
            first[a] = e
    # existing tracks
    st, tr = sb("GET", "/token_tracks?select=asset,inactive,ath_price,ath_pct,atl_pct,n_updates,entry_price,first_entry_ts")
    if st != 200:
        print("token_tracks not found — run schema_tracks.sql in Supabase.", tr); return
    have = {t["asset"]: t for t in (tr or [])}
    now = int(time.time())

    # prioritise: new assets first, then least-recently handled; skip inactive/old
    todo = []
    for a, e in first.items():
        cur = have.get(a)
        if cur and cur.get("inactive"): continue
        age_days = (now - e["event_ts"]) / 86400
        if age_days > MAX_DAYS and cur:                     # past horizon -> mark inactive once
            sb("PATCH", f"/token_tracks?asset=eq.{a}", {"inactive": True}, prefer="return=minimal")
            continue
        todo.append((a, e, cur, age_days))
    todo.sort(key=lambda x: (x[2] is not None, x[0]))       # new assets first
    todo = todo[:MAX_CALLS]

    done = 0
    for a, e, cur, age_days in todo:
        p = price_now(a); time.sleep(1.4)
        if p is None: continue
        entry = (cur or {}).get("entry_price") or e["ref_price"]
        pct = p/entry - 1 if entry else None
        ath_price = max(p, (cur or {}).get("ath_price") or 0)
        row = {
            "asset": a, "symbol": e.get("symbol"),
            "first_event_id": e["id"], "first_entry_ts": e["event_ts"], "entry_price": entry,
            "last_price": p, "last_pct": pct,
            "ath_price": ath_price, "ath_pct": (ath_price/entry - 1) if entry else None,
            "atl_pct": min(pct, (cur or {}).get("atl_pct", pct)) if pct is not None else None,
            "n_updates": ((cur or {}).get("n_updates") or 0) + 1,
            "inactive": age_days > MAX_DAYS,
            "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }
        sb("POST", "/token_tracks?on_conflict=asset", [row],
           prefer="resolution=merge-duplicates,return=minimal")
        done += 1
    print(f"tracked {done}/{len(todo)} assets (total known {len(first)}, MAX_CALLS {MAX_CALLS})", flush=True)


if __name__ == "__main__":
    main()
