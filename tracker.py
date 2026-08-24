#!/usr/bin/env python3
"""Long-horizon price tracker.

For every asset seen in `events`, periodically record its % change since the
FIRST recorded entry, plus running ATH/ATL. Writes only to `token_tracks` — it
reads `events` but never touches `events`/`samples`, so the windowed metrics are
untouched.

Tokens are tracked INDEFINITELY (dormant memecoins can rocket months later). To
keep that cheap, check cadence tiers by token age: recent tokens are checked every
run, older ones down to ~daily. The only hard cap is MAX_CALLS per run; as the
tracked set grows, stale tokens are prioritised (round-robin) so all get covered.

Purpose: quantify what fraction of KOL-entered tokens become long-term winners,
and which — separate from the fast in/out strategy.

Env: SUPABASE_URL, SUPABASE_KEY.  MAX_CALLS (price fetches per run, default 120).
"""
import json, os, time, urllib.request, urllib.error

SB  = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
MAX_CALLS = int(os.environ.get("MAX_CALLS", "120"))

def cadence(age_days):
    """min seconds between checks, by token age — recent = frequent, old = ~daily."""
    if age_days < 0.25: return 0            # every run (~30 min)
    if age_days < 2:    return 2*3600       # ~2h
    if age_days < 14:   return 12*3600      # ~12h
    return 24*3600                          # ~daily, forever

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
            if e.code in (429,500,502,503): time.sleep(1.5*(a+1)); continue
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
    st, evs = sb("GET", "/events?status=eq.done&select=asset,symbol,event_ts,ref_price,id&order=event_ts.asc")
    if st != 200 or not isinstance(evs, list):
        print("cannot read events:", st, evs); return
    first = {}
    for e in evs:
        a = e["asset"]
        if a and a not in first and e.get("ref_price"):
            first[a] = e
    st, tr = sb("GET", "/token_tracks?select=asset,inactive,ath_price,entry_price,n_updates,"
                       "consecutive_nulls,last_check_ts")
    if st != 200:
        print("token_tracks not found — run schema_tracks.sql in Supabase.", tr); return
    have = {t["asset"]: t for t in (tr or [])}
    now = int(time.time())

    due = []
    for a, e in first.items():
        cur = have.get(a)
        if cur and cur.get("inactive"): continue
        age_days = (now - e["event_ts"]) / 86400
        last = (cur or {}).get("last_check_ts") or 0
        if cur and (now - last) < cadence(age_days):   # not due yet for its age tier
            continue
        due.append((last, a, e, cur))                  # stalest first (new -> last=0)
    due.sort(key=lambda x: x[0])
    due = due[:MAX_CALLS]

    done = 0
    for last, a, e, cur in due:
        p = price_now(a); time.sleep(1.4)
        entry = (cur or {}).get("entry_price") or e["ref_price"]
        if p is None:
            sb("POST", "/token_tracks?on_conflict=asset",
               [{"asset": a, "symbol": e.get("symbol"), "first_event_id": e["id"],
                 "first_entry_ts": e["event_ts"], "entry_price": entry,
                 "consecutive_nulls": ((cur or {}).get("consecutive_nulls") or 0) + 1,
                 "last_check_ts": now,
                 "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}],
               prefer="resolution=merge-duplicates,return=minimal")
            continue
        pct = p/entry - 1 if entry else None
        ath_price = max(p, (cur or {}).get("ath_price") or 0)
        atl_prev = (cur or {}).get("atl_pct")
        sb("POST", "/token_tracks?on_conflict=asset",
           [{"asset": a, "symbol": e.get("symbol"), "first_event_id": e["id"],
             "first_entry_ts": e["event_ts"], "entry_price": entry,
             "last_price": p, "last_pct": pct,
             "ath_price": ath_price, "ath_pct": (ath_price/entry - 1) if entry else None,
             "atl_pct": (min(pct, atl_prev) if (atl_prev is not None and pct is not None) else pct),
             "n_updates": ((cur or {}).get("n_updates") or 0) + 1,
             "consecutive_nulls": 0, "last_check_ts": now,
             "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}],
           prefer="resolution=merge-duplicates,return=minimal")
        done += 1
    print(f"tracked {done}/{len(due)} due assets (total known {len(first)}, budget {MAX_CALLS})", flush=True)

if __name__ == "__main__":
    main()
