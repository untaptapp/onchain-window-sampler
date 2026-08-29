#!/usr/bin/env python3
"""Long-horizon price/mcap tracker for every mint seen in `trending_snapshots`.

For each token that ever appeared on any trending feed, periodically record price + mcap,
running ATH/ATL, and % vs the first trending sighting. Writes only to `trending_tracks` —
reads `trending_snapshots` but never writes it, so the snapshot feeds are untouched.

Purpose: (1) find long-term winners / moonshots that briefly trended then ran, and whether
anything at entry identifies them; (2) capture the full post-entry price/mcap path used later
to model exit timing (take-profit points, stop-loss levels, volume drop-off). Tokens are kept
FOREVER (dormant memecoins can rocket later); cadence tiers by age keep it cheap.

Price source: GeckoTerminal token endpoint (free, keyless) — price_usd + market_cap_usd/fdv_usd.
Env: SUPABASE_URL, SUPABASE_KEY.  MAX_CALLS (price fetches per run, default 120).
"""
import json, os, time, urllib.request, urllib.error
from collections import defaultdict

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
SOL_SOURCES = os.environ.get("SOL_SOURCES", "gmgn,solanatracker,geckoterminal,fomoscan")
KEY = os.environ["SUPABASE_KEY"]
MAX_CALLS = int(os.environ.get("MAX_CALLS", "120"))
UA = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}


def cadence(age_days):
    """min seconds between checks by token age — recent = frequent, old = ~daily."""
    if age_days < 0.25: return 0            # every run
    if age_days < 2:    return 2 * 3600     # ~2h
    if age_days < 14:   return 12 * 3600    # ~12h
    return 24 * 3600                        # ~daily, forever


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
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
    if len(out) >= cap:
        # Hitting the cap silently truncates exactly like the PostgREST 1000-row limit did.
        # Shout rather than return a short read that looks complete.
        print(f"!! sb_all cap reached ({cap}) for {path[:70]} — RESULT IS TRUNCATED, raise cap", flush=True)
    return out


def price_mcap(mint):
    """(price_usd, mcap_usd) from GeckoTerminal, or (None, None)."""
    try:
        req = urllib.request.Request(
            f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}", headers=UA)
        with urllib.request.urlopen(req, timeout=25) as r:
            a = json.loads(r.read())["data"]["attributes"]
        p = a.get("price_usd")
        mc = a.get("market_cap_usd") or a.get("fdv_usd")
        return (float(p) if p else None, float(mc) if mc else None)
    except Exception:
        return (None, None)


def first_sightings():
    """Earliest trending sighting per mint (mint -> {ts, source, price, mcap, symbol})."""
    # Solana sources only — trending_snapshots is multi-chain since source='gmgn_rh'
    # (Robinhood Chain, EVM addresses), and everything downstream of here assumes Solana.
    rows = sb_all(f"/trending_snapshots?source=in.({SOL_SOURCES})"
                  "&select=mint,captured_at,price,market_cap,handle,source"
                  "&order=captured_at.asc")
    first = {}
    if isinstance(rows, list):
        for r in rows:
            m = r["mint"]
            if m not in first:
                first[m] = {"ts": r["captured_at"] // 1000, "source": r.get("source"),
                            "price": r.get("price"), "mcap": r.get("market_cap"),
                            "symbol": r.get("handle")}
    return first


def main():
    first = first_sightings()
    if not first:
        print("no trending mints yet", flush=True); return
    # paginate: trending_tracks holds one row per trending mint forever, so it crosses the
    # 1000-row PostgREST cap within days. A silent truncation would make the tracker treat
    # already-tracked mints as new and reset their entry price.
    tr = sb_all("/trending_tracks?select=mint,inactive,entry_price,ath_price,ath_mcap,"
                "atl_pct,n_updates,consecutive_nulls,last_check_ts")
    have = {t["mint"]: t for t in tr}
    now = int(time.time())

    due = []
    for m, e in first.items():
        cur = have.get(m)
        if cur and cur.get("inactive"):
            continue
        age_days = (now - e["ts"]) / 86400
        last = (cur or {}).get("last_check_ts") or 0
        if cur and (now - last) < cadence(age_days):
            continue
        due.append((last, m, e, cur))
    due.sort(key=lambda x: x[0])           # stalest first (new mints have last=0)
    due = due[:MAX_CALLS]

    done = 0
    for last, m, e, cur in due:
        p, mc = price_mcap(m); time.sleep(1.4)
        entry_p = (cur or {}).get("entry_price") or e.get("price")
        base = {"mint": m, "symbol": e.get("symbol"), "first_seen_ts": e["ts"],
                "first_source": e.get("source"), "entry_price": entry_p, "entry_mcap": e.get("mcap"),
                "last_check_ts": now,
                "updated_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
        if p is None:
            base["consecutive_nulls"] = ((cur or {}).get("consecutive_nulls") or 0) + 1
            sb("POST", "/trending_tracks?on_conflict=mint", [base],
               prefer="resolution=merge-duplicates,return=minimal")
            continue
        pct = (p / entry_p - 1) if entry_p else None
        ath_price = max(p, (cur or {}).get("ath_price") or 0)
        ath_mcap = max(mc or 0, (cur or {}).get("ath_mcap") or 0)
        atl_prev = (cur or {}).get("atl_pct")
        base.update({
            "last_price": p, "last_mcap": mc, "last_pct": pct,
            "ath_price": ath_price, "ath_mcap": ath_mcap or None,
            "ath_pct": (ath_price / entry_p - 1) if entry_p else None,
            "atl_pct": (min(pct, atl_prev) if (atl_prev is not None and pct is not None) else pct),
            "n_updates": ((cur or {}).get("n_updates") or 0) + 1, "consecutive_nulls": 0,
        })
        sb("POST", "/trending_tracks?on_conflict=mint", [base],
           prefer="resolution=merge-duplicates,return=minimal")
        done += 1
    print(f"tracked {done}/{len(due)} due (total known {len(first)}, budget {MAX_CALLS})", flush=True)


if __name__ == "__main__":
    main()
