#!/usr/bin/env python3
"""Hourly DexScreener activity sweep of the ENTIRE rh_launches population.

WHY. The revival front-run's base rate — P(board entry within 60m | token is revival-aged and
actively trading) — read 27.4% from trending_bars, but bars exist mostly for tokens that DID
trend (559 of that measurement's 629 mints eventually trended), so the number is survivorship-
inflated by construction. This sweep observes activity for EVERY launch, dead or alive, giving
the denominator its missing members. It is also the natural live input for a front-run detector
later, and per B3 (deferred collectors: ask if the data is recoverable later — snapshots are not),
it starts now rather than when needed.

WHY DEXSCREENER. api.dexscreener.com is keyless, covers chain 'robinhood' including pons-v2
64-hex pools (which Kyber cannot route and GeckoTerminal resolves at ~5/min keyless), batches 30
addresses per call, and measured ~867 successful calls/min from one IP. We run at ~240/min out of
courtesy; the full table sweeps in ~20 minutes. No OHLCV here — GT bars remain the price path.

Storage discipline (A9): only tokens with a live pair AND any h1 activity are written
(~1-3k rows/sweep, ~100B each); the pass line records scanned/active so 'quiet' is measurable.
Fail-loud (A-SHORT): a batch whose response lacks the pairs key counts as failed; >20% failed
batches aborts the sweep rather than writing a sweep that silently under-counts activity.

Env: SUPABASE_URL, SUPABASE_KEY, RUN_SECONDS (default 0 = one sweep), PASS_INTERVAL (default
3600), SLEEP (default 0.25 s/call), MAX_AGE_DAYS (default 45 — sweep launches younger than this).
"""
import json, os, time, urllib.request, urllib.error

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "0"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "3600"))
SLEEP = float(os.environ.get("SLEEP", "0.25"))
MAX_AGE_DAYS = int(os.environ.get("MAX_AGE_DAYS", "45"))
DEX = "https://api.dexscreener.com/latest/dex/tokens/"
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    d = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=d, method=method, headers=h)
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


def sb_all(path, page=1000):
    out = []
    while True:
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
             "Range-Unit": "items", "Range": f"{len(out)}-{len(out) + page - 1}"}
        req = urllib.request.Request(SB + path, headers=h)
        chunk = None
        for a in range(5):
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    chunk = json.loads(r.read())
                break
            except urllib.error.HTTPError as e:
                if e.code == 416:
                    return out
                time.sleep(2 * (a + 1))
            except Exception:
                time.sleep(2 * (a + 1))
        if chunk is None:
            raise RuntimeError(f"page at offset {len(out)} never landed for {path[:60]}")
        out += chunk
        if len(chunk) < page:
            return out


def dex_batch(mints):
    """One DexScreener call for up to 30 mints. Returns list of robinhood pairs, or None if the
    request never landed / lacked the pairs key — the caller must count that, not treat it as
    'no activity' (B-THROTTLE's lesson, different API)."""
    u = DEX + ",".join(mints)
    for a in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=20) as r:
                j = json.loads(r.read())
            if not isinstance(j, dict) or "pairs" not in j:
                return None
            time.sleep(SLEEP)
            return [p for p in (j["pairs"] or []) if p.get("chainId") == "robinhood"]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (a + 1)); continue
            return None
        except Exception:
            time.sleep(1 + a)
    return None


def one_sweep():
    swept_at = int(time.time())
    lo = swept_at - MAX_AGE_DAYS * 86400
    launches = sb_all(f"/rh_launches?select=mint&created_at=gte.{lo}&order=created_at.asc,mint.asc")
    mints = sorted({(r.get("mint") or "").lower() for r in launches if r.get("mint")})
    if not mints:
        raise RuntimeError("rh_launches returned no mints — is rh-universe running?")
    rows, failed, nbatch = {}, 0, 0
    for i in range(0, len(mints), 30):
        nbatch += 1
        pairs = dex_batch(mints[i:i + 30])
        if pairs is None:
            failed += 1
            continue
        for p in pairs:
            m = (p.get("baseToken") or {}).get("address", "").lower()
            if m not in {x for x in mints[i:i + 30]}:
                continue
            v1 = float((p.get("volume") or {}).get("h1") or 0)
            tx = p.get("txns") or {}
            b1 = int((tx.get("h1") or {}).get("buys") or 0)
            s1 = int((tx.get("h1") or {}).get("sells") or 0)
            if v1 <= 0 and b1 + s1 == 0:
                continue                                    # inactive: observed, not stored
            best = rows.get(m)
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
            if best is None or liq > best["liq_usd"]:       # deepest pair represents the token
                rows[m] = {"mint": m, "swept_at": swept_at,
                           "price": float(p.get("priceUsd") or 0) or None,
                           "liq_usd": liq,
                           "vol_h1": v1,
                           "vol_h24": float((p.get("volume") or {}).get("h24") or 0),
                           "buys_h1": b1, "sells_h1": s1,
                           "pair_created_at": int(p["pairCreatedAt"] / 1000) if p.get("pairCreatedAt") else None}
    if nbatch and failed > nbatch * 0.2:
        raise RuntimeError(f"{failed}/{nbatch} DexScreener batches failed — refusing to write an "
                           "under-counted sweep")
    out = list(rows.values())
    for i in range(0, len(out), 500):
        st, body = sb("POST", "/rh_activity?on_conflict=mint,swept_at", out[i:i + 500],
                      prefer="resolution=ignore-duplicates,return=minimal")
        if st >= 300:
            raise RuntimeError(f"write failed {st}: {body}")
    print(f"sweep {swept_at}: scanned {len(mints):,} in {nbatch} batches ({failed} failed) -> "
          f"{len(out):,} active written", flush=True)
    return len(out)


def main():
    end = time.time() + RUN_SECONDS
    while True:
        t0 = time.time()
        one_sweep()
        if RUN_SECONDS == 0 or time.time() >= end:
            break
        time.sleep(max(30, PASS_INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    main()
