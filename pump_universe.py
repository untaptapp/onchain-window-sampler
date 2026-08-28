#!/usr/bin/env python3
"""PumpPortal launch-firehose collector — the population at risk for the prediction study.

WHY THIS EXISTS
---------------
The REST-based candidate universe FAILED its pre-registered coverage gate: only 3.7–9.4% of tokens
that went on to trend were in it beforehand. GeckoTerminal's paged listings reach the top ~200 pools
by 24h volume and the ~200 newest pools — a bimodal sample that misses the $10k–$1M band where
trending actually happens, and pages cap at 10 so no amount of paging reaches it.

This fixes coverage at the source. Every pump.fun token is recorded at birth, and 84.3% of observed
trending mints are pump.fun tokens (88.6% of resolved DEXes are pumpswap/pump-fun), so every future
trender in that share necessarily passes through this stream before it trends.

WHAT IS AND IS NOT FREE (measured, not assumed)
----------------------------------------------
Free : `subscribeNewToken` (~29/min ≈ 41k/day) and `subscribeMigration`.
PAID : `subscribeTokenTrade` / `subscribeAccountTrade` — the server replies
       "only available when connecting with an API key funded with at least 0.02 SOL".
So this supplies the POPULATION plus birth-time stats and graduation events — not volume or CVD.
Those features come from trending_bars, or from a funded key if we later decide the trade firehose
is worth ~0.02 SOL.

DESIGN NOTES (these are guardrails, learned the hard way)
--------------------------------------------------------
- Stores AGGREGATED launch records, never a raw trade tape. At ~41k rows/day (~7 MB/day) retention
  is mandatory, so `prune_pump_launches()` runs server-side each flush.
- Flushes incrementally (every FLUSH_SEC or FLUSH_ROWS), because writing only at the end of a run
  loses everything when the process is interrupted.
- Every row in a batch carries an identical key set — PostgREST rejects a ragged batch with a bare
  400.
- The socket DOES drop (observed mid-probe with no close frame). Reconnects with backoff, and
  counts drops rather than dying silently.
- Fails loud: exits after MAX_FAILS consecutive connection failures instead of idling as a zombie.

Env: SUPABASE_URL, SUPABASE_KEY. RUN_SECONDS (default 20000 ≈5.5h), FLUSH_SEC (30), FLUSH_ROWS (400).
"""
import asyncio, json, os, time, urllib.request, urllib.error

try:
    import websockets
except ImportError:
    raise SystemExit("pip install websockets")

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
WS_URL = os.environ.get("PUMPPORTAL_WS", "wss://pumpportal.fun/api/data")
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
FLUSH_SEC = int(os.environ.get("FLUSH_SEC", "30"))
FLUSH_ROWS = int(os.environ.get("FLUSH_ROWS", "400"))
MAX_FAILS = int(os.environ.get("MAX_FAILS", "8"))

COLS = ("mint", "created_at", "signature", "name", "symbol", "creator", "pool", "initial_buy",
        "sol_amount", "market_cap_sol", "v_sol_curve", "v_tokens_curve", "bonding_curve",
        "migrated_at", "migrated_pool")


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


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def row_of(m, now):
    """One launch record. EVERY key is always present — a ragged batch is rejected outright."""
    return {"mint": m.get("mint"), "created_at": now, "signature": m.get("signature"),
            "name": (m.get("name") or "")[:80] or None, "symbol": (m.get("symbol") or "")[:32] or None,
            "creator": m.get("traderPublicKey"), "pool": m.get("pool"),
            "initial_buy": _f(m.get("initialBuy")), "sol_amount": _f(m.get("solAmount")),
            "market_cap_sol": _f(m.get("marketCapSol")), "v_sol_curve": _f(m.get("vSolInBondingCurve")),
            "v_tokens_curve": _f(m.get("vTokensInBondingCurve")), "bonding_curve": m.get("bondingCurveKey"),
            "migrated_at": None, "migrated_pool": None}


STATS = {"created": 0, "migrated": 0, "written": 0, "drops": 0, "pruned": 0}


def flush(buf, migs):
    if buf:
        rows = list(buf.values())
        for i in range(0, len(rows), 500):
            st, _ = sb("POST", "/pump_launches?on_conflict=mint", rows[i:i + 500],
                       prefer="resolution=merge-duplicates,return=minimal")
            if st and 200 <= st < 300:
                STATS["written"] += len(rows[i:i + 500])
            else:
                print(f"!! launch write failed status={st}", flush=True)
                return False
        buf.clear()
    for mint, (ts, pool) in list(migs.items()):
        # A migration is a graduation — rare and a strong traction signal. PATCH so it never
        # clobbers the original birth stats.
        sb("PATCH", f"/pump_launches?mint=eq.{mint}", {"migrated_at": ts, "migrated_pool": pool},
           prefer="return=minimal")
    migs.clear()
    st, n = sb("POST", "/rpc/prune_pump_launches", {})
    if st == 200 and isinstance(n, int):
        STATS["pruned"] += n
    return True


async def run():
    end = time.time() + RUN_SECONDS
    buf, migs = {}, {}
    last_flush = time.time()
    fails = 0
    while time.time() < end:
        try:
            async with websockets.connect(WS_URL, open_timeout=25, ping_interval=20,
                                          ping_timeout=20, close_timeout=10) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                fails = 0
                while time.time() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        break                       # silent socket — reconnect rather than hang
                    m = json.loads(raw)
                    if "message" in m:
                        continue                    # subscription confirmations
                    now = int(time.time())
                    mint = m.get("mint")
                    if not mint:
                        continue
                    if m.get("txType") == "create":
                        buf[mint] = row_of(m, now); STATS["created"] += 1
                    else:
                        migs[mint] = (now, m.get("pool")); STATS["migrated"] += 1
                    if len(buf) >= FLUSH_ROWS or (time.time() - last_flush) >= FLUSH_SEC:
                        if not flush(buf, migs):
                            fails += 1
                        last_flush = time.time()
                        print(f"  {STATS}", flush=True)
        except Exception as e:
            STATS["drops"] += 1; fails += 1
            print(f"  [reconnect {fails}/{MAX_FAILS} after {type(e).__name__}]", flush=True)
            if fails >= MAX_FAILS:
                print("too many consecutive failures — exiting rather than idling as a zombie.",
                      flush=True)
                break
            await asyncio.sleep(min(30, 2 ** fails))
    flush(buf, migs)
    print(f"done {STATS}", flush=True)


if __name__ == "__main__":
    asyncio.run(run())
