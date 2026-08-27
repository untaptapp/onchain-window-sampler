#!/usr/bin/env python3
"""fomo trending-board poller.

Snapshots the fomoscan `/v2/leaderboard/tokens/trending` board on a fixed cadence
and appends every board entry to `trending_snapshots` (append-only). The board is
a LIVE snapshot with no per-token entry timestamp, so trending-ENTRY events, rank
velocity, and board tenure are reconstructed OFFLINE by diffing snapshots.

Thesis under test (research/trending-frontrun.md): can we detect a token in the
minutes BEFORE it enters the fomo trending section — an app-visibility event — and
front-run the inflow? Buying a token that is already ON the board = buying the
attention peak (dead, exactly like KOL-copying); the edge, if any, lives entirely
in the PRECURSOR. This collector builds the forward panel that test requires.

Cadence / cost: the board is polled every PASS_INTERVAL seconds. Each poll is
~25 fomoscan CU. At 900s (15 min) that is ~96 polls/day ≈ 72k CU/mo — under the
100k/mo pilot budget but leaving only ~28k for leaderboard/thesis calls. Dial
PASS_INTERVAL up (1200 → ~54k/mo) to widen headroom or down (600 → ~108k/mo,
needs the monthly reset) for finer entry-time resolution.

GitHub throttles frequent schedules (~40 min observed between short jobs), so —
like worker.py — each job SELF-POLLS for RUN_SECONDS and the cron is only a
restart heartbeat; `concurrency` keeps exactly one job running at a time.

Env: SUPABASE_URL, SUPABASE_KEY (service_role), FOMOSCAN_KEY.
     RUN_SECONDS   (default 20000 ≈ 5.5h, under the 6h hosted-runner cap)
     PASS_INTERVAL (default 900 = 15 min between board pulls)
     FOMO_BASE     (default https://api.fomoscan.sh)
"""
import json, os, time, urllib.request, urllib.error

SB   = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY  = os.environ["SUPABASE_KEY"]
FKEY = os.environ["FOMOSCAN_KEY"]
FOMO = os.environ.get("FOMO_BASE", "https://api.fomoscan.sh").rstrip("/")
RUN_SECONDS   = int(os.environ.get("RUN_SECONDS", "20000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "900"))


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


def fetch_board():
    req = urllib.request.Request(
        FOMO + "/v2/leaderboard/tokens/trending",
        headers={"Authorization": f"Bearer {FKEY}", "Accept": "application/json"})
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(2 * (a + 1)); continue
            print("fomo http", e.code, e.read()[:150], flush=True); return None
        except Exception:
            time.sleep(2 * (a + 1))
    return None


def one_pass(last_cap):
    """Pull the board once; append rows if it is a new capture. Returns capturedAt."""
    b = fetch_board()
    if not b or "entries" not in b:
        print("no board", flush=True); return last_cap
    cap = int(b.get("capturedAt") or int(time.time() * 1000))
    if cap == last_cap:                     # board hasn't recomputed since last pull
        print(f"pass captured_at={cap} unchanged — skip", flush=True); return cap
    polled = int(time.time())
    rows = []
    for e in b["entries"]:
        mint = e.get("id")
        if not mint:
            continue
        rows.append({
            "mint": mint, "captured_at": cap, "polled_at": polled,
            "rank": e.get("rank"), "handle": e.get("handle"), "label": e.get("label"),
            "volume": e.get("volume"), "market_cap": e.get("marketCap"),
            "price": e.get("price"), "liquidity": e.get("liquidity"),
        })
    if not rows:
        return cap
    st, _ = sb("POST", "/trending_snapshots?on_conflict=mint,captured_at",
               rows, prefer="resolution=merge-duplicates,return=minimal")
    ok = st in (200, 201, 204)
    print(f"pass captured_at={cap} rows={len(rows)} write={st}{'' if ok else ' FAIL'}", flush=True)
    return cap


def main():
    end = time.time() + RUN_SECONDS
    last_cap = 0
    n = 0
    while True:
        try:
            last_cap = one_pass(last_cap); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True)
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print(f"done {n} passes", flush=True)


if __name__ == "__main__":
    main()
