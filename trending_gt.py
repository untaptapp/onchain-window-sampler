#!/usr/bin/env python3
"""GeckoTerminal trending-pools poller — FREE substitute for the fomoscan board.

Writes to `trending_snapshots` with source='geckoterminal'. Chosen after fomoscan
disabled its free tier (2026-08-27): GeckoTerminal's /networks/solana/trending_pools
overlaps the fomo board ~58% (mint-level) — fomo trending ≈ general Solana volume-
trending — and GeckoTerminal is free (no key), broader, and RICHER: each pool carries
volume + txns(buys/sells/buyers/sellers) + price-change at m5/m15/m30/h1/h6/h24, the
pool address, and token age. Those extras (stored in `extra` jsonb) give many precursor
features — volume acceleration, buyer breadth — at the board level, no Helius tape needed.
See research/trending-frontrun.md (data-source finding) and monitoring-architecture.md.

Self-poll like the other collectors (cron = restart heartbeat; concurrency keeps one job).
GeckoTerminal free tier is ~30 req/min and keyless, so we poll finer than fomoscan.
Independent of the sampler — never touches events/samples.

Env: SUPABASE_URL, SUPABASE_KEY (service_role).
     RUN_SECONDS (default 20000 ≈5.5h), PASS_INTERVAL (default 300 = 5 min), GT_PAGES (default 2).
"""
import json, os, time, urllib.request, urllib.error

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "300"))
GT_PAGES = int(os.environ.get("GT_PAGES", "2"))
UA = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}


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


def gt(path, tries=4):
    for a in range(tries):
        try:
            r = urllib.request.Request("https://api.geckoterminal.com/api/v2" + path, headers=UA)
            with urllib.request.urlopen(r, timeout=30) as x:
                return json.loads(x.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(3 * (a + 1)); continue
            print("gt http", e.code, flush=True); return None
        except Exception:
            time.sleep(2 * (a + 1))
    return None


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def one_pass():
    """Pull trending pools; dedupe to one row per mint; append to trending_snapshots."""
    cap = int(time.time() * 1000)          # GeckoTerminal has no board timestamp — use poll time
    polled = int(time.time())
    # RANK IS POSITIONAL, SO A SKIPPED PAGE CORRUPTS EVERY RANK AFTER IT.
    # `rank` only increments on pages that came back, so skipping a failed page 2 and carrying on
    # gave page 3's tokens ranks 21-40 instead of 41-60 — and the snapshot was written anyway,
    # indistinguishable from a board that was genuinely that short. Ranks from a CONTIGUOUS PREFIX
    # of pages are correct; ranks after a hole are unknowable, because we cannot know how many rows
    # the missing page held. So stop at the first failure and record the depth actually collected.
    seen = set(); rows = []; rank = 0; pages_ok = 0; truncated = False
    for pg in range(1, GT_PAGES + 1):
        d = gt(f"/networks/solana/trending_pools?page={pg}")
        if not d:
            truncated = True
            print(f"  page {pg} did not land — board truncated at {rank} ranks "
                  f"(writing the good prefix, not a mis-ranked board)", flush=True)
            break
        if d:
            pages_ok += 1
            for p in d.get("data", []):
                a = p.get("attributes", {})
                base = p.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
                mint = base.split("_", 1)[1] if "_" in base else base
                if not mint or mint in seen:
                    continue
                seen.add(mint); rank += 1
                vol = a.get("volume_usd") or {}
                rows.append({
                    "mint": mint, "captured_at": cap, "polled_at": polled, "source": "geckoterminal",
                    "rank": rank, "handle": a.get("name"), "label": a.get("name"),
                    "volume": _f(vol.get("h24")),
                    "market_cap": _f(a.get("market_cap_usd")) or _f(a.get("fdv_usd")),
                    "price": _f(a.get("base_token_price_usd")),
                    "liquidity": _f(a.get("reserve_in_usd")),
                    "extra": {"pool": a.get("address"), "created": a.get("pool_created_at"),
                              "vol": vol, "txns": a.get("transactions"),
                              "pchg": a.get("price_change_percentage"),
                              # board depth this snapshot actually saw, so a consumer can tell a
                              # short board from a truncated read instead of guessing
                              "pages_ok": None, "board_truncated": None},
                })
        time.sleep(2.2)
    if not rows:
        print("no pools", flush=True); return 0
    for r in rows:                       # known only once the page loop has finished
        r["extra"]["pages_ok"] = pages_ok
        r["extra"]["board_truncated"] = truncated
    st, _ = sb("POST", "/trending_snapshots?on_conflict=mint,captured_at,source",
               rows, prefer="resolution=merge-duplicates,return=minimal")
    ok = st in (200, 201, 204)
    print(f"pass cap={cap} rows={len(rows)} pages={pages_ok}/{GT_PAGES}"
          f"{' TRUNCATED' if truncated else ''} write={st}{'' if ok else ' FAIL'}", flush=True)
    return len(rows) if ok else 0


def main():
    end = time.time() + RUN_SECONDS
    n = 0; fails = 0
    while True:
        try:
            got = one_pass(); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True); got = 0
        fails = fails + 1 if got == 0 else 0
        if fails >= 8:
            print(f"{fails} consecutive empty/failed passes — exiting.", flush=True); break
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print(f"done {n} passes", flush=True)


if __name__ == "__main__":
    main()
