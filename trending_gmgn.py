#!/usr/bin/env python3
"""GMGN trending poller — the richest board-level momentum + quality feed.

Polls GMGN OpenAPI `GET /v1/market/rank` and appends to `trending_snapshots` with
source='gmgn'. GMGN is a top Solana scanner, so its trending rank is itself a major
attention/visibility event. The rank row PRE-COMPUTES most of our precursor + quality
feature set — captured in `extra`: is_wash_trading, bundler_rate, sniper_count,
smart_degen_count (smart money), renowned_count (KOLs), rug_ratio, top_10_holder_rate,
holder_count, buys/sells, multi-window price-change, creation_timestamp (age), hot_level.
So this feed can replace most of the Helius-tape computation in trending_precursor.py.
See research/trending-data-sources.md.

Auth is DATA-mode only: X-APIKEY header + timestamp + client_id query params — NO request
signing (the Ed25519 GMGN_PRIVATE_KEY is only for swap/order routes, which we never call).

Self-poll pattern; budget-aware + fail-loud (exits cleanly on 429 / RATE_LIMIT).
Env: SUPABASE_URL, SUPABASE_KEY, GMGN_KEY.
     RUN_SECONDS (20000), PASS_INTERVAL (900 =15 min), GMGN_INTERVAL (5m), GMGN_CHAIN (sol), GMGN_LIMIT (100).
"""
import json, os, time, uuid, urllib.request, urllib.error, urllib.parse

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
GKEY = os.environ["GMGN_KEY"]
BASE = os.environ.get("GMGN_BASE", "https://openapi.gmgn.ai").rstrip("/")
INTERVAL = os.environ.get("GMGN_INTERVAL", "5m")
CHAIN = os.environ.get("GMGN_CHAIN", "sol")
# The `source` value is what keeps chains apart. trending_snapshots is unique on
# (mint, captured_at, source) and backtest.load_entries iterates an EXPLICIT source list, so a new
# chain written under its own source is invisible to the frozen Solana analysis by construction —
# no schema migration, and no way for Robinhood rows to leak into a pre-registered Solana track.
SOURCE = os.environ.get("SOURCE", "gmgn")
GLIMIT = int(os.environ.get("GMGN_LIMIT", "100"))
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "900"))

# rich fields to lift from each rank row into `extra` (precursor + quality, pre-computed)
EXTRA_KEYS = ["price_change_percent1m", "price_change_percent5m", "price_change_percent1h",
              "swaps", "buys", "sells", "holder_count", "top_10_holder_rate",
              "creation_timestamp", "open_timestamp", "launchpad_platform", "exchange",
              "hot_level", "is_wash_trading", "rug_ratio", "sniper_count", "smart_degen_count",
              "renowned_count", "bundler_rate", "entrapment_ratio", "rat_trader_amount_rate",
              "bluechip_owner_percentage", "renounced_mint", "renounced_freeze_account",
              "burn_ratio", "cto_flag", "is_og", "history_highest_market_cap",
              # EVM-only (Robinhood Chain and other EVM boards). Absent from Solana responses, and
              # the comprehension below is keyed on `k in it`, so Solana rows are unaffected.
              # NOT the "EVM analogue of rug risk" they were first taken for: measured over all
              # 6,500 collected gmgn_rh rows, is_honeypot / is_open_source / is_renounced are
              # CONSTANT (0 / 1 / 1) and carry zero information; buy_tax and sell_tax are 0 on
              # 95.7%; lock_percent is 0.95 on 91%. Populated is not informative — keep collecting
              # them, but see research/sybil-ladder-defense.md before using any of them as a gate.
              "is_honeypot", "buy_tax", "sell_tax", "is_open_source", "is_renounced",
              "lock_percent", "launchpad", "gas_fee", "top70_sniper_hold_rate",
              # Added 2026-08-30 after the revival-rug case study (research/sybil-ladder-defense.md).
              # All were already in the rank payload and were being discarded. Board history cannot
              # be backfilled, so an un-lifted field is permanently lost for every interval we ran
              # without it. All 18 populate on BOTH chains (verified against live rank responses).
              # Live pulls, n=100/chain, re-verified independently on a second pull: bot_degen_rate
              # p50 0.39-0.42 rh / 0.29-0.30 sol (the sybil-ladder signature); creator_close True on
              # ~48% rh / ~73% sol; image_dup nonzero on 37-46% either chain;
              # twitter_create_token_count max ~1,200 rh / 4,645 sol. One Solana deployer held 5-8
              # of the 100 board slots at once — the exact count moves with board churn, so treat it
              # as "one deployer routinely holds several percent of the board", not as a constant.
              # 17 of the 18 carry real variance; `is_show_alert` came back CONSTANT on both chains
              # (distinct=1, n=100 each), so it is collected for completeness but is subject to the
              # same warning as the block above — populated is not informative.
              # Cost: ~4 MB/day across both chains.
              # `total_supply` is here because mcap must be derivable from a point-in-time price.
              "creator", "creator_close", "creator_token_status", "bot_degen_count",
              "bot_degen_rate", "dev_team_hold_rate", "dev_token_burn_ratio", "initial_liquidity",
              "total_supply", "image_dup", "twitter_dup", "website_dup", "telegram_dup",
              "twitter_create_token_count", "twitter_rename_count", "twitter_change_flag",
              "twitter_del_post_token_count", "is_show_alert"]


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


def gmgn_rank():
    """Rank list on success, 'QUOTA' on 429/RATE_LIMIT, or None on other failure."""
    qs = urllib.parse.urlencode({"chain": CHAIN, "interval": INTERVAL, "order_by": "volume",
                                 "limit": GLIMIT, "timestamp": int(time.time()),
                                 "client_id": str(uuid.uuid4())})
    req = urllib.request.Request(f"{BASE}/v1/market/rank?{qs}",
                                 headers={"X-APIKEY": GKEY, "User-Agent": "gmgn-poller/1.0"})
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                env = json.loads(r.read())
            # envelope may double-nest: {code,data:{code,data:{rank:[...]}}}
            d = env.get("data", env)
            if isinstance(d, dict) and "data" in d:
                d = d["data"]
            rank = (d or {}).get("rank") if isinstance(d, dict) else None
            return rank if isinstance(rank, list) else None
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            if e.code == 429 or "RATE_LIMIT" in body:
                print(f"GMGN rate limit {e.code}: {body}", flush=True); return "QUOTA"
            print("GMGN http", e.code, body, flush=True); return None
        except Exception:
            time.sleep(2 * (a + 1))
    return None


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def one_pass():
    rank = gmgn_rank()
    if rank == "QUOTA":
        return "quota", 0
    if not rank:
        print("no rank", flush=True); return "fail", 0
    cap = int(time.time() * 1000); polled = int(time.time())
    seen = set(); rows = []; pos = 0
    for it in rank:
        mint = it.get("address")
        if not mint or mint in seen:
            continue
        seen.add(mint); pos += 1
        rows.append({
            "mint": mint, "captured_at": cap, "polled_at": polled, "source": SOURCE,
            "rank": pos, "handle": it.get("symbol"), "label": it.get("name"),
            "volume": _f(it.get("volume")), "market_cap": _f(it.get("market_cap")),
            "price": _f(it.get("price")), "liquidity": _f(it.get("liquidity")),
            "extra": {"interval": INTERVAL, **{k: it.get(k) for k in EXTRA_KEYS if k in it}},
        })
    if not rows:
        return "fail", 0
    st, _ = sb("POST", "/trending_snapshots?on_conflict=mint,captured_at,source",
               rows, prefer="resolution=merge-duplicates,return=minimal")
    ok = st in (200, 201, 204)
    print(f"pass chain={CHAIN} source={SOURCE} cap={cap} rows={len(rows)} "
          f"write={st}{'' if ok else ' FAIL'}", flush=True)
    return ("wrote" if ok else "fail"), len(rows)


def main():
    end = time.time() + RUN_SECONDS
    n = 0; fails = 0; bad = None
    while True:
        try:
            outcome, _g = one_pass(); n += 1
        except Exception as ex:
            print("pass error:", repr(ex), flush=True); outcome = "fail"
        if outcome == "quota":
            print("RATE LIMIT — exiting so the run status reflects it (cron re-checks).", flush=True)
            bad = "rate limit"; break
        fails = fails + 1 if outcome == "fail" else 0
        if fails >= 6:
            print(f"{fails} consecutive failures — exiting to avoid a silent zombie run.", flush=True)
            bad = f"{fails} consecutive failures"; break
        if time.time() >= end:
            break
        time.sleep(PASS_INTERVAL)
    print(f"done {n} passes", flush=True)
    # An early break must FAIL the run. Both breaks above used to exit 0, so a poller that gave up
    # after 2h of a 5.5h budget was indistinguishable from one that ran to completion — and the
    # board is the one feed that cannot be backfilled. On 2026-08-31 that exact exit left an 11.4h
    # hole (02:18-06:23 and 06:23-13:39) that nothing in the workflow list flagged.
    if bad:
        raise SystemExit(f"collector exited early: {bad} — {n} passes, "
                         f"{(time.time() - (end - RUN_SECONDS)) / 3600:.1f}h of {RUN_SECONDS/3600:.1f}h budget")


if __name__ == "__main__":
    main()
