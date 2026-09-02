#!/usr/bin/env python3
"""Materialise Robinhood board-entry paths into `trending_paths` (source='gmgn_rh').

This is a thin caller: the work is `materialize_rh_paths()` in the database (source of truth
`materialize_rh.sql`, but read pg_get_functiondef before trusting that file — C0b). Doing it in the
DB means one pass over trending_bars instead of an ~88 MB client read (A6/A9), and it needs only
SUPABASE_KEY rather than a management token in a workflow secret.

WHY NOT materialize_paths.py
----------------------------
That script is built on SOL denomination (`backtest.load_sol`) and Jupiter route quotes
(`venue_edge.load_routes`). Robinhood has neither, so running RH through it would crash or silently
produce SOL-adjusted returns for a chain that does not trade against SOL. Measured 2026-09-02,
`trending_paths` held ZERO EVM rows: every derived return in this project was Solana-only and RH
returns had never been materialised at all. The Solana-only columns stay NULL here rather than faked.

Returns are USD-denominated, which is right for this chain: the dominant quote asset is USDG, a USD
stablecoin (12 of the top 20 pools by GeckoTerminal; WETH is 4). The WETH-quoted minority does carry
an ETH/USD component — recorded, not silently adjusted.

Env: SUPABASE_URL, SUPABASE_KEY, ENTRY_TOL_S (default 300).
"""
import json, os, sys, time, urllib.error, urllib.request

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
ENTRY_TOL_S = int(os.environ.get("ENTRY_TOL_S", "300"))


def main():
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json",
         "User-Agent": "onchain-window-sampler/1.0"}
    body = json.dumps({"entry_tol_s": ENTRY_TOL_S}).encode()
    t = time.time()
    for a in range(4):
        try:
            req = urllib.request.Request(SB + "/rpc/materialize_rh_paths", data=body,
                                         headers=h, method="POST")
            with urllib.request.urlopen(req, timeout=300) as r:
                n = r.read().decode().strip()
            print(f"materialize_rh_paths -> {n} rows in {time.time() - t:.0f}s", flush=True)
            return
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode()[:300]
            # 57014 is a statement timeout returned as HTTP 500; read the BODY, not the code
            # (C-TIMEOUT). Retrying is only useful for a transient, so fail loud after four.
            if a == 3:
                raise SystemExit(f"materialize_rh_paths failed {e.code}: {body_txt}")
            print(f"  attempt {a + 1} failed {e.code}: {body_txt}", flush=True)
            time.sleep(5 * (a + 1))
        except Exception as e:
            if a == 3:
                raise SystemExit(f"materialize_rh_paths failed: {e!r}")
            time.sleep(5 * (a + 1))


if __name__ == "__main__":
    main()
