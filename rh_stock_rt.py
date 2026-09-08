#!/usr/bin/env python3
"""STOCK-TOKEN ROUND-TRIP LOGGER — maps the crossed-market regime on Robinhood Chain.

Measured 2026-09-07 (Sunday, US market closed): Kyber round trips USDG->stock->USDG were
NEGATIVE (-0.5%..-0.7% at $2.5k) on SPY/QQQ/NVDA/TSLA/GME for 1.5h+ — the pools were crossed,
fed by retail buying through a ~1%-fee frontend into one pool while the aggregator prices off
another. Open question this table answers: is that a weekend-only regime (does the gap close at
Monday 09:30 ET when the real market re-anchors?), and how often/at what size does it reopen.

Kyber quotes are LIVE-ONLY (B3): any interval not sampled is permanently un-costable, which is
why this collector starts now and runs continuously. Quotes are simulations — an rt_pct here is
an upper bound on the edge, not a fill.

TOKENS ARE PINNED BY ADDRESS, never resolved by symbol: the $14M "HOOD" pool on this chain is a
ticker-collision memecoin at $0.098 vs the $122 stock. Every address below was verified against
the real Friday close (within 5%) and decimals() on-chain (all 18; USDG is 6).

Env: SUPABASE_URL, SUPABASE_KEY, RUN_SECONDS (18000), PASS_INTERVAL (600).
Writes rh_stock_rt: one row per (sym, size, pass); rt_pct/buy_px/sell_px NULL on quote failure
(recorded, never zero). Storage ~0.3 MB/day. Workflow uses cancel-in-progress: true (C-CRON —
an unbackfillable live feed must never drop a restart).
"""
import json, os, time, urllib.request

import rh_chain as C

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "600"))
KYBER = "https://aggregator-api.kyberswap.com/robinhood/api/v1/routes"
H = {"User-Agent": "onchain-window-sampler/1.0"}

USDG = ("0x5fc5360d0400a0fd4f2af552add042d716f1d168", 6)
# addr-pinned, price-verified 2026-09-07; all decimals()==18 (checked on-chain)
TOKENS = {
    "SPY":  "0x117cc2133c37b721f49de2a7a74833232b3b4c0c",
    "QQQ":  "0xd5f3879160bc7c32ebb4dc785f8a4f505888de68",
    "NVDA": "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec",
    "TSLA": "0x322f0929c4625ed5bad873c95208d54e1c003b2d",
    "AAPL": "0xaf3d76f1834a1d425780943c99ea8a608f8a93f9",
    "GME":  "0x1b0e319c6a659f002271b69db8a7df2f911c153e",
    "MU":   "0xff080c8ce2e5feadaca0da81314ae59d232d4afd",
    "HIMS": "0xccee82fe024c36fa15e1005ede3e9e4787e23d09",
    "GLD":  "0xc9a981fee1f9dec688bb123ccdecc63d0debfc4e",
    "SGOV": "0x92fd66527192e3e61d4ddd13322aa222de86f9b5",
    "AMC":  "0x05a3d1cd21d0c88145e82600e62e7e496e0f222b",
    "DJT":  "0x1d11f0496982706c5e14a514d4e79f2e6bde4516",
}
SIZES = (2500, 25000)   # USD; $2.5k = retail clip, $25k = capacity probe


def route_out(tin, tout, amt):
    u = f"{KYBER}?tokenIn={tin}&tokenOut={tout}&amountIn={amt}"
    d = json.load(urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=20))
    return int(d["data"]["routeSummary"]["amountOut"])


def main():
    t_end = time.time() + RUN_SECONDS
    consec_dead = 0
    while True:
        ts = int(time.time())
        rows, ok = [], 0
        for sym, addr in TOKENS.items():
            for size in SIZES:
                if time.time() >= t_end:        # budget INSIDE the loop (C-TIMEOUT-b)
                    break
                row = {"ts": ts, "sym": sym, "token": addr, "size_usd": size,
                       "rt_pct": None, "buy_px": None, "sell_px": None}
                try:
                    amt = size * 10**USDG[1]
                    out = route_out(USDG[0], addr, amt)
                    time.sleep(1.0)
                    back = route_out(addr, USDG[0], out)
                    row["rt_pct"] = round(100 * (1 - back / amt), 4)
                    row["buy_px"] = round(size / (out / 10**18), 6)
                    row["sell_px"] = round((back / 10**USDG[1]) / (out / 10**18), 6)
                    ok += 1
                except Exception as e:
                    print(f"!! quote {sym} ${size}: {repr(e)[:80]}", flush=True)
                rows.append(row)
                time.sleep(1.0)
        if rows:
            # every row carries the same key set (A2); dupes on retry are ignored
            C.sb_write("/rh_stock_rt?on_conflict=sym,size_usd,ts", rows,
                       prefer="resolution=ignore-duplicates,return=minimal")
        worst = min((r["rt_pct"] for r in rows if r["rt_pct"] is not None), default=None)
        print(f"pass ts={ts}: {ok}/{len(rows)} quoted, most-negative rt={worst}", flush=True)
        consec_dead = consec_dead + 1 if ok == 0 else 0
        if consec_dead >= 5:
            raise SystemExit("5 consecutive passes with zero successful quotes — failing loud")
        if time.time() >= t_end:
            break
        time.sleep(max(0, PASS_INTERVAL - (int(time.time()) - ts)))


if __name__ == "__main__":
    main()
