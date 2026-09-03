#!/usr/bin/env python3
"""LONG.XYZ TICKER RESERVATIONS — read them from chain 4663, not from the app.

WHY NOT THE APP API
-------------------
`app.long.xyz` returns **403 to every automated fetch** (Cloudflare), as does
`robinhoodchain.blockscout.com` — both already recorded in
`rh-platform/research/data-sources.md`. So there is no app endpoint to poll. There does not need
to be: the reservation is enforced **on-chain by the Long factory**, and the factory both answers
a point query and emits the expiry in its creation event. That is strictly better than an app API —
authoritative, keyless, free, and impossible to rate-limit into staleness.

THE FACTORY               0x22e99278308b393ea1260859b181ad7e78f5eeed   (label `longxyz` in rh_universe.WATCH_DEFAULT)
  isTickerAvailable(string) -> bool     selector 0x22d38a76   -- point query, ground truth
  RESERVATION_DURATION()    -> uint256  selector 0x8f27bbc4   -- reads 86400 (24h), verified on-chain

Selectors were recovered by extracting PUSH4 constants from `eth_getCode` and brute-forcing
signature preimages; there is no verified source and no ABI to fetch.

THE EVENT   topic0 0xadc6f1f726f7c710f77ec06adc75f3bb964e5be19581b072c67f7b9b4039267b
  topics[1] new token   topics[2] new token   topics[3] PAIRED QUOTE ASSET (a stock token: NVDA,
  TSLA, SPCX...), which is NOT the creator -- 72.4% of a 2,145-event window matched a known stock
  token contract, and the AMD launch's topics[3] is the AMD stock token itself. Do not read it as
  a deployer EOA.
  data[2] keccak256(ticker)   data[3] createdAt   data[4] reservedUntil   data[5..] ticker string

`reservedUntil` is the CONTRACT'S OWN timestamp, so this file needs no block-time interpolation and
inherits none of that error (rh_chain._real_ts exists because a one-anchor estimate drifts ~30 min
at 3M blocks). Block numbers are used only to pick a scan range, where being early is harmless.

MEASURED 2026-09-03: 2,145 events / 900k blocks (~25.4h), 1,909 distinct tickers live-reserved.
Cross-checked against isTickerAvailable on 24 sampled tickers (12 predicted reserved, 12 predicted
lapsed): 24/24 agreement.

TICKER RULES, probed against the contract:
  * letters only    - any digit or punctuation REVERTS (not "unavailable" -- reverts)
  * 1-15 characters - 16+ REVERTS
  * case-insensitive - "amd" and "AMD" resolve to the same reservation

A reservation is a 24h ANTI-COPYCAT WINDOW, not ownership: once it lapses the ticker is free for
anyone again, including one already used by a live token. "Holding" a ticker is not possible.

Usage:
  python long_tickers.py AMD NVDA TSLA     # point query + expiry for named tickers
  python long_tickers.py --all             # every live reservation, soonest expiry first
  python long_tickers.py --all --json      # same, machine-readable
"""
import json, os, sys, time

import rh_chain as C

FACTORY = "0x22e99278308b393ea1260859b181ad7e78f5eeed"
TOPIC0 = "0xadc6f1f726f7c710f77ec06adc75f3bb964e5be19581b072c67f7b9b4039267b"
SEL_AVAIL = "0x22d38a76"
SEL_DURATION = "0x8f27bbc4"
CHUNK = int(os.environ.get("CHUNK_BLOCKS", "100000"))
# 0.1015 s/block measured 2026-09-03. Over-scan past 24h so a reservation made just inside the
# window cannot be missed; anything already expired is filtered by reservedUntil, not by range.
LOOKBACK_BLOCKS = int(os.environ.get("LOOKBACK_BLOCKS", "900000"))


def _enc_string(s):
    b = s.encode()
    return "%064x" % 32 + "%064x" % len(b) + b.hex() + "00" * ((-len(b)) % 32)


def is_available(ticker):
    """True/False, or None if the contract REVERTED (invalid ticker: non-letter, or >15 chars).

    None is not False. A revert says the ticker can never be launched as written; False says it is
    taken right now and frees later. Collapsing them would report an illegal ticker as merely busy.
    """
    try:
        r = C.rpc("eth_call", [{"to": FACTORY, "data": SEL_AVAIL + _enc_string(ticker)}, "latest"])
    except C.RpcError as e:
        if "revert" in str(e).lower():
            return None
        raise
    return bool(int(r, 16))


def reservation_duration():
    return int(C.rpc("eth_call", [{"to": FACTORY, "data": SEL_DURATION}, "latest"]), 16)


def _decode(log):
    d = log["data"][2:]
    w = [d[i:i + 64] for i in range(0, len(d), 64)]
    off = int(w[5], 16) // 32
    ln = int(w[off], 16)
    raw = "".join(w[off + 1:])[:ln * 2]
    return {"token": "0x" + log["topics"][1][-40:],
            "quote_asset": "0x" + log["topics"][3][-40:],
            "ticker": bytes.fromhex(raw).decode("utf-8", "replace"),
            "created": int(w[3], 16), "expires": int(w[4], 16),
            "block": int(log["blockNumber"], 16)}


def scan(lookback=LOOKBACK_BLOCKS):
    """Every creation event in the lookback window, decoded. Uses rh_chain.get_logs so an
    over-wide range bisects rather than failing (or, worse, returning a truncated list)."""
    head = int(C.rpc("eth_blockNumber", []), 16)
    rows, b = [], head - lookback
    while b <= head:
        hi = min(b + CHUNK, head)
        rows += [_decode(l) for l in C.get_logs(
            {"fromBlock": hex(b), "toBlock": hex(hi), "address": FACTORY, "topics": [TOPIC0]})]
        b = hi + 1
    return rows


def live_reservations(rows=None, now=None):
    """ticker(upper) -> row, for reservations not yet expired. Keeps the LATEST expiry per ticker:
    a ticker relaunched inside the window has two events, and the older one frees first."""
    now = now or int(time.time())
    best = {}
    for r in rows if rows is not None else scan():
        t = r["ticker"].upper()
        if r["expires"] > now and (t not in best or r["expires"] > best[t]["expires"]):
            best[t] = r
    return best


def _fmt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(ts))


def main(argv):
    now = int(time.time())
    want_json = "--json" in argv
    args = [a for a in argv if not a.startswith("--")]

    if "--all" in argv:
        live = live_reservations(now=now)
        out = sorted(live.values(), key=lambda r: r["expires"])
        if want_json:
            print(json.dumps({"now": now, "count": len(out), "reservations": out}, indent=2))
            return 0
        print(f"{len(out)} tickers reserved at {_fmt(now)}  (duration {reservation_duration()}s)\n")
        for r in out:
            print(f"  {r['ticker'][:15]:15s} free in {(r['expires']-now)/3600:6.2f}h  "
                  f"at {_fmt(r['expires'])}  token {r['token']}")
        return 0

    if not args:
        print(__doc__.strip().split("Usage:")[-1])
        return 2

    # Point query first (ground truth), then the log scan only to date the ones that are taken.
    status = {t: is_available(t) for t in args}
    taken = [t for t, v in status.items() if v is False]
    dates = live_reservations(now=now) if taken else {}
    for t in args:
        v = status[t]
        if v is None:
            print(f"{t:15s} INVALID  (letters only, 1-15 chars) -- can never be launched as written")
        elif v:
            print(f"{t:15s} AVAILABLE")
        else:
            r = dates.get(t.upper())
            if r:
                print(f"{t:15s} RESERVED until {_fmt(r['expires'])} "
                      f"({(r['expires']-now)/3600:.2f}h) by token {r['token']}")
            else:
                print(f"{t:15s} RESERVED, but no creation event in the last "
                      f"{LOOKBACK_BLOCKS:,} blocks -- widen LOOKBACK_BLOCKS")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
