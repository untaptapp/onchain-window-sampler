#!/usr/bin/env python3
"""INSIDER-COHORT WATCH — the door-#1 forward test, out of time by construction.

Watches the FROZEN roster in `rh_insider_roster` (frozen 2026-09-07 from the who-profits study:
tier A = 23 serial early buyers who realized +$5k+ with real money across >=2 early entries;
tier B = 83 allocation harvesters who realized +$5k+ selling tokens they never bought). Nothing
is ever added to the roster by this collector — a stale roster decaying to zero signals is a
RESULT (the cohort rotated wallets), not a defect to fix silently.

MECHANISM — watch the WALLETS, not the tokens. One eth_getLogs per direction per roster chunk
covers every token at once: Transfer logs with topic2 (recipient) in the roster = buys AND
allocations arriving; topic1 (sender) in the roster = their exits. Bookmarked by block in
`rh_state`, so missed windows are backfilled exactly and a dropped run loses nothing (writes
land before the bookmark advances). STORAGE IS FIRST-TOUCH ONLY: several tier-B wallets are
high-frequency bots (~34k transfers/hour across the roster — measured; raw storage would be
~100 MB/day, A9). Signal detection needs only the FIRST arrival per (token, wallet, dir); the
scan runs in ascending block order, so an ignore-duplicates upsert IS first-touch. Full
histories stay re-fetchable from the chain on demand.

SIGNAL (frozen definition, see research/insider-cohort-forward-test.md):
  tier A: >=2 distinct tier-A wallets RECEIVE the same token within 1800s.
  tier B: >=2 distinct tier-B wallets RECEIVE the same token within 3600s (distribution start).
One signal per (token, tier). At detection each signal is cost-quoted LIVE on Kyber
(native-denominated round trip at ~$250 and ~$100) — the event-conditioned execution number the
whole strategy question reduces to. Scoring (bars, follow-returns) happens in analysis, later,
against these timestamps; this file records and never evaluates.

Env: SUPABASE_URL, SUPABASE_KEY, RUN_SECONDS (18000), PASS_INTERVAL (120), MAX_RPC (6000/pass),
     CHUNK_BLOCKS (4000).
"""
import json, os, time, urllib.request

import rh_chain as C

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "18000"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "120"))
MAX_RPC = int(os.environ.get("MAX_RPC", "6000"))
CHUNK_BLOCKS = int(os.environ.get("CHUNK_BLOCKS", "4000"))
WINDOW_A, WINDOW_B = 1800, 3600
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
KYBER = "https://aggregator-api.kyberswap.com/robinhood/api/v1/routes"


def pad(addr):
    return "0x" + "00" * 12 + addr[2:].lower()


def kyber_rt(token, amount_wei):
    """Native->token->native round trip, % lost. None on any failure (quotes are best-effort —
    a missing quote must never stop the watch, but it is recorded as NULL, not zero."""
    try:
        h = {"User-Agent": "onchain-window-sampler/1.0"}
        u = f"{KYBER}?tokenIn={NATIVE}&tokenOut={token}&amountIn={amount_wei}"
        d = json.load(urllib.request.urlopen(urllib.request.Request(u, headers=h), timeout=20))
        out = int(d["data"]["routeSummary"]["amountOut"])
        u2 = f"{KYBER}?tokenIn={token}&tokenOut={NATIVE}&amountIn={out}"
        d2 = json.load(urllib.request.urlopen(urllib.request.Request(u2, headers=h), timeout=20))
        back = int(d2["data"]["routeSummary"]["amountOut"])
        return round(100 * (1 - back / amount_wei), 3)
    except Exception:
        return None


def sbj(path):
    """C.sb returns (status, body); unwrap and FAIL LOUD on anything but 2xx — a swallowed read
    error here would silently watch an empty roster (A-SHORT territory)."""
    st, body = C.sb("GET", path)
    if not (200 <= st < 300):
        raise RuntimeError(f"supabase GET {path[:60]} -> {st}: {str(body)[:120]}")
    return body or []


def state_get(key, default=None):
    r = sbj(f"/rh_state?key=eq.{key}&select=val")
    return r[0]["val"] if r else default


def state_set(key, val):
    C.sb("POST", "/rh_state?on_conflict=key", [{"key": key, "val": val}],
         prefer="resolution=merge-duplicates,return=minimal")


def main():
    t_end = time.time() + RUN_SECONDS
    roster = sbj("/rh_insider_roster?select=addr,tier")
    tier = {r["addr"].lower(): r["tier"] for r in roster}
    padded = [pad(a) for a in tier]
    if not padded:
        raise SystemExit("roster is EMPTY — refusing to watch nothing")
    print(f"watching {len(padded)} roster wallets "
          f"(A={sum(1 for t in tier.values() if t=='A')}, "
          f"B={sum(1 for t in tier.values() if t=='B')})", flush=True)
    def born_of(tk):
        r = sbj(f"/rh_launches?mint=eq.{tk}&select=created_at&limit=1")
        return r[0]["created_at"] if r and r[0].get("created_at") else None

    while True:
        C.reset_calls()
        head = C.refresh_head()[0]          # anchor tuple: (latest_block, ts, sec_per_block)
        bm = state_get("insider_watch_block")
        # ~0.1 s/block on this chain: 900k blocks ≈ the last 25h on a first-ever run
        start = int(bm) + 1 if bm else max(head - 900_000, 1)
        n_hits = n_sig = 0
        while start <= head and C.calls() < MAX_RPC and time.time() < t_end:
            end = min(start + CHUNK_BLOCKS - 1, head)
            hits = {}                      # (token, wallet, dir) -> earliest block this chunk
            for lo in range(0, len(padded), 40):
                sel = padded[lo:lo + 40]
                for pos, dr in ((2, "in"), (1, "out")):
                    topics = [C.TRANSFER, None, None]
                    topics[pos] = sel
                    try:
                        logs = C.get_logs({"topics": topics, "fromBlock": hex(start),
                                           "toBlock": hex(end)})
                    except Exception as e:
                        print(f"!! getLogs [{start},{end}] {dr}: {repr(e)[:90]} — pass ends, "
                              f"bookmark NOT advanced", flush=True)
                        logs = None
                    if logs is None:
                        start = None
                        break
                    for lg in logs:
                        tps = lg.get("topics") or []
                        if len(tps) != 3:
                            continue
                        w = C.topic_addr(tps[2] if dr == "in" else tps[1])
                        bn = int(lg["blockNumber"], 16)
                        key = ((lg["address"] or "").lower(), w, dr)
                        if key not in hits or bn < hits[key]:
                            hits[key] = bn
                if start is None:
                    break
            if start is None:
                break
            if hits:
                rows = [{"token": tk, "wallet": w, "dir": dr,
                         "block_number": bn, "ts": C.blk_to_ts(bn)}
                        for (tk, w, dr), bn in hits.items()]
                # ignore-duplicates = keep the FIRST write; ascending scan makes that first-touch
                C.sb_write("/rh_insider_touch?on_conflict=token,wallet,dir", rows,
                           prefer="resolution=ignore-duplicates,return=minimal")
                n_hits += len(rows)
            state_set("insider_watch_block", end)   # only after hits landed (at-least-once)
            print(f"  .. scanned to block {end} ({head-end} behind head), +{len(hits)} first-touches, "
                  f"rpc={C.calls()}", flush=True)
            start = end + 1

        # ---- signal detection over the recent hit window (DB-side read, idempotent upsert) ----
        now = int(time.time())
        recent = sbj(f"/rh_insider_touch?dir=eq.in&ts=gte.{now - 6*3600}"
                     "&select=token,wallet,ts&order=ts.asc")
        by_tok = {}
        for h in recent or []:
            by_tok.setdefault(h["token"], []).append((h["ts"], h["wallet"]))
        sig_rows = []
        for tk, ev in by_tok.items():
            for tname, win in (("A", WINDOW_A), ("B", WINDOW_B)):
                members = [(ts, w) for ts, w in ev if tier.get(w) == tname]
                seen = {}
                for ts, w in members:
                    seen.setdefault(w, ts)
                if len(seen) < 2:
                    continue
                times = sorted(seen.values())
                # first moment two DISTINCT wallets have arrived within the window
                fire = None
                for i in range(1, len(times)):
                    if times[i] - times[i - 1] <= win:
                        fire = times[i]
                        break
                if fire is None:
                    continue
                b = born_of(tk)
                sig_rows.append({"token": tk, "first_ts": times[0], "signal_ts": fire,
                                 "n_wallets": len(seen), "wallets": sorted(seen),
                                 "tier": tname,
                                 "token_age_s": (fire - b) if b else None,
                                 "q250_rt_pct": None, "q100_rt_pct": None,
                                 "detected_at": now})
        if sig_rows:
            have = {(r["token"], r["tier"]) for r in sbj("/rh_insider_signals?select=token,tier")}
            new = [r for r in sig_rows if (r["token"], r["tier"]) not in have]
            for r in new:      # quote at DETECTION — the event-conditioned cost sample
                r["q250_rt_pct"] = kyber_rt(r["token"], 10**17)      # ~\$250-450 of native
                r["q100_rt_pct"] = kyber_rt(r["token"], 4 * 10**16)
                print(f"SIGNAL tier-{r['tier']} {r['token'][:12]}… wallets={r['n_wallets']} "
                      f"age={r['token_age_s']}s rt250={r['q250_rt_pct']}% "
                      f"rt100={r['q100_rt_pct']}%", flush=True)
            if new:
                C.sb_write("/rh_insider_signals?on_conflict=token,tier", new)
                n_sig = len(new)
        print(f"pass done: {n_hits} hits, {n_sig} new signals, {C.calls()} rpc "
              f"(throttled {C.throttled()})", flush=True)
        if time.time() >= t_end:
            break
        time.sleep(PASS_INTERVAL)


if __name__ == "__main__":
    main()
