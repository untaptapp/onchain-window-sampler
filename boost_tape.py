#!/usr/bin/env python3
"""DexScreener BOOST window sampler — the on-chain trade tape around every Solana boost payment.

WHY THIS EXISTS
---------------
A DexScreener boost is an instant, unmoderated, chain-wide broadcast (pushed on
`wss://api.dexscreener.com/token-boosts/latest/v1`). Measured on the kontract launch
(research/dex-paid-signal.md): the first bot buy landed <1 s after the boost's `paymentTimestamp`,
119 wallets bought in 120 s, and the +0–60 s cohort realised 1.65–2.16x while everyone later broke
even. The questions this collector exists to answer, per event and across events:
  1. is a bundle/cluster present after the boost, and how often;
  2. the latency from payment to the first buy / first cluster (slot + block time);
  3. whether the same wallets recur across boosts;
  4. their config (size, Jito tip, priority fee, router program, `jitodontfront…` marker);
  5. OUR observed latency (`seen_at` − `paymentTimestamp`) versus theirs — can we be in front.
Plus the slow thesis: price/mcap at fixed horizons (5m, 15m, 1h, 6h, 24h) after the boost.

WHAT IT RECORDS
---------------
boost_events   : one row per (token, paymentTimestamp) — channel + seen_at, DexScreener pre-event
                 snapshot (pair, dex, mcap, liquidity, pair age), profile status, tape summary.
boost_trades   : the swap tape on the event's main pair from PRE_S before to POST_S after payment,
                 one row per token leg: wallet, side, size, tip, fee, programs, marker, slot.
boost_followups: DexScreener price/mcap/liquidity/volume at each horizon (point-in-time polls).

DESIGN NOTES (guardrails, learned here)
--------------------------------------
- The REST mirror of the boosts feed is a 60-s Cloudflare cache (47 s behind the socket on the
  first paired observation). The socket is the primary; REST is a fallback and is labelled as such
  in `channel`, so latency statistics are never pooled across channels.
- The socket handshake is a 90-item SNAPSHOT, not events — it seeds the dedupe set and is never
  written. Heartbeats are ignored.
- Multi-chain feed: only `chainId in CHAINS` is ever written (fail closed, like SOL_SOURCES); the
  on-chain tape decoder exists for `TAPE_CHAINS` (solana) — other chains get events + follow-ups and
  `tape_status='no_tape_chain'`, so a missing tape is a labelled fact, not an empty market.
- Event identity is (token, paymentTimestamp) from `orders/v1`, not the feed item — a token can be
  boosted repeatedly and the feed can deliver an item twice.
- The tape is pulled AFTER the post-window has elapsed, from Helius' parsed-transaction API on
  the pair address (100 tx/page); pages are capped (MAX_PAGES) and the cap is RECORDED
  (`tape_complete=false`) — a short tape must never read as a quiet market (A5 / A-SHORT).
- Helius' parsed-history endpoint returns only SUCCESSFUL txs (measured: 0 failed returned vs 135 on
  the RPC signature index for the same window). `n_failed` therefore comes from one budgeted
  `getSignaturesForAddress` call and is NULL, never 0, when the window is not fully covered.
- Prices in `boost_trades` are per-trade AVERAGE FILL prices (|ΔSOL|/|Δtoken| of that swap), real
  executed prints — never a chained reconstruction (D-PRICE). Mcap at the event comes from
  DexScreener's snapshot at `seen_at`, labelled as such.
- A Helius call budget per run (HELIUS_BUDGET). When it is spent, events are still recorded and
  followed up (cheap) and the tape is marked `skipped_budget` — loud in the summary, never silent.
- Every batch carries one key set (A2/A-SHAPE); writes flush per event (C3); abnormal exits raise.
- SINK=jsonl writes the same rows to OUT_DIR for a local run (used before the tables existed);
  `--load-jsonl DIR` replays such files into Supabase.

Env: SUPABASE_URL, SUPABASE_KEY (or SINK=jsonl + OUT_DIR), HELIUS_KEY (HELIUS_FREE_KEY accepted),
     CHAINS=solana,bsc,base,ethereum,robinhood,arc (events + follow-ups for all; tape for solana only), RUN_SECONDS=20000, PRE_S=120, POST_S=180, TAPE_DELAY_S=60, MAX_PAGES=8,
     HELIUS_BUDGET=3000, FOLLOWUPS=300,900,3600,21600,86400, REST_POLL_S=30.
"""
import asyncio, json, os, sys, time, random, collections, datetime, urllib.request, urllib.error, urllib.parse

try:
    import websockets
except ImportError:
    raise SystemExit("pip install websockets")

CHAINS = [c.strip() for c in os.environ.get("CHAINS", os.environ.get("CHAIN", "solana")).split(",") if c.strip()]
TAPE_CHAINS = {"solana"}   # on-chain tape decoder exists for these; other chains get events + follow-ups only
CHAIN = CHAINS[0]          # legacy single-chain name used in log lines
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20000"))
PRE_S = int(os.environ.get("PRE_S", "120"))
POST_S = int(os.environ.get("POST_S", "180"))
TAPE_DELAY_S = int(os.environ.get("TAPE_DELAY_S", "60"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "8"))
HELIUS_BUDGET = int(os.environ.get("HELIUS_BUDGET", "3000"))
FOLLOWUPS = [int(x) for x in os.environ.get("FOLLOWUPS", "300,900,3600,21600,86400").split(",")]
REST_POLL_S = int(os.environ.get("REST_POLL_S", "30"))
HELIUS_KEY = os.environ.get("HELIUS_KEY") or os.environ.get("HELIUS_FREE_KEY")
SINK = os.environ.get("SINK") or ("supabase" if os.environ.get("SUPABASE_URL") else "jsonl")
OUT_DIR = os.environ.get("OUT_DIR", "boost_out")
SB_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SB_KEY = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SECRET_KEY")
UA = {"User-Agent": "Mozilla/5.0"}
DS = "https://api.dexscreener.com"
WS_URL = "wss://api.dexscreener.com/token-boosts/latest/v1"
WSOL = "So11111111111111111111111111111111111111112"
# Jito tip accounts (mainnet, 2026). A transfer to any of these marks a bundled tx.
TIP_ACCOUNTS = {
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh", "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
}
BORING_PROGRAMS = {
    "11111111111111111111111111111111", "ComputeBudget111111111111111111111111111111",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
}

if not HELIUS_KEY:
    raise SystemExit("HELIUS_KEY missing")
if SINK == "supabase" and not (SB_URL and SB_KEY):
    raise SystemExit("SINK=supabase needs SUPABASE_URL and SUPABASE_KEY")

T0 = time.time()
STATS = collections.Counter()
helius_calls = 0


def log(*a):
    print(datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"), *a, flush=True)


# ----------------------------------------------------------------------------- http helpers
def http_json(url, method="GET", body=None, headers=None, timeout=30, retries=3):
    h = dict(UA)
    if headers:
        h.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, method=method, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                txt = r.read().decode()
                return r.status, (json.loads(txt) if txt else None)
        except urllib.error.HTTPError as e:
            txt = e.read().decode(errors="replace")[:300]
            if e.code in (429, 500, 502, 503, 504) and i < retries - 1:
                time.sleep(1.5 * (i + 1) + random.random())
                last = (e.code, txt)
                continue
            return e.code, txt
        except Exception as e:  # transport
            last = (0, repr(e)[:200])
            time.sleep(1.0 * (i + 1))
    return last if last else (0, "unknown")


def sb(method, path, body=None, prefer=None):
    h = {"apikey": SB_KEY, "Authorization": f"Bearer {SB_KEY}"}
    if prefer:
        h["Prefer"] = prefer
    # NOTE: no browser User-Agent on PostgREST with a secret key (A-UA)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB_URL + "/rest/v1" + path, data=data, method=method,
                                 headers={**h, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            txt = r.read().decode()
            return r.status, (json.loads(txt) if txt else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:400]


# ----------------------------------------------------------------------------- sinks
class Sink:
    def __init__(self):
        if SINK == "jsonl":
            os.makedirs(OUT_DIR, exist_ok=True)
            self.f = {t: open(os.path.join(OUT_DIR, f"{t}.jsonl"), "a") for t in
                      ("boost_events", "boost_trades", "boost_followups")}

    def write(self, table, rows, conflict):
        if not rows:
            return True
        keysets = {tuple(sorted(r)) for r in rows}
        if len(keysets) != 1:
            raise RuntimeError(f"ragged batch for {table}: {len(keysets)} key sets")  # A-SHAPE
        if SINK == "jsonl":
            for r in rows:
                self.f[table].write(json.dumps(r) + "\n")
            self.f[table].flush()
            return True
        ok = True
        for i in range(0, len(rows), 500):
            st, body = sb("POST", f"/{table}?on_conflict={conflict}", rows[i:i + 500],
                          prefer="resolution=merge-duplicates,return=minimal")
            if st not in (200, 201, 204):
                log(f"WRITE FAILED {table} {st} {body}")
                STATS["write_fail"] += 1
                ok = False
        return ok

    def existing_event_ids(self, since_s):
        if SINK != "supabase":
            return set(), {}
        ids, done = set(), {}
        st, rows = sb("GET", f"/boost_events?select=event_id,payment_ts,tape_status&chain=in.({','.join(CHAINS)})"
                             f"&payment_ts=gte.{int(since_s * 1000)}&order=event_id.asc&limit=1000")
        if st == 200:
            for r in rows:
                ids.add(r["event_id"])
            st2, fu = sb("GET", f"/boost_followups?select=event_id,horizon_s&chain=in.({','.join(CHAINS)})"
                                f"&ts=gte.{int(since_s)}&order=event_id.asc,horizon_s.asc&limit=1000")
            if st2 == 200:
                for r in fu:
                    done.setdefault(r["event_id"], set()).add(r["horizon_s"])
            if len(rows) == 1000 or (st2 == 200 and len(fu) == 1000):
                log("WARNING: resume read hit the 1000-row cap; some followups may be re-polled (A1)")
        else:
            log("resume read failed", st, rows)
        return ids, done


SINK_OBJ = Sink()

# ----------------------------------------------------------------------------- dexscreener
def ds_orders(chain, token):
    st, d = http_json(f"{DS}/orders/v1/{chain}/{token}")
    if st != 200 or not isinstance(d, dict):
        STATS["orders_fail"] += 1
        return None
    return d


def ds_pairs(chain, tokens):
    """Returns {token: best pair dict} — the deepest pair per token on `chain`. Missing 'pairs' = FAILURE (B-DEX)."""
    out = {}
    for i in range(0, len(tokens), 30):
        st, d = http_json(f"{DS}/latest/dex/tokens/{','.join(tokens[i:i + 30])}")
        if st != 200 or not isinstance(d, dict) or "pairs" not in d:
            STATS["pairs_fail"] += 1
            continue
        for p in d["pairs"] or []:
            if p.get("chainId") != chain:
                continue
            t = p["baseToken"]["address"]
            if t not in tokens[i:i + 30]:
                continue
            liq = (p.get("liquidity") or {}).get("usd") or 0
            if t not in out or liq > ((out[t].get("liquidity") or {}).get("usd") or 0):
                out[t] = p
    return out


def fnum(x):
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------------- event bookkeeping
class Event:
    __slots__ = ("event_id", "chain", "token", "amount", "total", "channel", "seen_at", "payment_ts", "pair",
                 "dex", "tape_status", "followups_done", "row")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k))


EVENTS = {}          # event_id -> Event (this run)
SEEN_ITEMS = set()   # (token, amount) items already handled (dedupe before the orders call)
QUEUE = asyncio.Queue()


def event_row_template():
    return dict(event_id=None, chain=None, token=None, amount=None, total_amount=None, channel=None,
                seen_at=None, payment_ts=None, our_lag_ms=None, pair_address=None, dex_id=None,
                pair_created_at=None, pair_age_s=None, price_usd=None, price_native=None, mcap_usd=None,
                liquidity_usd=None, vol_h1=None, buys_m5=None, sells_m5=None, profile_status=None,
                profile_paid_ts=None, n_prior_boosts=None, tape_status="pending", tape_complete=None,
                n_tx=None, n_failed=None, pre_trades=None, first_buy_ts=None, first_buy_slot=None,
                first_buy_lag_s=None, buys_60=None, wallets_60=None, sol_in_60=None, sells_60=None,
                sol_out_60=None, buys_180=None, sol_in_180=None, sol_out_180=None, clusters_60=None,
                first_cluster_lag_s=None, tipped_60=None, max_tip_60=None, marker_60=None,
                price_pre=None, price_60=None, price_180=None, helius_calls=None, created_at=None)


async def handle_item(item, channel, seen_at):
    """A boost feed item → an Event row (idempotent on (token, paymentTimestamp))."""
    chain, token, amount = item.get("chainId"), item.get("tokenAddress"), item.get("amount")
    key = (chain, token, amount)
    if key in SEEN_ITEMS:
        return
    SEEN_ITEMS.add(key)
    loop = asyncio.get_running_loop()
    orders = await loop.run_in_executor(None, ds_orders, chain, token)
    if orders is None:
        log("orders lookup failed; event dropped (counted)", token)
        STATS["event_dropped_no_orders"] += 1
        SEEN_ITEMS.discard(key)   # allow the REST fallback to retry it
        return
    # orders/v1 sits behind a 60-s cache: the boost we just saw may not be in it yet, and binding to the
    # token's PREVIOUS boost (months old) corrupts event_id, our_lag and the tape window. Require a boost
    # paid within FRESH_S of seen_at for live channels; otherwise wait and re-fetch, then give up loudly.
    FRESH_S = 900
    pay = None
    for attempt in range(6):
        boosts = [b for b in orders.get("boosts", []) if b.get("amount") == amount] or orders.get("boosts", [])
        recent = [b["paymentTimestamp"] for b in boosts if seen_at - FRESH_S <= b["paymentTimestamp"] / 1000 <= seen_at + 60]
        if recent:
            pay = max(recent)
            break
        await asyncio.sleep(15)
        orders = await loop.run_in_executor(None, ds_orders, chain, token) or orders
    if pay is None:
        STATS["event_no_fresh_boost"] += 1
        log("no boost within FRESH_S of seen_at after 6 tries; item dropped", chain, token, amount,
            "newest", max([b["paymentTimestamp"] for b in orders.get("boosts", [])], default=None))
        return
    event_id = f"{chain}:{token}:{pay}" if chain != "solana" else f"{token}:{pay}"
    if event_id in EVENTS or event_id in KNOWN_IDS:
        STATS["dup_event"] += 1
        return
    prof = [o for o in orders.get("orders", []) if o.get("type") == "tokenProfile"]
    pairs = await loop.run_in_executor(None, ds_pairs, chain, [token])
    p = pairs.get(token) or {}
    row = event_row_template()
    row.update(event_id=event_id, chain=chain, token=token, amount=amount, total_amount=item.get("totalAmount"),
               channel=channel, seen_at=int(seen_at * 1000), payment_ts=pay,
               our_lag_ms=int(seen_at * 1000 - pay), pair_address=p.get("pairAddress"), dex_id=p.get("dexId"),
               pair_created_at=p.get("pairCreatedAt"),
               pair_age_s=(pay - p["pairCreatedAt"]) / 1000 if p.get("pairCreatedAt") else None,
               price_usd=fnum(p.get("priceUsd")), price_native=fnum(p.get("priceNative")),
               mcap_usd=fnum(p.get("marketCap") or p.get("fdv")), liquidity_usd=fnum((p.get("liquidity") or {}).get("usd")),
               vol_h1=fnum((p.get("volume") or {}).get("h1")), buys_m5=((p.get("txns") or {}).get("m5") or {}).get("buys"),
               sells_m5=((p.get("txns") or {}).get("m5") or {}).get("sells"),
               profile_status=(prof[-1]["status"] if prof else None),
               profile_paid_ts=(max(o["paymentTimestamp"] for o in prof) if prof else None),
               n_prior_boosts=sum(1 for b in orders.get("boosts", []) if b["paymentTimestamp"] < pay),
               tape_status=("pending" if p.get("pairAddress") else "no_pair") if chain in TAPE_CHAINS else "no_tape_chain",
               created_at=int(time.time()))
    ev = Event(event_id=event_id, chain=chain, token=token, amount=amount, total=item.get("totalAmount"), channel=channel,
               seen_at=seen_at, payment_ts=pay, pair=p.get("pairAddress"), dex=p.get("dexId"),
               tape_status=row["tape_status"], followups_done=set(), row=row)
    EVENTS[event_id] = ev
    STATS["events"] += 1
    log(f"EVENT {channel} {chain} {token[:10]} x{amount} lag {row['our_lag_ms']} ms dex {row['dex_id']} "
        f"mcap {row['mcap_usd']} liq {row['liquidity_usd']} pair_age_s {row['pair_age_s']}")
    SINK_OBJ.write("boost_events", [row], "event_id")


# ----------------------------------------------------------------------------- feed tasks
async def ws_task():
    fails = 0
    while True:
        try:
            async with websockets.connect(WS_URL, additional_headers={"User-Agent": "Mozilla/5.0",
                                          "Origin": "https://dexscreener.com"}, open_timeout=20,
                                          ping_interval=20, ping_timeout=20) as ws:
                log("ws connected")
                fails = 0
                STATS["ws_connects"] += 1
                async for msg in ws:
                    now = time.time()
                    try:
                        d = json.loads(msg)
                    except Exception:
                        continue
                    if isinstance(d, dict) and "data" in d:          # handshake snapshot = baseline
                        for x in d["data"]:
                            SEEN_ITEMS.add((x.get("chainId"), x.get("tokenAddress"), x.get("amount")))
                        log(f"ws handshake {len(d['data'])} items seeded")
                        continue
                    if isinstance(d, dict) and d.get("type") == "heartbeat":
                        continue
                    items = d if isinstance(d, list) else ([d] if isinstance(d, dict) and "tokenAddress" in d
                                                            else (d.get("data") if isinstance(d, dict) else []) or [])
                    for x in items:
                        if isinstance(x, dict) and x.get("chainId") in CHAINS:
                            STATS["ws_items"] += 1
                            asyncio.create_task(handle_item(x, "ws", now))
                        else:
                            STATS["ws_items_other_chain"] += 1
        except Exception as e:
            fails += 1
            STATS["ws_drops"] += 1
            log("ws error", repr(e)[:160], "fails", fails)
            if fails >= 30:
                raise RuntimeError("websocket failed 30 times in a row")
            await asyncio.sleep(min(60, 3 * fails))


async def rest_task():
    """Fallback poll of the cached REST mirror. Labelled channel='rest' (60-s cache: never pool its lags)."""
    first = True
    loop = asyncio.get_running_loop()
    while True:
        st, d = await loop.run_in_executor(None, http_json, f"{DS}/token-boosts/latest/v1")
        now = time.time()
        if st == 200 and isinstance(d, list):
            for x in d:
                if x.get("chainId") not in CHAINS:
                    continue
                key = (x.get("chainId"), x.get("tokenAddress"), x.get("amount"))
                if first:
                    SEEN_ITEMS.add(key)
                elif key not in SEEN_ITEMS:
                    STATS["rest_items"] += 1
                    asyncio.create_task(handle_item(x, "rest", now))
            first = False
        else:
            STATS["rest_fail"] += 1
        await asyncio.sleep(REST_POLL_S)


# ----------------------------------------------------------------------------- tape
def helius_pages(address, lo_s, hi_s):
    """Parsed txs for `address` with lo_s <= timestamp <= hi_s, newest first. Returns (txs, complete)."""
    global helius_calls
    out, before, complete = [], None, False
    for page in range(MAX_PAGES):
        if helius_calls >= HELIUS_BUDGET:
            return out, False
        q = {"api-key": HELIUS_KEY, "limit": 100}
        if before:
            q["before"] = before
        st, d = http_json(f"https://api.helius.xyz/v0/addresses/{address}/transactions?{urllib.parse.urlencode(q)}",
                          timeout=90)
        helius_calls += 1
        if st != 200 or not isinstance(d, list):
            STATS["helius_fail"] += 1
            log("helius page failed", st, str(d)[:120])
            return out, False
        if not d:
            complete = True
            break
        for t in d:
            if t["timestamp"] <= hi_s:
                out.append(t)
        before = d[-1]["signature"]
        if d[-1]["timestamp"] < lo_s:
            complete = True
            break
    return [t for t in out if t["timestamp"] >= lo_s], complete


def count_failed(address, lo_s, hi_s):
    """Failed txs in the window from the signature index (the parsed-history endpoint OMITS failed txs —
    measured on the kontract replay: 0 returned vs 135 on the RPC index). One budgeted RPC call, newest
    1000 signatures only; returns None when the window is not fully covered (never a silent 0)."""
    global helius_calls
    if helius_calls >= HELIUS_BUDGET:
        return None
    before, n_fail, covered = None, 0, False
    for _ in range(3):
        body = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                "params": [address, {"limit": 1000, **({"before": before} if before else {})}]}
        st, d = http_json(f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}", method="POST", body=body, timeout=60)
        helius_calls += 1
        if st != 200 or not isinstance(d, dict) or "result" not in d:
            STATS["helius_fail"] += 1
            return None
        res = d["result"]
        if not res:
            covered = True
            break
        for s_ in res:
            bt_ = s_.get("blockTime") or 0
            if lo_s <= bt_ <= hi_s and s_.get("err") is not None:
                n_fail += 1
        before = res[-1]["signature"]
        if (res[-1].get("blockTime") or 0) < lo_s:
            covered = True
            break
        if helius_calls >= HELIUS_BUDGET:
            break
    return n_fail if covered else None


def tx_to_legs(t, token, event_id, chain="solana"):
    """One parsed tx → token legs for `token` with executed-fill economics."""
    if t.get("transactionError"):
        return []
    payer = t.get("feePayer")
    legs = []
    acc = {a["account"]: a for a in t.get("accountData", [])}
    payer_dsol = (acc.get(payer, {}).get("nativeBalanceChange") or 0) / 1e9
    tip = sum(n["amount"] for n in t.get("nativeTransfers", []) if n.get("toUserAccount") in TIP_ACCOUNTS) / 1e9
    progs, markers = set(), set()
    for i in t.get("instructions", []):
        if i.get("programId") not in BORING_PROGRAMS:
            progs.add(i["programId"])
        for a in i.get("accounts", []) or []:
            if a.startswith("jitodontfront"):
                markers.add(a)
    bought = sum(x["tokenAmount"] for x in t.get("tokenTransfers", []) if x.get("mint") == token and x.get("toUserAccount") == payer)
    sold = sum(x["tokenAmount"] for x in t.get("tokenTransfers", []) if x.get("mint") == token and x.get("fromUserAccount") == payer)
    net = bought - sold
    if abs(net) < 1e-9:
        return []
    side = "buy" if net > 0 else "sell"
    sol = abs(payer_dsol) - (t.get("fee", 0) / 1e9 if side == "buy" else 0)  # SOL moved for the swap (incl. tip)
    legs.append(dict(event_id=event_id, chain=chain, sig=t["signature"], slot=t["slot"], ts=t["timestamp"],
                     wallet=payer, side=side, token_amt=abs(net), sol_amt=round(sol, 9),
                     fill_price=(sol / abs(net)) if net else None, tip=round(tip, 9), fee=t.get("fee", 0) / 1e9,
                     programs=sorted(progs)[:6], marker=(sorted(markers)[0] if markers else None),
                     source=t.get("source")))
    return legs


def summarise(ev, legs, n_tx, n_failed, complete):
    pay = ev.payment_ts / 1000
    pre = [l for l in legs if l["ts"] < pay]
    post = sorted([l for l in legs if l["ts"] >= pay], key=lambda l: (l["slot"], l["sig"]))
    buys60 = [l for l in post if l["side"] == "buy" and l["ts"] <= pay + 60]
    sells60 = [l for l in post if l["side"] == "sell" and l["ts"] <= pay + 60]
    buys180 = [l for l in post if l["side"] == "buy" and l["ts"] <= pay + 180]
    sells180 = [l for l in post if l["side"] == "sell" and l["ts"] <= pay + 180]
    by_slot = collections.defaultdict(list)
    for l in buys60:
        by_slot[l["slot"]].append(l)
    clusters, first_cluster = 0, None
    for s, ls in sorted(by_slot.items()):
        if len({l["wallet"] for l in ls}) >= 3:
            sz = sorted(l["sol_amt"] for l in ls if l["sol_amt"] > 0)
            if sz and sz[-1] / sz[0] < 1.15:
                clusters += 1
                if first_cluster is None:
                    first_cluster = ls[0]["ts"] - pay
    fb = post[0] if (post and post[0]["side"] == "buy") else next((l for l in post if l["side"] == "buy"), None)

    def px_at(h):   # reference prices from real-size fills only — a dust print is a quote artifact (D-PRINT)
        c = [l for l in post if l["ts"] <= pay + h and l["fill_price"] and l["sol_amt"] >= 0.01]
        return c[-1]["fill_price"] if c else None

    r = ev.row
    r.update(tape_status="done", tape_complete=complete, n_tx=n_tx, n_failed=n_failed, pre_trades=len(pre),
             first_buy_ts=(fb["ts"] if fb else None), first_buy_slot=(fb["slot"] if fb else None),
             first_buy_lag_s=(round(fb["ts"] - pay, 3) if fb else None), buys_60=len(buys60),
             wallets_60=len({l["wallet"] for l in buys60}), sol_in_60=round(sum(l["sol_amt"] for l in buys60), 4),
             sells_60=len(sells60), sol_out_60=round(sum(l["sol_amt"] for l in sells60), 4), buys_180=len(buys180),
             sol_in_180=round(sum(l["sol_amt"] for l in buys180), 4), sol_out_180=round(sum(l["sol_amt"] for l in sells180), 4),
             clusters_60=clusters, first_cluster_lag_s=(round(first_cluster, 3) if first_cluster is not None else None),
             tipped_60=sum(1 for l in buys60 if l["tip"] > 0), max_tip_60=(max([l["tip"] for l in buys60], default=0)),
             marker_60=sum(1 for l in buys60 if l["marker"]),
             price_pre=next((l["fill_price"] for l in reversed(pre) if l["fill_price"] and l["sol_amt"] >= 0.01), None),
             price_60=px_at(60), price_180=px_at(180), helius_calls=helius_calls)


async def tape_task():
    loop = asyncio.get_running_loop()
    while True:
        now = time.time()
        for ev in list(EVENTS.values()):
            if ev.tape_status != "pending":
                continue
            if now < ev.payment_ts / 1000 + POST_S + TAPE_DELAY_S:
                continue
            if helius_calls >= HELIUS_BUDGET:
                ev.tape_status = "skipped_budget"
                ev.row.update(tape_status="skipped_budget", helius_calls=helius_calls)
                STATS["tape_skipped_budget"] += 1
                SINK_OBJ.write("boost_events", [ev.row], "event_id")
                continue
            ev.tape_status = "running"
            lo, hi = ev.payment_ts / 1000 - PRE_S, ev.payment_ts / 1000 + POST_S
            txs, complete = await loop.run_in_executor(None, helius_pages, ev.pair, lo, hi)
            legs = []
            for t in txs:
                legs += tx_to_legs(t, ev.token, ev.event_id, ev.chain)
            n_failed = await loop.run_in_executor(None, count_failed, ev.pair, lo, hi)
            summarise(ev, legs, len(txs), n_failed, complete)
            ev.tape_status = "done"
            ok = SINK_OBJ.write("boost_trades", legs, "event_id,sig,wallet,side") and \
                SINK_OBJ.write("boost_events", [ev.row], "event_id")
            STATS["tapes"] += 1
            r = ev.row
            log(f"TAPE {ev.token[:10]} tx {len(txs)} legs {len(legs)} complete {complete} first_buy_lag {r['first_buy_lag_s']}s "
                f"buys60 {r['buys_60']} wallets60 {r['wallets_60']} sol_in60 {r['sol_in_60']} clusters {r['clusters_60']} "
                f"tipped {r['tipped_60']} marker {r['marker_60']} helius {helius_calls}/{HELIUS_BUDGET} write_ok {ok}")
        await asyncio.sleep(5)


async def followup_task():
    loop = asyncio.get_running_loop()
    while True:
        now = time.time()
        due = collections.defaultdict(list)   # horizon -> [events]
        for ev in EVENTS.values():
            for h in FOLLOWUPS:
                if h not in ev.followups_done and now >= ev.payment_ts / 1000 + h:
                    due[h].append(ev)
        for h, evs0 in due.items():
          by_chain = collections.defaultdict(list)
          for e in evs0:
              by_chain[e.chain].append(e)
          for chain, evs in by_chain.items():
            tokens = list({e.token for e in evs})
            pairs = await loop.run_in_executor(None, ds_pairs, chain, tokens)
            ts = int(time.time())
            rows = []
            for e in evs:
                p = pairs.get(e.token)
                if p is None:
                    STATS["followup_missing"] += 1   # token no longer has pairs, or the call failed (counted)
                    e.followups_done.add(h)
                    continue
                rows.append(dict(event_id=e.event_id, chain=e.chain, horizon_s=h, ts=ts,
                                 pair_address=p.get("pairAddress"), dex_id=p.get("dexId"),
                                 price_usd=fnum(p.get("priceUsd")), price_native=fnum(p.get("priceNative")),
                                 mcap_usd=fnum(p.get("marketCap") or p.get("fdv")),
                                 liquidity_usd=fnum((p.get("liquidity") or {}).get("usd")),
                                 vol_h1=fnum((p.get("volume") or {}).get("h1")), vol_m5=fnum((p.get("volume") or {}).get("m5")),
                                 buys_m5=((p.get("txns") or {}).get("m5") or {}).get("buys"),
                                 sells_m5=((p.get("txns") or {}).get("m5") or {}).get("sells"),
                                 boosts_active=((p.get("boosts") or {}).get("active"))))
                e.followups_done.add(h)
            if rows:
                SINK_OBJ.write("boost_followups", rows, "event_id,horizon_s")
                STATS["followups"] += len(rows)
        await asyncio.sleep(20)


async def summary_task():
    while True:
        await asyncio.sleep(600)
        log("SUMMARY", dict(STATS), "events_in_mem", len(EVENTS), "helius", helius_calls)


KNOWN_IDS = set()


async def main():
    global KNOWN_IDS
    KNOWN_IDS, done = SINK_OBJ.existing_event_ids(time.time() - max(FOLLOWUPS) - 3600)
    # Resume follow-ups for recent events written by earlier runs (supabase only).
    if SINK == "supabase" and KNOWN_IDS:
        st, rows = sb("GET", f"/boost_events?select=event_id,chain,token,amount,total_amount,channel,seen_at,payment_ts,pair_address,dex_id,tape_status"
                             f"&chain=in.({','.join(CHAINS)})&payment_ts=gte.{int((time.time() - max(FOLLOWUPS) - 3600) * 1000)}&order=event_id.asc&limit=1000")
        if st == 200:
            for r in rows:
                ev = Event(event_id=r["event_id"], chain=r["chain"], token=r["token"], amount=r["amount"], total=r["total_amount"],
                           channel=r["channel"], seen_at=r["seen_at"] / 1000, payment_ts=r["payment_ts"], pair=r["pair_address"],
                           dex=r["dex_id"], tape_status=("resumed" if r["tape_status"] != "pending" else "pending"),
                           followups_done=set(done.get(r["event_id"], set())), row=None)
                if ev.tape_status == "pending":
                    ev.row = event_row_template()
                    ev.row.update({k: r[k] for k in r})
                    ev.row["chain"] = r["chain"]
                EVENTS[ev.event_id] = ev
            log(f"resumed {len(rows)} recent events for follow-ups")
    log(f"start sink={SINK} chains={CHAINS} run={RUN_SECONDS}s pre={PRE_S} post={POST_S} budget={HELIUS_BUDGET} known={len(KNOWN_IDS)}")
    tasks = [asyncio.create_task(c()) for c in (ws_task, rest_task, tape_task, followup_task, summary_task)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=RUN_SECONDS)
    except asyncio.TimeoutError:
        pass
    log("END", dict(STATS), "helius", helius_calls)
    if STATS["write_fail"]:
        raise SystemExit(f"{STATS['write_fail']} failed writes")   # C-EXIT0: never exit 0 on a lossy run


def load_jsonl(d):
    for table, conflict in (("boost_events", "event_id"), ("boost_trades", "event_id,sig,wallet,side"),
                            ("boost_followups", "event_id,horizon_s")):
        p = os.path.join(d, table + ".jsonl")
        if not os.path.exists(p):
            continue
        rows = {}
        for l in open(p):
            r = json.loads(l)
            rows[tuple(r[k] for k in conflict.split(","))] = r   # last write wins (events are re-written after tape)
        rows = list(rows.values())
        groups = collections.defaultdict(list)
        for r in rows:
            groups[tuple(sorted(r))].append(r)
        n = 0
        for g in groups.values():
            if SINK_OBJ.write(table, g, conflict):
                n += len(g)
        log(f"loaded {n}/{len(rows)} into {table}")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--load-jsonl":
        if SINK != "supabase":
            raise SystemExit("--load-jsonl needs SUPABASE_URL/SUPABASE_KEY")
        load_jsonl(sys.argv[2])
    else:
        asyncio.run(main())
