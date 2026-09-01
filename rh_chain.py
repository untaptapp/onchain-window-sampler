#!/usr/bin/env python3
"""Robinhood Chain (id 4663) RPC + Supabase helpers, shared by rh_universe.py and rh_tape.py.

The public RPC is free, keyless and unmetered, but it RATE LIMITS and it lies in two ways that
have already cost us data elsewhere in this project:

  * A JSON-RPC `error` is an ANSWER with HTTP 200 (B-RPC). `d.get("result")` turns
    `{"error": "log query timed out"}` into `None`, which then reads as "this token has no
    transfers" — a null result about the REQUEST masquerading as a fact about the world. Every
    helper here RAISES on `error`, and `get_logs` splits the block range on a size/timeout error
    rather than retrying it unchanged.
  * Without a User-Agent the endpoint 403s. That is not an auth failure (A3).

Rate limiting is usually SELF-inflicted (B5): two of our own processes racing each other produced
41 x 429 in 58 calls on 2026-09-01. Calls are paced globally through MIN_INTERVAL and back off
exponentially on 429, and `throttled()` is reported so a run that spent its life in backoff is
visible rather than merely slow.
"""
import json, os, time, urllib.error, urllib.request

RPC = os.environ.get("RH_RPC", "https://rpc.mainnet.chain.robinhood.com")
UA = {"Content-Type": "application/json", "User-Agent": "onchain-window-sampler/1.0"}
MIN_INTERVAL = float(os.environ.get("RPC_MIN_INTERVAL", "0.25"))

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "00" * 32

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1" if "SUPABASE_URL" in os.environ else None
KEY = os.environ.get("SUPABASE_KEY")


class RpcError(RuntimeError):
    """A JSON-RPC error object. Distinct from a transport failure: it is an answer."""


# ...except when it is not. This node fronts a pool of backends and reports THEIR transport
# failures inside a JSON-RPC error object with HTTP 200:
#   {'code': -32000, 'message': 'Post "http://10.31.75.191:8547/rpc": dial tcp: connection refused'}
#   {'code': -32000, 'message': 'Post "http://10.31.9.133:8547/rpc": EOF'}
# Those are retryable infrastructure blips wearing the costume of an answer, and raising on them
# killed a 3-day backfill partway through. B-RPC still holds for real answers ("log query timed
# out", "limit exceeded"): those must never be retried unchanged, which is what get_logs' bisection
# is for. So classify rather than treat every error object the same way.
_TRANSIENT = ("connection refused", "eof", "dial tcp", "context deadline", "connection reset",
              "no such host", "i/o timeout", "bad gateway", "service unavailable")


def _is_transient(msg):
    m = msg.lower()
    return any(k in m for k in _TRANSIENT)


_c = {"n": 0, "t": 0.0, "429": 0}


def calls():
    return _c["n"]


def throttled():
    return _c["429"]


def rpc(method, params, tries=7):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for a in range(tries):
        gap = MIN_INTERVAL - (time.time() - _c["t"])
        if gap > 0:
            time.sleep(gap)
        try:
            req = urllib.request.Request(RPC, data=body, headers=UA)
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.load(r)
            _c["n"] += 1
            _c["t"] = time.time()
            if "error" in d:
                raise RpcError(str(d["error"])[:200])
            return d["result"]
        except RpcError as e:
            _c["t"] = time.time()
            if _is_transient(str(e)) and a < tries - 1:
                last = e
                time.sleep(2 * (a + 1))
                continue
            raise
        except urllib.error.HTTPError as e:
            _c["t"] = time.time()
            last = e
            if e.code == 429:
                _c["429"] += 1
                time.sleep(min(60, 3 * (2 ** a)))
            else:
                time.sleep(1.5 * (a + 1))
        except Exception as e:
            _c["t"] = time.time()
            last = e
            time.sleep(1.5 * (a + 1))
    raise RuntimeError(f"rpc {method} failed after {tries} attempts: {last}")


_SPLITTY = ("more than", "limit", "range", "timed out", "timeout", "too large", "exceed", "10000")


def get_logs(params, depth=0):
    """eth_getLogs with automatic bisection on the 10,000-log cap and on timeouts.

    Bisecting rather than retrying is the point: the node is telling us the RANGE is too wide, and
    a retry of the same range returns the same error forever while looking like a flaky endpoint.
    """
    try:
        return rpc("eth_getLogs", [params])
    except RpcError as e:
        msg = str(e).lower()
        lo, hi = int(params["fromBlock"], 16), int(params["toBlock"], 16)
        if not any(k in msg for k in _SPLITTY) or hi <= lo or depth > 26:
            raise
        mid = (lo + hi) // 2
        return (get_logs(dict(params, fromBlock=hex(lo), toBlock=hex(mid)), depth + 1)
                + get_logs(dict(params, fromBlock=hex(mid + 1), toBlock=hex(hi)), depth + 1))


_anchor = None


def block_time():
    """Anchor seconds-per-block on two REAL blocks. Robinhood Chain runs ~0.1005 s/block, but
    hard-coding it makes every derived timestamp drift silently if the chain retunes."""
    global _anchor
    if _anchor is None:
        latest = int(rpc("eth_blockNumber", []), 16)
        span = min(20_000_000, latest)
        t_new = int(rpc("eth_getBlockByNumber", [hex(latest), False])["timestamp"], 16)
        t_old = int(rpc("eth_getBlockByNumber", [hex(latest - span), False])["timestamp"], 16)
        _anchor = (latest, t_new, (t_new - t_old) / span)
    return _anchor


def refresh_head():
    """Re-anchor on the current head. A long-running collector must not keep using the block
    number it saw at startup, or every timestamp it derives drifts by the run length."""
    global _anchor
    _anchor = None
    return block_time()


def blk_to_ts(bn):
    latest, t_new, bt = block_time()
    return int(t_new - (latest - bn) * bt)


def ts_to_blk(ts):
    latest, t_new, bt = block_time()
    return int(latest - (t_new - ts) / bt)


def topic_addr(topic):
    return "0x" + topic[-40:].lower()


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                t = r.read()
                return r.status, (json.loads(t) if t else None)
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503):
                time.sleep(1.5 * (a + 1))
                continue
            return e.code, e.read().decode()[:300]
        except Exception:
            time.sleep(1.5 * (a + 1))
    return 0, None


def sb_write(path, rows, prefer="resolution=merge-duplicates,return=minimal", chunk=500):
    """Bulk upsert, grouped by KEY SET.

    PostgREST rejects a batch whose objects have differing keys — PGRST102 'All object keys must
    match', HTTP 400, and the WHOLE batch is discarded (A-SHAPE). Splitting by hand does not hold
    because a row can be mutated by reference after the split decision, so group at write time and
    let shape stop mattering. Raises on failure: a silent write failure here is a collector that
    looks healthy and stores nothing.
    """
    if not rows:
        return 0
    groups = {}
    for r in rows:
        groups.setdefault(tuple(sorted(r)), []).append(r)
    wrote = 0
    for _, g in groups.items():
        for i in range(0, len(g), chunk):
            st, body = sb("POST", path, g[i:i + chunk], prefer=prefer)
            if not (st and 200 <= st < 300):
                raise RuntimeError(f"write to {path} failed status={st}: {str(body)[:300]}")
            wrote += len(g[i:i + chunk])
    return wrote


def sb_all(path, page=1000, cap=2_000_000):
    """Read through PostgREST's 1000-row cap.

    PostgREST truncates EVERY response at 1000 rows silently (A1), so a page size above that makes
    `len(rows) < page` true on the first page and the loop exits having read 1000 of N. Clamp it.
    RAISES on a page that never lands and on hitting the cap — a swallowed page error returns a
    SHORT list that reads as a complete one (A-SHORT), which is the same defect in a better suit.
    """
    page = min(page, 1000)
    out, off = [], 0
    while True:
        h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Range-Unit": "items",
             "Range": f"{off}-{off + page - 1}"}
        rows = None
        for a in range(5):
            try:
                with urllib.request.urlopen(urllib.request.Request(SB + path, headers=h),
                                            timeout=180) as r:
                    rows = json.load(r)
                break
            except urllib.error.HTTPError as e:
                if e.code == 416:
                    rows = []
                    break
                if a == 4:
                    raise RuntimeError(f"sb_all page {off} of {path}: {e.code} "
                                       f"{e.read().decode()[:200]}")
                time.sleep(2 * (a + 1))
            except Exception as e:
                if a == 4:
                    raise RuntimeError(f"sb_all page {off} of {path}: {e}")
                time.sleep(2 * (a + 1))
        out += rows
        if len(rows) < page:
            return out
        off += page
        if off > cap:
            raise RuntimeError(f"sb_all cap {cap} hit on {path} — result TRUNCATED, refusing")
