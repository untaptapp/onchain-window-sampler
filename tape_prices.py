#!/usr/bin/env python3
"""COMPLETE, CHAIN-PRICED V3/V4 swap record for the needle paper book (guardrail D-PRICE).

Why this exists. The shadow swap tape (`shadow-tape/swaps/…`, rh-platform ringout-shadow) has two
defects for price work: (1) until the 2026-09-13 00:00 UTC deploy it stored only swap AMOUNTS, and
the book's avg²/prev_end price reconstruction drifted without bound (~97% phantom needle fills);
(2) before 2026-09-12 23:30 UTC it held roughly HALF the chain's swaps (shadow README §14: tail
blocks of log windows read before the node indexed them), and its `ts` is ARRIVAL time, which runs
late during catch-up. Every V3/V4 Swap event carries the post-swap `sqrtPriceX96`, `liquidity` and
`tick`, and the node returns each log's exact `blockTimestamp`. This job stores exactly that.

Layout (bucket shadow-tape): `prices/v2/<chunk_start:09d>.jsonl.gz`, one object per CHUNK (10,000
≈ 18 min) blocks, rows {blk, li, ts, tx, kind, key, sp, liq, tick, a0, a1[, ts_exact]} — ts is the block timestamp,
exact or interpolated between real block timestamps 20 blocks apart (error < ~2 s); and
`prices/v2/<chunk_start:09d>.done.json` = {v, from, to, rows, ts_min, ts_max, fetched_at}. A chunk
is usable only over its marker's [from, to]; a chunk with no marker does not exist. The object is
written whole, read back and row-counted BEFORE its marker is written (A10). Logs from the last
SAFETY blocks are never fetched (the node's tail is not reliably indexed — that is defect 2).

Usage: tape_prices.py                 extend from the last covered block (or TAPE_START) to head−SAFETY
       tape_prices.py <from> <to>     (re)fetch a block range
Env: SUPABASE_URL, SUPABASE_KEY, RH_RPC (public by default), WIN (1000), MAX_BLOCKS (cap per run)."""
import gzip, io, json, os, sys, time, urllib.request, urllib.error, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rh_chain as C

BUCKET = "shadow-tape"
PREFIX = "prices/v2"
CHUNK = 10_000
WIN = int(os.environ.get("WIN", "1000"))
SAFETY = 1800
MAX_BLOCKS = int(os.environ.get("MAX_BLOCKS", "0")) or None
TAPE_START_TS = dt.datetime(2026, 9, 9, 23, 0, tzinfo=dt.timezone.utc).timestamp()
POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
T_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
T_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


def _s(x):
    v = int(x, 16)
    return v - (1 << 256) if v >= 1 << 255 else v


def decode(lg):
    """Row for a V3/V4 swap log, else None. Both families put amount0, amount1, sqrtPriceX96,
    liquidity, tick as data words 0–4 (verified against 621 receipts, V4 and ramses v3)."""
    t = lg.get("topics") or []
    if not t or t[0] not in (T_V4, T_V3):
        return None
    addr = lg["address"].lower()
    if t[0] == T_V4 and addr != POOL_MANAGER:
        return None
    d = lg["data"][2:]
    w = [d[i:i + 64] for i in range(0, len(d), 64)]
    if len(w) < 5:
        return None
    kind, key = ("v4", t[1]) if t[0] == T_V4 else ("v3", addr)
    return {"blk": int(lg["blockNumber"], 16), "li": int(lg["logIndex"], 16),
            "ts": int(lg["blockTimestamp"], 16) if lg.get("blockTimestamp") else None,
            "tx": lg["transactionHash"], "kind": kind, "key": key,
            "sp": str(int(w[2], 16)), "liq": str(int(w[3], 16)), "tick": _s(w[4]),
            "a0": str(_s(w[0])), "a1": str(_s(w[1]))}


def storage(method, path, data=None, ctype="application/json", upsert=False):
    h = {"apikey": C.KEY, "Authorization": f"Bearer {C.KEY}", "Content-Type": ctype}
    if upsert:
        h["x-upsert"] = "true"
    url = C.SB.replace("/rest/v1", "") + path
    last = None
    for a in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h, method=method), timeout=180) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            body = e.read()[:300]
            if method == "GET" and (e.code == 404 or (e.code == 400 and b"not_found" in body.lower().replace(b" ", b"_"))):
                return None                              # object absent: the ONLY quiet case
            last = f"{e.code} {body}"
        except Exception as e:
            last = e
        time.sleep(2 * (a + 1))
    raise RuntimeError(f"storage {method} {path} failed: {last}")


def markers():
    """{chunk_start: marker} for every chunk, via paged listing (names are unique, so offset paging is safe)."""
    names, off = [], 0
    while True:
        lst = json.loads(storage("POST", f"/storage/v1/object/list/{BUCKET}",
                                 json.dumps({"prefix": PREFIX + "/", "limit": 1000, "offset": off,
                                             "sortBy": {"column": "name", "order": "asc"}}).encode()))
        names += [x["name"] for x in lst]
        if len(lst) < 1000:
            break
        off += 1000
    out = {}
    for n in names:
        if n.endswith(".done.json"):
            raw = storage("GET", f"/storage/v1/object/{BUCKET}/{PREFIX}/{n}")
            if raw:
                m = json.loads(raw)
                if m.get("v") == 2:
                    out[int(n.split(".")[0])] = m
    return out


TS_STEP = 20      # exact timestamp every 20 blocks (~1.7 s at ~12 blocks/s); rows between are interpolated
# The public node returns blockTimestamp = 0 on EVERY log and throttles batched block reads (HTTP 429 for
# minutes after a 51-item batch), so block timestamps come from Alchemy when a key is present (16 CU/block,
# ~130 CU per 1,000 blocks of chain at TS_STEP 20); logs stay on the free public node.
TS_RPC = os.environ.get("TS_RPC") or (("https://robinhood-mainnet.g.alchemy.com/v2/" + os.environ["ALCHEMY_API_KEY"].strip())
                                      if os.environ.get("ALCHEMY_API_KEY") else C.RPC)


def block_ts_batch(blocks, tries=8):
    """{blk: timestamp} for a list of blocks in ONE JSON-RPC batch; every item must answer."""
    body = json.dumps([{"jsonrpc": "2.0", "id": i, "method": "eth_getBlockByNumber", "params": [hex(n), False]}
                       for i, n in enumerate(blocks)]).encode()
    last = None
    for att in range(tries):
        try:
            req = urllib.request.Request(TS_RPC, data=body, headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as r:
                out = json.load(r)
            got = {blocks[x["id"]]: int(x["result"]["timestamp"], 16) for x in out if isinstance(x, dict) and x.get("result")}
            if len(got) == len(blocks):
                return got
            last = f"{len(blocks) - len(got)} of {len(blocks)} items unanswered"
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
        except Exception as e:
            last = e
        time.sleep(min(60, 3 * (2 ** att)))
    raise RuntimeError(f"block timestamp batch {blocks[0]}..{blocks[-1]} failed: {last}")


def fetch(a, b):
    logs = C.get_logs({"fromBlock": hex(a), "toBlock": hex(b), "topics": [[T_V4, T_V3]]})
    rows = [r for r in (decode(lg) for lg in logs) if r]
    if any(not (a <= r["blk"] <= b) for r in rows):
        raise RuntimeError(f"getLogs {a}-{b} returned blocks outside the range")          # B-RPC: an answer can lie
    if rows and any(not r["ts"] for r in rows):
        # The public node returns blockTimestamp = 0 on historical logs. Bracket every row between two
        # REAL block timestamps TS_STEP blocks apart and interpolate (A7c: never extrapolate from one anchor).
        anchors = sorted(set(range(a, b + 1, TS_STEP)) | {b})
        ts = block_ts_batch(anchors)
        for r in rows:
            i = min(len(anchors) - 2, max(0, (r["blk"] - a) // TS_STEP))
            b0, b1 = anchors[i], anchors[i + 1]
            t0, t1 = ts[b0], ts[b1]
            if t1 < t0:
                raise RuntimeError(f"block timestamps decrease between {b0} and {b1}")
            r["ts"] = t0 + (t1 - t0) * (r["blk"] - b0) / (b1 - b0) if b1 > b0 else t0
            r["ts_exact"] = t0 == t1 or r["blk"] in (b0, b1)
    return rows


def do_chunk(start, lo, hi, extend=None):
    t0 = time.time()
    rows, frm = [], lo
    if extend:
        raw = storage("GET", f"/storage/v1/object/{BUCKET}/{PREFIX}/{start:09d}.jsonl.gz")
        if raw is None:
            raise RuntimeError(f"chunk {start}: marker without data file")
        rows = [json.loads(l) for l in gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode().splitlines() if l]
        if len(rows) != extend["rows"]:
            raise RuntimeError(f"chunk {start}: file has {len(rows)} rows, marker says {extend['rows']}")
        frm = extend["from"]
    for x in range(lo, hi + 1, WIN):
        rows += fetch(x, min(x + WIN - 1, hi))
    seen, uniq = set(), []
    for r in sorted(rows, key=lambda r: (r["blk"], r["li"])):
        k = (r["blk"], r["li"])
        if k not in seen:
            seen.add(k)
            uniq.append(r)
    # timestamps must be non-decreasing in block order — a cheap check that ts and blk belong together
    if any(b["ts"] < a["ts"] for a, b in zip(uniq, uniq[1:])):
        raise RuntimeError(f"chunk {start}: block timestamps decrease within the chunk")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as g:
        g.write("\n".join(json.dumps(r, separators=(",", ":")) for r in uniq).encode())
    storage("POST", f"/storage/v1/object/{BUCKET}/{PREFIX}/{start:09d}.jsonl.gz", buf.getvalue(), "application/gzip", upsert=True)
    back = storage("GET", f"/storage/v1/object/{BUCKET}/{PREFIX}/{start:09d}.jsonl.gz")
    n_back = sum(1 for l in gzip.GzipFile(fileobj=io.BytesIO(back)).read().decode().splitlines() if l) if back else -1
    if n_back != len(uniq):
        raise RuntimeError(f"chunk {start}: uploaded {len(uniq)} rows, read back {n_back}")
    mk = {"v": 2, "from": frm, "to": hi, "rows": len(uniq),
          "ts_min": uniq[0]["ts"] if uniq else None, "ts_max": uniq[-1]["ts"] if uniq else None, "fetched_at": int(time.time())}
    storage("POST", f"/storage/v1/object/{BUCKET}/{PREFIX}/{start:09d}.done.json", json.dumps(mk).encode(), upsert=True)
    print(f"chunk {start:09d}: blocks {frm}..{hi} rows {len(uniq)} "
          f"({dt.datetime.fromtimestamp(mk['ts_max'] or 0, dt.timezone.utc):%m-%d %H:%M} UTC, {time.time()-t0:.0f}s, rpc {C.calls()})", flush=True)
    return mk


def run(a=None, b=None):
    head = int(C.rpc("eth_blockNumber", []), 16)
    mks = markers()
    if a is None:
        covered = max((m["to"] for m in mks.values()), default=None)
        a = covered + 1 if covered is not None else C.ts_to_blk(TAPE_START_TS)
    b = min(b if b is not None else head - SAFETY, head - SAFETY)
    if MAX_BLOCKS:
        b = min(b, a + MAX_BLOCKS - 1)
    print(f"tape_prices: blocks {a}..{b} ({max(0, b - a + 1)} blocks, head {head}, {len(mks)} chunks on store)", flush=True)
    start = (a // CHUNK) * CHUNK
    while start <= b:
        lo, hi = max(a, start), min(b, start + CHUNK - 1)
        m = mks.get(start)
        if m and m["from"] <= lo and m["to"] >= hi:
            pass
        elif m and m["from"] <= lo <= m["to"] + 1:
            do_chunk(start, m["to"] + 1, hi, extend=m)
        else:
            do_chunk(start, lo, hi)
        start += CHUNK
    return b


if __name__ == "__main__":
    if len(sys.argv) == 3:
        run(int(sys.argv[1]), int(sys.argv[2]))
    else:
        run()
