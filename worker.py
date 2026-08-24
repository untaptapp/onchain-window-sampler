#!/usr/bin/env python3
"""Windowed on-chain activity sampler.

For a configured set of source addresses, detect new asset-acquisition events and,
a couple of minutes later, record the venue's trade window around each event for
microstructure analysis. State lives in Postgres (Supabase); each pass is
stateless, so this is safe to run from a scheduler. A short internal loop lets a
single scheduled run give near-continuous coverage between ticks.

Env:
  SUPABASE_URL, SUPABASE_KEY   (service key; kept in the scheduler's secret store)
  RPC_URL        (default: public mainnet RPC)
  RUN_SECONDS    (internal loop budget, default 270)
  PASS_INTERVAL  (seconds between passes, default 30)
  WINDOW_SEC     (post-event window to record, default 120)
  FRESH_SEC      (ignore events older than this at detect time, default 480)
"""
import calendar, json, os, time, urllib.request, urllib.error

SB   = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY  = os.environ["SUPABASE_KEY"]
RPC  = os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")
RUN_SECONDS   = int(os.environ.get("RUN_SECONDS", "270"))
PASS_INTERVAL = int(os.environ.get("PASS_INTERVAL", "30"))
WINDOW = int(os.environ.get("WINDOW_SEC", "120"))
FRESH  = int(os.environ.get("FRESH_SEC", "480"))
SNAP_MIN, SNAP_MAX, EXPIRE = 90, 720, 900

WSOL = "So11111111111111111111111111111111111111112"
STABLE = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
          "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNKB", WSOL}


def sb(method, path, body=None, prefer=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if prefer: h["Prefer"] = prefer
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SB + path, data=data, method=method, headers=h)
    for a in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                t = r.read();  return json.loads(t) if t else None
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503): time.sleep(1.5 * (a + 1)); continue
            raise
        except Exception:
            time.sleep(1.5 * (a + 1))
    return None


def rpc(method, params, tries=5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for a in range(tries):
        try:
            req = urllib.request.Request(RPC, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                j = json.loads(r.read())
                if "error" in j: raise RuntimeError(str(j["error"])[:60])
                return j.get("result")
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(min(20, 1.2 * (2 ** a))); continue
            time.sleep(1 + a)
        except Exception:
            time.sleep(1 + a)
    return None


def gt(path, tries=4):
    for a in range(tries):
        try:
            req = urllib.request.Request("https://api.geckoterminal.com/api/v2" + path,
                headers={"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(3 * (a + 1)); continue
            return None
        except Exception:
            time.sleep(2)
    return None


def toep(v):
    if isinstance(v, int): return v
    try: return int(calendar.timegm(time.strptime(str(v).replace('Z', '').split('.')[0], "%Y-%m-%dT%H:%M:%S")))
    except Exception: return None


def acquired_asset(tx, addr):
    meta = tx.get("meta") or {}
    if meta.get("err") is not None: return None
    def own(bals):
        o = {}
        for b in bals:
            if b.get("owner") != addr: continue
            a = b["uiTokenAmount"]; v = a.get("uiAmount")
            if v is None: v = float(a["uiAmountString"]) if a.get("uiAmountString") else 0.0
            o[b["mint"]] = v
        return o
    pre = own(meta.get("preTokenBalances") or []); post = own(meta.get("postTokenBalances") or [])
    gained = {m: post.get(m, 0) - pre.get(m, 0) for m in set(pre) | set(post) if m not in STABLE}
    cands = {m: d for m, d in gained.items() if d > 0}
    if not cands: return None
    allm = {b["mint"] for b in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or [])}
    if not any(m in STABLE for m in allm): return None
    return max(cands, key=cands.get)


def venue_for(asset):
    j = gt(f"/networks/solana/tokens/{asset}/pools?page=1"); time.sleep(0.4)
    if not (j and j.get("data")): return None
    def rsv(r):
        try: return float(r["attributes"].get("reserve_in_usd") or 0)
        except Exception: return 0.0
    return max(j["data"], key=rsv)["attributes"]["address"]


def asset_meta(asset):
    j = gt(f"/networks/solana/tokens/{asset}"); time.sleep(0.4)
    if not (j and j.get("data")): return None, None, None, None
    a = j["data"]["attributes"]
    fdv, price, sym = a.get("fdv_usd") or a.get("market_cap_usd"), a.get("price_usd"), a.get("symbol")
    try: supply = float(fdv) / float(price)
    except Exception: supply = None
    return supply, fdv, price, sym


def detect(sources):
    for s in sources:
        addr, label = s["address"], s.get("label")
        sigs = rpc("getSignaturesForAddress", [addr, {"limit": 10}]) or []
        if not sigs: continue
        cur = sb("GET", f"/cursors?address=eq.{addr}&select=last_sig")
        last = cur[0]["last_sig"] if cur else None
        now = int(time.time())
        for sg in sigs:                       # newest first
            if sg["signature"] == last: break
            if last is None: continue          # first sight of this source: set cursor, no backfill
            if sg.get("err"): continue
            bt = sg.get("blockTime") or 0
            if now - bt > FRESH: continue
            tx = rpc("getTransaction", [sg["signature"],
                     {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed", "commitment": "confirmed"}])
            if not tx: continue
            asset = acquired_asset(tx, addr)
            if not asset: continue
            venue = venue_for(asset)
            if not venue: continue
            sb("POST", "/events?on_conflict=id",
               [{"id": sg["signature"], "source_address": addr, "label": label, "asset": asset,
                 "venue": venue, "event_ts": bt, "event_slot": tx.get("slot"), "status": "pending"}],
               prefer="resolution=ignore-duplicates,return=minimal")
            print(f"[event] {label} {asset[:6]} @ {bt}", flush=True)
        sb("POST", "/cursors?on_conflict=address", [{"address": addr, "last_sig": sigs[0]["signature"]}],
           prefer="resolution=merge-duplicates,return=minimal")


def fill():
    pend = sb("GET", "/events?status=eq.pending&select=id,asset,venue,event_ts,label") or []
    now = int(time.time())
    for e in pend:
        age = now - e["event_ts"]
        if age < SNAP_MIN: continue
        if age > EXPIRE:
            sb("PATCH", f"/events?id=eq.{e['id']}", {"status": "expired"}, prefer="return=minimal"); continue
        et, asset = e["event_ts"], e["asset"]
        j = gt(f"/networks/solana/pools/{e['venue']}/trades")
        rows, seen = [], set()
        if j and j.get("data"):
            for t in j["data"]:
                a = t["attributes"]; ts = toep(a.get("block_timestamp"))
                if ts is None or ts < et - 15 or ts > et + WINDOW + 5: continue
                price = None
                if a.get("from_token_address") == asset and a.get("price_from_in_usd"): price = float(a["price_from_in_usd"])
                elif a.get("to_token_address") == asset and a.get("price_to_in_usd"): price = float(a["price_to_in_usd"])
                if price is None: continue
                tx = a.get("tx_hash")
                if tx in seen: continue
                seen.add(tx)
                rows.append({"event_id": e["id"], "tx": tx, "ts": ts, "side": a.get("kind"),
                             "actor": a.get("tx_from_address"), "price": price,
                             "notional": float(a.get("volume_in_usd") or 0)})
        if not rows:
            if age > SNAP_MAX:
                sb("PATCH", f"/events?id=eq.{e['id']}", {"status": "empty"}, prefer="return=minimal")
            continue
        sb("POST", "/samples?on_conflict=event_id,tx", rows, prefer="resolution=ignore-duplicates,return=minimal")
        ordered = sorted(rows, key=lambda x: x["ts"])
        ref = next((r["price"] for r in ordered if r["ts"] >= et), ordered[0]["price"])
        supply, fdv, _, sym = asset_meta(asset)
        cap = ref * supply if supply else None
        sb("PATCH", f"/events?id=eq.{e['id']}",
           {"status": "done", "ref_price": ref, "supply": supply, "ref_fdv": fdv,
            "ref_cap": cap, "symbol": sym, "n_samples": len(rows)}, prefer="return=minimal")
        print(f"[filled] {e['label']} {asset[:6]} n={len(rows)} cap={cap}", flush=True)


def one_pass():
    sources = sb("GET", "/sources?select=address,label&active=is.true") or []
    if not sources:
        print("no active sources — seed the sources table first", flush=True); return
    detect(sources)
    fill()


def main():
    deadline = time.time() + RUN_SECONDS
    n = 0
    while True:
        try: one_pass()
        except Exception as ex: print("pass error:", str(ex)[:120], flush=True)
        n += 1
        if time.time() >= deadline: break
        time.sleep(PASS_INTERVAL)
    print(f"done: {n} passes", flush=True)


if __name__ == "__main__":
    main()
