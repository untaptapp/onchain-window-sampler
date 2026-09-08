#!/usr/bin/env python3
"""Health check for rh-universe and the v4 pool-fee read it carries.

Answers ONE question: is the LAUNCH FIREHOSE still doing its job? The fee read added
2026-09-07 is an additive passenger on a collector whose essential output is the population
at risk; the failure mode worth catching is not "fees are missing" but "fees broke launches".

So this compares the launch write rate against the window BEFORE a reference time rather than
checking that the workflow is green — a collector that exits 0 on a dead feed shows green for
days (C5b), and a success message is not evidence of an effect (F2).

Usage:
    SINCE=<epoch seconds> python health_rh_universe.py      # default: 6h ago
Env: SUPABASE_URL, SUPABASE_KEY (map from SUPABASE_SECRET_KEY), GITHUB_KEY.
Exits non-zero on a problem so it can be wired to a schedule.
"""
import json, os, sys, time, urllib.request

SB = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1"
KEY = os.environ["SUPABASE_KEY"]
H = {"apikey": KEY, "Authorization": "Bearer " + KEY, "User-Agent": "ows-health/1.0"}
TOK = os.environ.get("GITHUB_KEY")
REPO = os.environ.get("REPO", "untaptapp/onchain-window-sampler")
SINCE = int(os.environ.get("SINCE", int(time.time()) - 6 * 3600))

def cnt(path):
    h=dict(H); h["Prefer"]="count=exact"; h["Range"]="0-0"
    r=urllib.request.urlopen(urllib.request.Request(SB+path,headers=h,method="HEAD"),timeout=90)
    return int(r.headers["Content-Range"].split("/")[1])
def get(path):
    return json.loads(urllib.request.urlopen(urllib.request.Request(SB+path,headers=H),timeout=90).read())
def gh(path):
    r=urllib.request.Request(f"https://api.github.com/repos/{REPO}{path}",
        headers={"Authorization":"Bearer "+TOK,"Accept":"application/vnd.github+json","User-Agent":"ows/1.0"})
    return json.loads(urllib.request.urlopen(r,timeout=60).read())

now=int(time.time()); fail=[]
print(f"=== rh-universe health @ {time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime(now))} ===")
print(f"    (reference time {time.strftime('%H:%M:%SZ', time.gmtime(SINCE))}, "
      f"{(now-SINCE)/3600:.2f}h ago)\n")

# 1. LAUNCH FIREHOSE — the essential job. Rate before vs after the reference.
print("1. rh_launches write rate (the job that must not break)")
pre_lo, pre_hi = SINCE-6*3600, SINCE
pre=cnt(f"/rh_launches?select=mint&first_seen_at=gte.{pre_lo}&first_seen_at=lt.{pre_hi}")
post=cnt(f"/rh_launches?select=mint&first_seen_at=gte.{SINCE}")
pre_h=pre/6.0; post_h=post/max((now-SINCE)/3600, 1/60)
print(f"     6h BEFORE reference: {pre:,} rows  ({pre_h:,.0f}/h)")
print(f"          since reference: {post:,} rows  ({post_h:,.0f}/h)")
if now-SINCE < 900:
    print("     -> too soon to compare rates; checking only that writes resumed")
    if post==0: fail.append("no launches written since reference")
elif post_h < pre_h*0.5:
    fail.append(f"launch rate fell {100*(1-post_h/max(pre_h,1e-9)):.0f}% ({post_h:,.0f}/h vs {pre_h:,.0f}/h)")
else:
    print(f"     -> {post_h/max(pre_h,1e-9):.2f}x baseline  OK")

# 2. BOOKMARK — is the scan advancing and near head?
bm=get("/rh_scan_state?scanner=eq.launches&select=last_block,updated_at")[0]
age=(now-bm["updated_at"])/60
print(f"\n2. scan bookmark: block {bm['last_block']:,}, updated {age:.1f} min ago")
if age>45: fail.append(f"bookmark stale ({age:.0f} min)")

# 3. POOL FEES — the new thing.
tot=cnt("/rh_pool_fees?select=pool_id")
new=cnt(f"/rh_pool_fees?select=pool_id&first_seen_at=gte.{SINCE}")
print(f"\n3. rh_pool_fees: {tot:,} rows total, {new:,} written since reference")
# The failure this exists to catch: launches flowing while fees do not. That is the signature of
# the fee write erroring out inside its own try/except -- isolated, so nothing breaks and nothing
# says so except one line in a 5-hour log. Only meaningful once launches have actually moved.
if post > 200 and new == 0:
    fail.append(f"pool fees STALLED: {post:,} launches written since reference but 0 fees "
                "(the fee write is erroring inside its try/except -- read the pass line)")
# invariants that must hold on EVERY row, checked server-side
bad_dyn=cnt("/rh_pool_fees?select=pool_id&fee_dynamic=is.true&fee_ppm=not.is.null")
bad_hook=cnt("/rh_pool_fees?select=pool_id&fee_dynamic=is.true&hooks=eq.0x0000000000000000000000000000000000000000")
bad_rng=cnt("/rh_pool_fees?select=pool_id&fee_ppm=gt.1000000")
print(f"     dynamic rows with a non-NULL fee   : {bad_dyn}   (must be 0)")
print(f"     dynamic rows with NO hook contract : {bad_hook}   (must be 0 — v4 requires one)")
print(f"     fee_ppm > 1e6 (impossible)         : {bad_rng}   (must be 0)")
for n,v in (("dynamic rows carrying a fee",bad_dyn),("dynamic rows without a hook",bad_hook),
            ("out-of-range fee_ppm",bad_rng)):
    if v: fail.append(f"{v} {n}")

# 4. WORKFLOW — did the run abort (e.g. the selftest SystemExit)?
if not TOK:
    # Running INSIDE the workflow: a run inspecting its own status is circular, and the
    # collector's own exit code already reports it. Skip rather than fake it.
    print("\n4. workflow runs: skipped (no GITHUB_KEY)")
    runs=[]
else:
    runs=gh("/actions/workflows/rh_universe.yml/runs?per_page=5")["workflow_runs"]
if runs: print("\n4. recent rh-universe runs")
for r in runs[:4]:
    print(f"     {r['id']}  {r['status']:12} {str(r['conclusion']):10} {r['created_at']}  {r['head_sha'][:7]}")
if runs and runs[0]["conclusion"] == "failure":
    fail.append(f"newest run {runs[0]['id']} FAILED")

# 5. NEIGHBOURS — the fee read adds 0 RPC calls, but verify the shared-RPC jobs are unaffected.
print("\n5. collectors sharing the public RPC")
for tbl,clock in (("rh_tape","computed_at"),):
    r=get(f"/{tbl}?select={clock}&order={clock}.desc&limit=1")
    if r:
        v=r[0][clock]; a=(now-int(v))/60
        print(f"     {tbl}: newest row {a:.0f} min old")
        if a>180: fail.append(f"{tbl} stale ({a:.0f} min)")
    else:
        print(f"     {tbl}: empty")

print("\n" + "="*60)
if fail:
    print("VERDICT: PROBLEM\n  - " + "\n  - ".join(fail)); sys.exit(1)
print("VERDICT: HEALTHY — launch firehose unaffected, fee invariants hold"); sys.exit(0)
