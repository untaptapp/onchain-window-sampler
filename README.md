# onchain-window-sampler

A small scheduled worker for **on-chain market-microstructure research**. For a
configured set of source addresses, it detects new asset-acquisition events and
records the venue's trade window (~2 minutes) around each event, so price paths
and participant flow can be studied at second resolution.

State and data live in Postgres (Supabase). Compute runs on a scheduled GitHub
Actions workflow. No servers to run, no local machine required.

## How it works

- `worker.py` — one stateless pass detects new events for each configured source,
  then records the trade window for events that are ~2 minutes old. A short
  internal loop lets one scheduled run cover the gap between ticks.
- `report.py` — reads completed events and summarises the windows.
- `schema.sql` — the four tables (`sources`, `cursors`, `events`, `samples`).
- `tracker.py` + `.github/workflows/track.yml` — an independent slow job recording long-horizon price change per observed asset into `token_tracks` (`schema_tracks.sql`). Does not touch `events`/`samples`.
- `trending.py` + `.github/workflows/trending.yml` — an independent poller that snapshots the fomo trending board (`/v2/leaderboard/tokens/trending`) into `trending_snapshots` (`schema_trending.sql`, `source='fomoscan'`). The board is a live snapshot with no entry timestamp, so trending-entry events / rank velocity / board tenure are reconstructed offline by diffing snapshots. Same self-poll pattern as `sample`. Needs a third secret, `FOMOSCAN_KEY`; budget-aware (exits cleanly if the fomoscan quota is exhausted). Does not touch `events`/`samples`.
- `trending_gt.py` + `.github/workflows/trending-gt.yml` — **free** alternative to the fomoscan poller: snapshots GeckoTerminal's Solana `trending_pools` into the same `trending_snapshots` table (`source='geckoterminal'`, richer `extra` jsonb: per-window volume + buys/sells/buyers/sellers + price-change). Keyless, polls every 5 min. Added after fomoscan disabled its free tier; fomo's board is ~general volume-trending (≈58% overlap), so this is a drop-in event source. Both feeds coexist via the `source` column.
- `trending_st.py` + `.github/workflows/trending-st.yml` — **Solana Tracker** momentum poller: `/tokens/trending/5m` (short-window → catches small tokens *surging*, which the slow general boards miss) into `trending_snapshots` (`source='solanatracker'`; `extra` carries multi-window price-change, pool buys/sells/volume, holders, token age, venue, sniper count). Needs `SOLANA_TRACKER_KEY`; free tier 2,500 req/mo so it polls every 30 min; budget-aware + fail-loud.
- `trending_quotes.py` + `.github/workflows/trending-quotes.yml` — **point-in-time execution-cost collector** (Jupiter routed quotes, free/keyless) into `trending_quotes` (`schema_trending_quotes.sql`). Quotes a size ladder (0.25/1/5 SOL) per trending mint on an age-tiered cadence (dense while young — that's when entries and most exits happen). Exists because pool TVL is a *weak* proxy for real cost: measured Spearman(TVL, routed impact) = **-0.41**, non-monotonic, and it misses trap pools (a $356k-TVL pool quoted **100% impact on a $106 order**); modelling impact as constant-product understates real cost by **~6x** (cost is dominated by a fixed fee/routing floor — 50x size raises cost only 2.7x). Cost is also a *moving* quantity (liquidity drains ~11% exactly when price falls, so a loser's sell leg is dearer than its buy leg), and **Jupiter has no historical quote API — any interval not sampled is permanently un-costable**. Reads snapshots, writes only quotes.
- `universe_poller.py` + `.github/workflows/universe.yml` — the **candidate universe** (`candidate_universe`, `schema_universe.sql`): point-in-time snapshots of several hundred Solana pools per 5-min sweep, from GeckoTerminal's paged pool listings (free, keyless). This is the **control group** for the prediction study. Comparing trending tokens against *random* Solana tokens is invalid — trending tokens are selected on liquidity/volume/age while random ones are mostly dead, so a model trained that way scores ~0.99 AUC by learning "is this token alive" and gives zero discrimination among real candidates. Controls must instead be drawn from the tokens that looked plausible **at the same instant** and did not trend (risk-set / incidence-density sampling). It also fixes the **recall ceiling** — a token that later trends but was never in this table can never be caught — so coverage is a blocking gate before modelling. Stores the full sweep unfiltered; eligibility filters belong in analysis where they can be pre-registered. See [prediction-methodology.md](../research/prediction-methodology.md).
- `trending_bars.py` + `.github/workflows/trending-bars.yml` — **minute-resolution price paths** (`trending_bars`, `schema_trending_bars.sql`) plus a pool-resolution cache (`trending_pools`). The board feeds sample at 5/15/30 min, which is far too coarse to evaluate an *exit*: measured on snapshot data a 20% trailing stop "fired" on only **6% of paths** — not because the drawdowns were absent but because a 30-min gap cannot see them, so every snapshot-derived exit statistic is a hold-return wearing a trailing-stop label. Minute bars carry **high/low**, the prices a stop or take-profit actually touches. GeckoTerminal OHLCV is free, keyless, and supports `before_timestamp` paging, so this **backfills the history we already collected** (unlike Jupiter quotes, which are live-only). Resumable and budgeted: least-covered mints first, `MIN_OBS` skips mints with no path.
- `backtest.py` — the **decision layer**: simulates stop / trail / take-profit / partial-exit rules on minute bars using intra-bar high & low (assuming the low is hit before the high, so results are pessimistic), SOL-denominated, net of measured Jupiter cost applied per leg at the liquidity prevailing at that moment. Screens entry filters on a **time-split holdout** and reports the in-sample winner's holdout result with a Bonferroni correction, so a filter has to survive having been chosen. Run locally; reads only.
- `trending_analyze.py` + `.github/workflows/trending-analyze.yml` — scheduled **analysis rollup** (hourly): turns raw `trending_snapshots` into `trending_outcomes` (per-entry features + static-horizon returns + **MFE / time-to-MFE / MAE**) and `trending_strategy_stats` (the auto-updating winning-subset hunt: per-filter n/mean/median/ex-top1/%win, `schema_trending_outcomes.sql`). Makes the analysis continuous — query the tables instead of re-running scripts. Reads snapshots, never writes them. Also maintains `sol_usd_ref` (SOL/USD 5-min bars from GeckoTerminal, accumulating beyond its ~3-day OHLCV window) and carries **both denominations**: every board feed stores token price in USD, so a raw USD return silently bundles the token bet with a SOL/USD bet we never took. A trade here is funded and settled in SOL, so SOL-denominated P&L is the real P&L — worth ~1.8pp on median returns (roughly half a median trade) and a systematic bias, not noise. Outcomes carry `sol_usd_entry`/`_mfe`/`_last` plus `mfe_pct_sol`/`last_ret_sol`; any horizon converts as `(1+r_usd)*(sol_entry/sol_exit)-1`.
- `trending_tracker.py` + `.github/workflows/trending-track.yml` — long-horizon price/**mcap** tracker for every mint seen in `trending_snapshots` (into `trending_tracks`, `schema_trending_tracks.sql`). Keeps following tokens **after they leave the board** (where moonshots and slow rugs show up) via GeckoTerminal (free); age-tiered cadence, tracked indefinitely. Feeds long-term-winner discovery and exit-timing modelling. Does not touch the snapshot feeds.
- `trending_gmgn.py` + `.github/workflows/trending-gmgn.yml` — **GMGN** poller (`GET /v1/market/rank`, 100 tokens/pass) into `trending_snapshots` (`source='gmgn'`). Richest board-level feed: `extra` pre-computes `is_wash_trading`, `bundler_rate`, `sniper_count`, `smart_degen_count`/`renowned_count` (smart-money & KOL holders), `rug_ratio`, `top_10_holder_rate`, buys/sells, multi-window price-change, age, `hot_level` — most of the precursor+quality feature set, free. Data auth is `X-APIKEY` only (a read-only `GMGN_KEY`; the Ed25519 signing key is swap-only, unused). Polls every 15 min; budget-aware + fail-loud.

## Storage policy (Supabase free tier: 500 MB db, 5 GB egress/month)

Minute bars are by far the largest cost (~425 B/row). Unbounded, at the observed case-arrival rate
they grow the database **360–1,280 MB per day** — the whole budget in under a day. Three rules keep
it free:

1. **`POST_H` is 3h, not 12h.** The exit study never reads past ~2h (MFE peaks around 52 min).
   `PRE_MIN` stays at 6h: that is the pre-trend window the front-run counterfactual needs.
2. **`prune_trending_bars()` runs every pass** — a server-side RPC deleting anything outside each
   mint's `[t0-6h, t0+3h]` window. It reclaimed 42% of the table on first run.
3. **Filter in the database, never in the client.** One full bar-table read is ~88 MB; the 5 GB
   monthly egress budget is only ~56 of those. Pulling rows to filter them is the expensive mistake.

The retired KOL-thesis `samples` table (86 MB, 41% of the database) was exported to
`archive/samples_export.jsonl.gz` and dropped — count-verified against the DB before deletion.

**When the streaming universe lands: store aggregates, never the raw tape.** PumpPortal emits
thousands of trades/minute; persisting that firehose would dwarf everything above. The T0 design is
rolling counters in memory with only periodic per-mint snapshots written.

## Setup (~10 minutes, all free)

1. **Supabase** → create a project (no card). In the SQL editor, run
   `schema.sql`, then run your private `seed_sources.sql` to load the source
   addresses. Copy the **Project URL** and the **service_role key** from
   Settings → API.
2. **GitHub** → create a **public** repo (public = unlimited Actions minutes)
   and push these files.
3. In the repo → **Settings → Secrets and variables → Actions**, add:
   - `SUPABASE_URL` = your project URL
   - `SUPABASE_KEY` = the service_role key
   - `FOMOSCAN_KEY` = fomoscan API key (only needed for the `trending` workflow)
4. **Actions** tab → enable workflows. The `sample` workflow runs every 5 minutes;
   you can also trigger it manually with **Run workflow**.

## Reading the data

Run locally against the same project:

```bash
export SUPABASE_URL=... SUPABASE_KEY=...
python report.py
```

or query the tables directly in Supabase.

## Configuration

Worker behaviour is controlled by environment variables (see the top of
`worker.py`): `RUN_SECONDS`, `PASS_INTERVAL`, `WINDOW_SEC`, `FRESH_SEC`, `RPC_URL`.

## Notes

- `seed_sources.sql` and `.env` are git-ignored and must never be committed.
- The default RPC is a public endpoint. Set `RPC_URL` to a dedicated endpoint if
  you hit rate limits.
