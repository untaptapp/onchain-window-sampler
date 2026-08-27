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
