-- fomo trending-board snapshots. Append-only time-series: one row per (token,
-- capture). The fomoscan /v2/leaderboard/tokens/trending endpoint is a LIVE
-- snapshot with no per-token entry timestamp, so trending-ENTRY events, rank
-- velocity, and board tenure are reconstructed OFFLINE by diffing consecutive
-- snapshots. Independent of the sampler — never touches events/samples.
-- Run once in the Supabase SQL editor (or via the Management API).

create table if not exists trending_snapshots (
  id           bigint generated always as identity primary key,
  captured_at  bigint not null,             -- fomoscan capturedAt (ms epoch) — the board's own clock
  polled_at    bigint not null,             -- our poll wall-clock (unix seconds)
  mint         text   not null,             -- token mint (entry.id)
  rank         int,                          -- board rank at this capture (1 = top)
  handle       text,
  label        text,
  volume       double precision,            -- board's volume figure (the ranking input)
  market_cap   double precision,
  price        double precision,
  liquidity    double precision,
  source       text not null default 'fomoscan',  -- 'fomoscan' | 'geckoterminal' (both feeds coexist)
  extra        jsonb                                -- source-specific richer fields (GeckoTerminal:
                                                    -- per-window vol + txns{buys,sells,buyers,sellers}
                                                    -- + price-change at m5/m15/m30/h1/h6/h24, pool, age)
);
-- If the table pre-dates these columns, add them:
-- alter table trending_snapshots add column if not exists source text not null default 'fomoscan';
-- alter table trending_snapshots add column if not exists extra jsonb;

-- One row per token per distinct board capture: makes the poller idempotent
-- (a retried/duplicate poll for the same capturedAt is a no-op upsert).
create unique index if not exists trending_snap_uniq on trending_snapshots(mint, captured_at);
-- per-token trajectory (rank/volume velocity, first-seen = entry event)
create index if not exists trending_snap_mint_idx on trending_snapshots(mint, captured_at);
-- whole-board scans by time
create index if not exists trending_snap_time_idx on trending_snapshots(captured_at);

alter table trending_snapshots enable row level security;
