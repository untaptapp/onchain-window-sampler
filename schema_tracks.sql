-- Long-horizon price tracking for assets seen in `events`. Separate from the
-- windowed sampler — it never writes to events/samples, so existing metrics are
-- untouched. Run once in the Supabase SQL editor (after schema.sql).

create table if not exists token_tracks (
  asset             text primary key,          -- token mint
  symbol            text,
  first_event_id    text,                        -- earliest event that referenced it
  first_entry_ts    bigint,                      -- unix seconds of that first KOL entry
  entry_price       double precision,            -- price at that first entry (USD)
  last_price        double precision,
  last_pct          double precision,            -- last_price / entry_price - 1
  ath_price         double precision,            -- best price seen since we started tracking
  ath_pct           double precision,
  atl_pct           double precision,            -- worst
  n_updates         int default 0,
  consecutive_nulls int default 0,               -- times GeckoTerminal returned no price
  last_check_ts     bigint,                       -- unix seconds of last check (for cadence)
  inactive          boolean default false,       -- manual off-switch only; NOT set by age
  first_seen        timestamptz default now(),
  updated_at        timestamptz default now()
);
-- Tracked INDEFINITELY: dormant tokens can rocket months later. Cadence tiers by
-- age keep coverage cheap (recent = frequent, old = ~daily) instead of dropping them.
create index if not exists token_tracks_check_idx on token_tracks(last_check_ts);

alter table token_tracks enable row level security;
