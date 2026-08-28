-- Long-horizon price/mcap tracking for every mint ever seen in `trending_snapshots`.
-- Keeps following tokens AFTER they leave the trending board — that's where moonshots
-- and slow rugs reveal themselves, and where the exit-timing (volume drop-off) patterns
-- live. Separate from token_tracks (that lane tracks KOL-copy `events`). Run once.

create table if not exists trending_tracks (
  mint              text primary key,
  symbol            text,
  first_seen_ts     bigint,                      -- earliest trending sighting (unix seconds)
  first_source      text,                        -- feed that first saw it (gmgn/geckoterminal/...)
  entry_price       double precision,            -- price at that first trending sighting (USD)
  entry_mcap        double precision,            -- mcap at first sighting (USD)
  last_price        double precision,
  last_mcap         double precision,
  last_pct          double precision,            -- last_price / entry_price - 1
  ath_price         double precision,            -- best price since we started tracking
  ath_mcap          double precision,
  ath_pct           double precision,            -- best multiple vs entry (moonshot detector)
  atl_pct           double precision,            -- worst drawdown vs entry (stop-loss study)
  n_updates         int default 0,
  consecutive_nulls int default 0,               -- times the price source returned nothing
  last_check_ts     bigint,                       -- unix seconds of last check (cadence)
  inactive          boolean default false,       -- manual off-switch only; NOT set by age
  first_tracked     timestamptz default now(),
  updated_at        timestamptz default now()
);
-- Tracked INDEFINITELY (a token that briefly trended can moon weeks later). Cadence tiers by
-- age keep coverage cheap (recent = frequent, old = ~daily) instead of dropping tokens.
create index if not exists trending_tracks_check_idx on trending_tracks(last_check_ts);
alter table trending_tracks enable row level security;
