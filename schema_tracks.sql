-- Long-horizon price tracking for assets seen in `events`. Separate from the
-- windowed sampler — it never writes to events/samples, so existing metrics are
-- untouched. Run once in the Supabase SQL editor (after schema.sql).

create table if not exists token_tracks (
  asset          text primary key,          -- token mint
  symbol         text,
  first_event_id text,                        -- earliest event that referenced it
  first_entry_ts bigint,                      -- unix seconds of that first KOL entry
  entry_price    double precision,            -- price at that first entry (USD)
  last_price     double precision,
  last_pct       double precision,            -- last_price / entry_price - 1
  ath_price      double precision,            -- best price seen since we started tracking
  ath_pct        double precision,
  atl_pct        double precision,            -- worst
  n_updates      int default 0,
  inactive       boolean default false,       -- stop once past the tracking horizon
  first_seen     timestamptz default now(),
  updated_at     timestamptz default now()
);
create index if not exists token_tracks_active_idx on token_tracks(inactive);

alter table token_tracks enable row level security;
