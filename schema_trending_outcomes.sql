-- Derived analysis layer, recomputed on a schedule by trending_analyze.py so the
-- winning-subset / exit study is always current WITHOUT manual script runs.
-- Reads trending_snapshots (the collected price paths) — never writes it.

-- One row per (source, mint) entry: entry features + forward outcomes.
create table if not exists trending_outcomes (
  source       text not null,
  mint         text not null,
  entry_ts     bigint,                 -- first trending sighting in this source (unix s)
  entry_rank   int,
  entry_price  double precision,
  entry_mcap   double precision,
  entry_extra  jsonb,                  -- entry snapshot's source-specific features (buyskew/bundle/…)
  ret_15m      double precision,       -- static-horizon forward returns from entry
  ret_30m      double precision,
  ret_1h       double precision,
  ret_2h       double precision,
  ret_4h       double precision,
  ret_6h       double precision,
  mfe_pct      double precision,       -- max favorable excursion (best price/entry-1) at snapshot res
  mfe_min      int,                     -- minutes from entry to the MFE peak
  mae_pct      double precision,       -- max adverse excursion (worst dip)
  last_ret     double precision,        -- return at latest observation
  n_obs        int,
  span_min     int,
  updated_at   timestamptz default now(),
  primary key (source, mint)
);
create index if not exists trending_outcomes_src_idx on trending_outcomes(source);

-- Time-series of per-filter strategy stats (the auto-updating winning-subset hunt).
create table if not exists trending_strategy_stats (
  run_ts       bigint not null,         -- when this rollup ran (unix s)
  source       text not null,
  filter_name  text not null,           -- 'ALL', 'buyskew>=0.6', 'bundle<0.1', …
  horizon      text not null,           -- 'mfe' | '1h' | '2h' | 'last'
  n            int,
  mean_ret     double precision,
  median_ret   double precision,
  extop1_ret   double precision,        -- mean after dropping the single best (lottery check)
  pct_win      double precision,
  primary key (run_ts, source, filter_name, horizon)
);
create index if not exists trending_stats_latest_idx on trending_strategy_stats(source, filter_name, run_ts desc);

alter table trending_outcomes enable row level security;
alter table trending_strategy_stats enable row level security;
