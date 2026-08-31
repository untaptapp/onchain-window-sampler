-- Minute-resolution price paths for every trending mint — the backtest substrate.
--
-- Why: the board feeds sample at 5/15/30 min, which is far too coarse to evaluate an EXIT rule.
-- Measured on snapshot data, a 20% trailing stop "fired" on only 6% of paths — not because the
-- drawdowns weren't there, but because a 30-minute gap cannot see them. Every exit number computed
-- from snapshots is therefore a hold-return wearing a trailing-stop label.
--
-- Minute bars carry HIGH and LOW, i.e. the intra-bar extremes a stop actually hits, so MFE/MAE,
-- time-to-peak and every stop/trail/TP rule become genuinely measurable. GeckoTerminal's OHLCV
-- endpoint is free, keyless, and supports `before_timestamp` paging — so unlike Jupiter quotes,
-- this backfills the ENTIRE history we already collected.

-- Pool resolution cache (a mint trades in many pools; we quote the deepest).
create table if not exists trending_pools (
  mint         text primary key,
  pool_address text,
  dex          text,
  reserve_usd  double precision,
  n_pools      int,
  resolved_at  bigint,
  ok           boolean default true      -- false = no pool found (delisted / unroutable)
);

-- Minute OHLCV. Stored per MINT (not per pool) so the analysis never has to join pool identity.
create table if not exists trending_bars (
  mint   text not null,
  ts     bigint not null,               -- bar open, unix seconds
  o      double precision,
  h      double precision,              -- intra-bar high: what a take-profit / MFE actually sees
  l      double precision,              -- intra-bar low:  what a stop-loss / trail actually hits
  c      double precision,
  vol    double precision,
  primary key (mint, ts)
);
-- NO separate (mint, ts) index. The PRIMARY KEY (mint, ts) already creates a unique btree on
-- exactly those columns, so a second index on them is pure cost: it was 119 MB (32% of this
-- table's total size), it duplicated every insert into the project's hottest write path, and it
-- competed with the PK for a 224 MB shared_buffers. It was dropped 2026-08-31. Do not re-add it.

alter table trending_pools enable row level security;
alter table trending_bars  enable row level security;
