-- The CANDIDATE UNIVERSE — the population at risk, and the control group for the prediction study.
--
-- Why this table is the long pole: predicting "which token is about to trend" requires controls
-- drawn from the tokens that looked like plausible candidates AT THE SAME INSTANT and did not
-- trend (risk-set / incidence-density sampling). Comparing trending tokens against RANDOM Solana
-- tokens is invalid — trending tokens are selected on liquidity/volume/age, random ones are mostly
-- dead, so a model trained that way scores ~0.99 AUC by learning "is this token alive at all" and
-- gives zero discrimination among the candidates production must actually choose between.
--
-- It also sets the RECALL CEILING: a token that later trends but was never in this table can never
-- be caught by any model. Coverage must be measured before any modelling.
--
-- Append-only point-in-time snapshots. Written by universe_poller.py; nothing reads it until the
-- coverage test in research/prediction-methodology.md §2 passes.

create table if not exists candidate_universe (
  pool_address text   not null,
  captured_at  bigint not null,          -- unix seconds, the instant these values were true
  mint         text,                     -- base token
  symbol       text,
  price_usd    double precision,
  liquidity    double precision,         -- reserve_in_usd
  mcap         double precision,
  fdv          double precision,
  vol_m5       double precision,
  vol_h1       double precision,
  vol_h24      double precision,
  txn_m5_buys  int,
  txn_m5_sells int,
  txn_h1_buys  int,
  txn_h1_sells int,
  pchg_m5      double precision,
  pchg_h1      double precision,
  pool_created bigint,                   -- for the age covariate used in control matching
  via          text,                     -- which sweep surfaced it (h24_vol / h1_vol / new_pools)
  primary key (pool_address, captured_at)
);
create index if not exists candidate_universe_mint_idx on candidate_universe(mint, captured_at desc);
create index if not exists candidate_universe_time_idx on candidate_universe(captured_at desc);

alter table candidate_universe enable row level security;
