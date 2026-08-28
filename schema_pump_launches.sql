-- The pump.fun LAUNCH FIREHOSE — the population at risk for the prediction study.
--
-- Why this exists: the REST-based candidate universe FAILED its coverage gate (only 3.7–9.4% of
-- tokens that went on to trend were in it beforehand — see research/prediction-methodology.md).
-- GeckoTerminal's paged listings only reach the top ~200 pools by 24h volume and the ~200 newest
-- pools, which is a bimodal sample that misses the $10k–$1M band where trending actually happens.
--
-- This table fixes coverage at the source: EVERY pump.fun token is recorded at birth, and 84.3% of
-- observed trending mints are pump.fun tokens (88.6% of resolved DEXes are pumpswap/pump-fun). So
-- every future trender in that 84% necessarily passes through here before it trends.
--
-- What it does NOT give: trade flow. PumpPortal's subscribeTokenTrade requires an API key funded
-- with >=0.02 SOL; only the creation and migration streams are free. So this supplies the
-- POPULATION and birth-time stats, not volume/CVD features — those come from trending_bars or a
-- funded key later.
--
-- Volume: ~29 creations/min (~41,000/day) at ~180 B/row = ~7 MB/day, so retention is mandatory.

create table if not exists pump_launches (
  mint            text primary key,
  created_at      bigint not null,        -- unix seconds, when the create event was received
  signature       text,
  name            text,
  symbol          text,
  creator         text,                   -- traderPublicKey of the deployer
  pool            text,
  initial_buy     double precision,       -- tokens bought by the creator in the create tx
  sol_amount      double precision,       -- SOL spent at creation — the only birth traction signal
  market_cap_sol  double precision,
  v_sol_curve     double precision,       -- virtual SOL reserves in the bonding curve
  v_tokens_curve  double precision,
  bonding_curve   text,
  migrated_at     bigint,                 -- set when a migration event arrives; graduation is a
  migrated_pool   text                    -- strong, rare traction signal and is free to observe
);
create index if not exists pump_launches_created_idx on pump_launches(created_at desc);
create index if not exists pump_launches_migrated_idx on pump_launches(migrated_at)
  where migrated_at is not null;

alter table pump_launches enable row level security;

-- DERIVED VIEW `pump_launches_usd` (created via the Management API, not this file):
--   sol_usd_at_birth, mcap_usd = market_cap_sol * sol_price, liq_usd = v_sol_curve * sol_price * 2
-- joined to the NEAREST sol_usd_ref bar. A view rather than stored columns, because a USD figure
-- written at insert time goes stale the moment SOL moves and needs re-backfilling whenever the SOL
-- reference improves.
--
-- IMPORTANT — birth size does NOT work as a matching covariate. Every pump.fun token starts on the
-- same bonding curve, so mcap_usd at birth is p25 $2,898 / median $2,946 / p90 $3,926: essentially
-- a constant. Risk-set matching needs size AT THE MATCHING TIME (T_case - L), which comes from the
-- minute bars (mcap = price * supply; pump.fun supply is ~1e9), not from these birth fields.
