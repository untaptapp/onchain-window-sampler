-- Point-in-time EXECUTION COST for every trending mint.
--
-- Why this exists: `trending_snapshots.liquidity` is pool TVL, and TVL is a WEAK proxy for what
-- a trade actually costs (measured Spearman(TVL, routed impact) = -0.41; a $356k-TVL pool has
-- quoted 100% impact on a $106 order). Execution cost must be QUOTED, not modelled — and it is a
-- point-in-time quantity that moves with price, so a single "latest" quote cannot represent the
-- cost at the moment we model an entry or an exit. Jupiter has NO historical quote API, so any
-- interval we don't sample is permanently un-costable. Hence: sample continuously, forever.
--
-- Source: Jupiter lite-api /swap/v1/quote (free, keyless) — the actual routed executable price
-- across every Solana DEX, not a model. Written by trending_quotes.py; read by trending_analyze.py.

create table if not exists trending_quotes (
  mint              text not null,
  quoted_at         bigint not null,        -- unix seconds (the point in time this cost was true)
  size_sol          double precision not null,
  size_usd          double precision,       -- Jupiter swapUsdValue for the input leg
  price_impact_pct  double precision,       -- routed price impact, PERCENT (0.68 = 0.68%)
  out_amount        numeric,                -- raw token units out (sizing / effective-price checks)
  route             text,                   -- e.g. 'Pump.fun Amm' or 'Meteora DLMM,Whirlpool'
  n_hops            int,
  liquidity         double precision,       -- board TVL at quote time, for cost-vs-TVL calibration
  price             double precision,       -- board price at quote time, to align with the path
  ok                boolean default true,   -- false = no route / quote failed (a trap-pool signal)
  primary key (mint, quoted_at, size_sol)
);
create index if not exists trending_quotes_mint_idx on trending_quotes(mint, quoted_at desc);
create index if not exists trending_quotes_time_idx on trending_quotes(quoted_at desc);

alter table trending_quotes enable row level security;
