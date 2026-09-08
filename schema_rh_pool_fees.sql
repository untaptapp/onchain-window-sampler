-- Uniswap v4 POOL FEES on Robinhood Chain (id 4663), read from the PoolManager's `Initialize`
-- event — the same log `rh_universe.py`'s `amm_shared` watch entry already fetches, so this costs
-- ZERO additional RPC calls.
--
-- WHY THIS TABLE EXISTS
-- --------------------
-- `backtest.py` charges FEE_DEFAULT = 0.25%/side for every non-pump.fun venue. On this chain that
-- is not a fee tier, it is a guess. Measured 2026-09-07 on 25 pools that board tokens actually
-- trade in: 8 dynamic, 9 at 0.00%, 3 at 0.25%, 1 at 2.00%, 1 at 81.00%, 1 at 84.02%. Uniswap v3
-- pools created that day were 57.6% at the 1.00% tier and only 12.3% at 0.30%.
--
-- WHY IT IS KEYED ON THE POOL, NOT THE MINT
-- -----------------------------------------
-- A fee is a property of the pool; one mint may have many pools at different fees. Keying on mint
-- would also collide with A7d: `rh_launches` writes `amm_shared` rows ignore-duplicates so a pool
-- event can never overwrite a launchpad birth — which means a mint first seen via its launchpad
-- would never receive a fee at all. `pool_id` joins directly to `trending_pools.pool_address`,
-- which for Robinhood v4 IS the 66-char v4 pool id (verified 25/25, 2026-09-07).
--
-- EVENT LAYOUT (verified on 195/195 live logs, 2026-09-07: 4 topics, 5 data words)
--   Initialize(PoolId indexed id, Currency indexed currency0, Currency indexed currency1,
--              uint24 fee, int24 tickSpacing, IHooks hooks, uint160 sqrtPriceX96, int24 tick)
--   topics[1]=pool id  topics[2..3]=currencies  data[0]=fee  data[1]=tickSpacing  data[2]=hooks
--
-- FAILING CLOSED ON DYNAMIC FEES (D17, A-ENTITY)
-- ----------------------------------------------
-- A v4 pool with the 0x800000 flag set has NO fixed fee: the hook decides it at swap time, so the
-- key carries a sentinel, not a rate. Storing the raw uint24 would let a consumer compute
-- 0x800000/1e6 = 838% and never notice. So `fee_ppm` is NULL whenever `fee_dynamic` is true, and
-- any arithmetic on it yields NULL rather than a confident wrong number. 14.9% of live pools
-- (29/195 in a 15-min window) are dynamic, and every one of them carried a non-zero hook contract,
-- which is what the protocol requires — that agreement is the extractor's self-test.
create table if not exists rh_pool_fees (
  pool_id       text primary key,          -- 66-char v4 pool id = topics[1]
  currency0     text not null,
  currency1     text not null,
  fee_ppm       int,                       -- static LP fee in ppm (1e6 = 100%). NULL iff dynamic.
  fee_dynamic   boolean not null,          -- 0x800000: fee set by the hook, per swap
  tick_spacing  int,
  hooks         text,                      -- zero address = no hook
  block_number  bigint not null,
  created_at    bigint,                    -- bracketed block time (A7c), epoch SECONDS (A7b)
  first_seen_at bigint not null            -- our write clock, epoch SECONDS
);
-- Join axis to trending_pools / the backtest.
create index if not exists rh_pool_fees_cur_idx on rh_pool_fees(currency0, currency1);
-- Ingest clock for mirror.py (C-NANO: incremental by write clock, not by market event time).
create index if not exists rh_pool_fees_seen_idx on rh_pool_fees(first_seen_at);

-- Same posture as rh_launches: the collector writes with the service key, which bypasses RLS.
alter table rh_pool_fees enable row level security;

-- GROWTH, stated rather than discovered (C0d). ~15.5k Initialize events/day measured 2026-09-07,
-- at ~200 B/row = ~1.1 GB/year. There is deliberately NO pruner: a pool's fee is the only thing
-- that lets a PAST trade in that pool be costed, so dropping old rows would silently re-create the
-- flat-rate guess this table exists to replace. Revisit against the 8 GB plan, not by reflex.

notify pgrst, 'reload schema';
