-- DexScreener BOOST window sampler (boost_tape.py). See research/dex-paid-signal.md.
--
-- One event = one (token, paymentTimestamp) boost on CHAIN. The feed is multi-chain; the collector
-- writes only its CHAIN, and `chain` is in every key so a second chain can never collide.
-- Timestamps: *_ts and seen_at are epoch MILLISECONDS (DexScreener's unit); ts/first_buy_ts/
-- created_at are epoch SECONDS (chain block time). Stated here because this database already has
-- three time units (guardrail A7b).

create table if not exists boost_events (
  event_id          text primary key,            -- "<token>:<payment_ts>"
  chain             text not null,
  token             text not null,
  amount            integer,                     -- boosts bought in this order (10/30/50/100/500)
  total_amount      integer,                     -- feed's running total for the token
  channel           text not null,               -- 'ws' | 'rest' (rest = 60-s cached mirror; never pool lags)
  seen_at           bigint not null,             -- ms, when OUR process saw the item
  payment_ts        bigint not null,             -- ms, DexScreener's paymentTimestamp
  our_lag_ms        integer,                     -- seen_at - payment_ts
  pair_address      text,                        -- deepest pair at seen_at (DexScreener)
  dex_id            text,
  pair_created_at   bigint,                      -- ms
  pair_age_s        double precision,
  price_usd         double precision,            -- DexScreener snapshot AT seen_at (point-in-time)
  price_native      double precision,
  mcap_usd          double precision,
  liquidity_usd     double precision,
  vol_h1            double precision,
  buys_m5           integer,
  sells_m5          integer,
  profile_status    text,                        -- Enhanced Token Info order status, if any
  profile_paid_ts   bigint,                      -- ms
  n_prior_boosts    integer,                     -- boosts on this token before this one
  tape_status       text not null,               -- pending | done | no_pair | skipped_budget
  tape_complete     boolean,                     -- false = page cap hit; a short tape is NOT a quiet market
  n_tx              integer,
  n_failed          integer,                     -- failed txs in the window (slippage-tolerance proxy)
  pre_trades        integer,                     -- token legs in [payment-PRE_S, payment)
  first_buy_ts      bigint,                      -- s
  first_buy_slot    bigint,
  first_buy_lag_s   double precision,            -- first_buy_ts - payment_ts/1000 (block-time resolution)
  buys_60           integer,
  wallets_60        integer,
  sol_in_60         double precision,
  sells_60          integer,
  sol_out_60        double precision,
  buys_180          integer,
  sol_in_180        double precision,
  sol_out_180       double precision,
  clusters_60       integer,                     -- same-slot >=3-wallet identical-size buy clusters
  first_cluster_lag_s double precision,
  tipped_60         integer,                     -- buys in first 60 s carrying a Jito tip
  max_tip_60        double precision,
  marker_60         integer,                     -- buys carrying a jitodontfront… account (terminal marker)
  price_pre         double precision,            -- SOL per token, last fill before payment
  price_60          double precision,
  price_180         double precision,
  helius_calls      integer,
  created_at        bigint                       -- s, row written
);
create index if not exists boost_events_pay_idx on boost_events(chain, payment_ts desc);
create index if not exists boost_events_token_idx on boost_events(token);

create table if not exists boost_trades (
  event_id    text not null references boost_events(event_id) on delete cascade,
  chain       text not null,
  sig         text not null,
  slot        bigint not null,
  ts          bigint not null,                   -- s (block time)
  wallet      text not null,                     -- fee payer
  side        text not null,                     -- buy | sell (net token delta of the fee payer)
  token_amt   double precision,
  sol_amt     double precision,                  -- |SOL moved| for the swap (buy: ex network fee)
  fill_price  double precision,                  -- sol_amt / token_amt — an executed average, never chained
  tip         double precision,                  -- SOL sent to a Jito tip account in this tx
  fee         double precision,                  -- network fee (base + priority)
  programs    jsonb,                             -- top-level non-system program ids (<=6)
  marker      text,                              -- jitodontfront… account if present
  source      text,                              -- Helius source label (PUMP_FUN, RAYDIUM, ...)
  primary key (event_id, sig, wallet, side)
);
create index if not exists boost_trades_wallet_idx on boost_trades(wallet);
create index if not exists boost_trades_slot_idx on boost_trades(event_id, slot);

create table if not exists boost_followups (
  event_id      text not null references boost_events(event_id) on delete cascade,
  chain         text not null,
  horizon_s     integer not null,
  ts            bigint not null,                 -- s, when polled (>= payment + horizon)
  pair_address  text,
  dex_id        text,
  price_usd     double precision,
  price_native  double precision,
  mcap_usd      double precision,
  liquidity_usd double precision,
  vol_h1        double precision,
  vol_m5        double precision,
  buys_m5       integer,
  sells_m5      integer,
  boosts_active integer,
  primary key (event_id, horizon_s)
);

alter table boost_events enable row level security;
alter table boost_trades enable row level security;
alter table boost_followups enable row level security;

-- Retention: boost_trades is the only table that grows meaningfully (~≤800 rows/event). At ~150
-- Solana boosts/day that is ≤30 MB/day worst case; add a prune once the first month is in.
-- After applying: notify pgrst, 'reload schema';

-- Minute/hour price path per event from GeckoTerminal OHLCV (boost_bars.py): per-POOL candles
-- (B2), minute from -30 min to +3 h and hour to +48 h. `res` = 'm' | 'h'. Backfillable, so a
-- backfill event gets the same path a live one does. ts is epoch SECONDS (bar open).
create table if not exists boost_bars (
  event_id  text not null references boost_events(event_id) on delete cascade,
  pool      text not null,
  res       text not null,
  ts        bigint not null,
  o double precision, h double precision, l double precision, c double precision, v double precision,
  primary key (event_id, res, ts)
);
alter table boost_bars enable row level security;
alter table boost_events add column if not exists bars_status text;   -- null | done | no_pool | failed
