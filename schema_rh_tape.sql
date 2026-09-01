-- WALLET-LEVEL TAPE FEATURES for Robinhood Chain (id 4663).
--
-- WHAT THIS ADDS THAT NOTHING ELSE CAN
-- Every feature we currently screen on comes from GMGN's board payload or from OHLCV bars, and
-- both describe a token's PRICE. Neither can see who is buying it. The winner profile found on
-- 2026-09-01 (revival below its own ATH + pre-entry volume, holdout winner-rate 26.0% vs a 20.5%
-- base) is a volume story, and volume is exactly the quantity a deployer can manufacture: Bankr
-- pays 59% of trading fees to the creating wallet ($22.23M to date), so there is a direct, funded
-- incentive to fake the signal we are about to trade on. Transfer logs are the only place that
-- distinguishes 300 real buyers from one wallet cycling 300 times.
--
-- It is therefore two things at once: a set of PREDICTIVE features (buyer breadth, new-wallet
-- rate, holder concentration) and a MANIPULATION SCREEN (round-tripping, circular flow, deployer
-- share). The screen matters more, because a profile that cannot tell manufactured volume from
-- organic volume will preferentially buy the manufactured kind — it is brighter on every metric.
--
-- POINT-IN-TIME BY CONSTRUCTION (D13, D-POSTOBS)
-- A row covers the half-open window [as_of - window_s, as_of). Nothing after `as_of` is read, so a
-- row is safe to use as a feature for a decision made AT `as_of`. For the front-running study the
-- case rows are written with as_of = the token's first board sighting, which is precisely the
-- moment a model must fire before.
--
-- AGGREGATES, NOT THE TAPE (A9). The raw transfer firehose is ~150k logs/hour chain-wide and
-- would exhaust the storage budget in days. Only per-window summaries are stored; the raw logs are
-- permanently re-derivable from the RPC.

create table if not exists rh_tape (
  mint              text   not null,
  as_of             bigint not null,      -- unix seconds; the window ENDS here (exclusive)
  window_s          int    not null,      -- window length in seconds
  arm               text,                 -- 'case' | 'control' — provenance of the task, NOT an
                                          -- eligibility claim; risk-set membership is decided at
                                          -- analysis time (E10)
  -- observability
  n_logs            int,                  -- transfers seen in the window
  truncated         boolean,              -- the scan hit its per-token log cap: features are a
                                          -- LOWER bound and must be excluded, not trusted
  pool              text,                 -- inferred venue (highest-degree counterparty)
  pool_degree_share double precision,     -- how dominant that counterparty is; low = pool unsure
  -- breadth
  n_wallets         int,
  n_buyers          int,
  n_sellers         int,
  buy_count         int,
  sell_count        int,
  -- concentration (of tokens BOUGHT from the pool, by wallet)
  top1_buy_share    double precision,
  top5_buy_share    double precision,
  buyer_hhi         double precision,     -- sum of squared shares; 1.0 = a single buyer
  -- manipulation structure
  round_trip_rate   double precision,     -- share of buyers that also sold inside the window
  circular_rate     double precision,     -- share of transfer VALUE moving wallet<->wallet,
                                          -- bypassing the pool entirely
  self_loop_n       int,                  -- A->B->A patterns observed
  creator_share     double precision,     -- share of transfer value touching the deployer
  new_wallet_rate   double precision,     -- buyers with no prior appearance in this token
  prior_ok          boolean,              -- did the prior-holder scan actually succeed? when
                                          -- false, new_wallet_rate is NULL rather than a default:
                                          -- a failed lookback once made every buyer look new
  -- flow
  net_buy_ratio     double precision,     -- buy_count / (buy_count + sell_count)
  vol_tokens        double precision,     -- total tokens moved (raw units, no decimals applied)
  computed_at       bigint not null,
  primary key (mint, as_of, window_s)
);
create index if not exists rh_tape_asof_idx on rh_tape(as_of desc);
create index if not exists rh_tape_arm_idx  on rh_tape(arm, as_of desc);

alter table rh_tape enable row level security;
