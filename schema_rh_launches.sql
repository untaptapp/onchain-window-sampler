-- The ROBINHOOD CHAIN LAUNCH FIREHOSE — the population at risk, and the control group for the
-- trending-prediction study on chain 4663.
--
-- WHY A FIREHOSE AND NOT A REST SWEEP
-- The Solana equivalent was first attempted as a REST sweep of GeckoTerminal pool listings
-- (`candidate_universe`) and FAILED its coverage gate: only 3.7-9.4% of tokens that went on to
-- trend were in it beforehand, because the paged listings only reach the top ~200 pools by 24h
-- volume and the ~200 newest, a bimodal sample that misses the $10k-$1M band where trending
-- actually happens. `pump_launches` fixed that at the source. This table is the same fix for
-- Robinhood, and it is CHEAPER here: the launchpad factories emit a creation event per token, so
-- `eth_getLogs` filtered by contract address enumerates the whole population for a handful of
-- calls. Measured 2026-09-01: a chain-wide scan for Transfer-from-zero costs 3.7M logs/day (78% of
-- it one high-frequency contract) to surface ~2,000 tokens that matter; the factory-filtered scan
-- gets the same tokens for ~0.1% of the log volume.
--
-- RECALL CEILING. A token that later trends but was never recorded here can never be caught by any
-- model, so `WATCH` in rh_universe.py must be audited against the board population as launchpads
-- come and go — `research/robinhood-expansion.md` carries the measured coverage. The board's
-- `launchpad` distribution (pons_v2 67%, pons 9%, longxyz 9%, pools_trade 3%, noxa 2%, o1_rwa 2%)
-- is the checklist; anything unwatched is a hole in the control arm, not merely missing rows.
--
-- ELIGIBILITY IS AN ANALYSIS-TIME DECISION (E10). This table stores EVERY launch, including tokens
-- that later trend. A mint is a valid control at time t iff its first board sighting is after t;
-- filtering "has ever trended" at collection time makes the control pool mean "never trended"
-- rather than "had not trended YET", which inflates every case/control difference. Do not add a
-- filter here.
--
-- Volume: ~5-20k launches/day chain-wide at ~180 B/row = ~1-4 MB/day. Retention is handled by
-- prune_rh_launches (see below), NOT by prune_trending_bars.

create table if not exists rh_launches (
  mint          text primary key,        -- the new token contract (lowercase hex)
  created_at    bigint not null,          -- unix seconds, derived from block number x block time
  block_number  bigint not null,
  launchpad     text,                     -- our label for the factory that emitted the event
  -- How created_at was derived, and therefore whether it may be used as a BIRTH.
  --   'launchpad' -> a token-creation event fired: created_at IS the birth.
  --   'pool'      -> a pair/pool-creation event on a shared AMM: created_at is only an UPPER BOUND.
  -- Measured 2026-09-02: 71.7% of amm_shared board mints were sighted BEFORE their recorded
  -- "birth", median 14.4h before. Computing a token age from a pool birth yields negative ages,
  -- and clamping a measurement window to one inverts the block range -- which eth_getLogs answers
  -- with an empty list, fabricating a "silent token". Writers must never let a pool row overwrite
  -- a launchpad row for the same mint (rh_universe writes pool rows ignore-duplicates).
  birth_kind    text,
  factory       text not null,            -- the contract whose log we matched
  topic0        text not null,            -- the creation event signature we matched
  creator       text,                     -- tx.from of the creating transaction, when resolved
  tx_hash       text,
  symbol        text,                     -- best-effort decode from the event data
  first_seen_at bigint not null           -- when OUR collector recorded it; block time is an
                                          -- estimate, this is not, so lag can be audited
);
create index if not exists rh_launches_created_idx on rh_launches(created_at desc);
create index if not exists rh_launches_block_idx   on rh_launches(block_number desc);
create index if not exists rh_launches_lp_idx      on rh_launches(launchpad, created_at desc);

alter table rh_launches enable row level security;

-- Scan bookmark, so a restarted collector resumes instead of rescanning from genesis. One row.
create table if not exists rh_scan_state (
  scanner      text primary key,          -- 'launches'
  last_block   bigint not null,
  updated_at   bigint not null
);
alter table rh_scan_state enable row level security;
