-- RETENTION for trending_bars — the single largest storage cost in the project.
--
-- ⚠️  THIS FILE IS A MIRROR. The authoritative definition is the deployed `prune_trending_bars`
-- function; read it with
--     select pg_get_functiondef(oid) from pg_proc where proname='prune_trending_bars';
-- before trusting this file. It drifted once already: this file kept an `min(t0)`-anchored version
-- long after the deployed function was fixed to anchor on the union of events, so re-applying it
-- would have re-introduced a bug that deletes the post-trend bars the outcome analysis depends on
-- AND the entire launch-sourced control arm.
--
-- Why retention exists: minute bars are ~425 B/row and grow ~49 MB/day at the observed mint-arrival
-- rate. Deletes are server-side on purpose — pulling bars client-side to filter them would burn the
-- 5 GB monthly egress budget (one full bar-table read is already ~88 MB).
--
-- Anchors on the UNION of a mint's events (launch, universe sighting, first AND last trending
-- sighting), keeping [least(anchors) - pre_h, greatest(anchors) + post_h]. A mint can be a control
-- long before it becomes a case; anchoring on min() alone closes the window post_h after the FIRST
-- event, which for a token that launches at T and trends at T+8h deletes exactly the post-trend
-- bars the outcome and exit analysis need.
--
-- ⚠️  post_h / long_post_h here MUST match POST_H / LONG_POST_H in trending_bars.py. If the pruner
-- is stricter than the collector, every pass re-fetches bars this job then deletes — a permanent
-- GeckoTerminal call leak that shows up as "no progress" rather than as an error.
--
-- post_h is PER-MINT: tokens at least a day old at first sighting ("Track A", the revival track)
-- get LONG_POST_H hours because their exit rule cannot resolve inside 3h — 60.5% of Track A trades
-- were still open at the 3h horizon and were being scored as if closed. Fresh launches keep the
-- short window; their stop fires inside 3h for 93.6% of trades, so the extra bars are pure cost.
--
-- Idempotent; safe to re-run.

create or replace function public.prune_trending_bars(
  pre_h integer default 6, post_h integer default 6, long_post_h integer default 24)
returns integer language plpgsql security definer as $$
declare n integer; begin
  with a as (
    select mint, min(captured_at)/1000 as t from trending_snapshots group by mint
    union all
    select mint, min(captured_at)      as t from candidate_universe where mint is not null group by mint
    union all
    select mint, min(created_at)       as t from pump_launches group by mint
    union all
    select mint, max(captured_at)/1000 as t from trending_snapshots group by mint),
  w as (select mint, min(t) as lo, max(t) as hi from a group by mint),
  win as (
    -- Age comes from the trending_mint_age VIEW so the collector and this job can never disagree
    -- about which mints are Track A. A NULL age (unknown, or nonsense data) means SHORT window:
    -- guessing "old" retains bars for most of the table.
    select w.mint, w.lo, w.hi,
           case when g.age_s >= 86400 then long_post_h else post_h end as ph
    from w left join trending_mint_age g on g.mint = w.mint)
  delete from trending_bars b using win
  where win.mint = b.mint
    and (b.ts < win.lo - pre_h*3600 or b.ts > win.hi + win.ph*3600);
  get diagnostics n = row_count;

  -- Bars for mints that no longer appear in ANY source table. pump_launches MUST be in this list:
  -- launch-sourced controls appear in no other table, so omitting it turns an orphan cleanup into a
  -- delete of the entire control arm. This became load-bearing once candidate_universe grew a
  -- retention window — without it, a control's bars are stranded forever once its universe row ages
  -- out, because the window delete above can only match mints that still have an anchor row.
  delete from trending_bars b
  where not exists (select 1 from trending_snapshots s where s.mint = b.mint)
    and not exists (select 1 from candidate_universe c where c.mint = b.mint)
    and not exists (select 1 from pump_launches p where p.mint = b.mint);

  return n;
end $$;


-- ---------------------------------------------------------------------------------------------
-- `extra` is 35 MB of trending_snapshots' 41 MB of row data, and only the FIRST snapshot per
-- (source, mint) is ever read — it carries the entry features. Every later row's copy is dead
-- weight that grows ~21 MB/day. Nulling them bounds the table's growth; the freed space is reused
-- by incoming rows even without a VACUUM FULL.
-- Verified before writing this: `extra` is read only as `pts[0].get("extra")` in
-- trending_analyze.py and as the first-seen row in backtest.load_entries.
create or replace function public.prune_snapshot_extra(grace_hours integer default 2)
returns integer language plpgsql security definer as $$
declare n integer; begin
  with firsts as (
    select distinct on (source, mint) id from trending_snapshots
    order by source, mint, captured_at asc)
  update trending_snapshots s set extra = null
  where s.extra is not null
    and s.captured_at < (extract(epoch from now()) - grace_hours*3600) * 1000
    and not exists (select 1 from firsts f where f.id = s.id);
  get diagnostics n = row_count;
  return n;
end $$;


-- candidate_universe is the risk-set control pool and a genuine time series (volume profiles for
-- controls), so it cannot be deduplicated to first-sighting — but it grows ~23 MB/day unbounded.
-- captured_at here is SECONDS, not milliseconds, unlike trending_snapshots.
create or replace function public.prune_candidate_universe(keep_days integer default 14)
returns integer language plpgsql security definer as $$
declare n integer; begin
  delete from candidate_universe
   where captured_at < extract(epoch from now()) - keep_days*86400;
  get diagnostics n = row_count;
  return n;
end $$;
