-- RETENTION for trending_bars — the single largest storage cost in the project.
--
-- Why this exists: minute bars are ~425 B/row, and at the observed case-arrival rate an unbounded
-- 12h post-entry window grows the database by 360–1,280 MB PER DAY, which exhausts a 500 MB budget
-- in under a day. The exit study never reads past ~2h (MFE peaks around 52 min) and the front-run
-- counterfactual needs 6h before first sighting, so anything outside [t0-6h, t0+3h] is dead weight.
--
-- Deletes are server-side on purpose: pulling bars client-side to filter them would burn the 5 GB
-- monthly egress budget (one full bar-table read is already ~88 MB).
--
-- Anchors on the UNION of a mint's events (launch, universe sighting, first AND last trending
-- sighting), not the earliest. A mint can be a control long before it becomes a case; anchoring on
-- min() alone closes the window post_h after the FIRST event, which for a token that launches at T
-- and trends at T+8h deletes exactly the post-trend bars the outcome and exit analysis need.
--
-- Safe to re-run; it is idempotent.

with t0 as (
  select mint, min(captured_at)/1000 as t0 from trending_snapshots group by mint
  union all
  select mint, min(captured_at)      as t0 from candidate_universe where mint is not null group by mint),
w as (select mint, min(t0) as t0 from t0 group by mint)
delete from trending_bars b
using w
where w.mint = b.mint
  and (b.ts < w.t0 - 6*3600 or b.ts > w.t0 + 3*3600);

-- Bars for mints that no longer appear in ANY source table.
-- pump_launches MUST be in this list. Launch-sourced controls appear in no other table, so
-- omitting it turns this from an orphan cleanup into a delete of the entire control arm. It
-- measured as harmless (0 rows) only because no launch control had bars yet — the blast radius
-- grows with every control we collect.
delete from trending_bars b
where not exists (
  select 1 from trending_snapshots s where s.mint = b.mint
  union all
  select 1 from candidate_universe c where c.mint = b.mint
  union all
  select 1 from pump_launches p where p.mint = b.mint);
