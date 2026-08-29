-- Bar-resolution path outcomes, materialised so raw minute bars become DROPPABLE.
--
-- Why: trending_bars grows ~59 MB/day and is unbounded in mint count (retention windows each mint's
-- bars, but never expires a mint), so the 500 MB free tier is exhausted in ~3 days. One row here is
-- ~250 B and replaces ~45 kB of bars, and it is computed at MINUTE resolution, so unlike
-- trending_outcomes (which reads 5/15/30-min snapshots) its exit statistics are real rather than
-- hold-returns wearing a stop-loss label.
--
-- What it deliberately does NOT do: let us evaluate an exit rule we have not thought of yet. That
-- is exactly how POST_H=3 burned us — a storage decision made for a fast rule ("MFE peaks around
-- 52 min") silently made the slower rule we later chose unevaluable for 60.5% of Track A. So this
-- table stores a GRID of horizon returns and the full MFE/MAE shape alongside the frozen rules'
-- results, and bar retention stays OFF by default. Turning it on trades future flexibility for
-- runway; that is a decision to make deliberately, not by drifting into it.

create table if not exists trending_paths (
  source        text not null,
  mint          text not null,
  entry_ts      bigint,                 -- first sighting in this source (unix s)
  entry_price   double precision,
  age_s         double precision,       -- token age at entry; NULL when unknown or nonsense
  -- static-horizon SOL-denominated net returns, the raw material for any future rule
  ret_15m double precision, ret_30m double precision, ret_1h double precision,
  ret_2h  double precision, ret_3h  double precision, ret_6h double precision,
  ret_12h double precision,
  mfe_pct double precision, mfe_min int,      -- shape: how high, how soon
  mae_pct double precision, mae_min int,      -- and how deep before that
  -- the FROZEN pre-registered rules (research/pre-registration.md)
  capped_net double precision, capped_exit_ts bigint, capped_closed boolean,
  runner_net double precision, runner_exit_ts bigint, runner_closed boolean,
  entry_venue text, exit_venue text,          -- point-in-time routed venue class per leg
  liq_entry double precision, liq_exit double precision,
  n_bars int, span_min int, last_bar_ts bigint,
  horizon_h double precision,                 -- the horizon these numbers were computed at
  computed_at timestamptz default now(),
  primary key (source, mint)
);
create index if not exists trending_paths_entry_idx on trending_paths(entry_ts);
alter table trending_paths enable row level security;

-- Bar retention, gated on the path being SAFELY captured first. A mint's bars are droppable only
-- when (a) a path row exists for it, (b) that row was computed at or after the horizon it needs,
-- and (c) its window closed keep_days ago. Without (a) and (b) this is silent data destruction.
-- Returns 0 and deletes nothing when keep_days <= 0 — the deliberate default.
create or replace function public.prune_bars_materialised(keep_days integer default 0)
returns integer language plpgsql security definer as $$
declare n integer; begin
  if keep_days is null or keep_days <= 0 then return 0; end if;
  with w as (
    select mint, max(last_bar_ts) as hi, min(horizon_h) as hh, count(*) as k
    from trending_paths group by mint)
  delete from trending_bars b using w
  where w.mint = b.mint
    and w.k > 0
    and w.hh >= 3
    and w.hi < extract(epoch from now()) - keep_days*86400;
  get diagnostics n = row_count;
  return n;
end $$;
