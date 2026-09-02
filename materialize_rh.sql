-- Materialise Robinhood board-entry paths into trending_paths (source='gmgn_rh').
--
-- Runs entirely in the database: one pass over trending_bars instead of an ~88 MB client read
-- (A6/A9), and callable through PostgREST /rpc with only SUPABASE_KEY -- no management token in a
-- workflow secret. authenticator/service_role carry statement_timeout=120s (C-TIMEOUT) and this
-- takes ~17s, so it fits with headroom.
--
-- DELIBERATE DIVERGENCE FROM THE SOLANA PATH: a horizon counts as reached when the CORPUS of EVM
-- bars has passed it, not when this token's own bars do. materialize_paths.py uses the latter,
-- which drops every token that stopped trading before the horizon -- and a token stops trading
-- mostly because it died, so that population is selected on the outcome (measured: only 62.2% of
-- Solana paths have a ret_3h; 37.8% have span_min < 180). Here a token that stopped trading exits
-- at its last traded price, which is a real outcome; only an UNFINISHED horizon is NULL (D18).
--
-- Solana-only columns (capped_net, runner_net, entry_venue, exit_venue, liq_*) are left NULL rather
-- than faked: they need SOL denomination and Jupiter quotes, neither of which exists on Robinhood.
-- Returns here are USD-denominated, which is correct because the dominant quote asset on this chain
-- is USDG, a USD stablecoin (12 of the top 20 pools; WETH is 4).
create or replace function public.materialize_rh_paths(entry_tol_s integer default 300)
returns integer
language plpgsql
security definer
as $function$
declare n integer;
begin
  with fs as (select mint, min(captured_at)/1000.0 t0 from trending_snapshots
              where source='gmgn_rh' group by mint),
       bb as (select mint, created_at born from rh_launches
              where birth_kind in ('launchpad','mint_event')),
       j  as (select fs.mint, fs.t0, bb.born from fs left join bb using (mint)),
       hh as (select max(ts) tmax from trending_bars where mint like '0x%'),
       e  as (select j.mint, j.t0, j.born, hh.tmax, en.o p_entry
              from j cross join hh
              left join lateral (select tb.o from trending_bars tb
                 where tb.mint=j.mint and tb.ts>=j.t0 and tb.ts<=j.t0+entry_tol_s
                 order by tb.ts limit 1) en on true
              where en.o is not null and en.o>0),
       w  as (select e.*, tb.ts, tb.c, tb.h, tb.l from e
              join trending_bars tb on tb.mint=e.mint and tb.ts>=e.t0 and tb.ts<=e.t0+43200),
       g  as (
         select mint, t0, born, p_entry, tmax,
                count(*) n_bars, max(ts) last_bar_ts,
                (array_agg(ts order by h desc))[1] hi_ts, max(h) hi_p,
                (array_agg(ts order by l asc))[1]  lo_ts, min(l) lo_p,
                (array_agg(c order by ts desc) filter (where ts<=t0+900))[1]   c15m,
                (array_agg(c order by ts desc) filter (where ts<=t0+1800))[1]  c30m,
                (array_agg(c order by ts desc) filter (where ts<=t0+3600))[1]  c1h,
                (array_agg(c order by ts desc) filter (where ts<=t0+7200))[1]  c2h,
                (array_agg(c order by ts desc) filter (where ts<=t0+10800))[1] c3h,
                (array_agg(c order by ts desc) filter (where ts<=t0+21600))[1] c6h,
                (array_agg(c order by ts desc) filter (where ts<=t0+43200))[1] c12h
         from w group by mint, t0, born, p_entry, tmax)
  insert into trending_paths (source, mint, entry_ts, entry_price, age_s, n_bars, last_bar_ts,
        span_min, horizon_h, mfe_pct, mfe_min, mae_pct, mae_min,
        ret_15m, ret_30m, ret_1h, ret_2h, ret_3h, ret_6h, ret_12h, computed_at)
  select 'gmgn_rh', mint, t0::bigint, p_entry,
         case when born is not null then t0-born end,
         n_bars, last_bar_ts, ((last_bar_ts-t0)/60)::int, 3.0,
         case when hi_p>0 then hi_p/p_entry-1 end, ((hi_ts-t0)/60)::int,
         case when lo_p>0 then lo_p/p_entry-1 end, ((lo_ts-t0)/60)::int,
         case when tmax>=t0+900   and c15m is not null then c15m/p_entry-1 end,
         case when tmax>=t0+1800  and c30m is not null then c30m/p_entry-1 end,
         case when tmax>=t0+3600  and c1h  is not null then c1h /p_entry-1 end,
         case when tmax>=t0+7200  and c2h  is not null then c2h /p_entry-1 end,
         case when tmax>=t0+10800 and c3h  is not null then c3h /p_entry-1 end,
         case when tmax>=t0+21600 and c6h  is not null then c6h /p_entry-1 end,
         case when tmax>=t0+43200 and c12h is not null then c12h/p_entry-1 end,
         now()
  from g
  on conflict (source, mint) do update set
    entry_ts=excluded.entry_ts, entry_price=excluded.entry_price, age_s=excluded.age_s,
    n_bars=excluded.n_bars, last_bar_ts=excluded.last_bar_ts, span_min=excluded.span_min,
    mfe_pct=excluded.mfe_pct, mfe_min=excluded.mfe_min,
    mae_pct=excluded.mae_pct, mae_min=excluded.mae_min,
    ret_15m=excluded.ret_15m, ret_30m=excluded.ret_30m, ret_1h=excluded.ret_1h,
    ret_2h=excluded.ret_2h, ret_3h=excluded.ret_3h, ret_6h=excluded.ret_6h,
    ret_12h=excluded.ret_12h, computed_at=excluded.computed_at;
  get diagnostics n = row_count;
  return n;
end $function$;
