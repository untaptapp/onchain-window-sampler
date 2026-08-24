-- Schema for the on-chain window sampler. Run once in the Supabase SQL editor.
-- Tables are locked with RLS and no anon policies; the worker uses the service
-- key, which bypasses RLS. Nothing here is readable with the public anon key.

create table if not exists sources (
  address  text primary key,
  label    text,
  active   boolean default true
);

create table if not exists cursors (
  address    text primary key,
  last_sig   text,
  updated_at timestamptz default now()
);

create table if not exists events (
  id             text primary key,          -- source transaction signature
  source_address text,
  label          text,
  asset          text,                       -- acquired token
  symbol         text,
  venue          text,                       -- market/pool address
  event_ts       bigint,                     -- unix seconds
  event_slot     bigint,
  ref_price      double precision,           -- price at the event
  supply         double precision,
  ref_fdv        double precision,
  ref_cap        double precision,           -- reference capitalisation at the event
  n_samples      int,
  status         text default 'pending',     -- pending | done | empty | expired
  created_at     timestamptz default now()
);
create index if not exists events_status_idx on events(status);
create index if not exists events_cap_idx    on events(ref_cap);

create table if not exists samples (
  event_id  text references events(id) on delete cascade,
  tx        text,
  ts        bigint,
  side      text,                            -- buy | sell
  actor     text,
  price     double precision,
  notional  double precision,
  primary key (event_id, tx)
);
create index if not exists samples_event_idx on samples(event_id);

alter table sources enable row level security;
alter table cursors enable row level security;
alter table events  enable row level security;
alter table samples enable row level security;
