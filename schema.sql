-- Gold Rate Bot — Postgres schema
--
-- Apply with:  psql -d goldbot -f schema.sql
-- Safe to re-run: every statement is IF NOT EXISTS / idempotent.

-- ── Users & quota ────────────────────────────────────────────────────

create table if not exists users (
    user_id    bigint primary key,
    first_name text,
    username   text,
    reg_city   text,
    reg_step   text,
    subscribed boolean     not null default false,
    created_at timestamptz not null default now()
);

-- One row per gold query. Replaces the daily_count column: gives correct
-- IST-based quota windows for free, plus usage analytics.
create table if not exists query_log (
    id         bigserial primary key,
    user_id    bigint      not null references users(user_id) on delete cascade,
    queried_at timestamptz not null default now()
);

create index if not exists query_log_user_time
    on query_log (user_id, queried_at desc);

-- ── Rates (Phase 4) ──────────────────────────────────────────────────

create table if not exists city_rates (
    rate_date  date          not null,
    city       text          not null,
    state      text,
    karat      smallint      not null check (karat in (18, 22, 24)),
    per_gram   numeric(12,2) not null,
    per_10g    numeric(12,2),
    fetched_at timestamptz   not null default now(),
    primary key (rate_date, city, karat)
);

create index if not exists city_rates_lookup
    on city_rates (lower(city), karat, rate_date desc);

-- One alert per user keeps the commands simple (/alert below 14500).
-- triggered_at is set when it fires and cleared when a new alert is set,
-- so an alert notifies once rather than on every refresh.
create table if not exists alerts (
    user_id      bigint primary key references users(user_id) on delete cascade,
    city         text          not null,
    karat        smallint      not null default 22,
    direction    text          not null check (direction in ('below', 'above')),
    threshold    numeric(12,2) not null check (threshold > 0),
    created_at   timestamptz   not null default now(),
    triggered_at timestamptz
);

create index if not exists alerts_pending
    on alerts (lower(city), karat) where triggered_at is null;

-- Contract month is stored as data, not hard-coded in the bot, so a
-- contract roll can never silently blank out the MCX block.
create table if not exists mcx_rates (
    rate_date         date primary key,
    gold_contract     text,
    gold_per_10g      numeric(12,2),
    gold_change_pct   numeric(6,2),
    silver_contract   text,
    silver_per_kg     numeric(12,2),
    silver_change_pct numeric(6,2),
    fetched_at        timestamptz not null default now()
);
