"""
rates_store.py — price history in Postgres (Phase 4).

Everything here is optional. The bot calls these only when the Postgres
backend is active; with the JSON backend the pool is None and the history
features report themselves as unavailable rather than failing.

Split deliberately in two:

  * pure helpers (sparkline, pct_change, format_delta, alert_fires) — no I/O,
    fully unit-testable without a database
  * async query functions — take an asyncpg pool as their first argument

Numeric columns come back from asyncpg as Decimal; every function here
converts to float at the boundary so callers never deal with Decimal.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional, Sequence

log = logging.getLogger(__name__)

DEFAULT_KARAT = 22
TREND_DAYS = 7

# ── Pure helpers ──────────────────────────────────────────────────────────────

_BLOCKS = "▁▂▃▄▅▆▇█"


def sparkline(values: Sequence[float]) -> str:
    """Render a series as block characters. Flat series render mid-height."""
    values = [float(v) for v in values]
    if not values:
        return ""
    low, high = min(values), max(values)
    if high == low:
        return _BLOCKS[len(_BLOCKS) // 2] * len(values)
    span = high - low
    return "".join(
        _BLOCKS[min(len(_BLOCKS) - 1, int((v - low) / span * (len(_BLOCKS) - 1) + 0.5))]
        for v in values
    )


def pct_change(old: float, new: float) -> float:
    """Percentage move from old to new. Returns 0.0 when old is 0 or missing."""
    try:
        old, new = float(old), float(new)
    except (TypeError, ValueError):
        return 0.0
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def format_delta(previous: float | None, current: float | None,
                 previous_date: date | None = None) -> str:
    """
    One-line day-over-day move, e.g. "🔺 ₹120 (0.82%) vs 08 Sep".

    Returns "" when there is no comparison point — the caller can append it
    unconditionally.
    """
    if previous is None or current is None:
        return ""
    try:
        previous, current = float(previous), float(current)
    except (TypeError, ValueError):
        return ""

    diff = current - previous
    when = f" vs {previous_date:%d %b}" if previous_date else ""

    if abs(diff) < 0.005:
        return f"➡️ unchanged{when}"

    arrow = "🔺" if diff > 0 else "🔻"
    return f"{arrow} ₹{abs(diff):,.0f} ({abs(pct_change(previous, current)):.2f}%){when}"


def alert_fires(direction: str, threshold: float, price: float) -> bool:
    """Whether a price crosses an alert threshold."""
    try:
        threshold, price = float(threshold), float(price)
    except (TypeError, ValueError):
        return False
    if direction == "below":
        return price <= threshold
    if direction == "above":
        return price >= threshold
    return False


def find_contract(mcx: dict, metal: str) -> Optional[tuple[str, dict]]:
    """
    Locate a futures contract without hard-coding the month.

    Feed keys look like "gold_june_futures" and roll over; scanning means a
    roll cannot silently blank the MCX block.
    """
    for key, value in (mcx or {}).items():
        if not isinstance(value, dict):
            continue
        lowered = key.lower()
        if metal in lowered and "futures" in lowered:
            return key, value
    return None


def contract_label(key: str, metal: str) -> str:
    """"gold_december_futures" → "Gold Dec"."""
    parts = [p for p in key.lower().split("_") if p and p not in (metal, "futures")]
    month = parts[0][:3].title() if parts else ""
    return f"{metal.title()} {month}".strip()


def _num(value) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _parse_date(value) -> Optional[date]:
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def rows_from_payload(data: dict, rate_date: date) -> list[tuple]:
    """
    Flatten a rates payload into city_rates tuples.

    Pure, so the reshaping is testable without a database. per_gram is
    derived from per_10g when the feed omits it.
    """
    rows = []
    for city in data.get("cities", []) or []:
        if not isinstance(city, dict):
            continue
        name = (city.get("city") or "").strip()
        if not name:
            continue
        state = (city.get("state") or "").strip() or None

        for karat in (24, 22, 18):
            block = city.get(f"{karat}K")
            if not isinstance(block, dict):
                continue
            per_10g = _num(block.get("per_10g"))
            per_gram = _num(block.get("per_gram"))
            if per_gram is None and per_10g is not None:
                per_gram = per_10g / 10
            if per_gram is None:
                continue
            rows.append((rate_date, name, state, karat, per_gram, per_10g))
    return rows


def mcx_row_from_payload(mcx: dict, rate_date: date) -> Optional[tuple]:
    """Flatten the mcx_rates block into one mcx_rates row, or None."""
    gold = find_contract(mcx, "gold")
    if not gold:
        return None
    gold_key, gold_val = gold
    gold_price = _num(gold_val.get("price_per_10g"))
    if gold_price is None:
        return None

    silver_key = silver_price = silver_change = None
    silver = find_contract(mcx, "silver")
    if silver:
        silver_key, silver_val = silver
        silver_price = _num(silver_val.get("price_per_kg"))
        silver_change = _num(silver_val.get("change_percent")) or 0.0

    return (
        rate_date,
        contract_label(gold_key, "gold"),
        gold_price,
        _num(gold_val.get("change_percent")) or 0.0,
        contract_label(silver_key, "silver") if silver_key else None,
        silver_price,
        silver_change,
    )


# ── Ingest ────────────────────────────────────────────────────────────────────

_UPSERT_CITY = """
insert into city_rates (rate_date, city, state, karat, per_gram, per_10g)
values ($1, $2, $3, $4, $5, $6)
on conflict (rate_date, city, karat) do update
   set per_gram   = excluded.per_gram,
       per_10g    = excluded.per_10g,
       state      = coalesce(excluded.state, city_rates.state),
       fetched_at = now()
"""

_UPSERT_MCX = """
insert into mcx_rates (rate_date, gold_contract, gold_per_10g, gold_change_pct,
                       silver_contract, silver_per_kg, silver_change_pct)
values ($1, $2, $3, $4, $5, $6, $7)
on conflict (rate_date) do update
   set gold_contract     = excluded.gold_contract,
       gold_per_10g      = excluded.gold_per_10g,
       gold_change_pct   = excluded.gold_change_pct,
       silver_contract   = excluded.silver_contract,
       silver_per_kg     = excluded.silver_per_kg,
       silver_change_pct = excluded.silver_change_pct,
       fetched_at        = now()
"""


async def ingest(pool, data: dict, rate_date: date | None = None) -> int:
    """
    Upsert one rates payload. Returns the number of city rows written.

    Idempotent — safe to call on every refresh; re-ingesting the same day
    just refreshes the values.
    """
    if pool is None or not data:
        return 0

    rate_date = (
        rate_date
        or _parse_date((data.get("metadata") or {}).get("date"))
        or date.today()
    )

    rows = rows_from_payload(data, rate_date)
    if not rows:
        log.warning("Ingest: payload produced no usable rows")
        return 0

    mcx_row = mcx_row_from_payload(data.get("mcx_rates") or {}, rate_date)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(_UPSERT_CITY, rows)
            if mcx_row:
                await conn.execute(_UPSERT_MCX, *mcx_row)

    log.info("Ingested %d rate rows for %s", len(rows), rate_date)
    return len(rows)


# ── Reads ─────────────────────────────────────────────────────────────────────


async def get_series(pool, city: str, karat: int = DEFAULT_KARAT,
                     days: int = TREND_DAYS) -> list[dict]:
    """Daily prices for a city, oldest first."""
    if pool is None:
        return []
    rows = await pool.fetch(
        """
        select rate_date, per_gram
          from city_rates
         where lower(city) = lower($1) and karat = $2
           and rate_date > current_date - $3::int
         order by rate_date
        """,
        city, karat, days,
    )
    return [{"date": r["rate_date"], "price": float(r["per_gram"])} for r in rows]


async def get_change(pool, city: str, karat: int = DEFAULT_KARAT) -> Optional[dict]:
    """
    The two most recent observations for a city.

    Returns {"current", "previous", "previous_date"} or None when there is
    no prior day to compare against yet.
    """
    if pool is None:
        return None
    rows = await pool.fetch(
        """
        select rate_date, per_gram
          from city_rates
         where lower(city) = lower($1) and karat = $2
         order by rate_date desc
         limit 2
        """,
        city, karat,
    )
    if len(rows) < 2:
        return None
    return {
        "current": float(rows[0]["per_gram"]),
        "current_date": rows[0]["rate_date"],
        "previous": float(rows[1]["per_gram"]),
        "previous_date": rows[1]["rate_date"],
    }


async def get_stats(pool, city: str, karat: int = DEFAULT_KARAT,
                    days: int = TREND_DAYS) -> Optional[dict]:
    """High/low with their dates over the window."""
    if pool is None:
        return None
    row = await pool.fetchrow(
        """
        select min(per_gram) as low, max(per_gram) as high, count(*) as n
          from city_rates
         where lower(city) = lower($1) and karat = $2
           and rate_date > current_date - $3::int
        """,
        city, karat, days,
    )
    if not row or not row["n"]:
        return None

    dates = await pool.fetch(
        """
        select per_gram, rate_date from city_rates
         where lower(city) = lower($1) and karat = $2
           and rate_date > current_date - $3::int
           and per_gram in ($4, $5)
        """,
        city, karat, days, row["low"], row["high"],
    )
    low_date = high_date = None
    for r in dates:
        if float(r["per_gram"]) == float(row["low"]):
            low_date = r["rate_date"]
        if float(r["per_gram"]) == float(row["high"]):
            high_date = r["rate_date"]

    return {
        "low": float(row["low"]), "low_date": low_date,
        "high": float(row["high"]), "high_date": high_date,
        "points": row["n"],
    }


async def has_history(pool, days: int = 2) -> bool:
    """Whether enough history exists for comparisons to be meaningful."""
    if pool is None:
        return False
    n = await pool.fetchval(
        "select count(distinct rate_date) from city_rates "
        "where rate_date > current_date - $1::int",
        days + 1,
    )
    return bool(n and n >= 2)


# ── Alerts ────────────────────────────────────────────────────────────────────


async def set_alert(pool, user_id: int, city: str, karat: int,
                    direction: str, threshold: float) -> None:
    """One alert per user. Re-arms triggered_at so a new alert can fire."""
    await pool.execute(
        """
        insert into alerts (user_id, city, karat, direction, threshold,
                            created_at, triggered_at)
        values ($1, $2, $3, $4, $5, now(), null)
        on conflict (user_id) do update
           set city = excluded.city, karat = excluded.karat,
               direction = excluded.direction, threshold = excluded.threshold,
               created_at = now(), triggered_at = null
        """,
        user_id, city, karat, direction, threshold,
    )


async def get_alert(pool, user_id: int) -> Optional[dict]:
    if pool is None:
        return None
    row = await pool.fetchrow("select * from alerts where user_id = $1", user_id)
    if not row:
        return None
    return {
        "city": row["city"], "karat": row["karat"],
        "direction": row["direction"], "threshold": float(row["threshold"]),
        "triggered_at": row["triggered_at"],
    }


async def clear_alert(pool, user_id: int) -> bool:
    if pool is None:
        return False
    result = await pool.execute("delete from alerts where user_id = $1", user_id)
    return result.endswith(" 1")


async def due_alerts(pool) -> list[dict]:
    """
    Alerts whose threshold the latest price has crossed and which have not
    fired yet. The condition is evaluated in SQL so a single round trip
    covers every user.
    """
    if pool is None:
        return []
    rows = await pool.fetch(
        """
        with latest as (
            select distinct on (lower(city), karat)
                   lower(city) as city_key, karat, per_gram, rate_date
              from city_rates
             order by lower(city), karat, rate_date desc
        )
        select a.user_id, a.city, a.karat, a.direction, a.threshold,
               l.per_gram, l.rate_date
          from alerts a
          join latest l
            on l.city_key = lower(a.city) and l.karat = a.karat
         where a.triggered_at is null
           and ( (a.direction = 'below' and l.per_gram <= a.threshold)
              or (a.direction = 'above' and l.per_gram >= a.threshold) )
        """
    )
    return [
        {
            "user_id": r["user_id"], "city": r["city"], "karat": r["karat"],
            "direction": r["direction"], "threshold": float(r["threshold"]),
            "price": float(r["per_gram"]), "rate_date": r["rate_date"],
        }
        for r in rows
    ]


async def mark_triggered(pool, user_id: int) -> None:
    await pool.execute(
        "update alerts set triggered_at = now() where user_id = $1", user_id
    )
