"""
ingest.py — load rate snapshots into Postgres.

    # ingest the currently published feed
    python ingest.py

    # backfill from local files (your old date-stamped Drive exports work here)
    python ingest.py data/history/*.json
    python ingest.py gold_rates_india_2026-05-22.json

The bot already ingests on every refresh, so this is for backfilling history
and for one-off repairs. Upserts are idempotent — re-running is harmless.

The snapshot date comes from metadata.date; if that's missing, it is taken
from a YYYY-MM-DD in the filename.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from datetime import date, datetime

import rates_store

DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")


def date_for(path: str, payload: dict) -> date | None:
    """metadata.date wins; otherwise fall back to a date in the filename."""
    meta_date = (payload.get("metadata") or {}).get("date")
    try:
        return datetime.fromisoformat(str(meta_date)).date()
    except (TypeError, ValueError):
        pass

    match = DATE_IN_NAME.search(os.path.basename(path))
    if match:
        try:
            return datetime.fromisoformat(match.group(1)).date()
        except ValueError:
            pass
    return None


async def run(paths: list[str]) -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        sys.exit("DATABASE_URL is not set — Postgres is required for ingest.")

    import asyncpg
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    failures = 0

    try:
        if not paths:
            from rates_fetcher import fetch_gold_data

            data = await fetch_gold_data()
            if data is None:
                sys.exit("Could not fetch the live feed — nothing ingested.")
            written = await rates_store.ingest(pool, data)
            print(f"live feed: {written} rows")
            return 0

        for path in paths:
            try:
                with open(path, encoding="utf-8") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError) as exc:
                print(f"  SKIP {path}: {exc}", file=sys.stderr)
                failures += 1
                continue

            snapshot_date = date_for(path, payload)
            if snapshot_date is None:
                print(f"  SKIP {path}: no date in metadata or filename",
                      file=sys.stderr)
                failures += 1
                continue

            written = await rates_store.ingest(pool, payload, snapshot_date)
            print(f"  {snapshot_date}  {written:4d} rows  {os.path.basename(path)}")

    finally:
        await pool.close()

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(sys.argv[1:])))
