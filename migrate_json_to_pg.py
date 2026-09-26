"""
migrate_json_to_pg.py — one-shot import of subscribers.json into Postgres.

    export DATABASE_URL='postgresql:///goldbot?host=/var/run/postgresql'
    python migrate_json_to_pg.py            # dry run, prints what it would do
    python migrate_json_to_pg.py --commit   # actually write

Idempotent: existing users are left alone (on conflict do nothing), and it
refuses to run twice against the same rows. Keep subscribers.json afterwards
as a backup — nothing deletes it.

Query history: the JSON only stores counts, not timestamps, so total_queries
is reconstructed as backdated rows at the user's signup time. Any queries
already made *today* are inserted at now(), so migrating does not silently
hand everyone a fresh daily quota.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime

from storage import IST, SUBSCRIBERS_FILE, ist_today


async def migrate(commit: bool) -> None:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        sys.exit("DATABASE_URL is not set.")

    if not os.path.exists(SUBSCRIBERS_FILE):
        sys.exit(f"{SUBSCRIBERS_FILE} not found — nothing to migrate.")

    with open(SUBSCRIBERS_FILE, encoding="utf-8") as f:
        users = json.load(f)

    import asyncpg
    conn = await asyncpg.connect(dsn=dsn)
    today = ist_today()

    try:
        migrated = skipped = logged = 0

        for uid, u in users.items():
            user_id = int(uid)

            exists = await conn.fetchval(
                "select 1 from users where user_id = $1", user_id
            )
            if exists:
                print(f"  skip  {user_id} ({u.get('first_name') or '?'}) — already present")
                skipped += 1
                continue

            created = u.get("created")
            created_at = (
                datetime.fromisoformat(created).replace(tzinfo=IST)
                if created else datetime.now(IST)
            )

            total = int(u.get("total_queries") or 0)
            today_count = (
                int(u.get("daily_count") or 0)
                if u.get("last_query_date") == today else 0
            )
            historical = max(0, total - today_count)

            print(f"  add   {user_id} ({u.get('first_name') or '?'}) "
                  f"city={u.get('reg_city') or '-'} "
                  f"subscribed={bool(u.get('subscribed'))} "
                  f"queries={total} (today={today_count})")

            if not commit:
                migrated += 1
                continue

            async with conn.transaction():
                await conn.execute(
                    """
                    insert into users
                        (user_id, first_name, username, reg_city, reg_step,
                         subscribed, created_at)
                    values ($1, nullif($2,''), nullif($3,''), $4, $5, $6, $7)
                    on conflict (user_id) do nothing
                    """,
                    user_id,
                    u.get("first_name") or "",
                    u.get("username") or "",
                    u.get("reg_city"),
                    u.get("reg_step"),
                    bool(u.get("subscribed")),
                    created_at,
                )
                if historical:
                    await conn.executemany(
                        "insert into query_log (user_id, queried_at) values ($1,$2)",
                        [(user_id, created_at)] * historical,
                    )
                if today_count:
                    now = datetime.now(IST)
                    await conn.executemany(
                        "insert into query_log (user_id, queried_at) values ($1,$2)",
                        [(user_id, now)] * today_count,
                    )
                logged += historical + today_count

            migrated += 1

        verb = "would migrate" if not commit else "migrated"
        print(f"\n{verb} {migrated} user(s), skipped {skipped}, "
              f"{logged} query_log row(s).")
        if not commit:
            print("Dry run — re-run with --commit to write.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate(commit="--commit" in sys.argv))
