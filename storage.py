"""
storage.py — user, registration and quota persistence.

Two interchangeable backends behind one async interface:

    JsonStorage      — subscribers.json, written atomically (tmp file + rename).
                       Default. No database required.
    PostgresStorage  — asyncpg pool. Used when DATABASE_URL is set.

Pick one with the environment, not with code:

    unset DATABASE_URL   → JSON
    set   DATABASE_URL   → Postgres

Both enforce the daily quota on an **IST** day boundary, so the reset time
matches what /help promises users regardless of the server's timezone.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

# India has no DST, so a fixed offset is exact and avoids a tzdata dependency.
IST = timezone(timedelta(hours=5, minutes=30))

DAILY_QUERY_LIMIT = int(os.environ.get("DAILY_QUERY_LIMIT", "50"))

# Registration steps
REG_STEP_NONE = None
REG_STEP_CITY = "city"

SUBSCRIBERS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "subscribers.json"
)


def ist_today() -> str:
    """Today's date in IST, as an ISO string."""
    return datetime.now(IST).date().isoformat()


# ── Interface ─────────────────────────────────────────────────────────────────


class Storage:
    """Common interface. Both backends implement every method."""

    async def start(self) -> None: ...
    async def close(self) -> None: ...

    async def get_reg_step(self, user_id: int) -> Optional[str]: ...
    async def set_reg_step(self, user_id: int, step: Optional[str],
                           first_name: str = "", username: str = "") -> None: ...
    async def set_reg_city(self, user_id: int, city: str) -> None: ...
    async def complete_registration(self, user_id: int) -> None: ...
    async def cancel_registration(self, user_id: int) -> None: ...

    async def is_subscribed(self, user_id: int) -> bool: ...
    async def unsubscribe(self, user_id: int) -> None: ...

    async def daily_used(self, user_id: int) -> int: ...
    async def record_query(self, user_id: int, first_name: str = "",
                           username: str = "") -> None: ...
    async def get_info(self, user_id: int) -> dict: ...
    async def get_all_subscribers(self) -> list[int]: ...

    async def daily_remaining(self, user_id: int) -> int:
        return max(0, DAILY_QUERY_LIMIT - await self.daily_used(user_id))

    async def can_query(self, user_id: int) -> bool:
        return await self.daily_used(user_id) < DAILY_QUERY_LIMIT


# ── JSON backend ──────────────────────────────────────────────────────────────


class JsonStorage(Storage):
    """
    File-backed store, compatible with the existing subscribers.json format.

    Writes go through a temp file + os.replace, so an interrupted write can
    never truncate the real file — the previous version survives intact.
    """

    def __init__(self, path: str = SUBSCRIBERS_FILE):
        self.path = path
        self._users: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._users = await asyncio.to_thread(self._read)
        log.info("JSON storage: loaded %d user(s) from %s",
                 len(self._users), self.path)

    async def close(self) -> None:
        await self._flush()

    # -- disk --

    def _read(self) -> dict:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            log.error("Could not read %s (%s) — starting empty. "
                      "The file was NOT overwritten; inspect it before restarting.",
                      self.path, exc)
            return {}

    def _write(self, users: dict) -> None:
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".subscribers-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(users, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)      # atomic on POSIX and Windows
        except OSError as exc:
            log.error("Failed to save users: %s", exc)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    async def _flush(self) -> None:
        await asyncio.to_thread(self._write, dict(self._users))

    # -- helpers --

    def _ensure(self, user_id: int, first_name: str = "", username: str = "") -> dict:
        uid = str(user_id)
        user = self._users.get(uid)
        if user is None:
            user = {
                "subscribed": False,
                "daily_count": 0,
                "last_query_date": None,
                "total_queries": 0,
                "first_name": first_name,
                "username": username,
                "created": datetime.now(IST).isoformat(),
                "reg_step": REG_STEP_NONE,
                "reg_city": None,
            }
            self._users[uid] = user
            log.info("New user: %s (%s)", first_name or uid, username or "no username")
        else:
            if first_name:
                user["first_name"] = first_name
            if username:
                user["username"] = username
            user.setdefault("reg_step", REG_STEP_NONE)
            user.setdefault("reg_city", None)
        return user

    @staticmethod
    def _roll_day(user: dict) -> None:
        today = ist_today()
        if user.get("last_query_date") != today:
            user["daily_count"] = 0
            user["last_query_date"] = today

    # -- registration --

    async def get_reg_step(self, user_id: int) -> Optional[str]:
        user = self._users.get(str(user_id))
        return user.get("reg_step") if user else None

    async def set_reg_step(self, user_id, step, first_name="", username="") -> None:
        async with self._lock:
            self._ensure(user_id, first_name, username)["reg_step"] = step
            await self._flush()

    async def set_reg_city(self, user_id: int, city: str) -> None:
        async with self._lock:
            self._ensure(user_id)["reg_city"] = city
            await self._flush()

    async def complete_registration(self, user_id: int) -> None:
        async with self._lock:
            user = self._ensure(user_id)
            user["subscribed"] = True
            user["reg_step"] = REG_STEP_NONE
            await self._flush()

    async def cancel_registration(self, user_id: int) -> None:
        async with self._lock:
            self._ensure(user_id)["reg_step"] = REG_STEP_NONE
            await self._flush()

    # -- subscription --

    async def is_subscribed(self, user_id: int) -> bool:
        user = self._users.get(str(user_id))
        return bool(user and user.get("subscribed"))

    async def unsubscribe(self, user_id: int) -> None:
        async with self._lock:
            if str(user_id) in self._users:
                self._users[str(user_id)]["subscribed"] = False
                await self._flush()

    # -- quota --

    async def daily_used(self, user_id: int) -> int:
        user = self._users.get(str(user_id))
        if not user:
            return 0
        self._roll_day(user)
        return user.get("daily_count", 0)

    async def record_query(self, user_id, first_name="", username="") -> None:
        async with self._lock:
            user = self._ensure(user_id, first_name, username)
            self._roll_day(user)
            user["daily_count"] = user.get("daily_count", 0) + 1
            user["total_queries"] = user.get("total_queries", 0) + 1
            await self._flush()

    async def get_info(self, user_id: int) -> dict:
        user = self._users.get(str(user_id))
        if user:
            self._roll_day(user)
        used = user.get("daily_count", 0) if user else 0
        return {
            "daily_used": used,
            "daily_remaining": max(0, DAILY_QUERY_LIMIT - used),
            "daily_limit": DAILY_QUERY_LIMIT,
            "total_queries": user.get("total_queries", 0) if user else 0,
            "is_subscribed": bool(user and user.get("subscribed")),
            "reg_city": user.get("reg_city") if user else None,
        }

    async def get_all_subscribers(self) -> list[int]:
        return [int(uid) for uid, u in self._users.items() if u.get("subscribed")]


# ── Postgres backend ──────────────────────────────────────────────────────────

# Start of the current IST day, as a timestamptz. Used for quota windows.
_IST_DAY_START = (
    "(date_trunc('day', now() at time zone 'Asia/Kolkata') at time zone 'Asia/Kolkata')"
)


class PostgresStorage(Storage):
    """asyncpg-backed store. Requires schema.sql to have been applied."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def start(self) -> None:
        import asyncpg  # imported lazily so JSON users need not install it

        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5)
        async with self.pool.acquire() as conn:
            n = await conn.fetchval("select count(*) from users")
        log.info("Postgres storage: connected, %d user(s)", n)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def _ensure(self, user_id: int, first_name: str = "", username: str = "") -> None:
        await self.pool.execute(
            """
            insert into users (user_id, first_name, username)
            values ($1, nullif($2,''), nullif($3,''))
            on conflict (user_id) do update
              set first_name = coalesce(nullif($2,''), users.first_name),
                  username   = coalesce(nullif($3,''), users.username)
            """,
            user_id, first_name, username,
        )

    # -- registration --

    async def get_reg_step(self, user_id: int) -> Optional[str]:
        return await self.pool.fetchval(
            "select reg_step from users where user_id = $1", user_id
        )

    async def set_reg_step(self, user_id, step, first_name="", username="") -> None:
        await self._ensure(user_id, first_name, username)
        await self.pool.execute(
            "update users set reg_step = $2 where user_id = $1", user_id, step
        )

    async def set_reg_city(self, user_id: int, city: str) -> None:
        await self._ensure(user_id)
        await self.pool.execute(
            "update users set reg_city = $2 where user_id = $1", user_id, city
        )

    async def complete_registration(self, user_id: int) -> None:
        await self.pool.execute(
            "update users set subscribed = true, reg_step = null where user_id = $1",
            user_id,
        )

    async def cancel_registration(self, user_id: int) -> None:
        await self.pool.execute(
            "update users set reg_step = null where user_id = $1", user_id
        )

    # -- subscription --

    async def is_subscribed(self, user_id: int) -> bool:
        return bool(await self.pool.fetchval(
            "select subscribed from users where user_id = $1", user_id
        ))

    async def unsubscribe(self, user_id: int) -> None:
        await self.pool.execute(
            "update users set subscribed = false where user_id = $1", user_id
        )

    # -- quota --

    async def daily_used(self, user_id: int) -> int:
        return await self.pool.fetchval(
            f"select count(*) from query_log "
            f"where user_id = $1 and queried_at >= {_IST_DAY_START}",
            user_id,
        ) or 0

    async def record_query(self, user_id, first_name="", username="") -> None:
        await self._ensure(user_id, first_name, username)
        await self.pool.execute(
            "insert into query_log (user_id) values ($1)", user_id
        )

    async def get_info(self, user_id: int) -> dict:
        row = await self.pool.fetchrow(
            f"""
            select u.subscribed, u.reg_city,
                   (select count(*) from query_log q
                     where q.user_id = u.user_id
                       and q.queried_at >= {_IST_DAY_START})  as daily_used,
                   (select count(*) from query_log q
                     where q.user_id = u.user_id)             as total_queries
              from users u where u.user_id = $1
            """,
            user_id,
        )
        if row is None:
            return {
                "daily_used": 0, "daily_remaining": DAILY_QUERY_LIMIT,
                "daily_limit": DAILY_QUERY_LIMIT, "total_queries": 0,
                "is_subscribed": False, "reg_city": None,
            }
        used = row["daily_used"]
        return {
            "daily_used": used,
            "daily_remaining": max(0, DAILY_QUERY_LIMIT - used),
            "daily_limit": DAILY_QUERY_LIMIT,
            "total_queries": row["total_queries"],
            "is_subscribed": row["subscribed"],
            "reg_city": row["reg_city"],
        }

    async def get_all_subscribers(self) -> list[int]:
        rows = await self.pool.fetch("select user_id from users where subscribed")
        return [r["user_id"] for r in rows]


# ── Factory ───────────────────────────────────────────────────────────────────


def build_storage() -> Storage:
    """Choose a backend from the environment. Call .start() before use."""
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if dsn:
        return PostgresStorage(dsn)
    return JsonStorage()
