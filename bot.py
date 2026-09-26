"""
Gold Rate Telegram Bot
======================
Serves 24K / 22K / 18K gold rates city-wise, fetched over HTTPS from the
JSON published by the scrape workflow (see .github/workflows/scrape.yml).

Commands
--------
/start        — Welcome message + how to use
/help         — Show all commands and usage
/gold         — Rates for the default city
/gold <city>  — Rates for a specific city
/cities       — Browse all available cities
/subscribe    — Register (free)
/unsubscribe  — Stop daily messages
/status       — Usage and subscription status
/cancel       — Abort an in-progress registration

Notes
-----
* All messages use parse_mode=HTML. Every value that originates from a user
  or from the rates feed is passed through html.escape() before it is
  interpolated — Markdown mode used to break outright on names like "*".
* Rate data is refreshed on a timer; it is never read once and cached forever.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import sys
from datetime import datetime

# Fix for Python 3.14+ on Windows
if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except DeprecationWarning:
        pass

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import pytz

# ── Setup ──────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

# Load .env BEFORE importing our own modules — some of them read settings at
# import time. Does not override variables already in the environment, so
# systemd's EnvironmentFile always wins on the server.
load_dotenv()

import rates_store  # noqa: E402
import storage as store_mod  # noqa: E402
from rates_fetcher import (  # noqa: E402
    configured_source, fetch_gold_data, max_age_days, snapshot_age_days,
)
from storage import REG_STEP_CITY  # noqa: E402

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
DEFAULT_CITY = os.environ.get("DEFAULT_CITY", "Chennai")
DAILY_HOUR = int(os.environ.get("DAILY_MESSAGE_HOUR", "10"))
DAILY_MINUTE = int(os.environ.get("DAILY_MESSAGE_MINUTE", "0"))
REFRESH_MINUTES = int(os.environ.get("RATES_REFRESH_MINUTES", "60"))
IST = pytz.timezone("Asia/Kolkata")

PAGE_SIZE = 24          # 8 rows of 3 buttons — comfortably inside Telegram's limits
COLUMNS = 3

# Set during startup.
STORE: store_mod.Storage = None  # type: ignore[assignment]


def esc(value) -> str:
    """Escape a value for parse_mode=HTML."""
    return html.escape(str(value), quote=False)


# ── Rates cache ────────────────────────────────────────────────────────────────


class Rates:
    """
    Holds the current rates snapshot.

    refresh() replaces the snapshot only when the fetch succeeds, so a
    transient network failure leaves the last good data in place instead of
    blanking the bot out.
    """

    def __init__(self) -> None:
        self.data: dict | None = None
        self.index: dict[str, dict] = {}
        self.meta: dict = {}
        self.mcx: dict = {}

    @property
    def age_days(self) -> int | None:
        return snapshot_age_days(self.data) if self.data else None

    @property
    def ready(self) -> bool:
        """
        Loaded AND fresh. A snapshot that was fine when fetched still ages in
        memory if the feed stops updating, so this is re-checked every time.
        """
        if not self.index:
            return False
        age = self.age_days
        return age is None or age <= max_age_days()

    @property
    def city_count(self) -> int:
        return len(self.index)

    def sorted_cities(self) -> list[dict]:
        return sorted(self.data.get("cities", []) if self.data else [],
                      key=lambda c: c.get("city", ""))

    async def refresh(self) -> bool:
        data = await fetch_gold_data()
        if data is None:
            log.warning("Rates refresh failed — keeping previous snapshot "
                        "(%d cities)", self.city_count)
            return False

        index = {
            c["city"].lower(): c
            for c in data.get("cities", [])
            if isinstance(c, dict) and c.get("city")
        }
        if not index:
            log.warning("Rates refresh produced no usable cities — keeping previous")
            return False

        self.data, self.index = data, index
        self.meta = data.get("metadata", {}) or {}
        self.mcx = data.get("mcx_rates", {}) or {}
        log.info("Rates refreshed: %d cities, date=%s",
                 len(index), self.meta.get("date", "unknown"))
        return True


RATES = Rates()


# ── Price history (Phase 4) ───────────────────────────────────────────────────


def history_pool():
    """
    The asyncpg pool when the Postgres backend is active, otherwise None.

    Every history feature checks this and degrades to an explanatory message
    rather than an error, so the JSON backend stays fully usable.
    """
    return getattr(STORE, "pool", None)


HISTORY_DISABLED = (
    "📈 <b>Price history isn't enabled.</b>\n\n"
    "This feature needs the Postgres backend. Rates still work normally — "
    "try /gold or /cities."
)

HISTORY_EMPTY = (
    "📈 <b>Not enough history yet.</b>\n\n"
    "I need at least two days of data before I can show a trend. "
    "Check back tomorrow!"
)


async def ingest_rates() -> None:
    """Persist the current snapshot into city_rates / mcx_rates."""
    pool = history_pool()
    if pool is None or not RATES.ready:
        return
    try:
        await rates_store.ingest(pool, RATES.data)
    except Exception as exc:
        # History is a nice-to-have; never let it take the bot down.
        log.error("Rate ingest failed: %s", exc)


async def fire_due_alerts(app: Application) -> None:
    """Notify users whose price threshold has been crossed, once each."""
    pool = history_pool()
    if pool is None:
        return
    try:
        due = await rates_store.due_alerts(pool)
    except Exception as exc:
        log.error("Alert check failed: %s", exc)
        return

    for alert in due:
        arrow = "🔻" if alert["direction"] == "below" else "🔺"
        text = (
            f"{arrow} <b>Price Alert — {esc(alert['city'])}</b>\n\n"
            f"{alert['karat']}K is now <b>{fmt_inr(alert['price'])}/g</b>, "
            f"{alert['direction']} your ₹{alert['threshold']:,.0f} threshold.\n"
            f"<i>As of {alert['rate_date']:%d %b %Y}</i>\n\n"
            "The alert has been cleared. Use /alert to set a new one."
        )
        try:
            await app.bot.send_message(chat_id=alert["user_id"], text=text,
                                       parse_mode="HTML")
            await rates_store.mark_triggered(pool, alert["user_id"])
            log.info("Alert fired for user %s (%s %s %s)", alert["user_id"],
                     alert["city"], alert["direction"], alert["threshold"])
        except Forbidden:
            await rates_store.mark_triggered(pool, alert["user_id"])
            await STORE.unsubscribe(alert["user_id"])
        except Exception as exc:
            log.warning("Could not deliver alert to %s: %s", alert["user_id"], exc)


async def delta_line(city_name: str) -> str:
    """Day-over-day move for a city, or "" when history isn't available."""
    pool = history_pool()
    if pool is None:
        return ""
    try:
        change = await rates_store.get_change(pool, city_name)
    except Exception as exc:
        log.warning("Delta lookup failed for %s: %s", city_name, exc)
        return ""
    if not change:
        return ""
    return rates_store.format_delta(
        change["previous"], change["current"], change["previous_date"]
    )


async def refresh_rates_job(app: Application) -> None:
    """Hourly: refresh the feed, persist it, then check alerts."""
    if not await RATES.refresh():
        return
    await ingest_rates()
    await fire_due_alerts(app)


# ── Formatters ────────────────────────────────────────────────────────────────


def fmt_inr(amount) -> str:
    """Format a number in the Indian grouping style: ₹1,58,360."""
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return "₹N/A"

    sign = "-" if amount < 0 else ""
    s = str(abs(amount))
    if len(s) <= 3:
        return f"{sign}₹{s}"
    last3, rest = s[-3:], s[:-3]
    groups = []
    while len(rest) > 2:
        groups.insert(0, rest[-2:])
        rest = rest[:-2]
    if rest:
        groups.insert(0, rest)
    return f"{sign}₹{','.join(groups)},{last3}"


def _karat_block(title: str, karat: dict) -> list[str]:
    """Render one purity block, skipping weights the feed doesn't carry."""
    if not karat:
        return []
    lines = [f"<b>{esc(title)}</b>"]
    for key, label in (("per_10g", "10g"), ("per_8g", " 8g"), ("per_gram", " 1g")):
        if karat.get(key):
            lines.append(f"  {label} → {fmt_inr(karat[key])}")
    lines.append("")
    return lines


def city_message(city_obj: dict, delta: str = "") -> str:
    """
    Build the rates message for one city.

    `delta` is the optional day-over-day line from the price history; it is
    always safe to pass "" when history isn't available.
    """
    city = esc(city_obj.get("city", "Unknown"))
    state = esc(city_obj.get("state", ""))
    date = esc(RATES.meta.get("date", "Today"))

    lines = [f"🥇 <b>Gold Rates — {city}</b>"]
    lines.append(f"📍 {state}  |  📅 {date}" if state else f"📅 {date}")
    age = RATES.age_days
    if age is not None and age >= 2:
        # Yesterday's rates before the morning upload are normal; older isn't.
        lines.append(f"⚠️ <i>Rates last updated {age} days ago</i>")
    if delta:
        lines.append(delta)
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    lines += _karat_block("24K (Pure Gold)", city_obj.get("24K", {}))
    lines += _karat_block("22K (Hallmark Jewellery)", city_obj.get("22K", {}))
    lines += _karat_block("18K (Light Jewellery)", city_obj.get("18K", {}))

    while lines and lines[-1] == "":
        lines.pop()

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("<i>Rates are indicative retail averages.</i>")
    lines.append("<i>Excludes 3% GST &amp; making charges.</i>")
    return "\n".join(lines)


def _change_arrow(change) -> tuple[str, str]:
    try:
        change = float(change)
    except (TypeError, ValueError):
        return "", ""
    arrow = "🟢 ▲" if change >= 0 else "🔴 ▼"
    return arrow, f"{abs(change)}%"


def mcx_message() -> str:
    """Build the MCX futures snippet. Empty string when the feed has none."""
    if not RATES.mcx:
        return ""

    gold = rates_store.find_contract(RATES.mcx, "gold")
    if gold is None:
        log.debug("No gold futures contract found in mcx_rates keys: %s",
                  list(RATES.mcx))
        return ""

    gold_key, gold_val = gold
    price = gold_val.get("price_per_10g")
    if price is None:
        return ""

    date = esc(RATES.mcx.get("date", ""))
    lines = [f"\n📊 <b>MCX Futures {date}</b>".rstrip()]

    arrow, pct = _change_arrow(gold_val.get("change_percent", 0))
    lines.append(
        f"{esc(rates_store.contract_label(gold_key, 'gold'))} → "
        f"{fmt_inr(price)}/10g  {arrow} {pct}".rstrip()
    )

    silver = rates_store.find_contract(RATES.mcx, "silver")
    if silver:
        silver_key, silver_val = silver
        s_price = silver_val.get("price_per_kg")
        if s_price is not None:
            s_arrow, s_pct = _change_arrow(silver_val.get("change_percent", 0))
            lines.append(
                f"{esc(rates_store.contract_label(silver_key, 'silver'))} → "
                f"{fmt_inr(s_price)}/kg  {s_arrow} {s_pct}".rstrip()
            )

    return "\n".join(lines)


# ── Prompts ───────────────────────────────────────────────────────────────────

LIMIT_PROMPT = (
    "🚫 <b>Daily limit reached!</b>\n\n"
    "You've used all your gold queries for today.\n"
    "Your limit resets at midnight IST.\n\n"
    "📬 You'll still get your <b>free daily gold rates</b> at 10 AM IST."
)

SUBSCRIBE_PROMPT = (
    "🔒 <b>Subscription Required</b>\n\n"
    "You need to subscribe before using the bot.\n"
    "It's completely <b>free</b> — just register with your details.\n\n"
    "👉 Use /subscribe to get started!"
)

NO_DATA = (
    "❌ <b>Gold rate data is not available right now.</b>\n"
    "Please try again in a few minutes."
)


# ── Guards ────────────────────────────────────────────────────────────────────


async def require_subscription(update: Update) -> bool:
    """Reply with the signup prompt and return False if the user isn't registered."""
    if await STORE.is_subscribed(update.effective_user.id):
        return True
    await update.effective_message.reply_text(SUBSCRIBE_PROMPT, parse_mode="HTML")
    return False


async def quota_available(update: Update) -> bool:
    """
    Check remaining quota WITHOUT consuming any.

    Consumption happens in charge_query(), only once a city has actually
    been resolved — a typo shouldn't cost the user a query.
    """
    if await STORE.can_query(update.effective_user.id):
        return True
    await update.effective_message.reply_text(LIMIT_PROMPT, parse_mode="HTML")
    return False


async def charge_query(update: Update) -> str:
    """Record one successful query. Returns a hint about the remaining balance."""
    user = update.effective_user
    await STORE.record_query(user.id, user.first_name or "", user.username or "")
    remaining = await STORE.daily_remaining(user.id)

    if remaining == 0:
        return "\n\n⚠️ <i>This was your last query for today!</i>"
    if remaining <= 3:
        return f"\n\nℹ️ <i>You have {remaining} queries remaining today.</i>"
    return ""


def resolve_city(name: str) -> tuple[dict | None, list[str]]:
    """
    Look up a city. Returns (exact_or_sole_match, candidate_keys).

    An exact hit wins; otherwise a unique prefix match wins; otherwise the
    caller gets the candidate list to disambiguate.
    """
    key = name.strip().lower()
    exact = RATES.index.get(key)
    if exact:
        return exact, []

    matches = sorted(c for c in RATES.index if c.startswith(key))
    if len(matches) == 1:
        return RATES.index[matches[0]], []
    return None, matches


# ── City keyboard ─────────────────────────────────────────────────────────────


def city_keyboard(page: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build one page of city buttons. Telegram caps keyboard size, so paginate."""
    cities = RATES.sorted_cities()
    pages = max(1, (len(cities) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, pages - 1))

    chunk = cities[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows, row = [], []
    for city in chunk:
        name = city.get("city", "")
        if not name:
            continue
        row.append(InlineKeyboardButton(name, callback_data=f"city:{name.lower()}"))
        if len(row) == COLUMNS:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Prev", callback_data=f"page:{page - 1}"))
    if pages > 1:
        nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ▶", callback_data=f"page:{page + 1}"))
    if nav:
        rows.append(nav)

    text = (
        f"🏙 <b>{len(cities)} Cities Available</b>\n\n"
        "Tap a city to see gold rates:"
    )
    return text, InlineKeyboardMarkup(rows)


# ── Handlers ──────────────────────────────────────────────────────────────────


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = esc(user.first_name or "there")
    total = RATES.city_count

    if await STORE.is_subscribed(user.id):
        info = await STORE.get_info(user.id)
        city = f" (📍 {esc(info['reg_city'])})" if info.get("reg_city") else ""
        msg = (
            f"👋 <b>Welcome back, {name}!</b>{city} 🥇\n\n"
            f"I have gold rates for <b>{total} cities</b> across India.\n\n"
            "<b>Quick Start:</b>\n"
            f"🏙 /gold — Rates for <b>{esc(DEFAULT_CITY)}</b>\n"
            "🔍 /gold &lt;city&gt; — Rates for any city\n"
            "   <i>Example:</i> /gold Mumbai\n\n"
            "📋 /cities — Browse all cities\n"
            "📊 /status — Check your subscription\n"
            "❓ /help — See all commands\n\n"
            f"🆓 <i>You get {store_mod.DAILY_QUERY_LIMIT} free queries per day!</i>"
        )
    else:
        msg = (
            "👋 <b>Welcome to Gold Rate Bot!</b> 🥇\n\n"
            f"Get daily gold rates for <b>{total} cities</b> across India.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🔒 <b>To use this bot, you need to subscribe first.</b>\n"
            "It's completely <b>free</b> — just provide your details!\n\n"
            "👉 Use /subscribe to register now."
        )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_subscription(update):
        return
    msg = (
        "❓ <b>Gold Rate Bot — Help</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>Commands:</b>\n\n"
        "🏠 /start — Welcome message\n"
        f"🥇 /gold — Rates for <b>{esc(DEFAULT_CITY)}</b>\n"
        "🔍 /gold &lt;city&gt; — Rates for a city\n"
        "📋 /cities — Browse all cities\n"
        "📈 /trend — 7-day price history\n"
        "🔔 /alert — Notify me at a price\n"
        "💳 /subscribe — Register / subscription status\n"
        "📊 /status — Subscription status\n"
        "🚫 /unsubscribe — Stop daily messages\n"
        "❌ /cancel — Cancel registration\n"
        "❓ /help — This help message\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 <b>Tips:</b>\n"
        "• Type a city name directly to look it up\n"
        f"• {store_mod.DAILY_QUERY_LIMIT} free queries per day "
        "(resets at midnight IST)\n"
        "• Browsing /cities is free — only rate lookups count\n"
        "• Subscribers get daily gold rates at 10 AM IST"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def send_city_rates(update: Update, city_obj: dict) -> None:
    """Charge one query and send the rates for a resolved city."""
    hint = await charge_query(update)
    delta = await delta_line(city_obj.get("city", ""))
    text = city_message(city_obj, delta) + mcx_message() + hint
    await update.effective_message.reply_text(text, parse_mode="HTML")


async def cmd_gold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_subscription(update):
        return
    if not RATES.ready:
        await update.message.reply_text(NO_DATA, parse_mode="HTML")
        return
    if not await quota_available(update):
        return

    city_name = " ".join(context.args).strip() if context.args else DEFAULT_CITY
    city_obj, matches = resolve_city(city_name)

    if city_obj:
        await send_city_rates(update, city_obj)
        return

    if matches:
        names = "\n".join(f"• {esc(RATES.index[m]['city'])}" for m in matches[:10])
        extra = f"\n<i>…and {len(matches) - 10} more</i>" if len(matches) > 10 else ""
        await update.message.reply_text(
            f"🔍 Found multiple matches for <b>{esc(city_name)}</b>:\n\n"
            f"{names}{extra}\n\nPlease use the full city name.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            f"❌ City <b>{esc(city_name)}</b> not found.\n\n"
            "Use /cities to browse all available cities.",
            parse_mode="HTML",
        )


async def cmd_cities(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Browsing the list is free — only an actual rate lookup costs a query."""
    if not await require_subscription(update):
        return
    if not RATES.ready:
        await update.message.reply_text(NO_DATA, parse_mode="HTML")
        return

    text, markup = city_keyboard(0)
    await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")


async def callback_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle a city button tap.

    Callback data is client-supplied, so this path enforces the same
    subscription and quota checks as the text commands.
    """
    query = update.callback_query
    user = update.effective_user

    if not await STORE.is_subscribed(user.id):
        await query.answer("Use /subscribe to register first.", show_alert=True)
        return
    if not await STORE.can_query(user.id):
        await query.answer("Daily limit reached. Resets at midnight IST.",
                           show_alert=True)
        return
    if not RATES.ready:
        await query.answer("Rate data unavailable.", show_alert=True)
        return

    await query.answer()

    city_obj = RATES.index.get(query.data.removeprefix("city:"))
    if not city_obj:
        await query.answer("City not found.", show_alert=True)
        return

    await STORE.record_query(user.id, user.first_name or "", user.username or "")
    remaining = await STORE.daily_remaining(user.id)
    hint = (f"\n\nℹ️ <i>{remaining} queries left today.</i>"
            if remaining <= 3 else "")

    back = InlineKeyboardMarkup([[
        InlineKeyboardButton("◀ Back to Cities", callback_data="page:0")
    ]])
    await query.edit_message_text(
        city_message(city_obj, await delta_line(city_obj.get("city", "")))
        + mcx_message() + hint,
        parse_mode="HTML",
        reply_markup=back,
    )


async def callback_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Paginate the city list (also serves the Back button)."""
    query = update.callback_query
    await query.answer()

    if not RATES.ready:
        await query.edit_message_text(NO_DATA, parse_mode="HTML")
        return

    try:
        page = int(query.data.removeprefix("page:"))
    except ValueError:
        page = 0

    text, markup = city_keyboard(page)
    await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")


async def callback_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The page-counter button is a label, not an action."""
    await update.callback_query.answer()


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """A bare city name looks up rates; during registration it's the answer."""
    user = update.effective_user

    if await STORE.get_reg_step(user.id) is not None:
        await handle_registration_input(update, context)
        return

    if not await require_subscription(update):
        return
    if not RATES.ready:
        await update.message.reply_text(NO_DATA, parse_mode="HTML")
        return
    if not await quota_available(update):
        return

    text = update.message.text.strip()
    city_obj, matches = resolve_city(text)

    if city_obj:
        await send_city_rates(update, city_obj)
    elif matches:
        names = "\n".join(f"• {esc(RATES.index[m]['city'])}" for m in matches[:8])
        await update.message.reply_text(
            f"🔍 Did you mean:\n{names}\n\nType the full city name or use /cities.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            f"💡 Try: /gold {esc(text)}\nor use /cities to browse all cities.",
            parse_mode="HTML",
        )


# ── Price history commands (Phase 4) ──────────────────────────────────────────


async def _preferred_city(update: Update, args: list[str]) -> str:
    """Explicit argument, else the user's registered city, else the default."""
    if args:
        return " ".join(args).strip()
    info = await STORE.get_info(update.effective_user.id)
    return info.get("reg_city") or DEFAULT_CITY


async def cmd_trend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """7-day price history for a city."""
    if not await require_subscription(update):
        return

    pool = history_pool()
    if pool is None:
        await update.message.reply_text(HISTORY_DISABLED, parse_mode="HTML")
        return
    if not RATES.ready:
        await update.message.reply_text(NO_DATA, parse_mode="HTML")
        return
    if not await quota_available(update):
        return

    requested = await _preferred_city(update, context.args)
    city_obj, matches = resolve_city(requested)
    if not city_obj:
        suggestion = (
            "\n\nDid you mean: "
            + ", ".join(esc(RATES.index[m]["city"]) for m in matches[:5])
            if matches else ""
        )
        await update.message.reply_text(
            f"❌ City <b>{esc(requested)}</b> not found.{suggestion}",
            parse_mode="HTML",
        )
        return

    city = city_obj["city"]
    karat = rates_store.DEFAULT_KARAT

    try:
        series = await rates_store.get_series(pool, city, karat)
        stats = await rates_store.get_stats(pool, city, karat)
    except Exception as exc:
        log.error("Trend query failed for %s: %s", city, exc)
        await update.message.reply_text(
            "⚠️ Couldn't read the price history. Try again later."
        )
        return

    if len(series) < 2:
        await update.message.reply_text(HISTORY_EMPTY, parse_mode="HTML")
        return

    hint = await charge_query(update)
    prices = [point["price"] for point in series]
    change = rates_store.format_delta(prices[0], prices[-1], series[0]["date"])

    lines = [
        f"📈 <b>Gold Trend — {esc(city)}</b>  <i>({karat}K per gram)</i>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"<code>{rates_store.sparkline(prices)}</code>",
        f"<i>{len(prices)} days</i>",
        "",
        f"Now    <b>{fmt_inr(prices[-1])}</b>/g",
    ]
    if change:
        lines.append(f"Change {change}")
    if stats:
        lines.append(
            f"High   {fmt_inr(stats['high'])}"
            + (f" <i>({stats['high_date']:%d %b})</i>" if stats["high_date"] else "")
        )
        lines.append(
            f"Low    {fmt_inr(stats['low'])}"
            + (f" <i>({stats['low_date']:%d %b})</i>" if stats["low_date"] else "")
        )
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("<i>Set a price alert with /alert</i>")

    await update.message.reply_text("\n".join(lines) + hint, parse_mode="HTML")


ALERT_USAGE = (
    "🔔 <b>Price Alerts</b>\n\n"
    "Get notified when gold crosses a price:\n\n"
    "<code>/alert below 14500</code> — when it drops to ₹14,500/g\n"
    "<code>/alert above 15200</code> — when it rises to ₹15,200/g\n"
    "<code>/alert off</code> — cancel your alert\n"
    "<code>/alert</code> — show your current alert\n\n"
    "<i>Alerts use 22K in your registered city and fire once.</i>"
)


async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set, show or clear a price alert."""
    if not await require_subscription(update):
        return

    pool = history_pool()
    if pool is None:
        await update.message.reply_text(HISTORY_DISABLED, parse_mode="HTML")
        return

    user_id = update.effective_user.id
    args = [a.lower() for a in (context.args or [])]

    # No arguments — show the current alert.
    if not args:
        existing = await rates_store.get_alert(pool, user_id)
        if not existing:
            await update.message.reply_text(ALERT_USAGE, parse_mode="HTML")
            return
        state = "✅ armed" if not existing["triggered_at"] else "☑️ already fired"
        await update.message.reply_text(
            f"🔔 <b>Your alert</b>\n\n"
            f"{esc(existing['city'])} {existing['karat']}K "
            f"<b>{existing['direction']} ₹{existing['threshold']:,.0f}</b>/g\n"
            f"Status: {state}\n\n"
            "<code>/alert off</code> to cancel.",
            parse_mode="HTML",
        )
        return

    if args[0] in ("off", "clear", "cancel", "remove"):
        removed = await rates_store.clear_alert(pool, user_id)
        await update.message.reply_text(
            "🔕 <b>Alert cleared.</b>" if removed else "ℹ️ You had no alert set.",
            parse_mode="HTML",
        )
        return

    if len(args) < 2 or args[0] not in ("below", "above"):
        await update.message.reply_text(ALERT_USAGE, parse_mode="HTML")
        return

    direction = args[0]
    raw = args[1].replace(",", "").replace("₹", "").strip()
    try:
        threshold = float(raw)
    except ValueError:
        await update.message.reply_text(
            f"❌ <b>{esc(args[1])}</b> isn't a number.\n\n" + ALERT_USAGE,
            parse_mode="HTML",
        )
        return
    if threshold <= 0:
        await update.message.reply_text("❌ The price must be greater than zero.")
        return

    requested = await _preferred_city(update, [])
    city_obj, _ = resolve_city(requested)
    if not city_obj:
        await update.message.reply_text(
            f"❌ I don't have rates for <b>{esc(requested)}</b>.\n"
            "Use /cities to see which cities are covered.",
            parse_mode="HTML",
        )
        return

    city = city_obj["city"]
    karat = rates_store.DEFAULT_KARAT

    try:
        await rates_store.set_alert(pool, user_id, city, karat, direction, threshold)
    except Exception as exc:
        log.error("Could not set alert for %s: %s", user_id, exc)
        await update.message.reply_text("⚠️ Couldn't save that alert. Try again later.")
        return

    current = (city_obj.get(f"{karat}K") or {}).get("per_gram")
    now_line = f"\nCurrent: <b>{fmt_inr(current)}</b>/g" if current else ""
    await update.message.reply_text(
        f"🔔 <b>Alert set!</b>\n\n"
        f"I'll message you when {esc(city)} {karat}K goes "
        f"<b>{direction} ₹{threshold:,.0f}</b>/g.{now_line}\n\n"
        "<i>It fires once, then clears. Use /alert off to cancel.</i>",
        parse_mode="HTML",
    )


# ── Subscription ──────────────────────────────────────────────────────────────


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if await STORE.is_subscribed(user.id):
        info = await STORE.get_info(user.id)
        await update.message.reply_text(
            "✅ <b>You're already registered!</b>\n\n"
            f"🏙 City: {esc(info.get('reg_city') or 'Not provided')}\n\n"
            "You'll get daily gold rates at 10 AM IST.\n"
            "Use /unsubscribe to stop.",
            parse_mode="HTML",
        )
        return

    await STORE.set_reg_step(user.id, REG_STEP_CITY,
                             user.first_name or "", user.username or "")
    await update.message.reply_text(
        "📋 <b>Registration</b>\n\n"
        "Please enter your <b>city</b> to complete registration:\n"
        "<i>(e.g. Chennai, Mumbai, Delhi)</i>",
        parse_mode="HTML",
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await STORE.is_subscribed(user.id):
        await update.message.reply_text(
            "ℹ️ You're not subscribed.\n"
            "Use /subscribe to get daily gold rates at 10 AM IST.",
            parse_mode="HTML",
        )
        return

    await STORE.unsubscribe(user.id)
    await update.message.reply_text(
        "👋 <b>Unsubscribed.</b>\n\n"
        "You won't receive daily messages anymore.\n"
        "Use /subscribe to re-enable anytime.",
        parse_mode="HTML",
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    info = await STORE.get_info(update.effective_user.id)

    if not info["is_subscribed"]:
        await update.message.reply_text(
            "❌ <b>You are not registered yet.</b>\n\n"
            "Use /subscribe to register for free!",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "📊 <b>Your Status</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏙 City: {esc(info.get('reg_city') or 'Not provided')}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📬 Daily updates: ✅ Active (10 AM IST)\n\n"
        f"🔍 Today's queries: <b>{info['daily_used']}/{info['daily_limit']}</b>\n"
        f"📊 Remaining today: <b>{info['daily_remaining']}</b>\n"
        f"📈 Total queries: {info['total_queries']}\n",
        parse_mode="HTML",
    )


async def handle_registration_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip()

    if await STORE.get_reg_step(user.id) != REG_STEP_CITY:
        return

    await STORE.set_reg_city(user.id, text)
    await STORE.complete_registration(user.id)
    await update.message.reply_text(
        "🎉 <b>Registration Complete!</b>\n\n"
        f"🏙 City: {esc(text)}\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✅ You can now use the bot!\n"
        "🥇 Try /gold to get current gold rates.\n"
        f"📊 You get <b>{store_mod.DAILY_QUERY_LIMIT} free queries per day</b>.",
        parse_mode="HTML",
    )
    log.info("User %s completed registration (city=%s)", user.id, text)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if await STORE.get_reg_step(user.id) is not None:
        await STORE.cancel_registration(user.id)
        await update.message.reply_text(
            "❌ <b>Registration cancelled.</b>\n\n"
            "Use /subscribe to start again anytime.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            "ℹ️ Nothing to cancel.\nUse /subscribe to register.",
            parse_mode="HTML",
        )


# ── Daily broadcast ───────────────────────────────────────────────────────────


async def weekly_section(city_name: str) -> str:
    """A 7-day sparkline summary, appended to the Sunday broadcast."""
    pool = history_pool()
    if pool is None:
        return ""
    try:
        series = await rates_store.get_series(pool, city_name)
    except Exception as exc:
        log.warning("Weekly digest query failed: %s", exc)
        return ""
    if len(series) < 3:
        return ""

    prices = [point["price"] for point in series]
    change = rates_store.pct_change(prices[0], prices[-1])
    arrow = "🔺" if change > 0 else ("🔻" if change < 0 else "➡️")
    return (
        "\n\n📅 <b>This week</b>\n"
        f"<code>{rates_store.sparkline(prices)}</code>\n"
        f"{arrow} {abs(change):.2f}% over {len(prices)} days"
    )


async def send_daily_update(app: Application):
    """Refresh the rates, persist them, then broadcast to every subscriber."""
    await RATES.refresh()
    await ingest_rates()

    subscribers = await STORE.get_all_subscribers()
    if not subscribers:
        log.info("Daily update: no subscribers, skipping")
        return
    if not RATES.ready:
        log.warning("Daily update: no rate data available, skipping")
        return

    city_obj = RATES.index.get(DEFAULT_CITY.lower())
    if not city_obj:
        log.warning("Daily update: default city '%s' not in feed", DEFAULT_CITY)
        return

    weekly = await weekly_section(city_obj["city"]) if (
        datetime.now(IST).weekday() == 6      # Sunday
    ) else ""

    text = (
        "🌅 <b>Good Morning! Daily Gold Update</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        + city_message(city_obj, await delta_line(city_obj["city"]))
        + mcx_message()
        + weekly
        + "\n\n<i>Use /gold &lt;city&gt; for other cities.</i>"
    )

    sent = failed = removed = 0
    for chat_id in subscribers:
        try:
            await app.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
            sent += 1
        except Forbidden:
            # User blocked the bot or deleted the chat. Without this they'd be
            # retried at 10 AM forever.
            await STORE.unsubscribe(chat_id)
            removed += 1
            log.info("Unsubscribed %s — bot is blocked", chat_id)
        except Exception as exc:
            failed += 1
            log.warning("Daily update to %s failed: %s", chat_id, exc)
        await asyncio.sleep(0.05)      # stay under Telegram's broadcast rate limit

    log.info("Daily update: %d sent, %d failed, %d unsubscribed",
             sent, failed, removed)


# ── Errors ────────────────────────────────────────────────────────────────────


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled exception: %s", context.error, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong. Please try again later."
            )
        except Exception:
            pass


# ── Main ──────────────────────────────────────────────────────────────────────


async def main_async():
    global STORE

    log.info("Starting Gold Rate Bot...")

    STORE = store_mod.build_storage()
    await STORE.start()

    if await RATES.refresh():
        await ingest_rates()
    else:
        log.warning("Initial rate fetch failed — the bot will start and keep "
                    "retrying every %d minute(s).", REFRESH_MINUTES)

    if history_pool() is not None:
        log.info("Price history enabled — /trend and /alert are active")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("gold", cmd_gold))
    app.add_handler(CommandHandler("cities", cmd_cities))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("trend", cmd_trend))
    app.add_handler(CommandHandler("alert", cmd_alert))

    app.add_handler(CallbackQueryHandler(callback_city, pattern=r"^city:"))
    app.add_handler(CallbackQueryHandler(callback_page, pattern=r"^page:"))
    app.add_handler(CallbackQueryHandler(callback_noop, pattern=r"^noop$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_error_handler(error_handler)

    await app.initialize()
    await app.start()

    scheduler = AsyncIOScheduler(timezone=IST)
    scheduler.add_job(
        refresh_rates_job,
        trigger=IntervalTrigger(minutes=REFRESH_MINUTES),
        args=[app],
        id="refresh_rates",
        name="Refresh gold rates",
        replace_existing=True,
    )
    scheduler.add_job(
        send_daily_update,
        trigger=CronTrigger(hour=DAILY_HOUR, minute=DAILY_MINUTE, timezone=IST),
        args=[app],
        id="daily_gold_update",
        name="Daily Gold Rate Update",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler started — refresh every %dm, broadcast at %02d:%02d IST",
             REFRESH_MINUTES, DAILY_HOUR, DAILY_MINUTE)

    await app.updater.start_polling(
        allowed_updates=Update.ALL_TYPES, drop_pending_updates=True
    )
    log.info("Bot is running. Press Ctrl+C to stop.")

    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        log.info("Shutting down...")
        scheduler.shutdown(wait=False)
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        await STORE.close()


def main():
    if not BOT_TOKEN:
        log.critical("TELEGRAM_BOT_TOKEN is not set.")
        sys.exit(1)
    if configured_source() is None:
        log.critical("No rates source configured — set RATES_URL (GitHub feed) "
                     "or GDRIVE_FOLDER_ID (Google Drive). See .env.example.")
        sys.exit(1)

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("Bot stopped.")


if __name__ == "__main__":
    main()
