# Gold Rate Telegram Bot

Serves India's city-wise gold rates (24K / 22K / 18K) over Telegram.

```
your scraper ──> Google Drive  (gold_rates_india_YYYY-MM-DD.json)
                     └─> GitHub Action, hourly 08:00–14:00 IST
                         validate → commit data/latest.json + data/history/
                              └─> bot reads RATES_URL, refreshes hourly
                                       └─> Telegram users
```

The bot can also read Drive directly (`GDRIVE_*` settings, no `RATES_URL`) —
useful before the GitHub side is set up.

## Layout

```
gold_bot/
├── bot.py                       Handlers, formatters, scheduler
├── rates_fetcher.py             Fetch + validate the rates JSON over HTTPS
├── storage.py                   Users, registration, quota (JSON or Postgres)
├── rates_store.py               Price history, trends, alerts (Postgres)
├── ingest.py                    Load snapshots / backfill history
├── migrate_json_to_pg.py        One-shot subscribers.json → Postgres import
├── schema.sql                   Postgres schema
├── .env.example                 Config template — copy to .env
├── deploy/
│   ├── DEPLOY.md                Ubuntu 24/7 runbook (start here to deploy)
│   ├── goldbot.service          systemd unit for the bot
│   └── goldbackup.{service,timer}   Nightly pg_dump
├── scraper/
│   ├── fetch_source.py          Pulls the newest rates file from Drive
│   ├── validate.py              Blocks bad data from being published
│   └── requirements.txt         Workflow-only dependencies
├── data/                        Written by the workflow — don't edit by hand
│   ├── latest.json
│   └── history/YYYY-MM-DD.json
└── .github/workflows/scrape.yml Hourly: Drive → validate → commit
```

## Quick start (local)

```bash
pip install -r requirements.txt
cp .env.example .env      # then fill in TELEGRAM_BOT_TOKEN and one rates source
python bot.py
```

For the server, follow [deploy/DEPLOY.md](deploy/DEPLOY.md).

## Configuration

Set these in `.env` locally, or `/etc/goldbot/goldbot.env` under systemd.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `TELEGRAM_BOT_TOKEN` | yes | — | Token from @BotFather |
| `RATES_URL` | one source | — | URL of the published `latest.json` |
| `RATES_TOKEN` | no | — | Fine-grained read-only PAT, for a private repo |
| `GDRIVE_FOLDER_ID` | one source | — | Read Drive directly instead |
| `GDRIVE_CREDENTIALS_FILE` | with Drive | `credentials.json` | Service-account key |
| `GDRIVE_FILE_BASE` | no | `gold_rates_india` | Drive filename prefix |
| `RATES_SOURCE` | no | inferred | Force `url` or `drive` |
| `RATES_MAX_AGE_DAYS` | no | `3` | Older rates are withheld, not shown as current |
| `RATES_REFRESH_MINUTES` | no | `60` | How often to re-fetch the feed |
| `DATABASE_URL` | no | — | Unset ⇒ JSON file. Set ⇒ Postgres |
| `DEFAULT_CITY` | no | `Chennai` | City used by a bare `/gold` |
| `DAILY_QUERY_LIMIT` | no | `50` | Rate lookups per user per IST day |
| `DAILY_MESSAGE_HOUR` / `_MINUTE` | no | `10` / `0` | Broadcast time, IST |

## Commands

| Command | Description |
|---|---|
| `/start` | Welcome message + usage guide |
| `/help` | Full command reference |
| `/gold` | Rates for the default city |
| `/gold Mumbai` | Rates for a specific city |
| `/cities` | Browse all cities as tap buttons (paginated) |
| `/trend` | 7-day sparkline, change, high/low † |
| `/trend Mumbai` | Trend for a specific city † |
| `/alert below 14500` | Notify me when 22K drops to ₹14,500/g † |
| `/alert above 15200` | Notify me when it rises to ₹15,200/g † |
| `/alert` / `/alert off` | Show or cancel your alert † |
| `/subscribe` | Register — free, one step |
| `/unsubscribe` | Stop the daily message |
| `/status` | Quota and subscription status |
| `/cancel` | Abort an in-progress registration |

† Needs the Postgres backend. Without it these commands explain that history
isn't enabled; everything else works unchanged.

Typing a city name directly works too, and unique prefixes resolve
("Ban" → Bangalore). Browsing `/cities` is free — only an actual rate lookup
counts against the daily quota.

## Price history

With `DATABASE_URL` set, every refresh upserts the snapshot into `city_rates`
and `mcx_rates`. That history powers:

- **Day-over-day deltas** on every rate message — "🔺 ₹120 (0.82%) vs 08 Sep"
- **`/trend`** — 7-day sparkline, net change, high and low with dates
- **`/alert`** — one threshold per user, checked after each refresh, fires
  once and then clears so it can't spam
- **Weekly digest** — a sparkline summary appended to the Sunday broadcast

Backfill from older snapshots, including your old date-stamped exports (the
date is read from `metadata.date`, falling back to a `YYYY-MM-DD` in the
filename):

```bash
python ingest.py data/history/*.json
```

Ingest and alerts are wrapped so a database problem degrades the history
features without taking rate lookups down.

## Storage backends

`storage.py` exposes one async interface with two implementations, chosen by
whether `DATABASE_URL` is set:

- **JSON** (`subscribers.json`) — the default. Writes go through a temp file
  and `os.replace`, so an interrupted write cannot truncate the file.
- **Postgres** — `users` + `query_log` tables. Quota is counted from
  `query_log` over an IST day window, so the reset time is correct regardless
  of the server's timezone.

Switching is an environment change plus a one-shot migration; see Phase 3 of
the runbook. `subscribers.json` is left in place as a rollback path.

## Data format

```json
{
  "metadata": { "date": "2026-09-09", "unit": "per 10 grams" },
  "mcx_rates": {
    "date": "2026-09-09",
    "gold_october_futures": { "price_per_10g": 161000, "change_percent": 1.2 }
  },
  "cities": [
    {
      "city": "Chennai",
      "state": "Tamil Nadu",
      "24K": { "per_10g": 160900, "per_gram": 16090 },
      "22K": { "per_10g": 147490, "per_gram": 14749 },
      "18K": { "per_10g": 123340, "per_gram": 12334 }
    }
  ]
}
```

MCX contract keys are discovered by pattern (`*gold*futures*`), not hard-coded,
so a contract roll doesn't silently blank the MCX block.

## Failure behaviour

| Situation | Result |
|---|---|
| Feed unreachable / invalid | Last good snapshot is kept; retried next cycle |
| Feed stops updating | 2+ days old: shown with a warning. Over `RATES_MAX_AGE_DAYS`: withheld |
| Workflow finds no new Drive file for a full day | Run fails and GitHub emails you |
| Data resumes after a long gap | Validator re-baselines instead of blocking on the price jump |
| Feed unreachable at startup | Bot starts, replies "unavailable", keeps retrying |
| User blocked the bot | Auto-unsubscribed during the broadcast |
| Quota exhausted | Lookups refused; browsing and the daily message continue |
| Unknown city | Prefix suggestions, and the query is **not** charged |
| Interrupted write | Previous `subscribers.json` survives intact |

All messages use `parse_mode="HTML"` with every user- and feed-supplied value
passed through `html.escape()`. Markdown mode was previously used and broke
outright on names containing `*` or `_`.
