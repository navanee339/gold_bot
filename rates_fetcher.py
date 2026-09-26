"""
rates_fetcher.py — fetch the gold rates JSON from one of two sources.

    RATES_SOURCE=url    One stable HTTPS URL (the GitHub feed, Phase 2).
    RATES_SOURCE=drive  Date-stamped files in a Google Drive folder via a
                        service account — the original pipeline.

If RATES_SOURCE is unset it is inferred: RATES_URL set → url, otherwise
GDRIVE_FOLDER_ID set → drive.

    from rates_fetcher import fetch_gold_data
    data = await fetch_gold_data()      # dict, or None on any failure
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx  # provided by python-telegram-bot

log = logging.getLogger(__name__)

TIMEOUT = 10.0
IST = timezone(timedelta(hours=5, minutes=30))


def max_age_days() -> int:
    """Rates older than this are treated as unavailable (read at call time)."""
    try:
        return int(os.environ.get("RATES_MAX_AGE_DAYS", "3"))
    except ValueError:
        return 3


def snapshot_age_days(data: dict | None) -> Optional[int]:
    """Days between metadata.date and today in IST, or None if undated."""
    raw = ((data or {}).get("metadata") or {}).get("date")
    try:
        snapshot = datetime.fromisoformat(str(raw)[:10]).date()
    except (TypeError, ValueError):
        return None
    return (datetime.now(IST).date() - snapshot).days


def configured_source() -> Optional[str]:
    """Which source is configured: "url", "drive", or None if neither."""
    explicit = os.environ.get("RATES_SOURCE", "").strip().lower()
    if explicit in ("url", "drive"):
        return explicit
    if os.environ.get("RATES_URL", "").strip():
        return "url"
    if os.environ.get("GDRIVE_FOLDER_ID", "").strip():
        return "drive"
    return None


def validate(data: object) -> Optional[dict]:
    """
    Reject anything that isn't a usable rates payload.

    Returning None here means the bot keeps serving the last good data
    rather than replacing it with something broken.
    """
    if not isinstance(data, dict):
        log.error("Rates payload is not a JSON object")
        return None

    cities = data.get("cities")
    if not isinstance(cities, list) or not cities:
        log.error("Rates payload has no usable 'cities' array")
        return None

    priced = sum(
        1 for c in cities
        if isinstance(c, dict) and c.get("city") and (
            c.get("24K") or c.get("22K") or c.get("18K")
        )
    )
    if priced == 0:
        log.error("Rates payload has %d cities but none carry prices", len(cities))
        return None
    if priced < len(cities) / 2:
        log.warning("Only %d of %d cities carry prices", priced, len(cities))

    # A URL feed has no date in its name, so a stopped scraper would otherwise
    # leave the bot serving old prices as if they were today's.
    age = snapshot_age_days(data)
    if age is None:
        log.warning("Rates payload has no usable metadata.date — freshness unchecked")
    elif age > max_age_days():
        log.error("Rates are %d days old (metadata.date=%s), over the %d-day limit — "
                  "refusing them. Has the scraper stopped uploading?",
                  age, data["metadata"]["date"], max_age_days())
        return None

    return data


# ── URL source ────────────────────────────────────────────────────────────────


async def _fetch_url(url: str) -> Optional[object]:
    token = os.environ.get("RATES_TOKEN", "").strip()
    headers = {}
    if token:
        # Private repo: use the contents API URL
        # (api.github.com/repos/OWNER/REPO/contents/data/latest.json); this
        # media type makes it return the raw file instead of a JSON envelope.
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json",
        }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as exc:
        log.error("Rates fetch returned HTTP %s for %s",
                  exc.response.status_code, url)
    except httpx.HTTPError as exc:
        log.error("Rates fetch failed: %s", exc)
    except ValueError as exc:
        log.error("Rates response is not valid JSON: %s", exc)
    return None


# ── Google Drive source ───────────────────────────────────────────────────────


def _fetch_drive() -> Optional[object]:
    """
    Blocking Drive download — always called via asyncio.to_thread.

    Looks for <GDRIVE_FILE_BASE>_<today IST>.json, falling back to
    yesterday's file for the hours before today's upload lands.
    """
    credentials_file = os.environ.get("GDRIVE_CREDENTIALS_FILE", "credentials.json")
    folder_id = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
    base = os.environ.get("GDRIVE_FILE_BASE", "gold_rates_india")

    if not folder_id:
        log.error("GDRIVE_FOLDER_ID is not set")
        return None
    if not os.path.exists(credentials_file):
        log.error("Service account credentials file not found: %s", credentials_file)
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
    except ImportError:
        log.error("Google API libraries not installed. "
                  "Run: pip install google-api-python-client google-auth")
        return None

    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as exc:
        log.error("Google Drive authentication failed: %s", exc)
        return None

    today = datetime.now(IST).date()
    candidates = [f"{base}_{today.isoformat()}.json",
                  f"{base}_{(today - timedelta(days=1)).isoformat()}.json"]

    target = None
    for name in candidates:
        try:
            files = service.files().list(
                q=f"'{folder_id}' in parents and name = '{name}' and trashed = false",
                fields="files(id, name, modifiedTime)",
                pageSize=1,
            ).execute().get("files", [])
        except Exception as exc:
            log.error("Failed to list Drive folder: %s", exc)
            return None
        if files:
            target = files[0]
            break
        log.warning("Drive file '%s' not found", name)

    if target is None:
        log.error("None of %s found in Drive folder. Is it shared with the "
                  "service account email?", candidates)
        return None

    try:
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, service.files().get_media(fileId=target["id"]))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return json.loads(buffer.getvalue().decode("utf-8"))
    except json.JSONDecodeError as exc:
        log.error("Drive file '%s' is not valid JSON: %s", target["name"], exc)
    except Exception as exc:
        log.error("Failed to download '%s' from Drive: %s", target["name"], exc)
    return None


# ── Entry point ───────────────────────────────────────────────────────────────


async def fetch_gold_data(url: str | None = None) -> Optional[dict]:
    """Fetch and validate the rates JSON. Returns None on any failure."""
    source = "url" if url else configured_source()

    if source == "url":
        url = url or os.environ.get("RATES_URL", "").strip()
        if not url:
            log.error("RATES_SOURCE=url but RATES_URL is not set")
            return None
        payload = await _fetch_url(url)
    elif source == "drive":
        payload = await asyncio.to_thread(_fetch_drive)
    else:
        log.error("No rates source configured — set RATES_URL or GDRIVE_FOLDER_ID")
        return None

    if payload is None:
        return None

    data = validate(payload)
    if data is not None:
        log.info("Loaded gold rates for %d cities (source=%s)",
                 len(data["cities"]), source)
    return data
