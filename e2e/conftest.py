"""Shared fixtures for E2E tests."""

from __future__ import annotations

import logging
from typing import Generator

import pytest

from e2e.config import (
    AUTH_STATE_FILE,
    BASE_URL,
    CALENDAR_TOKENS_FILE,
    CLIENT1_EMAIL,
    CLIENT2_EMAIL,
    MAIN_ACCOUNT_EMAIL,
    TEST_EVENT_PREFIX,
)
from e2e.helpers.event_factory import EventTracker
from e2e.helpers.google_calendar import CalendarTestClient, load_client

logger = logging.getLogger(__name__)


# ── Precondition checks ──────────────────────────────────────────────────────

def pytest_configure(config):
    """Fail fast if auth files are missing."""
    if not CALENDAR_TOKENS_FILE.exists():
        pytest.exit(
            f"Calendar tokens not found at {CALENDAR_TOKENS_FILE}.\n"
            "Run: python -m e2e.auth.get_calendar_tokens",
            returncode=1,
        )


# ── Calendar API clients ─────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def main_calendar_client() -> CalendarTestClient:
    """Google Calendar API client for the main/home account."""
    return load_client(MAIN_ACCOUNT_EMAIL)


@pytest.fixture(scope="session")
def client1_calendar_client() -> CalendarTestClient:
    """Google Calendar API client for client account 1."""
    return load_client(CLIENT1_EMAIL)


@pytest.fixture(scope="session")
def client2_calendar_client() -> CalendarTestClient:
    """Google Calendar API client for client account 2."""
    return load_client(CLIENT2_EMAIL)


# ── Calendar IDs (primary calendar for each account) ─────────────────────────

@pytest.fixture(scope="session")
def main_calendar_id() -> str:
    """Primary calendar ID for the main account (usually the email)."""
    return MAIN_ACCOUNT_EMAIL


@pytest.fixture(scope="session")
def client1_calendar_id() -> str:
    return CLIENT1_EMAIL


@pytest.fixture(scope="session")
def client2_calendar_id() -> str:
    return CLIENT2_EMAIL


# ── Event tracker (per-test auto-cleanup) ─────────────────────────────────────

@pytest.fixture
def event_tracker() -> Generator[EventTracker, None, None]:
    """Create and auto-cleanup test events."""
    tracker = EventTracker()
    yield tracker
    tracker.cleanup()


# ── Global cleanup: remove stale E2E events before the session ────────────────

@pytest.fixture(scope="session", autouse=True)
def cleanup_stale_test_events(
    main_calendar_client: CalendarTestClient,
    client1_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    main_calendar_id: str,
    client1_calendar_id: str,
    client2_calendar_id: str,
):
    """Remove any leftover [E2E-TEST] events from previous runs."""
    pairs = [
        (main_calendar_client, main_calendar_id),
        (client1_calendar_client, client1_calendar_id),
        (client2_calendar_client, client2_calendar_id),
    ]
    for client, cal_id in pairs:
        try:
            stale = client.find_events_by_prefix(cal_id, TEST_EVENT_PREFIX)
            for event in stale:
                try:
                    client.delete_event(cal_id, event["id"])
                    logger.info("Cleaned up stale event: %s", event.get("summary"))
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Stale cleanup failed for %s: %s", cal_id, exc)

    yield  # tests run here

    # Post-session cleanup (same logic)
    for client, cal_id in pairs:
        try:
            stale = client.find_events_by_prefix(cal_id, TEST_EVENT_PREFIX)
            for event in stale:
                try:
                    client.delete_event(cal_id, event["id"])
                except Exception:
                    pass
        except Exception:
            pass


# ── Playwright browser context (with saved auth state) ────────────────────────

@pytest.fixture(scope="session")
def browser_context_args():
    """Inject saved auth state into every Playwright browser context."""
    if AUTH_STATE_FILE.exists():
        return {"storage_state": str(AUTH_STATE_FILE)}
    return {}


@pytest.fixture(scope="session")
def base_url():
    """Base URL for Playwright's page.goto() calls."""
    return BASE_URL
