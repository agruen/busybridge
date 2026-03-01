"""Test 11: All-day event sync — all-day on client1 → all-day copy on main + all-day busy block on client2."""

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_all_day, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_all_day_event_copies_to_main(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    event_tracker: EventTracker,
):
    """An all-day event on client1 gets an all-day copy on main."""
    summary = make_summary("allday-main")
    start, end = future_all_day(days_from_now=2)

    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        all_day=True,
    )

    main_copy = wait_for_event(
        main_calendar_client,
        main_calendar_id,
        match=lambda e: summary in e.get("summary", ""),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=summary,
        description="all-day copy on main",
    )

    # Verify it's still an all-day event (uses "date" not "dateTime")
    assert "date" in main_copy.get("start", {}), "Main copy should be an all-day event"


def test_all_day_event_creates_busy_block_on_client2(
    client1_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """An all-day event on client1 creates an all-day busy block on client2."""
    summary = make_summary("allday-busy")
    start, end = future_all_day(days_from_now=3)

    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        all_day=True,
        # Default transparency is "opaque" (busy) → should create busy block
    )

    busy_block = wait_for_event(
        client2_calendar_client,
        client2_calendar_id,
        match=lambda e: (
            BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
            and "date" in e.get("start", {})
        ),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=BUSY_BLOCK_MARKER,
        description="all-day busy block on client2",
    )

    assert "date" in busy_block.get("start", {}), "Busy block should be an all-day event"
