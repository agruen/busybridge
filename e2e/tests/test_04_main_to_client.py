"""Test 5: Main native event → busy blocks on all client calendars."""

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_main_event_creates_busy_block_on_client1(
    main_calendar_client: CalendarTestClient,
    client1_calendar_client: CalendarTestClient,
    main_calendar_id: str,
    client1_calendar_id: str,
    event_tracker: EventTracker,
):
    """A native event on main creates a busy block on client1."""
    summary = make_summary("main-to-c1")
    start, end = future_time_slot(hours_from_now=7)

    event_tracker.create(
        main_calendar_client,
        main_calendar_id,
        summary,
        start,
        end,
    )

    wait_for_event(
        client1_calendar_client,
        client1_calendar_id,
        match=lambda e: (
            BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
        ),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=BUSY_BLOCK_MARKER,
        time_min=start,
        time_max=end,
        description="busy block on client1 from main event",
    )


def test_main_event_creates_busy_block_on_client2(
    main_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    main_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """A native event on main creates a busy block on client2."""
    summary = make_summary("main-to-c2")
    start, end = future_time_slot(hours_from_now=8)

    event_tracker.create(
        main_calendar_client,
        main_calendar_id,
        summary,
        start,
        end,
    )

    wait_for_event(
        client2_calendar_client,
        client2_calendar_id,
        match=lambda e: (
            BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
        ),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=BUSY_BLOCK_MARKER,
        time_min=start,
        time_max=end,
        description="busy block on client2 from main event",
    )
