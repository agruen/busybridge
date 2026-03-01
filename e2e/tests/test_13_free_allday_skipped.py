"""Test 13: Free/transparent all-day event skipped — no busy block created."""

import time

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT, WEBHOOK_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_all_day, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_free_allday_event_no_busy_block(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """
    A free (transparent) all-day event on client1 should be copied to main
    but should NOT create a busy block on client2.

    Per should_create_busy_block: all-day + transparency=="transparent" → skip.
    """
    summary = make_summary("free-allday")
    start, end = future_all_day(days_from_now=4)

    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        all_day=True,
        transparency="transparent",  # "Free" in Google Calendar
    )

    # The event may still be copied to main (sync_client_event_to_main copies
    # all non-cancelled events), so wait for that first
    wait_for_event(
        main_calendar_client,
        main_calendar_id,
        match=lambda e: summary in e.get("summary", ""),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=summary,
        description="main copy of free all-day event",
    )

    # Now wait a bit more and verify NO busy block was created on client2
    time.sleep(WEBHOOK_SYNC_TIMEOUT)

    c2_events = client2_calendar_client.list_events(
        client2_calendar_id, q=BUSY_BLOCK_MARKER,
    )
    # Filter to busy blocks that are all-day events on the same day
    matching_blocks = [
        e for e in c2_events
        if (
            BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
            and "date" in e.get("start", {})
            and e["start"].get("date") == start
        )
    ]
    assert len(matching_blocks) == 0, (
        f"Free all-day event should not create a busy block, but found: {matching_blocks}"
    )
