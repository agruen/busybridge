"""Test 8: Edit propagation — editing a client event updates main copy and busy blocks."""

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event, wait_for_event_updated

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_edit_client1_event_propagates_to_main(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    event_tracker: EventTracker,
):
    """Editing a client1 event's summary updates the copy on main."""
    original_summary = make_summary("edit-orig")
    start, end = future_time_slot(hours_from_now=11)

    source = event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        original_summary,
        start,
        end,
    )

    # Wait for the copy on main
    wait_for_event(
        main_calendar_client,
        main_calendar_id,
        match=lambda e: original_summary in e.get("summary", ""),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=original_summary,
        description="main copy before edit",
    )

    # Edit the source event
    updated_summary = original_summary.replace("edit-orig", "edit-updated")
    client1_calendar_client.update_event(
        client1_calendar_id,
        source["id"],
        {"summary": updated_summary},
    )

    # Wait for the main copy to reflect the edit
    wait_for_event(
        main_calendar_client,
        main_calendar_id,
        match=lambda e: updated_summary in e.get("summary", ""),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=updated_summary,
        description="main copy after edit",
    )


def test_edit_client1_event_time_updates_busy_block_time(
    client1_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """Moving a client1 event to a new time updates the busy block time on client2."""
    summary = make_summary("edit-time")
    start, end = future_time_slot(hours_from_now=12)

    source = event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
    )

    # Wait for busy block on client2
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
        description="busy block before time change",
    )

    # Move the event 2 hours later
    new_start, new_end = future_time_slot(hours_from_now=14)
    client1_calendar_client.update_event(
        client1_calendar_id,
        source["id"],
        {
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        },
    )

    # Wait for the busy block to appear at the new time
    wait_for_event(
        client2_calendar_client,
        client2_calendar_id,
        match=lambda e: (
            BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
        ),
        timeout=PERIODIC_SYNC_TIMEOUT,
        search_query=BUSY_BLOCK_MARKER,
        time_min=new_start,
        time_max=new_end,
        description="busy block after time change",
    )
