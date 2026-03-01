"""Test 12: Declined event skipped — declined on client1 → no copy on main, no busy block."""

import time

import pytest

from e2e.config import CLIENT1_EMAIL, WEBHOOK_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_declined_event_not_synced(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """
    An event on client1 where the user has declined should NOT be synced
    to main and should NOT create a busy block on client2.

    We create the event with the user as a declined attendee.
    """
    summary = make_summary("declined-skip")
    start, end = future_time_slot(hours_from_now=17)

    # Create event with client1 user as a declined attendee
    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
        attendees=[
            {"email": CLIENT1_EMAIL, "responseStatus": "declined", "self": True},
        ],
    )

    # Wait long enough for sync to run
    time.sleep(WEBHOOK_SYNC_TIMEOUT)

    # Verify NO copy on main
    main_events = main_calendar_client.list_events(
        main_calendar_id, q=summary, time_min=start, time_max=end,
    )
    matching = [e for e in main_events if summary in e.get("summary", "")]
    assert len(matching) == 0, (
        f"Declined event should not be copied to main, but found: {matching}"
    )

    # Verify NO busy block on client2 at that time
    c2_events = client2_calendar_client.list_events(
        client2_calendar_id, q=BUSY_BLOCK_MARKER, time_min=start, time_max=end,
    )
    busy_blocks = [
        e for e in c2_events
        if BUSY_BLOCK_MARKER in e.get("summary", "") and "Busy" in e.get("summary", "")
    ]
    assert len(busy_blocks) == 0, (
        f"Declined event should not create busy block, but found: {busy_blocks}"
    )
