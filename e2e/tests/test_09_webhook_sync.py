"""Test 10: Webhook speed test — sync within 30s."""

import pytest

from e2e.config import WEBHOOK_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event

pytestmark = [pytest.mark.api_only]

BUSY_BLOCK_MARKER = "[BusyBridge]"


def test_webhook_sync_speed_main_copy(
    client1_calendar_client: CalendarTestClient,
    main_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    main_calendar_id: str,
    event_tracker: EventTracker,
):
    """Event on client1 appears on main within the webhook timeout (30s)."""
    summary = make_summary("webhook-speed")
    start, end = future_time_slot(hours_from_now=15)

    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
        summary,
        start,
        end,
    )

    wait_for_event(
        main_calendar_client,
        main_calendar_id,
        match=lambda e: summary in e.get("summary", ""),
        timeout=WEBHOOK_SYNC_TIMEOUT,
        search_query=summary,
        description="main copy (webhook speed test)",
    )


def test_webhook_sync_speed_busy_block(
    client1_calendar_client: CalendarTestClient,
    client2_calendar_client: CalendarTestClient,
    client1_calendar_id: str,
    client2_calendar_id: str,
    event_tracker: EventTracker,
):
    """Busy block on client2 appears within the webhook timeout (30s)."""
    summary = make_summary("webhook-speed-bb")
    start, end = future_time_slot(hours_from_now=16)

    event_tracker.create(
        client1_calendar_client,
        client1_calendar_id,
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
        timeout=WEBHOOK_SYNC_TIMEOUT,
        search_query=BUSY_BLOCK_MARKER,
        time_min=start,
        time_max=end,
        description="busy block (webhook speed test)",
    )
