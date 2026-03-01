"""Tests 6 & 7: Delete cascade — source deletion removes copies and busy blocks."""

import time

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event, wait_for_event_gone

pytestmark = pytest.mark.api_only

BUSY_BLOCK_MARKER = "[BusyBridge]"


class TestDeleteFromClient:
    """Delete on client1 → main copy gone + client2 busy block gone."""

    def test_delete_client_event_removes_main_copy_and_busy_block(
        self,
        client1_calendar_client: CalendarTestClient,
        main_calendar_client: CalendarTestClient,
        client2_calendar_client: CalendarTestClient,
        client1_calendar_id: str,
        main_calendar_id: str,
        client2_calendar_id: str,
        event_tracker: EventTracker,
    ):
        summary = make_summary("del-client-cascade")
        start, end = future_time_slot(hours_from_now=9)

        # Step 1: Create event on client1 and wait for full cascade
        source = event_tracker.create(
            client1_calendar_client,
            client1_calendar_id,
            summary,
            start,
            end,
        )

        # Wait for main copy
        wait_for_event(
            main_calendar_client,
            main_calendar_id,
            match=lambda e: summary in e.get("summary", ""),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=summary,
            description="main copy before delete",
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
            description="busy block on client2 before delete",
        )

        # Step 2: Delete the source event
        client1_calendar_client.delete_event(client1_calendar_id, source["id"])

        # Step 3: Verify main copy disappears
        wait_for_event_gone(
            main_calendar_client,
            main_calendar_id,
            match=lambda e: summary in e.get("summary", ""),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=summary,
            description="main copy after delete",
        )

        # Step 4: Verify busy block on client2 disappears
        wait_for_event_gone(
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
            description="busy block on client2 after delete",
        )


class TestDeleteFromMain:
    """Delete on main → busy blocks gone from both clients."""

    def test_delete_main_event_removes_busy_blocks(
        self,
        main_calendar_client: CalendarTestClient,
        client1_calendar_client: CalendarTestClient,
        client2_calendar_client: CalendarTestClient,
        main_calendar_id: str,
        client1_calendar_id: str,
        client2_calendar_id: str,
        event_tracker: EventTracker,
    ):
        summary = make_summary("del-main-cascade")
        start, end = future_time_slot(hours_from_now=10)

        # Step 1: Create event directly on main
        source = event_tracker.create(
            main_calendar_client,
            main_calendar_id,
            summary,
            start,
            end,
        )

        # Wait for busy blocks on both clients
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
            description="busy block on client1 before delete",
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
            description="busy block on client2 before delete",
        )

        # Step 2: Delete the main event
        main_calendar_client.delete_event(main_calendar_id, source["id"])

        # Step 3: Verify busy blocks disappear from both clients
        wait_for_event_gone(
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
            description="busy block on client1 after delete",
        )

        wait_for_event_gone(
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
            description="busy block on client2 after delete",
        )
