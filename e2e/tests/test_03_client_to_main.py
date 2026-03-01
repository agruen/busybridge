"""Test 3 & 4: Client event → full cascade (copy on main + busy block on other client)."""

import pytest

from e2e.config import PERIODIC_SYNC_TIMEOUT, WEBHOOK_SYNC_TIMEOUT
from e2e.helpers.event_factory import EventTracker, future_time_slot, make_summary
from e2e.helpers.google_calendar import CalendarTestClient
from e2e.helpers.sync_waiter import wait_for_event, wait_for_event_gone

pytestmark = pytest.mark.api_only

# ── Managed event prefix used by BusyBridge ───────────────────────────────────
BUSY_BLOCK_MARKER = "[BusyBridge]"


class TestClient1ToMainCascade:
    """Create on client1 → copy with details on main + busy block on client2 (not client1)."""

    def test_client1_event_copies_to_main(
        self,
        client1_calendar_client: CalendarTestClient,
        main_calendar_client: CalendarTestClient,
        client1_calendar_id: str,
        main_calendar_id: str,
        event_tracker: EventTracker,
    ):
        """An event on client1 gets a detailed copy on main."""
        summary = make_summary("c1-to-main")
        start, end = future_time_slot(hours_from_now=2)

        event_tracker.create(
            client1_calendar_client,
            client1_calendar_id,
            summary,
            start,
            end,
            description="Test description from client1",
            location="Test Location",
        )

        # Wait for the copy to appear on main
        main_copy = wait_for_event(
            main_calendar_client,
            main_calendar_id,
            match=lambda e: summary in e.get("summary", ""),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=summary,
            description="main copy of client1 event",
        )

        # Verify details were preserved
        assert summary in main_copy["summary"]

    def test_client1_event_creates_busy_block_on_client2(
        self,
        client1_calendar_client: CalendarTestClient,
        client2_calendar_client: CalendarTestClient,
        client1_calendar_id: str,
        client2_calendar_id: str,
        event_tracker: EventTracker,
    ):
        """An event on client1 creates a busy block on client2."""
        summary = make_summary("c1-busy-c2")
        start, end = future_time_slot(hours_from_now=3)

        event_tracker.create(
            client1_calendar_client,
            client1_calendar_id,
            summary,
            start,
            end,
        )

        # Wait for busy block on client2
        busy_block = wait_for_event(
            client2_calendar_client,
            client2_calendar_id,
            match=lambda e: (
                BUSY_BLOCK_MARKER in e.get("summary", "")
                and "Busy" in e.get("summary", "")
            ),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=BUSY_BLOCK_MARKER,
            description="busy block on client2",
        )

        assert BUSY_BLOCK_MARKER in busy_block["summary"]

    def test_client1_event_no_busy_block_on_self(
        self,
        client1_calendar_client: CalendarTestClient,
        client1_calendar_id: str,
        event_tracker: EventTracker,
    ):
        """An event on client1 does NOT create a busy block back on client1."""
        summary = make_summary("c1-no-self-block")
        start, end = future_time_slot(hours_from_now=4)

        event_tracker.create(
            client1_calendar_client,
            client1_calendar_id,
            summary,
            start,
            end,
        )

        # Give sync time to run, then check there's no BusyBridge busy block
        # that overlaps this time on client1 (other than the original event)
        import time
        time.sleep(WEBHOOK_SYNC_TIMEOUT)

        events = client1_calendar_client.list_events(
            client1_calendar_id,
            q=BUSY_BLOCK_MARKER,
            time_min=start,
            time_max=end,
        )

        # Filter to busy blocks at the same time
        self_blocks = [
            e for e in events
            if BUSY_BLOCK_MARKER in e.get("summary", "")
            and "Busy" in e.get("summary", "")
        ]
        assert len(self_blocks) == 0, (
            f"Found unexpected busy block(s) on client1 (origin): {self_blocks}"
        )


class TestClient2ToMainCascade:
    """Create on client2 → copy with details on main + busy block on client1 (not client2)."""

    def test_client2_event_copies_to_main(
        self,
        client2_calendar_client: CalendarTestClient,
        main_calendar_client: CalendarTestClient,
        client2_calendar_id: str,
        main_calendar_id: str,
        event_tracker: EventTracker,
    ):
        """An event on client2 gets a detailed copy on main."""
        summary = make_summary("c2-to-main")
        start, end = future_time_slot(hours_from_now=5)

        event_tracker.create(
            client2_calendar_client,
            client2_calendar_id,
            summary,
            start,
            end,
            description="Test description from client2",
        )

        main_copy = wait_for_event(
            main_calendar_client,
            main_calendar_id,
            match=lambda e: summary in e.get("summary", ""),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=summary,
            description="main copy of client2 event",
        )

        assert summary in main_copy["summary"]

    def test_client2_event_creates_busy_block_on_client1(
        self,
        client2_calendar_client: CalendarTestClient,
        client1_calendar_client: CalendarTestClient,
        client2_calendar_id: str,
        client1_calendar_id: str,
        event_tracker: EventTracker,
    ):
        """An event on client2 creates a busy block on client1."""
        summary = make_summary("c2-busy-c1")
        start, end = future_time_slot(hours_from_now=6)

        event_tracker.create(
            client2_calendar_client,
            client2_calendar_id,
            summary,
            start,
            end,
        )

        busy_block = wait_for_event(
            client1_calendar_client,
            client1_calendar_id,
            match=lambda e: (
                BUSY_BLOCK_MARKER in e.get("summary", "")
                and "Busy" in e.get("summary", "")
            ),
            timeout=PERIODIC_SYNC_TIMEOUT,
            search_query=BUSY_BLOCK_MARKER,
            description="busy block on client1",
        )

        assert BUSY_BLOCK_MARKER in busy_block["summary"]
