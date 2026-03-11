"""Sweep [TEST-BB] events from all calendars."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sidecar.framework.event_factory import SENTINEL_PREFIX, TEST_EVENT_PREFIX
from sidecar.infra.calendar_client import CalendarTestClient

logger = logging.getLogger(__name__)


class CleanupManager:
    """Track and clean up test events."""

    def __init__(self):
        self._tracked: list[tuple[CalendarTestClient, str, str]] = []

    def track(
        self, client: CalendarTestClient, calendar_id: str, event_id: str
    ) -> None:
        """Register an event for cleanup at end of test."""
        self._tracked.append((client, calendar_id, event_id))

    async def cleanup_tracked(self) -> None:
        """Delete all tracked events (best-effort)."""
        for client, calendar_id, event_id in self._tracked:
            try:
                client.delete_event(calendar_id, event_id)
            except Exception:
                pass
        self._tracked.clear()

    @staticmethod
    async def sweep_all(
        clients: list[tuple[CalendarTestClient, str]],
        prefix: str = TEST_EVENT_PREFIX,
        time_window_days: int = 7,
    ) -> int:
        """
        Delete all events with the test prefix from all calendars.

        Args:
            clients: List of (client, calendar_id) pairs.
            prefix: Event summary prefix to match.
            time_window_days: How far back/forward to look.

        Returns:
            Number of events deleted.
        """
        now = datetime.now(timezone.utc)
        time_min = (now - timedelta(days=time_window_days)).isoformat()
        time_max = (now + timedelta(days=time_window_days)).isoformat()
        total_deleted = 0

        for client, calendar_id in clients:
            try:
                events = client.find_events_by_prefix(
                    calendar_id, prefix, time_min=time_min, time_max=time_max
                )
                for event in events:
                    # Skip sentinel events — they're long-lived and managed separately
                    if (event.get("summary") or "").startswith(SENTINEL_PREFIX):
                        continue
                    try:
                        client.delete_event(calendar_id, event["id"])
                        total_deleted += 1
                    except Exception as e:
                        logger.debug(
                            "Failed to delete %s on %s: %s",
                            event["id"], calendar_id, e,
                        )
            except Exception as e:
                logger.warning(
                    "Failed to sweep %s on %s: %s", prefix, calendar_id, e
                )

        logger.info(
            "Cleanup sweep: deleted %d [TEST-BB] events across %d calendars",
            total_deleted, len(clients),
        )
        return total_deleted
