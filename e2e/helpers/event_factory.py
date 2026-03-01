"""Generate uniquely-named test events with auto-cleanup tracking."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from e2e.config import TEST_EVENT_PREFIX


def make_summary(label: str = "test") -> str:
    """Return a unique event summary like '[E2E-TEST] test-abc123'."""
    short_id = uuid.uuid4().hex[:8]
    return f"{TEST_EVENT_PREFIX} {label}-{short_id}"


def future_time_slot(
    hours_from_now: float = 1.0,
    duration_hours: float = 1.0,
) -> tuple[str, str]:
    """Return (start_iso, end_iso) for a timed event in the near future."""
    now = datetime.now(timezone.utc)
    start = now + timedelta(hours=hours_from_now)
    end = start + timedelta(hours=duration_hours)
    return start.isoformat(), end.isoformat()


def future_all_day(days_from_now: int = 1) -> tuple[str, str]:
    """Return (start_date, end_date) for an all-day event."""
    today = datetime.now(timezone.utc).date()
    start = today + timedelta(days=days_from_now)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


class EventTracker:
    """
    Tracks events created during a test so they can be cleaned up.

    Usage (via the conftest ``event_tracker`` fixture):
        event = event_tracker.create(client, calendar_id, summary, start, end)
        # ... assertions ...
        # cleanup happens automatically at fixture teardown
    """

    def __init__(self):
        self._created: list[tuple] = []  # (client, calendar_id, event_id)

    def track(self, client, calendar_id: str, event_id: str) -> None:
        """Register an event for later cleanup."""
        self._created.append((client, calendar_id, event_id))

    def create(
        self,
        client,
        calendar_id: str,
        summary: str,
        start: str,
        end: str,
        *,
        all_day: bool = False,
        description: str = "",
        location: str = "",
        transparency: str = "opaque",
        attendees: Optional[list[dict]] = None,
    ) -> dict:
        """Create an event and auto-register it for cleanup."""
        event = client.create_event(
            calendar_id,
            summary,
            start,
            end,
            all_day=all_day,
            description=description,
            location=location,
            transparency=transparency,
            attendees=attendees,
        )
        self.track(client, calendar_id, event["id"])
        return event

    def cleanup(self) -> None:
        """Delete all tracked events (best-effort, ignores 404s)."""
        for client, calendar_id, event_id in self._created:
            try:
                client.delete_event(calendar_id, event_id)
            except Exception:
                pass  # Already deleted or inaccessible
        self._created.clear()
