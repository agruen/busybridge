"""Create [TEST-BB] prefixed events with unique IDs."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

TEST_EVENT_PREFIX = "[TEST-BB]"
SENTINEL_PREFIX = "[TEST-BB-SENTINEL]"
LIFECYCLE_PREFIX = "[TEST-BB-LIFECYCLE]"


class EventFactory:
    """Generate uniquely-named test events."""

    def __init__(self, run_id: str):
        self.run_id = run_id

    def make_summary(self, label: str = "test") -> str:
        """Return a unique summary like '[TEST-BB] run123-test-abc12345'."""
        short_id = uuid.uuid4().hex[:8]
        return f"{TEST_EVENT_PREFIX} {self.run_id}-{label}-{short_id}"

    @staticmethod
    def future_time_slot(
        hours_from_now: float = 2.0,
        duration_hours: float = 1.0,
    ) -> tuple[str, str]:
        """Return (start_iso, end_iso) for a timed event in the near future."""
        now = datetime.now(timezone.utc)
        start = now + timedelta(hours=hours_from_now)
        end = start + timedelta(hours=duration_hours)
        return start.isoformat(), end.isoformat()

    @staticmethod
    def future_all_day(days_from_now: int = 2) -> tuple[str, str]:
        """Return (start_date, end_date) for an all-day event."""
        today = datetime.now(timezone.utc).date()
        start = today + timedelta(days=days_from_now)
        end = start + timedelta(days=1)
        return start.isoformat(), end.isoformat()

    @staticmethod
    def future_multi_day(days_from_now: int = 2, span: int = 3) -> tuple[str, str]:
        """Return (start_date, end_date) for a multi-day all-day event."""
        today = datetime.now(timezone.utc).date()
        start = today + timedelta(days=days_from_now)
        end = start + timedelta(days=span)
        return start.isoformat(), end.isoformat()

    @staticmethod
    def time_slot_at_offset(
        hours_offset: float,
        duration_hours: float = 1.0,
        timezone_name: Optional[str] = None,
    ) -> tuple[str, str]:
        """Return (start_iso, end_iso) at a specific offset from now."""
        now = datetime.now(timezone.utc)
        start = now + timedelta(hours=hours_offset)
        end = start + timedelta(hours=duration_hours)
        if timezone_name:
            # Return without Z suffix for named timezone use
            fmt = "%Y-%m-%dT%H:%M:%S"
            return start.strftime(fmt), end.strftime(fmt)
        return start.isoformat(), end.isoformat()
