"""Poll-based helpers that wait for sync results on Google Calendar."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from sidecar.infra.calendar_client import CalendarTestClient

logger = logging.getLogger(__name__)


class SyncWaiter:
    """Async polling helpers for waiting on sync outcomes."""

    def __init__(self, timeout: float = 120.0, poll_interval: float = 3.0):
        self.timeout = timeout
        self.poll_interval = poll_interval

    async def wait_for_event(
        self,
        client: CalendarTestClient,
        calendar_id: str,
        match: Callable[[dict], bool],
        *,
        timeout: Optional[float] = None,
        search_query: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        description: str = "event",
    ) -> dict:
        """Poll until a matching event appears. Returns the event."""
        _timeout = timeout or self.timeout
        elapsed = 0.0

        while elapsed < _timeout:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                for event in events:
                    if match(event):
                        logger.info(
                            "Found %s on %s: %s",
                            description, calendar_id, event.get("summary"),
                        )
                        return event
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

        raise TimeoutError(
            f"Timed out after {_timeout}s waiting for {description} on {calendar_id}"
        )

    async def wait_for_gone(
        self,
        client: CalendarTestClient,
        calendar_id: str,
        match: Callable[[dict], bool],
        *,
        timeout: Optional[float] = None,
        search_query: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        description: str = "event",
    ) -> None:
        """Poll until no matching event exists."""
        _timeout = timeout or self.timeout
        elapsed = 0.0

        while elapsed < _timeout:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                if not any(match(e) for e in events):
                    logger.info("Confirmed %s gone from %s", description, calendar_id)
                    return
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

        raise TimeoutError(
            f"Timed out after {_timeout}s waiting for {description} to disappear from {calendar_id}"
        )

    async def wait_for_event_updated(
        self,
        client: CalendarTestClient,
        calendar_id: str,
        match: Callable[[dict], bool],
        check: Callable[[dict], bool],
        *,
        timeout: Optional[float] = None,
        search_query: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
        description: str = "updated event",
    ) -> dict:
        """Poll until a matching event also satisfies check."""
        _timeout = timeout or self.timeout
        elapsed = 0.0

        while elapsed < _timeout:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                for event in events:
                    if match(event) and check(event):
                        logger.info(
                            "Found %s on %s: %s",
                            description, calendar_id, event.get("summary"),
                        )
                        return event
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

        raise TimeoutError(
            f"Timed out after {_timeout}s waiting for {description} on {calendar_id}"
        )
