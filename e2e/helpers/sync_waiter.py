"""Poll-based helpers that wait for sync results on Google Calendar."""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

from e2e.config import POLL_INTERVAL, WEBHOOK_SYNC_TIMEOUT
from e2e.helpers.google_calendar import CalendarTestClient

logger = logging.getLogger(__name__)


def wait_for_event(
    client: CalendarTestClient,
    calendar_id: str,
    match: Callable[[dict], bool],
    *,
    timeout: float = WEBHOOK_SYNC_TIMEOUT,
    poll_interval: float = POLL_INTERVAL,
    search_query: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    description: str = "event",
) -> dict:
    """
    Poll until an event matching *match* appears on *calendar_id*.

    Returns the first matching event dict.
    Raises TimeoutError if not found within *timeout* seconds.
    """
    deadline = time.monotonic() + timeout
    last_error = None

    while time.monotonic() < deadline:
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
                        description,
                        calendar_id,
                        event.get("summary"),
                    )
                    return event
        except Exception as exc:
            last_error = exc
            logger.debug("Poll error (will retry): %s", exc)

        time.sleep(poll_interval)

    msg = f"Timed out after {timeout}s waiting for {description} on {calendar_id}"
    if last_error:
        msg += f" (last error: {last_error})"
    raise TimeoutError(msg)


def wait_for_event_gone(
    client: CalendarTestClient,
    calendar_id: str,
    match: Callable[[dict], bool],
    *,
    timeout: float = WEBHOOK_SYNC_TIMEOUT,
    poll_interval: float = POLL_INTERVAL,
    search_query: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    description: str = "event",
) -> None:
    """
    Poll until NO event matching *match* exists on *calendar_id*.

    Returns None on success.
    Raises TimeoutError if the event is still present after *timeout* seconds.
    """
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
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

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Timed out after {timeout}s waiting for {description} to disappear from {calendar_id}"
    )


def wait_for_event_updated(
    client: CalendarTestClient,
    calendar_id: str,
    match: Callable[[dict], bool],
    check: Callable[[dict], bool],
    *,
    timeout: float = WEBHOOK_SYNC_TIMEOUT,
    poll_interval: float = POLL_INTERVAL,
    search_query: Optional[str] = None,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
    description: str = "updated event",
) -> dict:
    """
    Poll until an event matching *match* also satisfies *check*.

    This is useful for verifying that an edit has propagated — the event
    already exists, but we're waiting for a specific field to change.

    Returns the matching event dict.
    Raises TimeoutError if the check doesn't pass within *timeout* seconds.
    """
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
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
                        description,
                        calendar_id,
                        event.get("summary"),
                    )
                    return event
        except Exception as exc:
            logger.debug("Poll error (will retry): %s", exc)

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Timed out after {timeout}s waiting for {description} on {calendar_id}"
    )
