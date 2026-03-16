"""Poll-based helpers that wait for sync results on Google Calendar."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from sidecar.infra.calendar_client import CalendarTestClient

logger = logging.getLogger(__name__)


class SyncWaiter:
    """Async polling helpers for waiting on sync outcomes.

    Each wait method has two timeout phases:

    1. **Fast path** (``timeout``): the webhook + verification pipeline should
       deliver the change within this window.  This is what we *expect*.
    2. **Self-heal window** (``self_heal_timeout``): if the fast path misses,
       we keep polling and let the regular 5-minute periodic sync catch it.
       If the data appears here the system is *working* — just slow.  The test
       still passes but is flagged as ``slow_pass``.

    Only if both windows expire is a real ``TimeoutError`` raised.
    """

    def __init__(
        self,
        timeout: float = 120.0,
        poll_interval: float = 3.0,
        self_heal_timeout: float = 420.0,
    ):
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.self_heal_timeout = self_heal_timeout
        self.slow_healed = False

    def reset(self) -> None:
        """Reset per-test state. Called before each test run."""
        self.slow_healed = False

    # ------------------------------------------------------------------
    # wait_for_event
    # ------------------------------------------------------------------
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
        _hard = max(_timeout, self.self_heal_timeout)
        elapsed = 0.0
        warned = False

        while elapsed < _hard:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                for event in events:
                    if match(event):
                        if warned:
                            logger.info(
                                "Self-healed: found %s on %s after %.0fs "
                                "(fast path timeout was %.0fs): %s",
                                description, calendar_id, elapsed,
                                _timeout, event.get("summary"),
                            )
                            self.slow_healed = True
                        else:
                            logger.info(
                                "Found %s on %s: %s",
                                description, calendar_id, event.get("summary"),
                            )
                        return event
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

            if not warned and elapsed >= _timeout:
                warned = True
                logger.warning(
                    "Fast path timeout (%.0fs) waiting for %s on %s — "
                    "waiting for periodic sync to self-heal (up to %.0fs)…",
                    _timeout, description, calendar_id, _hard,
                )

        raise TimeoutError(
            f"Timed out after {_hard:.0f}s (including self-heal) "
            f"waiting for {description} on {calendar_id}"
        )

    # ------------------------------------------------------------------
    # wait_for_gone
    # ------------------------------------------------------------------
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
        _hard = max(_timeout, self.self_heal_timeout)
        elapsed = 0.0
        warned = False

        while elapsed < _hard:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                if not any(match(e) for e in events):
                    if warned:
                        logger.info(
                            "Self-healed: confirmed %s gone from %s after %.0fs "
                            "(fast path timeout was %.0fs)",
                            description, calendar_id, elapsed, _timeout,
                        )
                        self.slow_healed = True
                    else:
                        logger.info(
                            "Confirmed %s gone from %s",
                            description, calendar_id,
                        )
                    return
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

            if not warned and elapsed >= _timeout:
                warned = True
                logger.warning(
                    "Fast path timeout (%.0fs) waiting for %s gone from %s — "
                    "waiting for periodic sync to self-heal (up to %.0fs)…",
                    _timeout, description, calendar_id, _hard,
                )

        raise TimeoutError(
            f"Timed out after {_hard:.0f}s (including self-heal) "
            f"waiting for {description} to disappear from {calendar_id}"
        )

    # ------------------------------------------------------------------
    # wait_for_event_updated
    # ------------------------------------------------------------------
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
        _hard = max(_timeout, self.self_heal_timeout)
        elapsed = 0.0
        warned = False

        while elapsed < _hard:
            try:
                events = client.list_events(
                    calendar_id,
                    q=search_query,
                    time_min=time_min,
                    time_max=time_max,
                )
                for event in events:
                    if match(event) and check(event):
                        if warned:
                            logger.info(
                                "Self-healed: found %s on %s after %.0fs "
                                "(fast path timeout was %.0fs): %s",
                                description, calendar_id, elapsed,
                                _timeout, event.get("summary"),
                            )
                            self.slow_healed = True
                        else:
                            logger.info(
                                "Found %s on %s: %s",
                                description, calendar_id, event.get("summary"),
                            )
                        return event
            except Exception as exc:
                logger.debug("Poll error (will retry): %s", exc)

            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

            if not warned and elapsed >= _timeout:
                warned = True
                logger.warning(
                    "Fast path timeout (%.0fs) waiting for %s on %s — "
                    "waiting for periodic sync to self-heal (up to %.0fs)…",
                    _timeout, description, calendar_id, _hard,
                )

        raise TimeoutError(
            f"Timed out after {_hard:.0f}s (including self-heal) "
            f"waiting for {description} on {calendar_id}"
        )
