"""Persistent sentinel events for long-term sync verification."""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

from sidecar.framework.base import TestResult, TestStatus
from sidecar.framework.event_factory import SENTINEL_PREFIX

if TYPE_CHECKING:
    from sidecar.framework.base import TestContext
    from sidecar.infra.calendar_client import CalendarTestClient

logger = logging.getLogger(__name__)

STATE_FILE = "/data/test-logs/sentinels.json"
VERIFICATION_INTERVAL = 420  # 7 minutes


@dataclass
class SentinelSpec:
    """Definition of one sentinel event type."""
    label: str
    all_day: bool = False
    duration_hours: float = 1.0
    days_out: int = 10
    description: str = ""
    location: str = ""
    recurrence: list[str] = field(default_factory=list)
    multi_day_span: int = 0  # 0 = single day


SENTINEL_SPECS = [
    SentinelSpec(label="timed-1hr", days_out=10),
    SentinelSpec(label="allday", all_day=True, days_out=12),
    SentinelSpec(label="multiday", all_day=True, days_out=11, multi_day_span=3),
    SentinelSpec(
        label="with-metadata", days_out=10, duration_hours=1.5,
        description="Sentinel test event with metadata for persistence testing",
        location="123 Test Street, Testville",
    ),
    SentinelSpec(
        label="recurring-weekly", days_out=9,
        recurrence=["RRULE:FREQ=WEEKLY;COUNT=8"],
    ),
    SentinelSpec(label="short-30min", days_out=13, duration_hours=0.5),
]


@dataclass
class SentinelState:
    """Persisted state for one sentinel event."""
    spec_label: str
    summary: str
    origin_calendar_id: str
    origin_event_id: str
    client_calendar_db_id: int
    created_at: str
    start_time: str
    end_time: str
    last_verified_at: Optional[str] = None
    last_status: Optional[str] = None
    consecutive_failures: int = 0


class SentinelManager:
    """Manage long-lived sentinel events and verify them periodically."""

    def __init__(self, ctx: TestContext, on_result: Callable):
        self._ctx = ctx
        self._on_result = on_result
        self._sentinels: list[SentinelState] = []
        self._shutdown = False

    async def run(self) -> None:
        """Main loop: reconcile sentinels, then verify periodically."""
        try:
            self._load_state()
            await self._reconcile()

            # Wait for initial sync to process new sentinels
            logger.info("Sentinel: waiting 60s for initial sync...")
            for _ in range(60):
                if self._shutdown:
                    return
                await _async_sleep(1)

            while not self._shutdown:
                await self._verify_all()
                # Sleep with responsive shutdown
                for _ in range(VERIFICATION_INTERVAL):
                    if self._shutdown:
                        return
                    await _async_sleep(1)
        except Exception:
            logger.exception("Sentinel manager crashed")

    def request_shutdown(self) -> None:
        self._shutdown = True

    # ------------------------------------------------------------------ #
    # State persistence                                                    #
    # ------------------------------------------------------------------ #

    def _load_state(self) -> None:
        path = Path(STATE_FILE)
        if not path.exists():
            self._sentinels = []
            return
        try:
            data = json.loads(path.read_text())
            self._sentinels = [
                SentinelState(**s) for s in data.get("sentinels", [])
            ]
            logger.info("Sentinel: loaded %d sentinels from state file", len(self._sentinels))
        except Exception:
            logger.warning("Sentinel: failed to load state file, starting fresh")
            self._sentinels = []

    def _save_state(self) -> None:
        path = Path(STATE_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        data = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "sentinels": [asdict(s) for s in self._sentinels],
        }
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(str(tmp), str(path))

    # ------------------------------------------------------------------ #
    # Reconciliation — create missing sentinels                           #
    # ------------------------------------------------------------------ #

    async def _reconcile(self) -> None:
        """Ensure all sentinel specs have a live event on a client calendar."""
        acct = self._ctx.accounts[0]
        client_cals = [
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        ]
        if not client_cals:
            logger.warning("Sentinel: no client calendars available")
            return

        # Use first client calendar as origin
        origin_cal = client_cals[0]
        origin_client: CalendarTestClient = origin_cal["client"]
        origin_cal_id: str = origin_cal["google_calendar_id"]
        origin_db_id: int = origin_cal["calendar"]["id"]

        existing_labels = {s.spec_label for s in self._sentinels}
        created = 0

        for spec in SENTINEL_SPECS:
            # Already have this sentinel?
            if spec.label in existing_labels:
                existing = next(s for s in self._sentinels if s.spec_label == spec.label)
                # Verify the origin event still exists
                event = await _in_thread(origin_client.get_event, existing.origin_calendar_id, existing.origin_event_id)
                if event:
                    continue
                # Event is gone — remove stale state and recreate
                logger.info("Sentinel: %s origin event gone, recreating", spec.label)
                self._sentinels = [s for s in self._sentinels if s.spec_label != spec.label]

            # Create the sentinel event
            summary = f"{SENTINEL_PREFIX} {spec.label}"
            start, end = self._compute_times(spec)

            event = await _in_thread(
                origin_client.create_event,
                origin_cal_id,
                summary=summary,
                start=start,
                end=end,
                all_day=spec.all_day,
                description=spec.description,
                location=spec.location,
                recurrence=spec.recurrence or None,
            )

            self._sentinels.append(SentinelState(
                spec_label=spec.label,
                summary=summary,
                origin_calendar_id=origin_cal_id,
                origin_event_id=event["id"],
                client_calendar_db_id=origin_db_id,
                created_at=datetime.now(timezone.utc).isoformat(),
                start_time=start,
                end_time=end,
            ))
            created += 1
            logger.info("Sentinel: created %s (%s)", spec.label, event["id"])

        # Trigger sync if new sentinels were created or if any have failed
        any_failed = any(s.consecutive_failures > 0 for s in self._sentinels)
        if created:
            self._save_state()

        if created or any_failed:
            try:
                acct_user_id = acct["user_id"]
                await self._ctx.api.trigger_user_sync(acct_user_id)
            except Exception:
                logger.warning("Sentinel: failed to trigger sync after reconcile")
            if created:
                logger.info("Sentinel: created %d new sentinels, triggered sync", created)
            else:
                logger.info("Sentinel: triggered sync for %d failing sentinels",
                            sum(1 for s in self._sentinels if s.consecutive_failures > 0))
        else:
            logger.info("Sentinel: all %d sentinels healthy", len(self._sentinels))

    def _compute_times(self, spec: SentinelSpec) -> tuple[str, str]:
        """Compute start/end times for a sentinel spec."""
        if spec.all_day:
            today = datetime.now(timezone.utc).date()
            start = today + timedelta(days=spec.days_out)
            if spec.multi_day_span:
                end = start + timedelta(days=spec.multi_day_span)
            else:
                end = start + timedelta(days=1)
            return start.isoformat(), end.isoformat()
        else:
            now = datetime.now(timezone.utc)
            start = now + timedelta(days=spec.days_out)
            # Round to nearest hour for clean times
            start = start.replace(minute=0, second=0, microsecond=0)
            end = start + timedelta(hours=spec.duration_hours)
            return start.isoformat(), end.isoformat()

    # ------------------------------------------------------------------ #
    # Verification                                                        #
    # ------------------------------------------------------------------ #

    async def _verify_all(self) -> None:
        """Verify all sentinels and report results."""
        logger.info("Sentinel: starting verification pass (%d sentinels)", len(self._sentinels))
        for sentinel in self._sentinels:
            if self._shutdown:
                return
            result = await self._verify_one(sentinel)
            await self._on_result(result)

            if result.status == TestStatus.PASSED:
                sentinel.consecutive_failures = 0
                sentinel.last_status = "passed"
            else:
                sentinel.consecutive_failures += 1
                sentinel.last_status = "failed"
                if sentinel.consecutive_failures >= 3:
                    logger.warning(
                        "Sentinel: %s has failed %d consecutive verifications",
                        sentinel.spec_label, sentinel.consecutive_failures,
                    )
            sentinel.last_verified_at = datetime.now(timezone.utc).isoformat()

        self._save_state()

    async def _verify_one(self, sentinel: SentinelState) -> TestResult:
        """Verify a single sentinel event across all calendars and DB."""
        start_t = time.monotonic()
        errors: list[str] = []
        details: dict = {}
        spec = next((s for s in SENTINEL_SPECS if s.label == sentinel.spec_label), None)

        acct = self._ctx.accounts[0]

        try:
            # 1. Origin event still exists?
            origin_client = self._find_client(sentinel.origin_calendar_id)
            if not origin_client:
                errors.append(f"No client for origin calendar {sentinel.origin_calendar_id[:20]}")
            else:
                origin_event = await _in_thread(
                    origin_client.get_event,
                    sentinel.origin_calendar_id, sentinel.origin_event_id,
                )
                if not origin_event:
                    errors.append("Origin event missing from client calendar")
                else:
                    details["origin_status"] = origin_event.get("status")

            # 2. Main calendar copy exists?
            main_client = acct.get("main_client")
            main_cal_id = acct.get("main_calendar_id")
            main_event = None

            if main_client and main_cal_id:
                main_events = await _in_thread(
                    main_client.find_events_by_prefix,
                    main_cal_id, sentinel.summary,
                    time_min=self._search_time_min(sentinel),
                    time_max=self._search_time_max(sentinel),
                )
                if not main_events:
                    errors.append("Main calendar copy missing")
                else:
                    main_event = main_events[0]
                    details["main_event_id"] = main_event["id"]

                    # Check metadata preservation
                    if spec and spec.description:
                        desc = main_event.get("description", "")
                        if spec.description not in desc:
                            errors.append("Description not preserved on main copy")

                    if spec and spec.location:
                        loc = main_event.get("location", "")
                        if spec.location not in loc:
                            errors.append("Location not preserved on main copy")
            else:
                errors.append("No main calendar client available")

            # 3. Busy blocks on other client calendars?
            client_cals = [
                c for c in acct["calendars"]
                if c["calendar_type"] == "client"
                and c["google_calendar_id"] != sentinel.origin_calendar_id
            ]
            for cal_info in client_cals:
                cal_client: CalendarTestClient = cal_info["client"]
                cal_id = cal_info["google_calendar_id"]
                try:
                    busy_events = await _in_thread(
                        cal_client.list_events,
                        cal_id,
                        q="BusyBridge",
                        time_min=self._search_time_min(sentinel),
                        time_max=self._search_time_max(sentinel),
                    )
                    has_block = any(
                        "BusyBridge" in (e.get("summary") or "")
                        for e in busy_events
                    )
                    if not has_block:
                        errors.append(f"No busy block on {cal_id[:25]}")
                except Exception as e:
                    errors.append(f"Error checking busy block on {cal_id[:25]}: {e}")

            # 4. DB mapping exists?
            user_id = acct["user_id"]
            mappings = await self._ctx.db.get_event_mappings(user_id)
            found = [
                m for m in mappings
                if m.get("origin_event_id") == sentinel.origin_event_id
                and m.get("deleted_at") is None
            ]
            if not found:
                errors.append("No DB mapping for sentinel")
            else:
                details["mapping_id"] = found[0]["id"]
                db_main_id = found[0].get("main_event_id", "")
                actual_main_id = main_event["id"] if main_event else ""
                # Recurring instances have IDs like "baseId_20260318T230000Z"
                ids_match = (
                    db_main_id == actual_main_id
                    or actual_main_id.startswith(db_main_id + "_")
                )
                if main_event and not ids_match:
                    errors.append(
                        f"DB main_event_id mismatch: "
                        f"{db_main_id} != {actual_main_id}"
                    )
                # Check busy blocks in DB
                blocks = await self._ctx.db.get_busy_blocks(found[0]["id"])
                details["db_busy_blocks"] = len(blocks)
                if len(client_cals) > 0 and len(blocks) == 0:
                    errors.append("No busy blocks in DB for sentinel mapping")

        except Exception as e:
            errors.append(f"Verification exception: {type(e).__name__}: {e}")
            logger.error("Sentinel verify error for %s: %s\n%s",
                         sentinel.spec_label, e, traceback.format_exc())

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED
        details["checks_failed"] = len(errors)

        return TestResult(
            test_name=f"Sentinel:{sentinel.spec_label}",
            suite="sentinel",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details=details,
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _find_client(self, calendar_id: str) -> Optional[CalendarTestClient]:
        """Find a CalendarTestClient that can access the given calendar."""
        acct = self._ctx.accounts[0]
        for cal_info in acct["calendars"]:
            if cal_info["google_calendar_id"] == calendar_id:
                return cal_info["client"]
        # Try matching by email in clients dict
        for email, client in acct["clients"].items():
            if email == calendar_id:
                return client
        return None

    def _parse_sentinel_time(self, time_str: str) -> datetime:
        """Parse a sentinel time string (may be date-only or full ISO)."""
        try:
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                # Date-only string like "2026-03-21" parsed as midnight
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return datetime.now(timezone.utc)

    def _search_time_min(self, sentinel: SentinelState) -> str:
        """Get search window start for a sentinel (RFC3339)."""
        start = self._parse_sentinel_time(sentinel.start_time)
        return (start - timedelta(hours=1)).isoformat()

    def _search_time_max(self, sentinel: SentinelState) -> str:
        """Get search window end for a sentinel (RFC3339)."""
        end = self._parse_sentinel_time(sentinel.end_time)
        spec = next((s for s in SENTINEL_SPECS if s.label == sentinel.spec_label), None)
        if spec and spec.recurrence:
            return (end + timedelta(weeks=8)).isoformat()
        return (end + timedelta(hours=1)).isoformat()


async def _async_sleep(seconds: float) -> None:
    """Wrapper to allow patching in tests."""
    import asyncio
    await asyncio.sleep(seconds)


async def _in_thread(fn, *args, **kwargs):
    """Run a blocking function in a thread to avoid blocking the event loop."""
    import asyncio
    from functools import partial
    if kwargs:
        fn = partial(fn, **kwargs)
    return await asyncio.to_thread(fn, *args)
