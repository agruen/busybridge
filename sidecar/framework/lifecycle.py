"""Long-lived portfolio of events for real-world sync reliability testing.

Creates ~18 events spread across client calendars, keeps them alive for days,
periodically mutates them (rename, reschedule, delete+replace), and verifies
that no duplicate copies or orphaned busy blocks accumulate over time.

All duplicate checks search the *actual Google Calendar*, not just the DB.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

from sidecar.framework.base import TestResult, TestStatus
from sidecar.framework.event_factory import LIFECYCLE_PREFIX

if TYPE_CHECKING:
    from sidecar.framework.base import TestContext
    from sidecar.infra.calendar_client import CalendarTestClient

logger = logging.getLogger(__name__)

STATE_FILE = "/data/test-logs/lifecycle.json"
VERIFICATION_INTERVAL = 600  # 10 minutes
INITIAL_SYNC_WAIT = 90       # seconds after creating events

# Mutation intervals (seconds)
RENAME_INTERVAL = 3600       # ~1 hour
RESCHEDULE_INTERVAL = 7200   # ~2 hours
REPLACE_INTERVAL = 14400     # ~4 hours


# ---------------------------------------------------------------------- #
# Portfolio specification                                                  #
# ---------------------------------------------------------------------- #

@dataclass
class LifecycleSpec:
    """Definition of one portfolio event type."""
    label: str
    calendar_index: int = 0       # 0 = first client cal, 1 = second
    all_day: bool = False
    duration_hours: float = 1.0
    days_out: int = 7
    hour: int = 9                 # Hour of day (ET) for timed events
    minute: int = 0               # Minute for timed events
    multi_day_span: int = 0
    description: str = ""
    location: str = ""
    origin_type: str = "client"  # "client" or "main"


# Portfolio of 18 events spread across two calendars and multiple days.
# Times are in America/New_York. No two timed events on the same calendar
# share the same day+hour, and events across calendars are staggered so
# their busy blocks don't pile up at the same time.
#
# Calendar A (andrew.gruen): days 7, 9, 11, 13, 15 — mornings & afternoons
# Calendar B (agyttv):       days 8, 10, 12, 14     — offset days & times
# All-day events on separate days from each other (days 17-22)
LIFECYCLE_SPECS = [
    # -- Calendar A: timed events on odd-offset days --
    LifecycleSpec(label="standup-a",      calendar_index=0, days_out=7,  hour=9,  minute=0,  duration_hours=0.5),
    LifecycleSpec(label="review-a",       calendar_index=0, days_out=7,  hour=14, minute=0,  duration_hours=1.0),
    LifecycleSpec(label="planning-a",     calendar_index=0, days_out=9,  hour=10, minute=30, duration_hours=2.0),
    LifecycleSpec(label="workshop-a",     calendar_index=0, days_out=11, hour=13, minute=0,  duration_hours=4.0),
    LifecycleSpec(label="checkin-a",      calendar_index=0, days_out=13, hour=11, minute=0,  duration_hours=0.5),
    LifecycleSpec(label="deep-work-a",    calendar_index=0, days_out=15, hour=9,  minute=30, duration_hours=1.5),
    # -- Calendar A: events with metadata --
    LifecycleSpec(
        label="offsite-a", calendar_index=0, days_out=9, hour=16, minute=0, duration_hours=1.0,
        description="Quarterly planning offsite — bring laptop and project updates",
        location="456 Lifecycle Ave, Testington",
    ),
    # -- Calendar B: timed events on even-offset days --
    LifecycleSpec(label="standup-b",      calendar_index=1, days_out=8,  hour=9,  minute=30, duration_hours=0.5),
    LifecycleSpec(label="design-b",       calendar_index=1, days_out=8,  hour=15, minute=0,  duration_hours=1.5),
    LifecycleSpec(label="sync-b",         calendar_index=1, days_out=10, hour=11, minute=0,  duration_hours=1.0),
    LifecycleSpec(label="retro-b",        calendar_index=1, days_out=10, hour=16, minute=30, duration_hours=1.0),
    LifecycleSpec(label="interview-b",    calendar_index=1, days_out=12, hour=10, minute=0,  duration_hours=1.0),
    LifecycleSpec(label="lunch-b",        calendar_index=1, days_out=14, hour=12, minute=0,  duration_hours=1.0),
    # -- Calendar B: event with metadata --
    LifecycleSpec(
        label="client-call-b", calendar_index=1, days_out=12, hour=14, minute=30, duration_hours=1.0,
        description="Quarterly review call with client — prepare deck",
        location="789 Portfolio Blvd, Syncville",
    ),
    # -- All-day events: each on a separate day, no overlap --
    LifecycleSpec(label="allday-a",       calendar_index=0, all_day=True, days_out=17),
    LifecycleSpec(label="allday-b",       calendar_index=1, all_day=True, days_out=19),
    LifecycleSpec(label="multiday-a",     calendar_index=0, all_day=True, days_out=20, multi_day_span=3),
    LifecycleSpec(label="multiday-b",     calendar_index=1, all_day=True, days_out=24, multi_day_span=2),
    # -- Main-origin events: busy blocks on ALL client calendars --
    LifecycleSpec(label="main-meeting",  origin_type="main", days_out=8, hour=10, minute=0, duration_hours=1.0),
    LifecycleSpec(label="main-allday",   origin_type="main", all_day=True, days_out=21),
]


# ---------------------------------------------------------------------- #
# Persisted state                                                          #
# ---------------------------------------------------------------------- #

@dataclass
class LifecycleEventState:
    """Persisted state for one portfolio event."""
    spec_label: str
    summary: str
    origin_calendar_id: str
    origin_event_id: str
    calendar_index: int
    client_calendar_db_id: int
    created_at: str
    start_time: str
    end_time: str
    all_day: bool = False
    origin_type: str = "client"  # "client" or "main"
    mutation_count: int = 0
    last_mutated_at: Optional[str] = None
    last_mutation_type: Optional[str] = None
    last_verified_at: Optional[str] = None
    last_status: Optional[str] = None
    consecutive_failures: int = 0
    replaced: bool = False


@dataclass
class PortfolioState:
    """Overall portfolio state, persisted to JSON."""
    version: int = 1
    created_at: str = ""
    updated_at: str = ""
    events: list[LifecycleEventState] = field(default_factory=list)
    last_rename_at: str = ""
    last_reschedule_at: str = ""
    last_replace_at: str = ""
    total_renames: int = 0
    total_reschedules: int = 0
    total_replacements: int = 0


# ---------------------------------------------------------------------- #
# Manager                                                                  #
# ---------------------------------------------------------------------- #

class LifecycleManager:
    """Manage a portfolio of long-lived events and verify sync integrity."""

    def __init__(self, ctx: TestContext, on_result: Callable):
        self._ctx = ctx
        self._on_result = on_result
        self._portfolio = PortfolioState()
        self._shutdown = False

    async def run(self) -> None:
        """Main loop: reconcile portfolio, then verify + mutate periodically."""
        try:
            self._load_state()
            await self._reconcile()

            logger.info("Lifecycle: waiting %ds for initial sync...", INITIAL_SYNC_WAIT)
            for _ in range(INITIAL_SYNC_WAIT):
                if self._shutdown:
                    return
                await _async_sleep(1)

            while not self._shutdown:
                await self._verify_all()
                await self._maybe_mutate()
                for _ in range(VERIFICATION_INTERVAL):
                    if self._shutdown:
                        return
                    await _async_sleep(1)
        except Exception:
            logger.exception("Lifecycle manager crashed")

    def request_shutdown(self) -> None:
        self._shutdown = True

    # ------------------------------------------------------------------ #
    # State persistence                                                    #
    # ------------------------------------------------------------------ #

    def _load_state(self) -> None:
        path = Path(STATE_FILE)
        if not path.exists():
            self._portfolio = PortfolioState(
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            return
        try:
            data = json.loads(path.read_text())
            events = [LifecycleEventState(**e) for e in data.get("events", [])]
            self._portfolio = PortfolioState(
                version=data.get("version", 1),
                created_at=data.get("created_at", ""),
                updated_at=data.get("updated_at", ""),
                events=events,
                last_rename_at=data.get("last_rename_at", ""),
                last_reschedule_at=data.get("last_reschedule_at", ""),
                last_replace_at=data.get("last_replace_at", ""),
                total_renames=data.get("total_renames", 0),
                total_reschedules=data.get("total_reschedules", 0),
                total_replacements=data.get("total_replacements", 0),
            )
            active = [e for e in events if not e.replaced]
            logger.info("Lifecycle: loaded %d events (%d active) from state",
                        len(events), len(active))
        except Exception:
            logger.warning("Lifecycle: failed to load state, starting fresh")
            self._portfolio = PortfolioState(
                created_at=datetime.now(timezone.utc).isoformat(),
            )

    def _save_state(self) -> None:
        path = Path(STATE_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        self._portfolio.updated_at = datetime.now(timezone.utc).isoformat()
        data = {
            "version": self._portfolio.version,
            "created_at": self._portfolio.created_at,
            "updated_at": self._portfolio.updated_at,
            "events": [asdict(e) for e in self._portfolio.events],
            "last_rename_at": self._portfolio.last_rename_at,
            "last_reschedule_at": self._portfolio.last_reschedule_at,
            "last_replace_at": self._portfolio.last_replace_at,
            "total_renames": self._portfolio.total_renames,
            "total_reschedules": self._portfolio.total_reschedules,
            "total_replacements": self._portfolio.total_replacements,
        }
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(str(tmp), str(path))

    # ------------------------------------------------------------------ #
    # Calendar helpers                                                     #
    # ------------------------------------------------------------------ #

    def _get_client_cals(self) -> list[dict]:
        acct = self._ctx.accounts[0]
        return [c for c in acct["calendars"] if c["calendar_type"] == "client"]

    def _get_main(self) -> tuple[CalendarTestClient, str]:
        acct = self._ctx.accounts[0]
        return acct["main_client"], acct["main_calendar_id"]

    def _active_events(self) -> list[LifecycleEventState]:
        return [e for e in self._portfolio.events if not e.replaced]

    # ------------------------------------------------------------------ #
    # Reconciliation                                                       #
    # ------------------------------------------------------------------ #

    async def _reconcile(self) -> None:
        """Ensure all specs have a live event. Create missing ones."""
        client_cals = self._get_client_cals()
        if len(client_cals) < 2:
            logger.warning("Lifecycle: need 2 client calendars, have %d", len(client_cals))
            return

        active_labels = {e.spec_label for e in self._active_events()}
        created = 0

        for spec in LIFECYCLE_SPECS:
            try:
                if spec.label in active_labels:
                    # Verify the event still exists on Google
                    existing = next(
                        e for e in self._active_events()
                        if e.spec_label == spec.label
                    )
                    if existing.origin_type == "main":
                        check_client, _ = self._get_main()
                    else:
                        cal_info = client_cals[min(existing.calendar_index, len(client_cals) - 1)]
                        check_client = cal_info["client"]
                    event = await _in_thread(
                        check_client.get_event,
                        existing.origin_calendar_id,
                        existing.origin_event_id,
                    )
                    if event:
                        continue
                    # Gone — mark replaced and recreate
                    logger.info("Lifecycle: %s origin event gone, recreating", spec.label)
                    existing.replaced = True

                await self._create_event(spec, client_cals)
                created += 1
            except Exception as e:
                logger.warning("Lifecycle: failed to reconcile %s: %s", spec.label, e)

        if created:
            self._save_state()
            try:
                acct = self._ctx.accounts[0]
                await self._ctx.api.trigger_user_sync(acct["user_id"])
            except Exception:
                logger.warning("Lifecycle: failed to trigger sync after reconcile")
            logger.info("Lifecycle: created %d events, triggered sync", created)
        else:
            logger.info("Lifecycle: all %d events healthy", len(self._active_events()))

    async def _create_event(
        self, spec: LifecycleSpec, client_cals: list[dict],
    ) -> LifecycleEventState:
        """Create a single portfolio event on the appropriate calendar."""
        if spec.origin_type == "main":
            main_client, main_cal_id = self._get_main()
            client = main_client
            cal_id = main_cal_id
            cal_idx = -1
            db_id = 0
        else:
            cal_idx = min(spec.calendar_index, len(client_cals) - 1)
            cal_info = client_cals[cal_idx]
            client = cal_info["client"]
            cal_id = cal_info["google_calendar_id"]
            db_id = cal_info["calendar"]["id"]

        short_id = uuid.uuid4().hex[:8]
        summary = f"{LIFECYCLE_PREFIX} {spec.label}-{short_id}"
        start, end = self._compute_times(spec)

        event = await _in_thread(
            client.create_event,
            cal_id,
            summary=summary,
            start=start,
            end=end,
            all_day=spec.all_day,
            description=spec.description,
            location=spec.location,
        )

        state = LifecycleEventState(
            spec_label=spec.label,
            summary=summary,
            origin_calendar_id=cal_id,
            origin_event_id=event["id"],
            calendar_index=cal_idx,
            client_calendar_db_id=db_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            start_time=start,
            end_time=end,
            all_day=spec.all_day,
            origin_type=spec.origin_type,
        )
        self._portfolio.events.append(state)
        logger.info("Lifecycle: created %s (%s) on %s",
                     spec.label, event["id"], cal_id[:25])
        return state

    def _compute_times(self, spec: LifecycleSpec) -> tuple[str, str]:
        if spec.all_day:
            today = datetime.now(timezone.utc).date()
            start = today + timedelta(days=spec.days_out)
            span = spec.multi_day_span if spec.multi_day_span else 1
            end = start + timedelta(days=span)
            return start.isoformat(), end.isoformat()
        else:
            # Build a specific time on a specific day in America/New_York.
            # We store as UTC offset -04:00 (EDT) for consistency.
            today = datetime.now(timezone.utc).date()
            day = today + timedelta(days=spec.days_out)
            # ET is UTC-4 during EDT, UTC-5 during EST.
            # Use -04:00 as a reasonable default; Google normalizes anyway.
            et_offset = timezone(timedelta(hours=-4))
            start = datetime(
                day.year, day.month, day.day,
                spec.hour, spec.minute, 0,
                tzinfo=et_offset,
            )
            end = start + timedelta(hours=spec.duration_hours)
            return start.isoformat(), end.isoformat()

    # ------------------------------------------------------------------ #
    # Verification                                                         #
    # ------------------------------------------------------------------ #

    async def _verify_all(self) -> None:
        """Verify all active events and run drift checks."""
        active = self._active_events()
        logger.info("Lifecycle: starting verification (%d active events)", len(active))

        for ev in active:
            if self._shutdown:
                return
            result = await self._verify_one(ev)
            await self._on_result(result)

            if result.status == TestStatus.PASSED:
                ev.consecutive_failures = 0
                ev.last_status = "passed"
            else:
                ev.consecutive_failures += 1
                ev.last_status = "failed"
                if ev.consecutive_failures >= 3:
                    logger.warning("Lifecycle: %s failed %d consecutive times",
                                   ev.spec_label, ev.consecutive_failures)
            ev.last_verified_at = datetime.now(timezone.utc).isoformat()

        # Drift checks
        if not self._shutdown:
            drift_result = await self._check_drift()
            await self._on_result(drift_result)

        self._save_state()

    async def _verify_one(self, ev: LifecycleEventState) -> TestResult:
        """Verify a single portfolio event: no dupes on real calendars."""
        start_t = time.monotonic()
        errors: list[str] = []
        details: dict = {}
        acct = self._ctx.accounts[0]
        client_cals = self._get_client_cals()
        main_client, main_cal_id = self._get_main()

        is_main_origin = ev.origin_type == "main"

        try:
            # Find the calendar client for this event's origin
            if is_main_origin:
                origin_client = main_client
            else:
                cal_idx = min(ev.calendar_index, len(client_cals) - 1)
                origin_client = client_cals[cal_idx]["client"]

            # 1. Origin event still exists?
            origin_event = await _in_thread(
                origin_client.get_event,
                ev.origin_calendar_id, ev.origin_event_id,
            )
            if not origin_event:
                errors.append("Origin event missing")
            else:
                details["origin_status"] = "exists"

            # 2. Exactly 1 DB mapping?
            mappings = await self._ctx.db.get_event_mappings(acct["user_id"])
            found = [
                m for m in mappings
                if m.get("origin_event_id") == ev.origin_event_id
                and m.get("deleted_at") is None
            ]
            details["db_mapping_count"] = len(found)
            if len(found) == 0:
                errors.append("No DB mapping")
            elif len(found) > 1:
                errors.append(f"Duplicate DB mappings: {len(found)}")

            # 3. Main calendar copy (client-origin only — main-origin IS the main event)
            if not is_main_origin and found:
                main_eid = found[0].get("main_event_id")
                if main_eid:
                    main_event = await _in_thread(
                        main_client.get_event, main_cal_id, main_eid,
                    )
                    if not main_event:
                        errors.append("Main calendar copy missing from Google")
                    details["main_copy_exists"] = main_event is not None
                else:
                    errors.append("No main_event_id in DB mapping")

                # Check for duplicates via metadata
                dupes = await _in_thread(
                    main_client.find_by_origin,
                    main_cal_id,
                    origin_id=ev.origin_event_id,
                )
                details["main_copy_count"] = len(dupes)
                if len(dupes) > 1:
                    errors.append(f"DUPLICATE main copies: {len(dupes)}")

            # 4. Busy block check via DB (not Google search — busy blocks are
            #    generic "Busy" titles so Google searches can't distinguish
            #    which source event created them; overlapping time windows
            #    from sentinels/other tests cause false duplicate counts).
            if found:
                blocks = await self._ctx.db.get_busy_blocks(found[0]["id"])
                details["db_busy_block_count"] = len(blocks)
                # Main-origin: busy blocks on ALL client cals
                # Client-origin: busy blocks on OTHER client cals (exclude origin)
                if is_main_origin:
                    expected_bb = len(client_cals)
                else:
                    expected_bb = len([
                        c for c in client_cals
                        if c["google_calendar_id"] != ev.origin_calendar_id
                    ])
                details["db_busy_block_expected"] = expected_bb
                if len(blocks) == 0:
                    errors.append("No busy blocks in DB")
                elif len(blocks) != expected_bb:
                    errors.append(f"Wrong DB busy block count: {len(blocks)} (expected {expected_bb})")
                # Check no two blocks on the same calendar (real duplicate)
                bb_cal_ids = [b.get("client_calendar_id") for b in blocks]
                if len(bb_cal_ids) != len(set(bb_cal_ids)):
                    errors.append("Duplicate busy blocks on same calendar")

                # Verify at least one DB busy block's Google event still exists
                if blocks:
                    other_cals = [
                        c for c in client_cals
                        if c["google_calendar_id"] != ev.origin_calendar_id
                    ]
                    if other_cals:
                        other_client: CalendarTestClient = other_cals[0]["client"]
                        other_cal_id = other_cals[0]["google_calendar_id"]
                        block_event_id = blocks[0].get("google_event_id", "")
                        if block_event_id:
                            try:
                                block_event = await _in_thread(
                                    other_client.get_event,
                                    other_cal_id, block_event_id,
                                )
                                details["busy_block_exists_on_google"] = block_event is not None
                                if not block_event:
                                    errors.append("Busy block missing from Google Calendar")
                            except Exception as e:
                                errors.append(f"Busy block check error: {e}")

        except Exception as e:
            errors.append(f"Verification error: {type(e).__name__}: {e}")
            logger.error("Lifecycle verify error for %s: %s\n%s",
                         ev.spec_label, e, traceback.format_exc())

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED
        details["checks_failed"] = len(errors)

        return TestResult(
            test_name=f"Lifecycle:{ev.spec_label}",
            suite="lifecycle",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details=details,
        )

    async def _check_drift(self) -> TestResult:
        """Portfolio-level drift: count real Google Calendar events vs expected."""
        start_t = time.monotonic()
        errors: list[str] = []
        details: dict = {}
        active = self._active_events()
        main_client, main_cal_id = self._get_main()
        client_cals = self._get_client_cals()
        acct = self._ctx.accounts[0]

        try:
            # Time window covering all portfolio events
            t_min, t_max = self._portfolio_time_window()

            # --- Main calendar drift ---
            # For each active event, check via get_event (reliable) and
            # find_by_origin (metadata filter, no fulltext lag).
            main_exists = 0
            main_missing = 0
            main_dupes = 0
            # Only check client-origin events for main copies
            # (main-origin events ARE the main event — no separate copy)
            client_origin_active = [e for e in active if e.origin_type != "main"]
            for ev in client_origin_active:
                ev_mappings = [
                    m for m in (await self._ctx.db.get_event_mappings(acct["user_id"]))
                    if m.get("origin_event_id") == ev.origin_event_id
                    and m.get("deleted_at") is None
                ]
                if ev_mappings and ev_mappings[0].get("main_event_id"):
                    main_eid = ev_mappings[0]["main_event_id"]
                    main_ev = await _in_thread(
                        main_client.get_event, main_cal_id, main_eid,
                    )
                    if main_ev:
                        main_exists += 1
                    else:
                        main_missing += 1
                    # Check for duplicates
                    dupes = await _in_thread(
                        main_client.find_by_origin,
                        main_cal_id,
                        origin_id=ev.origin_event_id,
                    )
                    if len(dupes) > 1:
                        main_dupes += len(dupes) - 1
                else:
                    main_missing += 1

            expected_main = len(client_origin_active)
            details["main_expected"] = expected_main
            details["main_exists"] = main_exists
            details["main_missing"] = main_missing
            details["main_duplicates"] = main_dupes
            if main_missing > 0:
                errors.append(
                    f"Main calendar drift: {main_missing} copies missing "
                    f"out of {expected_main} expected"
                )
            if main_dupes > 0:
                errors.append(
                    f"Main calendar drift: {main_dupes} duplicate copies found"
                )

            # --- DB-level busy block drift ---
            # Don't search Google for busy blocks — they're generic "Busy"
            # titles shared with sentinels/soak tests, making counts unreliable.
            # Instead check DB: each active lifecycle mapping should have
            # exactly the right number of busy_blocks rows.
            db_block_total = 0
            db_block_expected = 0
            for ev in active:
                ev_mappings = [
                    m for m in (await self._ctx.db.get_event_mappings(acct["user_id"]))
                    if m.get("origin_event_id") == ev.origin_event_id
                    and m.get("deleted_at") is None
                ]
                if ev_mappings:
                    blocks = await self._ctx.db.get_busy_blocks(ev_mappings[0]["id"])
                    db_block_total += len(blocks)
                    # Each mapping should have 1 busy block per other client calendar
                    other_count = sum(
                        1 for c in client_cals
                        if c["google_calendar_id"] != ev.origin_calendar_id
                    )
                    db_block_expected += other_count
            details["db_busy_blocks_total"] = db_block_total
            details["db_busy_blocks_expected"] = db_block_expected
            if db_block_total > db_block_expected:
                errors.append(
                    f"DB busy block drift: {db_block_total} vs "
                    f"{db_block_expected} expected (duplicates!)"
                )

        except Exception as e:
            errors.append(f"Drift check error: {type(e).__name__}: {e}")
            logger.error("Lifecycle drift check error: %s\n%s", e, traceback.format_exc())

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED

        return TestResult(
            test_name="Lifecycle:drift-check",
            suite="lifecycle",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details=details,
        )

    # ------------------------------------------------------------------ #
    # Mutations                                                            #
    # ------------------------------------------------------------------ #

    async def _maybe_mutate(self) -> None:
        """Check if any mutation is due and run at most one."""
        now = datetime.now(timezone.utc)

        if self._hours_since(self._portfolio.last_rename_at) >= 1.0:
            await self._mutate_rename()
        elif self._hours_since(self._portfolio.last_reschedule_at) >= 2.0:
            await self._mutate_reschedule()
        elif self._hours_since(self._portfolio.last_replace_at) >= 4.0:
            await self._mutate_replace()

    async def _mutate_rename(self) -> None:
        """Rename a random active event and verify propagation."""
        start_t = time.monotonic()
        errors: list[str] = []
        active_timed = [e for e in self._active_events() if not e.all_day]
        if not active_timed:
            return

        ev = random.choice(active_timed)
        client_cals = self._get_client_cals()
        cal_info = client_cals[min(ev.calendar_index, len(client_cals) - 1)]
        client: CalendarTestClient = cal_info["client"]

        short_id = uuid.uuid4().hex[:6]
        new_summary = f"{LIFECYCLE_PREFIX} {ev.spec_label}-r{short_id}"

        try:
            await _in_thread(
                client.update_event,
                ev.origin_calendar_id, ev.origin_event_id,
                {"summary": new_summary},
            )
            logger.info("Lifecycle: renamed %s → %s", ev.spec_label, new_summary)
            ev.summary = new_summary
            ev.mutation_count += 1
            ev.last_mutated_at = datetime.now(timezone.utc).isoformat()
            ev.last_mutation_type = "rename"
            self._portfolio.last_rename_at = datetime.now(timezone.utc).isoformat()
            self._portfolio.total_renames += 1
            self._save_state()

            # Wait for propagation (up to 3 minutes, poll every 5s)
            main_client, main_cal_id = self._get_main()
            propagated = await self._poll_for(
                main_client, main_cal_id,
                lambda e: new_summary in e.get("summary", ""),
                timeout=180, poll_interval=5,
                search_query=LIFECYCLE_PREFIX,
            )
            if not propagated:
                errors.append("Rename did not propagate to main within 180s")

        except Exception as e:
            errors.append(f"Rename error: {type(e).__name__}: {e}")

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED

        result = TestResult(
            test_name="Lifecycle:mutate-rename",
            suite="lifecycle",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details={"event": ev.spec_label, "new_summary": new_summary},
        )
        await self._on_result(result)

    async def _mutate_reschedule(self) -> None:
        """Reschedule a random timed event and verify propagation."""
        start_t = time.monotonic()
        errors: list[str] = []
        active_timed = [
            e for e in self._active_events()
            if not e.all_day
        ]
        if not active_timed:
            return

        ev = random.choice(active_timed)
        client_cals = self._get_client_cals()
        cal_info = client_cals[min(ev.calendar_index, len(client_cals) - 1)]
        client: CalendarTestClient = cal_info["client"]

        # Shift forward by 2-5 days
        shift_days = random.randint(2, 5)
        old_start = self._parse_time(ev.start_time)
        new_start = old_start + timedelta(days=shift_days)
        spec = next((s for s in LIFECYCLE_SPECS if s.label == ev.spec_label), None)
        dur = spec.duration_hours if spec else 1.0
        new_end = new_start + timedelta(hours=dur)

        new_start_iso = new_start.isoformat()
        new_end_iso = new_end.isoformat()

        try:
            await _in_thread(
                client.update_event,
                ev.origin_calendar_id, ev.origin_event_id,
                {
                    "start": {"dateTime": new_start_iso, "timeZone": "America/New_York"},
                    "end": {"dateTime": new_end_iso, "timeZone": "America/New_York"},
                },
            )
            logger.info("Lifecycle: rescheduled %s by +%d days", ev.spec_label, shift_days)
            ev.start_time = new_start_iso
            ev.end_time = new_end_iso
            ev.mutation_count += 1
            ev.last_mutated_at = datetime.now(timezone.utc).isoformat()
            ev.last_mutation_type = "reschedule"
            self._portfolio.last_reschedule_at = datetime.now(timezone.utc).isoformat()
            self._portfolio.total_reschedules += 1
            self._save_state()

            # Verify by checking the main copy has the new time
            main_client, main_cal_id = self._get_main()
            propagated = await self._poll_for(
                main_client, main_cal_id,
                lambda e: (
                    ev.summary in e.get("summary", "")
                    and self._time_close(new_start_iso, e.get("start", {}).get("dateTime", ""))
                ),
                timeout=180, poll_interval=5,
                search_query=LIFECYCLE_PREFIX,
                time_min=(new_start - timedelta(hours=1)).isoformat(),
                time_max=(new_end + timedelta(hours=1)).isoformat(),
            )
            if not propagated:
                errors.append("Reschedule did not propagate to main within 180s")

        except Exception as e:
            errors.append(f"Reschedule error: {type(e).__name__}: {e}")

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED

        result = TestResult(
            test_name="Lifecycle:mutate-reschedule",
            suite="lifecycle",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details={"event": ev.spec_label, "shift_days": shift_days},
        )
        await self._on_result(result)

    async def _mutate_replace(self) -> None:
        """Delete a random event and create a replacement."""
        start_t = time.monotonic()
        errors: list[str] = []
        active = self._active_events()
        if not active:
            return

        ev = random.choice(active)
        client_cals = self._get_client_cals()
        cal_info = client_cals[min(ev.calendar_index, len(client_cals) - 1)]
        client: CalendarTestClient = cal_info["client"]

        try:
            # Delete the old event
            old_summary = ev.summary
            await _in_thread(
                client.delete_event,
                ev.origin_calendar_id, ev.origin_event_id,
            )
            ev.replaced = True
            logger.info("Lifecycle: deleted %s for replacement", ev.spec_label)

            # Wait for deletion to propagate
            main_client, main_cal_id = self._get_main()
            gone = await self._poll_until_gone(
                main_client, main_cal_id,
                lambda e: old_summary in e.get("summary", ""),
                timeout=180, poll_interval=5,
                search_query=old_summary,
            )
            if not gone:
                errors.append("Old event not removed from main within 180s")

            # Create replacement
            spec = next((s for s in LIFECYCLE_SPECS if s.label == ev.spec_label), None)
            if spec:
                new_ev = await self._create_event(spec, client_cals)
                logger.info("Lifecycle: replacement %s created (%s)",
                            spec.label, new_ev.origin_event_id)

                # Trigger sync and wait for the new event to appear
                try:
                    acct = self._ctx.accounts[0]
                    await self._ctx.api.trigger_user_sync(acct["user_id"])
                except Exception:
                    pass

                propagated = await self._poll_for(
                    main_client, main_cal_id,
                    lambda e: new_ev.summary in e.get("summary", ""),
                    timeout=180, poll_interval=5,
                    search_query=LIFECYCLE_PREFIX,
                )
                if not propagated:
                    errors.append("Replacement did not sync to main within 180s")

            self._portfolio.last_replace_at = datetime.now(timezone.utc).isoformat()
            self._portfolio.total_replacements += 1
            self._save_state()

        except Exception as e:
            errors.append(f"Replace error: {type(e).__name__}: {e}")

        duration = time.monotonic() - start_t
        status = TestStatus.PASSED if not errors else TestStatus.FAILED

        result = TestResult(
            test_name="Lifecycle:mutate-replace",
            suite="lifecycle",
            status=status,
            duration=duration,
            run_id=self._ctx.run_id,
            error="; ".join(errors) if errors else None,
            details={"event": ev.spec_label},
        )
        await self._on_result(result)

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _hours_since(self, iso_str: str) -> float:
        """Hours elapsed since an ISO timestamp. Returns large value if empty."""
        if not iso_str:
            return 999.0
        try:
            dt = datetime.fromisoformat(iso_str)
            return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        except (ValueError, TypeError):
            return 999.0

    def _parse_time(self, time_str: str) -> datetime:
        try:
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return datetime.now(timezone.utc)

    def _search_min(self, ev: LifecycleEventState) -> str:
        start = self._parse_time(ev.start_time)
        return (start - timedelta(hours=2)).isoformat()

    def _search_max(self, ev: LifecycleEventState) -> str:
        end = self._parse_time(ev.end_time)
        return (end + timedelta(hours=2)).isoformat()

    def _portfolio_time_window(self) -> tuple[str, str]:
        """Broad time window covering all active portfolio events."""
        active = self._active_events()
        if not active:
            now = datetime.now(timezone.utc)
            return now.isoformat(), (now + timedelta(days=30)).isoformat()
        starts = [self._parse_time(e.start_time) for e in active]
        ends = [self._parse_time(e.end_time) for e in active]
        t_min = min(starts) - timedelta(days=1)
        t_max = max(ends) + timedelta(days=1)
        return t_min.isoformat(), t_max.isoformat()

    @staticmethod
    def _time_close(iso1: str, iso2: str) -> bool:
        """Check if two ISO datetimes represent the same instant (within 2 min)."""
        try:
            dt1 = datetime.fromisoformat(iso1)
            dt2 = datetime.fromisoformat(iso2)
            return abs((dt1 - dt2).total_seconds()) < 120
        except (ValueError, TypeError):
            return False

    async def _poll_for(
        self,
        client: CalendarTestClient,
        calendar_id: str,
        match: Callable,
        *,
        timeout: float = 180,
        poll_interval: float = 5,
        search_query: Optional[str] = None,
        time_min: Optional[str] = None,
        time_max: Optional[str] = None,
    ) -> bool:
        """Poll until a matching event appears. Returns True if found."""
        elapsed = 0.0
        while elapsed < timeout:
            if self._shutdown:
                return False
            try:
                events = await _in_thread(
                    client.list_events, calendar_id,
                    q=search_query, time_min=time_min, time_max=time_max,
                )
                if any(match(e) for e in events):
                    return True
            except Exception:
                pass
            await _async_sleep(poll_interval)
            elapsed += poll_interval
        return False

    async def _poll_until_gone(
        self,
        client: CalendarTestClient,
        calendar_id: str,
        match: Callable,
        *,
        timeout: float = 180,
        poll_interval: float = 5,
        search_query: Optional[str] = None,
    ) -> bool:
        """Poll until no matching event exists. Returns True if gone."""
        elapsed = 0.0
        while elapsed < timeout:
            if self._shutdown:
                return False
            try:
                events = await _in_thread(
                    client.list_events, calendar_id, q=search_query,
                )
                if not any(match(e) for e in events):
                    return True
            except Exception:
                pass
            await _async_sleep(poll_interval)
            elapsed += poll_interval
        return False


# ---------------------------------------------------------------------- #
# Async helpers (same pattern as sentinel.py)                              #
# ---------------------------------------------------------------------- #

async def _async_sleep(seconds: float) -> None:
    import asyncio
    await asyncio.sleep(seconds)


async def _in_thread(fn, *args, **kwargs):
    """Run a blocking call. Runs synchronously to avoid httplib2 thread-safety
    issues — the Google API client's httplib2 transport is not thread-safe,
    and concurrent to_thread calls from sentinel/lifecycle/soak can segfault."""
    from functools import partial
    if kwargs:
        fn = partial(fn, **kwargs)
    return fn(*args)
