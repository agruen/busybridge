"""Core sync engine."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from app.config import get_settings
from app.database import get_database, get_setting
from app.sync.google_calendar import GoogleCalendarClient
from app.sync.rules import (
    sync_client_event_to_main,
    sync_main_event_to_clients,
    handle_deleted_client_event,
    handle_deleted_main_event,
    propagate_rsvp_to_client,
    propagate_time_to_client,
    sync_personal_event_to_all,
    handle_deleted_personal_event,
)

logger = logging.getLogger(__name__)

# Per-calendar locks to prevent concurrent syncs (e.g. webhook + periodic overlap).
_calendar_locks: dict[str, asyncio.Lock] = {}
_calendar_locks_guard = asyncio.Lock()

# Track pending verification resyncs so we don't stack duplicates.
_pending_verification: set[str] = set()

# Set of user IDs currently running cleanup.  While set, the main-calendar
# sync must treat any "cancelled" events as a full-sync (no cascade deletes).
_cleanup_in_progress: set[int] = set()

# Cleanup progress tracking — keyed by user_id.
_cleanup_progress: dict[int, dict] = {}

# Manual sync progress tracking — keyed by client_calendar_id.
_sync_progress: dict[int, dict] = {}


def get_cleanup_progress(user_id: int) -> Optional[dict]:
    """Return the current cleanup progress for a user, or None."""
    return _cleanup_progress.get(user_id)


def _update_cleanup_progress(user_id: int, **kwargs) -> None:
    """Update cleanup progress for a user."""
    if user_id in _cleanup_progress:
        _cleanup_progress[user_id].update(kwargs)


def get_sync_progress(client_calendar_id: int) -> Optional[dict]:
    """Return the current manual sync progress for a calendar, or None."""
    return _sync_progress.get(client_calendar_id)


def _update_sync_progress(client_calendar_id: int, **kwargs) -> None:
    """Update manual sync progress for a calendar."""
    if client_calendar_id in _sync_progress:
        _sync_progress[client_calendar_id].update(kwargs)


async def _get_calendar_lock(key: str) -> asyncio.Lock:
    """Get or create an asyncio lock for a specific calendar sync key."""
    async with _calendar_locks_guard:
        if key not in _calendar_locks:
            _calendar_locks[key] = asyncio.Lock()
        return _calendar_locks[key]


def _event_has_managed_prefix(event: dict, prefix: str) -> bool:
    """Check whether an event is BusyBridge-managed.

    Matches if the prefix appears anywhere in the summary (busy blocks may
    have a leading lock emoji 🔐) OR anywhere in the description (main-
    calendar copies store metadata there).  Also matches events that carry
    our extended-property sync tag.
    """
    normalized_prefix = (prefix or "").strip().lower()
    if not normalized_prefix:
        return False

    summary = (event.get("summary") or "").strip().lower()
    if normalized_prefix in summary:
        return True

    description = (event.get("description") or "").lower()
    if normalized_prefix in description:
        return True

    # Belt-and-suspenders: also match on the sync engine tag
    ext = event.get("extendedProperties", {}).get("private", {})
    settings = get_settings()
    if ext.get(settings.calendar_sync_tag) == "true":
        return True

    return False


async def is_sync_paused() -> bool:
    """Check if sync is globally paused."""
    setting = await get_setting("sync_paused")
    return setting and setting.get("value_plain") == "true"


_VERIFICATION_DELAY = 25  # seconds — long enough for cross-session consistency


def _schedule_event_verification(
    key: str,
    sync_fn,
    calendar_id: int,
    event_ids: list[str],
) -> None:
    """Schedule targeted re-fetch of specific events after a webhook sync.

    Google Calendar API has cross-session eventual consistency: reads from
    one OAuth session can return stale data for 15-30s after a different
    session writes.  Webhook-triggered syncs may consume stale snapshots,
    advancing the sync token past the change permanently.

    Instead of re-syncing the entire calendar, this re-fetches only the
    events that were just processed (typically 1-3 from a webhook), waits
    for consistency to settle, then re-processes them with fresh data.
    """
    if key in _pending_verification:
        return
    _pending_verification.add(key)

    from app.utils.tasks import create_background_task

    async def _do_verification():
        try:
            await asyncio.sleep(_VERIFICATION_DELAY)
            if await is_sync_paused():
                return
            lock = await _get_calendar_lock(key)
            async with lock:
                await sync_fn(calendar_id, verify_ids=event_ids)
        finally:
            _pending_verification.discard(key)

    create_background_task(
        _do_verification(),
        f"verify_{key}_{len(event_ids)}events",
    )


async def trigger_sync_for_calendar(
    client_calendar_id: int,
    debounce: float = 0,
    track_progress: bool = False,
) -> None:
    """Trigger sync for a specific client calendar.

    Args:
        debounce: seconds to wait before syncing, allowing rapid-fire
                  updates to settle (avoids syncing stale snapshots when
                  Google's eventual consistency hasn't caught up yet).
        track_progress: if True, update ``_sync_progress`` so the UI
                        can poll for live status (settling countdown,
                        sync activity, completion).
    """
    if await is_sync_paused():
        logger.info("Sync is paused, skipping calendar sync")
        if track_progress:
            _sync_progress[client_calendar_id] = {
                "status": "error", "message": "Syncing is paused",
            }
        return

    try:
        if debounce > 0:
            total = int(debounce)
            if track_progress:
                _sync_progress[client_calendar_id] = {
                    "status": "settling", "remaining": total, "total": total,
                }
            for i in range(total):
                if track_progress:
                    _update_sync_progress(client_calendar_id, remaining=total - i)
                await asyncio.sleep(1)
            if track_progress:
                _update_sync_progress(client_calendar_id, remaining=0)

        if track_progress:
            _sync_progress[client_calendar_id] = {
                "status": "syncing", "step": "Fetching events from Google...",
            }

        lock = await _get_calendar_lock(f"client:{client_calendar_id}")
        async with lock:
            processed = await _sync_client_calendar(client_calendar_id)

        if track_progress:
            synced = len(processed) if processed else 0
            _sync_progress[client_calendar_id] = {
                "status": "complete", "events_processed": synced,
            }

        if debounce > 0 and processed:
            _schedule_event_verification(
                f"client:{client_calendar_id}",
                _sync_client_calendar,
                client_calendar_id,
                processed,
            )
    except Exception as e:
        logger.exception(f"Manual sync failed for calendar {client_calendar_id}: {e}")
        if track_progress:
            _sync_progress[client_calendar_id] = {
                "status": "error", "message": str(e)[:200],
            }


async def _sync_client_calendar(
    client_calendar_id: int,
    verify_ids: list[str] | None = None,
) -> list[str]:
    """Internal: perform sync for a client calendar (must be called under lock).

    When *verify_ids* is provided, re-fetches those specific events by ID
    instead of doing an incremental list.  This is used for targeted
    verification of events that may have been synced with stale data.

    Returns the list of non-cancelled event IDs that were processed.
    """
    db = await get_database()

    # Get calendar info
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, u.email as user_email,
                  u.main_calendar_id, css.sync_token
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           JOIN users u ON cc.user_id = u.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.id = ? AND cc.is_active = TRUE""",
        (client_calendar_id,)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        logger.warning(f"Calendar {client_calendar_id} not found or inactive")
        return

    if not calendar["main_calendar_id"]:
        logger.warning(f"User {calendar['user_id']} has no main calendar configured")
        return

    user_id = calendar["user_id"]
    client_email = calendar["google_account_email"]
    source_label = calendar["display_name"] or calendar["google_calendar_id"] or client_email
    if client_email not in source_label:
        source_label = f"{source_label} ({client_email})"
    main_calendar_id = calendar["main_calendar_id"]
    user_email = calendar["user_email"]
    color_id = calendar["color_id"]

    # Initialised before the try so it's always accessible in the except block.
    event_errors = []

    try:
        from app.auth.google import get_valid_access_token

        # Get tokens
        client_token = await get_valid_access_token(user_id, client_email)
        main_token = await get_valid_access_token(user_id, user_email)

        client = GoogleCalendarClient(client_token)
        main_client = GoogleCalendarClient(main_token)

        # Check if SA mode is active for this user
        sa_main_client = None
        cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
        sa_row = await cursor.fetchone()
        if sa_row and sa_row["sa_tier"] == 2:
            from app.auth.service_account import get_sa_main_client
            sa_main_client = get_sa_main_client(main_calendar_id)
            if sa_main_client is None:
                # SA access lost — reset tier and fall back
                logger.warning(f"SA access lost for user {user_id}, resetting sa_tier to 0")
                await db.execute("UPDATE users SET sa_tier = 0 WHERE id = ?", (user_id,))
                await db.commit()

        # Fetch events from client calendar
        if verify_ids is not None:
            # Targeted verification: re-fetch specific events by ID
            events = []
            for eid in verify_ids:
                try:
                    ev = client.get_event(calendar["google_calendar_id"], eid)
                    if ev:
                        events.append(ev)
                except Exception:
                    pass  # event may have been deleted since initial sync
            new_sync_token = None
            sync_token = True  # skip full-sync branch in state update
            logger.info(
                f"Verification: re-fetched {len(events)}/{len(verify_ids)} "
                f"events for calendar {client_calendar_id}"
            )
        else:
            sync_token = calendar["sync_token"]
            result = client.list_events(calendar["google_calendar_id"], sync_token=sync_token)

            if result.get("sync_token_expired"):
                # Need full sync
                logger.info(f"Sync token expired for calendar {client_calendar_id}, doing full sync")
                result = client.list_events(calendar["google_calendar_id"])

            events = result["events"]
            new_sync_token = result.get("next_sync_token")

        # Deduplicate: if the same event appears multiple times (e.g. rapid
        # create-then-move before sync runs), keep only the latest entry so we
        # sync the final state rather than an intermediate snapshot.
        seen: dict[str, int] = {}
        for idx, ev in enumerate(events):
            seen[ev["id"]] = idx
        if len(seen) < len(events):
            logger.info(f"Deduplicating {len(events)} events to {len(seen)} unique")
            events = [events[i] for i in sorted(seen.values())]

        logger.info(f"Syncing {len(events)} events from client calendar {client_calendar_id}")

        # Process each event
        synced_count = 0
        deleted_count = 0
        processed_ids: list[str] = []

        for event in events:
            try:
                if event.get("status") == "cancelled":
                    # Handle deletion
                    await handle_deleted_client_event(
                        user_id=user_id,
                        client_calendar_id=client_calendar_id,
                        event_id=event["id"],
                        main_calendar_id=main_calendar_id,
                        main_client=main_client,
                        recurring_event_id=event.get("recurringEventId"),
                        original_start_time=event.get("originalStartTime"),
                        sa_main_client=sa_main_client,
                    )
                    deleted_count += 1
                else:
                    # If this is a busy block we created and it was moved, revert it
                    if client.is_our_event(event):
                        await _revert_busy_block_on_client(
                            client, client_calendar_id,
                            calendar["google_calendar_id"], event,
                        )
                        continue

                    # Sync event to main
                    main_event_id = await sync_client_event_to_main(
                        client=client,
                        main_client=main_client,
                        event=event,
                        user_id=user_id,
                        client_calendar_id=client_calendar_id,
                        main_calendar_id=main_calendar_id,
                        client_email=client_email,
                        source_label=source_label,
                        color_id=color_id,
                        main_email=user_email,
                        sa_main_client=sa_main_client,
                    )

                    if main_event_id:
                        # Create busy blocks on other client calendars
                        main_event = main_client.get_event(main_calendar_id, main_event_id)
                        if main_event:
                            await sync_main_event_to_clients(
                                main_client=main_client,
                                event=main_event,
                                user_id=user_id,
                                main_calendar_id=main_calendar_id,
                                user_email=user_email,
                            )
                        synced_count += 1
                    processed_ids.append(event["id"])

            except Exception as e:
                logger.error(f"Error processing event {event.get('id')}: {e}")
                event_errors.append(f"{event.get('id')}: {e}")

        if event_errors:
            raise RuntimeError(
                f"{len(event_errors)} event(s) failed while syncing calendar {client_calendar_id}. "
                f"First error: {event_errors[0]}"
            )

        # Update sync state (skip in verification mode — token is unchanged)
        if verify_ids is not None:
            logger.info(
                f"Verification completed for calendar {client_calendar_id}: "
                f"{synced_count} updated"
            )
            return processed_ids

        now = datetime.utcnow().isoformat()
        if sync_token:
            await db.execute(
                """UPDATE calendar_sync_state SET
                   sync_token = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE client_calendar_id = ?""",
                (new_sync_token, now, client_calendar_id)
            )
        else:
            await db.execute(
                """UPDATE calendar_sync_state SET
                   sync_token = ?, last_full_sync = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE client_calendar_id = ?""",
                (new_sync_token, now, now, client_calendar_id)
            )
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'sync', 'success', ?)""",
            (
                user_id,
                client_calendar_id,
                json.dumps({"synced": synced_count, "deleted": deleted_count}),
            )
        )
        await db.commit()

        logger.info(f"Sync completed for calendar {client_calendar_id}: {synced_count} synced, {deleted_count} deleted")
        return processed_ids

    except Exception as e:
        logger.exception(f"Sync failed for calendar {client_calendar_id}: {e}")

        # Update failure count
        await db.execute(
            """UPDATE calendar_sync_state SET
               consecutive_failures = consecutive_failures + 1,
               last_error = ?
               WHERE client_calendar_id = ?""",
            (str(e), client_calendar_id)
        )
        await db.commit()

        # Log failure — include per-event errors so the log is actionable.
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'sync', 'failure', ?)""",
            (
                user_id,
                client_calendar_id,
                json.dumps({"error": str(e), "failed_events": event_errors}),
            )
        )
        await db.commit()

        # Check if we need to send alert
        cursor = await db.execute(
            "SELECT consecutive_failures FROM calendar_sync_state WHERE client_calendar_id = ?",
            (client_calendar_id,)
        )
        row = await cursor.fetchone()
        if row and row["consecutive_failures"] >= 5:
            from app.alerts.email import queue_alert
            await queue_alert(
                alert_type="sync_failures",
                user_id=user_id,
                calendar_id=client_calendar_id,
                details=f"Calendar sync has failed {row['consecutive_failures']} consecutive times. Last error: {str(e)}"
            )

    return []


async def trigger_sync_for_main_calendar(user_id: int, debounce: float = 0) -> None:
    """Trigger sync for a user's main calendar."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping main calendar sync")
        return

    if debounce > 0:
        await asyncio.sleep(debounce)

    lock = await _get_calendar_lock(f"main:{user_id}")
    async with lock:
        processed = await _sync_main_calendar(user_id)

    if debounce > 0 and processed:
        _schedule_event_verification(
            f"main:{user_id}",
            _sync_main_calendar,
            user_id,
            processed,
        )


async def _revert_if_moved(
    cal_client: GoogleCalendarClient,
    calendar_id: str,
    event: dict,
    mapping: dict,
) -> bool:
    """Revert time changes on a managed event by comparing against stored mapping.

    Compares the current event start/end against the stored mapping values.
    If they differ, patches the event back to the original times.

    Returns True if a revert was performed.
    """
    start = event.get("start", {})
    end = event.get("end", {})
    is_all_day = "date" in start

    if is_all_day:
        current_start = start.get("date")
        current_end = end.get("date")
    else:
        current_start = start.get("dateTime")
        current_end = end.get("dateTime")

    stored_start = mapping["event_start"]
    stored_end = mapping["event_end"]

    # Compare as parsed datetimes to handle timezone offset differences.
    # e.g. "2026-02-10T15:30:00-08:00" and "2026-02-10T18:30:00-05:00" are
    # the same instant but differ as strings.
    try:
        times_match = (
            datetime.fromisoformat(current_start) == datetime.fromisoformat(stored_start)
            and datetime.fromisoformat(current_end) == datetime.fromisoformat(stored_end)
        )
    except (ValueError, TypeError):
        # Fall back to string comparison for dates or unparseable values
        times_match = current_start == stored_start and current_end == stored_end

    if times_match:
        return False

    logger.warning(
        f"Reverting move on managed event {event['id']} on {calendar_id} "
        f"(was {current_start}–{current_end}, restoring {stored_start}–{stored_end})"
    )

    if mapping.get("is_all_day"):
        patch = {
            "start": {"date": stored_start},
            "end": {"date": stored_end},
        }
    else:
        tz = start.get("timeZone") or end.get("timeZone")
        patch_start = {"dateTime": stored_start}
        patch_end = {"dateTime": stored_end}
        if tz:
            patch_start["timeZone"] = tz
            patch_end["timeZone"] = tz
        patch = {"start": patch_start, "end": patch_end}

    try:
        cal_client.patch_event(calendar_id, event["id"], patch)
        return True
    except Exception as e:
        logger.error(f"Failed to revert move on event {event['id']}: {e}")
        return False


async def _revert_busy_block_on_client(
    client: GoogleCalendarClient,
    client_calendar_id: int,
    google_calendar_id: str,
    event: dict,
) -> bool:
    """Check if a client-calendar event is a busy block we own; if moved, revert it.

    Looks up the event in busy_blocks. If found, compares times against the
    source event_mapping and reverts if they differ.

    Returns True if a revert was performed (caller should skip normal processing).
    """
    db = await get_database()
    event_id = event.get("id")

    cursor = await db.execute(
        """SELECT bb.id, em.event_start, em.event_end, em.is_all_day
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           WHERE bb.busy_block_event_id = ? AND bb.client_calendar_id = ?""",
        (event_id, client_calendar_id),
    )
    row = await cursor.fetchone()
    if not row:
        return False

    return await _revert_if_moved(
        client, google_calendar_id, event, dict(row)
    )


async def _revert_personal_block_on_main(
    main_client: GoogleCalendarClient,
    main_calendar_id: str,
    user_id: int,
    event: dict,
    sa_main_client: Optional[GoogleCalendarClient] = None,
) -> bool:
    """Check if a main-calendar event is a personal-origin busy block; if moved, revert.

    Returns True if a revert was performed.
    """
    db = await get_database()
    event_id = event.get("id")

    cursor = await db.execute(
        """SELECT em.event_start, em.event_end, em.is_all_day
           FROM event_mappings em
           WHERE em.user_id = ? AND em.main_event_id = ? AND em.origin_type = 'personal'""",
        (user_id, event_id),
    )
    row = await cursor.fetchone()
    if not row:
        return False

    write_client = sa_main_client if sa_main_client is not None else main_client
    return await _revert_if_moved(
        write_client, main_calendar_id, event, dict(row)
    )


async def _retry_missing_busy_blocks(
    main_client: GoogleCalendarClient,
    main_calendar_id: str,
    user_id: int,
    user_email: str,
) -> int:
    """Find event_mappings missing busy blocks on some client calendars and retry.

    Returns the number of busy blocks successfully created.
    """
    db = await get_database()

    # Get all active client calendars for this user
    cursor = await db.execute(
        """SELECT cc.id FROM client_calendars cc
           WHERE cc.user_id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'client'""",
        (user_id,)
    )
    client_cal_rows = await cursor.fetchall()
    client_cal_ids = {row["id"] for row in client_cal_rows}

    if not client_cal_ids:
        return 0

    # Find event_mappings that are missing busy blocks on at least one client calendar.
    # For client-origin mappings, the origin calendar is excluded (no self-block).
    cursor = await db.execute(
        """SELECT em.id, em.main_event_id, em.origin_type, em.origin_calendar_id
           FROM event_mappings em
           WHERE em.user_id = ? AND em.main_event_id IS NOT NULL AND em.deleted_at IS NULL""",
        (user_id,)
    )
    mappings = await cursor.fetchall()

    if not mappings:
        return 0

    # Build set of (mapping_id, client_calendar_id) pairs that already have busy blocks
    cursor = await db.execute(
        """SELECT bb.event_mapping_id, bb.client_calendar_id
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           WHERE em.user_id = ?""",
        (user_id,)
    )
    existing_pairs = {(row["event_mapping_id"], row["client_calendar_id"]) for row in await cursor.fetchall()}

    # Find mappings with gaps
    missing = []
    for m in mappings:
        expected_cals = client_cal_ids.copy()
        # Client-origin events don't get a busy block on their origin calendar
        if m["origin_type"] == "client" and m["origin_calendar_id"] in expected_cals:
            expected_cals.discard(m["origin_calendar_id"])

        for cal_id in expected_cals:
            if (m["id"], cal_id) not in existing_pairs:
                missing.append(m)
                break  # only need to add mapping once

    if not missing:
        return 0

    logger.info(f"Found {len(missing)} event(s) with missing busy blocks for user {user_id}, retrying")

    created = 0
    for m in missing:
        try:
            event = main_client.get_event(main_calendar_id, m["main_event_id"])
            if event and event.get("status") != "cancelled":
                blocks = await sync_main_event_to_clients(
                    main_client=main_client,
                    event=event,
                    user_id=user_id,
                    main_calendar_id=main_calendar_id,
                    user_email=user_email,
                )
                created += len(blocks)
        except Exception as e:
            logger.warning(f"Retry failed for event {m['main_event_id']}: {e}")

    if created:
        logger.info(f"Retry created {created} missing busy block(s) for user {user_id}")

    return created


async def _sync_main_calendar(
    user_id: int,
    verify_ids: list[str] | None = None,
) -> list[str]:
    """Internal: perform main calendar sync (must be called under lock).

    Returns the list of non-cancelled event IDs that were processed.
    """

    db = await get_database()

    # Get user info
    cursor = await db.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    )
    user = await cursor.fetchone()

    if not user or not user["main_calendar_id"]:
        logger.warning(f"User {user_id} not found or no main calendar")
        return []

    main_calendar_id = user["main_calendar_id"]
    user_email = user["email"]

    # Get sync state
    cursor = await db.execute(
        "SELECT * FROM main_calendar_sync_state WHERE user_id = ?", (user_id,)
    )
    sync_state = await cursor.fetchone()

    if not sync_state:
        await db.execute(
            "INSERT INTO main_calendar_sync_state (user_id) VALUES (?)",
            (user_id,)
        )
        await db.commit()
        sync_state = {"sync_token": None}

    # Initialised before the try so it's always accessible in the except block.
    event_errors = []

    try:
        from app.auth.google import get_valid_access_token
        main_token = await get_valid_access_token(user_id, user_email)
        main_client = GoogleCalendarClient(main_token)

        # Check if SA mode is active for this user
        sa_main_client = None
        cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
        sa_row = await cursor.fetchone()
        if sa_row and sa_row["sa_tier"] == 2:
            from app.auth.service_account import get_sa_main_client
            sa_main_client = get_sa_main_client(main_calendar_id)
            if sa_main_client is None:
                logger.warning(f"SA access lost for user {user_id}, resetting sa_tier to 0")
                await db.execute("UPDATE users SET sa_tier = 0 WHERE id = ?", (user_id,))
                await db.commit()

        # Fetch events
        if verify_ids is not None:
            events = []
            for eid in verify_ids:
                try:
                    ev = main_client.get_event(main_calendar_id, eid)
                    if ev:
                        events.append(ev)
                except Exception:
                    pass
            new_sync_token = None
            sync_token = True
            is_full_sync = False
            logger.info(
                f"Verification: re-fetched {len(events)}/{len(verify_ids)} "
                f"main events for user {user_id}"
            )
        else:
            sync_token = sync_state["sync_token"] if sync_state else None
            is_full_sync = sync_token is None
            result = main_client.list_events(main_calendar_id, sync_token=sync_token)

            if result.get("sync_token_expired"):
                logger.info(f"Main calendar sync token expired for user {user_id}, doing full sync")
                result = main_client.list_events(main_calendar_id)
                is_full_sync = True

            events = result["events"]
            new_sync_token = result.get("next_sync_token")

        # Deduplicate: keep only the latest entry per event ID.
        seen_main: dict[str, int] = {}
        for idx, ev in enumerate(events):
            seen_main[ev["id"]] = idx
        if len(seen_main) < len(events):
            logger.info(f"Deduplicating {len(events)} main events to {len(seen_main)} unique")
            events = [events[i] for i in sorted(seen_main.values())]

        logger.info(f"Processing {len(events)} events from main calendar for user {user_id}")

        # Process events
        processed_ids: list[str] = []
        for event in events:
            try:
                if event.get("status") == "cancelled":
                    # If cleanup is in progress, treat as full sync to
                    # prevent cascading deletes to client calendars.
                    effective_full_sync = is_full_sync or (user_id in _cleanup_in_progress)
                    await handle_deleted_main_event(
                        user_id,
                        event["id"],
                        recurring_event_id=event.get("recurringEventId"),
                        original_start_time=event.get("originalStartTime"),
                        is_full_sync=effective_full_sync,
                    )
                else:
                    # Check for client-origin mapping (used for RSVP and edit guards).
                    cursor = await db.execute(
                        """SELECT * FROM event_mappings
                           WHERE user_id = ? AND main_event_id = ? AND origin_type = 'client'""",
                        (user_id, event["id"]),
                    )
                    client_origin_mapping = await cursor.fetchone()

                    # Skip events that BusyBridge created on the main calendar
                    # (personal busy blocks, etc.).  These are managed by their
                    # origin sync flows and must NOT be re-synced as main-origin
                    # events — doing so creates duplicate busy blocks on clients.
                    # Client-origin events also have the sync tag but need RSVP/
                    # edit-guard processing, so they are NOT skipped here.
                    if not client_origin_mapping and main_client.is_our_event(event):
                        # Still revert if someone moved a personal busy block.
                        await _revert_personal_block_on_main(
                            main_client, main_calendar_id, user_id, event,
                            sa_main_client=sa_main_client,
                        )
                        logger.info(f"Skipping our own event on main calendar: {event.get('id')} ({event.get('summary', '')[:40]})")
                        continue

                    # Guard: revert time/location changes on non-editable events.
                    # Use SA client for the revert patch when available.
                    if client_origin_mapping and not client_origin_mapping["user_can_edit"]:
                        revert_client = sa_main_client if sa_main_client is not None else main_client
                        reverted = await _revert_if_moved(
                            revert_client, main_calendar_id, event, dict(client_origin_mapping)
                        )
                        if reverted:
                            # We patched the main event back; skip further
                            # sync so the stale times don't propagate.
                            continue

                    # Propagate time changes on editable events back to the client calendar.
                    if client_origin_mapping and client_origin_mapping["user_can_edit"]:
                        await propagate_time_to_client(
                            user_id=user_id,
                            main_event=event,
                            mapping=dict(client_origin_mapping),
                        )

                    # Check for RSVP changes on client-origin events.
                    if client_origin_mapping and client_origin_mapping["rsvp_status"] is not None:
                        current_rsvp = None
                        for attendee in event.get("attendees", []):
                            if attendee.get("self"):
                                current_rsvp = attendee.get("responseStatus")
                                break

                        if current_rsvp and current_rsvp != client_origin_mapping["rsvp_status"]:
                            await propagate_rsvp_to_client(
                                user_id=user_id,
                                main_event=event,
                                mapping=dict(client_origin_mapping),
                                new_rsvp_status=current_rsvp,
                            )

                    # Sync busy blocks (handles time changes, etc.)
                    await sync_main_event_to_clients(
                        main_client=main_client,
                        event=event,
                        user_id=user_id,
                        main_calendar_id=main_calendar_id,
                        user_email=user_email,
                    )
                    processed_ids.append(event["id"])
            except Exception as e:
                logger.error(f"Error processing main event {event.get('id')}: {e}")
                event_errors.append(f"{event.get('id')}: {e}")

        if event_errors:
            raise RuntimeError(
                f"{len(event_errors)} event(s) failed while syncing main calendar for user {user_id}. "
                f"First error: {event_errors[0]}"
            )

        # In verification mode, skip state updates and retries
        if verify_ids is not None:
            logger.info(
                f"Verification completed for main calendar user {user_id}: "
                f"{len(processed_ids)} re-processed"
            )
            return processed_ids

        # Retry missing busy blocks: find event_mappings that should have
        # busy blocks on client calendars but don't (e.g. due to prior token failures).
        retry_count = await _retry_missing_busy_blocks(
            main_client, main_calendar_id, user_id, user_email
        )

        # Update sync state
        now = datetime.utcnow().isoformat()
        if is_full_sync:
            await db.execute(
                """UPDATE main_calendar_sync_state SET
                   sync_token = ?, last_full_sync = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE user_id = ?""",
                (new_sync_token, now, now, user_id)
            )
        else:
            await db.execute(
                """UPDATE main_calendar_sync_state SET
                   sync_token = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE user_id = ?""",
                (new_sync_token, now, user_id)
            )
        await db.commit()

        logger.info(f"Main calendar sync completed for user {user_id}")

        # Log success
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'sync_main', 'success', ?)""",
            (
                user_id,
                json.dumps({
                    "events_processed": len(events),
                    "is_full_sync": is_full_sync,
                    "busy_blocks_retried": retry_count,
                }),
            )
        )
        await db.commit()
        return processed_ids

    except Exception as e:
        logger.exception(f"Main calendar sync failed for user {user_id}: {e}")

        await db.execute(
            """UPDATE main_calendar_sync_state SET
               consecutive_failures = consecutive_failures + 1,
               last_error = ?
               WHERE user_id = ?""",
            (str(e), user_id)
        )
        await db.commit()

        # Log failure — include per-event errors so the log is actionable.
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'sync_main', 'failure', ?)""",
            (
                user_id,
                json.dumps({"error": str(e), "failed_events": event_errors}),
            )
        )
        await db.commit()

    return []


async def trigger_sync_for_personal_calendar(personal_calendar_id: int, debounce: float = 0) -> None:
    """Trigger sync for a specific personal calendar."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping personal calendar sync")
        return

    if debounce > 0:
        await asyncio.sleep(debounce)

    lock = await _get_calendar_lock(f"personal:{personal_calendar_id}")
    async with lock:
        processed = await _sync_personal_calendar(personal_calendar_id)

    if debounce > 0 and processed:
        _schedule_event_verification(
            f"personal:{personal_calendar_id}",
            _sync_personal_calendar,
            personal_calendar_id,
            processed,
        )


async def _sync_personal_calendar(
    personal_calendar_id: int,
    verify_ids: list[str] | None = None,
) -> list[str]:
    """Internal: perform sync for a personal calendar (must be called under lock).

    Returns the list of non-cancelled event IDs that were processed.
    """
    db = await get_database()

    # Get calendar info
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, u.email as user_email,
                  u.main_calendar_id, css.sync_token
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           JOIN users u ON cc.user_id = u.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'personal'""",
        (personal_calendar_id,)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        logger.warning(f"Personal calendar {personal_calendar_id} not found or inactive")
        return []

    if not calendar["main_calendar_id"]:
        logger.warning(f"User {calendar['user_id']} has no main calendar configured")
        return []

    user_id = calendar["user_id"]
    personal_email = calendar["google_account_email"]
    main_calendar_id = calendar["main_calendar_id"]
    user_email = calendar["user_email"]

    event_errors = []

    try:
        from app.auth.google import get_valid_access_token

        personal_token = await get_valid_access_token(user_id, personal_email)
        main_token = await get_valid_access_token(user_id, user_email)

        personal_client = GoogleCalendarClient(personal_token)
        main_client = GoogleCalendarClient(main_token)

        # Check if SA mode is active
        sa_main_client = None
        cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
        sa_row = await cursor.fetchone()
        if sa_row and sa_row["sa_tier"] == 2:
            from app.auth.service_account import get_sa_main_client
            sa_main_client = get_sa_main_client(main_calendar_id)
            if sa_main_client is None:
                logger.warning(f"SA access lost for user {user_id}, resetting sa_tier to 0")
                await db.execute("UPDATE users SET sa_tier = 0 WHERE id = ?", (user_id,))
                await db.commit()

        # Fetch events from personal calendar
        if verify_ids is not None:
            events = []
            for eid in verify_ids:
                try:
                    ev = personal_client.get_event(calendar["google_calendar_id"], eid)
                    if ev:
                        events.append(ev)
                except Exception:
                    pass
            new_sync_token = None
            sync_token = True
            logger.info(
                f"Verification: re-fetched {len(events)}/{len(verify_ids)} "
                f"personal events for calendar {personal_calendar_id}"
            )
        else:
            sync_token = calendar["sync_token"]
            result = personal_client.list_events(calendar["google_calendar_id"], sync_token=sync_token)

            if result.get("sync_token_expired"):
                logger.info(f"Sync token expired for personal calendar {personal_calendar_id}, doing full sync")
                result = personal_client.list_events(calendar["google_calendar_id"])

            events = result["events"]
            new_sync_token = result.get("next_sync_token")

        logger.info(f"Syncing {len(events)} events from personal calendar {personal_calendar_id}")

        synced_count = 0
        deleted_count = 0
        processed_ids: list[str] = []

        for event in events:
            try:
                if event.get("status") == "cancelled":
                    await handle_deleted_personal_event(
                        user_id=user_id,
                        personal_calendar_id=personal_calendar_id,
                        event_id=event["id"],
                        main_calendar_id=main_calendar_id,
                        main_client=main_client,
                        recurring_event_id=event.get("recurringEventId"),
                        original_start_time=event.get("originalStartTime"),
                        sa_main_client=sa_main_client,
                    )
                    deleted_count += 1
                else:
                    main_event_id = await sync_personal_event_to_all(
                        personal_client=personal_client,
                        main_client=main_client,
                        event=event,
                        user_id=user_id,
                        personal_calendar_id=personal_calendar_id,
                        main_calendar_id=main_calendar_id,
                        user_email=user_email,
                        sa_main_client=sa_main_client,
                    )
                    if main_event_id:
                        synced_count += 1
                    processed_ids.append(event["id"])

            except Exception as e:
                logger.error(f"Error processing personal event {event.get('id')}: {e}")
                event_errors.append(f"{event.get('id')}: {e}")

        if event_errors:
            raise RuntimeError(
                f"{len(event_errors)} event(s) failed while syncing personal calendar {personal_calendar_id}. "
                f"First error: {event_errors[0]}"
            )

        if verify_ids is not None:
            logger.info(
                f"Verification completed for personal calendar {personal_calendar_id}: "
                f"{synced_count} updated"
            )
            return processed_ids

        # Update sync state
        now = datetime.utcnow().isoformat()
        if sync_token:
            await db.execute(
                """UPDATE calendar_sync_state SET
                   sync_token = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE client_calendar_id = ?""",
                (new_sync_token, now, personal_calendar_id)
            )
        else:
            await db.execute(
                """UPDATE calendar_sync_state SET
                   sync_token = ?, last_full_sync = ?, last_incremental_sync = ?,
                   consecutive_failures = 0, last_error = NULL
                   WHERE client_calendar_id = ?""",
                (new_sync_token, now, now, personal_calendar_id)
            )
        await db.commit()

        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'sync_personal', 'success', ?)""",
            (user_id, personal_calendar_id,
             json.dumps({"synced": synced_count, "deleted": deleted_count}))
        )
        await db.commit()

        logger.info(f"Personal calendar sync completed for calendar {personal_calendar_id}: {synced_count} synced, {deleted_count} deleted")
        return processed_ids

    except Exception as e:
        logger.exception(f"Personal calendar sync failed for calendar {personal_calendar_id}: {e}")

        await db.execute(
            """UPDATE calendar_sync_state SET
               consecutive_failures = consecutive_failures + 1,
               last_error = ?
               WHERE client_calendar_id = ?""",
            (str(e), personal_calendar_id)
        )
        await db.commit()

        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'sync_personal', 'failure', ?)""",
            (user_id, personal_calendar_id,
             json.dumps({"error": str(e), "failed_events": event_errors}))
        )
        await db.commit()

    return []


async def trigger_sync_for_user(user_id: int) -> None:
    """Trigger sync for all of a user's calendars."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping user sync")
        return

    db = await get_database()

    # Get all active client calendars
    cursor = await db.execute(
        """SELECT id, calendar_type FROM client_calendars
           WHERE user_id = ? AND is_active = TRUE""",
        (user_id,)
    )
    calendars = await cursor.fetchall()

    # Sync each calendar based on type
    for cal in calendars:
        if cal["calendar_type"] == "personal":
            await trigger_sync_for_personal_calendar(cal["id"])
        else:
            await trigger_sync_for_calendar(cal["id"])

    # Sync main calendar
    await trigger_sync_for_main_calendar(user_id)


async def cleanup_disconnected_calendar(client_calendar_id: int, user_id: int) -> None:
    """
    Clean up when a calendar is disconnected.

    1. Delete all busy blocks we created on this calendar
    2. Delete all events synced from this calendar to main
    3. Delete busy blocks on other calendars that were for events from this calendar
    """
    db = await get_database()

    # Get calendar info
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, u.email as user_email, u.main_calendar_id
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           JOIN users u ON cc.user_id = u.id
           WHERE cc.id = ?""",
        (client_calendar_id,)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        return

    logger.info(f"Cleaning up disconnected calendar {client_calendar_id}")

    try:
        from app.auth.google import get_valid_access_token

        deleted_busy_block_ids: set[int] = set()
        mappings_to_delete: set[int] = set()

        # 1. Delete busy blocks on this calendar
        cursor = await db.execute(
            "SELECT * FROM busy_blocks WHERE client_calendar_id = ?",
            (client_calendar_id,)
        )
        busy_blocks = await cursor.fetchall()

        if busy_blocks:
            try:
                token = await get_valid_access_token(user_id, calendar["google_account_email"])
                client = GoogleCalendarClient(token)

                for block in busy_blocks:
                    try:
                        client.delete_event(calendar["google_calendar_id"], block["busy_block_event_id"])
                        deleted_busy_block_ids.add(block["id"])
                    except Exception as e:
                        logger.warning(f"Failed to delete busy block: {e}")

            except Exception as e:
                logger.warning(f"Failed to get token for cleanup: {e}")

        # 2. Delete events synced from this calendar to main
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE origin_calendar_id = ? AND deleted_at IS NULL""",
            (client_calendar_id,)
        )
        mappings = await cursor.fetchall()

        if mappings and calendar["main_calendar_id"]:
            try:
                main_token = await get_valid_access_token(user_id, calendar["user_email"])
                main_client = GoogleCalendarClient(main_token)

                # Use SA client for main calendar deletions when available
                sa_main_client = None
                cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
                sa_row = await cursor.fetchone()
                if sa_row and sa_row["sa_tier"] == 2:
                    from app.auth.service_account import get_sa_main_client
                    sa_main_client = get_sa_main_client(calendar["main_calendar_id"])
                delete_client = sa_main_client if sa_main_client is not None else main_client

                for mapping in mappings:
                    mapping_cleanup_ok = True

                    if mapping["main_event_id"]:
                        try:
                            delete_client.delete_event(calendar["main_calendar_id"], mapping["main_event_id"])
                        except Exception as e:
                            logger.warning(f"Failed to delete main event: {e}")
                            mapping_cleanup_ok = False

                    # Delete busy blocks for this event on other calendars
                    cursor = await db.execute(
                        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
                           FROM busy_blocks bb
                           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                           WHERE bb.event_mapping_id = ?""",
                        (mapping["id"],)
                    )
                    other_blocks = await cursor.fetchall()

                    for block in other_blocks:
                        if block["client_calendar_id"] == client_calendar_id:
                            if block["id"] not in deleted_busy_block_ids:
                                mapping_cleanup_ok = False
                            continue

                        try:
                            other_token = await get_valid_access_token(user_id, block["google_account_email"])
                            other_client = GoogleCalendarClient(other_token)
                            other_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                            deleted_busy_block_ids.add(block["id"])
                        except Exception as e:
                            logger.warning(f"Failed to delete busy block on other calendar: {e}")
                            mapping_cleanup_ok = False

                    if mapping_cleanup_ok:
                        mappings_to_delete.add(mapping["id"])

            except Exception as e:
                logger.warning(f"Failed to clean up main calendar events: {e}")

        # Delete local busy block records only for events that were actually deleted remotely.
        if deleted_busy_block_ids:
            placeholders = ",".join("?" * len(deleted_busy_block_ids))
            await db.execute(
                f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
                tuple(deleted_busy_block_ids)
            )

        # Delete mappings only when all remote cleanup operations succeeded.
        if mappings_to_delete:
            placeholders = ",".join("?" * len(mappings_to_delete))
            await db.execute(
                f"DELETE FROM busy_blocks WHERE event_mapping_id IN ({placeholders})",
                tuple(mappings_to_delete)
            )
            await db.execute(
                f"DELETE FROM event_mappings WHERE id IN ({placeholders})",
                tuple(mappings_to_delete)
            )

        # Delete webhook channels
        await db.execute(
            "DELETE FROM webhook_channels WHERE client_calendar_id = ?",
            (client_calendar_id,)
        )

        # Delete sync state
        await db.execute(
            "DELETE FROM calendar_sync_state WHERE client_calendar_id = ?",
            (client_calendar_id,)
        )

        await db.commit()
        retained_mappings = len(mappings) - len(mappings_to_delete)
        if retained_mappings > 0:
            logger.warning(
                f"Cleanup completed with {retained_mappings} mapping(s) retained "
                f"for calendar {client_calendar_id} due to remote deletion failures"
            )
        else:
            logger.info(f"Cleanup completed for calendar {client_calendar_id}")

        # Audit entry so admins can see disconnect cleanups without trawling logs.
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'disconnect_cleanup', ?, ?)""",
            (
                user_id,
                client_calendar_id,
                "warning" if retained_mappings > 0 else "success",
                json.dumps({
                    "mappings_cleaned": len(mappings_to_delete),
                    "mappings_retained": retained_mappings,
                    "busy_blocks_deleted": len(deleted_busy_block_ids),
                }),
            ),
        )
        await db.commit()

    except Exception as e:
        logger.exception(f"Error during calendar cleanup: {e}")


async def cleanup_managed_events_for_user(user_id: int) -> dict:
    """
    Remove BusyBridge-managed events for a user.

    Uses two strategies for full coverage:
    1. Internal DB references (event_mappings and busy_blocks)
    2. Prefix search sweep across main and client calendars
    """
    db = await get_database()
    settings = get_settings()
    managed_prefix = (settings.managed_event_prefix or "").strip()

    summary = {
        "status": "ok",
        "managed_event_prefix": managed_prefix,
        "db_client_mappings_checked": 0,
        "db_busy_blocks_checked": 0,
        "db_main_events_deleted": 0,
        "db_busy_blocks_deleted": 0,
        "prefix_calendars_scanned": 0,
        "prefix_matches_found": 0,
        "prefix_events_deleted": 0,
        "local_mappings_deleted": 0,
        "local_busy_blocks_deleted": 0,
        "errors": [],
    }

    _cleanup_progress[user_id] = {
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "step": "clearing_tokens",
        "step_label": "Clearing sync tokens...",
        "current": 0,
        "total": 0,
        "summary": None,
    }

    _cleanup_in_progress.add(user_id)
    try:
        result = await _cleanup_managed_events_impl(user_id, db, managed_prefix, summary)
        _cleanup_progress[user_id].update(
            status="completed",
            step="done",
            step_label="Cleanup complete.",
            summary=result,
        )
        return result
    except Exception:
        _cleanup_progress[user_id].update(
            status="error",
            step="error",
            step_label="Cleanup failed with an error.",
            summary=summary,
        )
        raise
    finally:
        _cleanup_in_progress.discard(user_id)


async def _cleanup_managed_events_impl(
    user_id: int, db, managed_prefix: str, summary: dict
) -> dict:
    """Inner implementation — runs while _cleanup_in_progress flag is set."""
    cursor = await db.execute(
        "SELECT id, email, main_calendar_id FROM users WHERE id = ?",
        (user_id,),
    )
    user = await cursor.fetchone()
    if not user:
        summary["status"] = "error"
        summary["errors"].append(f"user_not_found:{user_id}")
        return summary

    from app.auth.google import get_valid_access_token
    from googleapiclient.errors import HttpError

    client_cache: dict[str, GoogleCalendarClient] = {}
    token_failures: set[str] = set()

    async def _throttled_delete(
        client: GoogleCalendarClient, calendar_id: str, event_id: str,
    ) -> None:
        """Delete with retry on rate limits and a small delay between calls."""
        for attempt in range(4):
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(client.delete_event, calendar_id, event_id),
                    timeout=15,
                )
                await asyncio.sleep(0.15)  # ~6 req/s, well under Google's limits
                return
            except asyncio.TimeoutError:
                logger.warning("Delete timed out for %s (attempt %d)", event_id[:20], attempt + 1)
                if attempt == 3:
                    raise
            except HttpError as e:
                if e.resp.status == 403 and "rateLimitExceeded" in str(e):
                    wait = 2 ** attempt  # 1, 2, 4, 8 seconds
                    logger.info("Rate limited, waiting %ds before retry (%s)", wait, event_id[:20])
                    await asyncio.sleep(wait)
                else:
                    raise
        # Final attempt — let it raise
        await asyncio.wait_for(
            asyncio.to_thread(client.delete_event, calendar_id, event_id),
            timeout=15,
        )

    async def _get_client_for_email(email: str) -> Optional[GoogleCalendarClient]:
        normalized_email = (email or "").strip().lower()
        if not normalized_email:
            return None

        if normalized_email in client_cache:
            return client_cache[normalized_email]
        if normalized_email in token_failures:
            return None

        try:
            access_token = await get_valid_access_token(user_id, email)
            client_cache[normalized_email] = GoogleCalendarClient(access_token)
            return client_cache[normalized_email]
        except Exception as e:
            token_failures.add(normalized_email)
            summary["errors"].append(f"token:{email}:{type(e).__name__}")
            logger.warning(f"Failed to get token for cleanup user={user_id}, email={email}: {e}")
            return None

    # ------------------------------------------------------------------ #
    # Pass 0: Clear all sync tokens BEFORE deleting any remote events.    #
    # When cleanup deletes events from Google, webhooks fire immediately   #
    # and can trigger a sync.  If that sync runs with an incremental      #
    # token it will see the deletions and cascade-delete the original     #
    # client events -- catastrophic data loss.  By clearing tokens first, #
    # any racing sync will be a *full* sync, which has safe logic that    #
    # never cascades deletes back to client calendars.                    #
    # ------------------------------------------------------------------ #
    cursor = await db.execute(
        """SELECT id FROM client_calendars WHERE user_id = ? AND is_active = TRUE""",
        (user_id,),
    )
    active_cal_ids = [row["id"] for row in await cursor.fetchall()]
    if active_cal_ids:
        placeholders = ",".join("?" * len(active_cal_ids))
        await db.execute(
            f"UPDATE calendar_sync_state SET sync_token = NULL WHERE client_calendar_id IN ({placeholders})",
            tuple(active_cal_ids),
        )
    await db.execute(
        "UPDATE main_calendar_sync_state SET sync_token = NULL WHERE user_id = ?",
        (user_id,),
    )
    await db.commit()
    logger.info("Cleanup: cleared all sync tokens for user %s before remote deletions", user_id)

    # Track which mappings/blocks were successfully cleaned remotely so we only
    # delete DB records for those -- otherwise we lose tracking for retry.
    cleaned_mapping_ids: set[int] = set()
    failed_mapping_ids: set[int] = set()

    logger.info("Cleanup: starting Pass 1a (main calendar events) for user %s", user_id)
    # Pass 1a: DB-driven cleanup of BusyBridge-created events on the main calendar.
    # This covers:
    #   - client-origin copies (events synced FROM client calendars TO main)
    #   - personal-origin busy blocks (privacy blocks for personal events)
    # EXCLUDES main-origin events — those are the user's own events on the
    # main calendar; we only created busy blocks FOR them on client calendars
    # (handled by Pass 1b).  Deleting them would destroy user data.
    cursor = await db.execute(
        """SELECT id, main_event_id, origin_type
           FROM event_mappings
           WHERE user_id = ? AND origin_type IN ('client', 'personal')
           AND deleted_at IS NULL AND main_event_id IS NOT NULL""",
        (user_id,),
    )
    main_cal_mappings = await cursor.fetchall()
    summary["db_client_mappings_checked"] = len(main_cal_mappings)

    if main_cal_mappings and user["main_calendar_id"]:
        main_client = await _get_client_for_email(user["email"])
        # Use SA client for main calendar deletions when available
        sa_delete_client = None
        cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
        sa_row = await cursor.fetchone()
        if sa_row and sa_row["sa_tier"] == 2:
            from app.auth.service_account import get_sa_main_client
            sa_delete_client = get_sa_main_client(user["main_calendar_id"])
        delete_client = sa_delete_client or main_client
        if delete_client:
            _update_cleanup_progress(
                user_id, step="deleting_main_events",
                step_label="Deleting managed events from main calendar...",
                current=0, total=len(main_cal_mappings),
            )
            for i, mapping in enumerate(main_cal_mappings):
                try:
                    await _throttled_delete(
                        delete_client,
                        user["main_calendar_id"],
                        mapping["main_event_id"],
                    )
                    summary["db_main_events_deleted"] += 1
                    cleaned_mapping_ids.add(mapping["id"])
                except Exception as e:
                    failed_mapping_ids.add(mapping["id"])
                    summary["errors"].append(f"main_delete:{mapping['main_event_id']}:{type(e).__name__}")
                    logger.warning(
                        "Failed DB-driven main event delete for user=%s event=%s: %s",
                        user_id,
                        mapping["main_event_id"],
                        e,
                    )
                _update_cleanup_progress(user_id, current=i + 1)
        else:
            # No client available — mark ALL mappings as failed so their
            # DB records are retained for retry.
            for mapping in main_cal_mappings:
                failed_mapping_ids.add(mapping["id"])
            summary["errors"].append("main_calendar_token_unavailable")

    logger.info("Cleanup: Pass 1a done (%d deleted, %d failed). Starting Pass 1b (busy blocks) for user %s",
                summary["db_main_events_deleted"], len(failed_mapping_ids), user_id)
    # Pass 1b: DB-driven cleanup of busy blocks on client calendars.
    cursor = await db.execute(
        """SELECT bb.id, bb.event_mapping_id, bb.busy_block_event_id,
                  cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE em.user_id = ?""",
        (user_id,),
    )
    busy_blocks = await cursor.fetchall()
    summary["db_busy_blocks_checked"] = len(busy_blocks)

    _update_cleanup_progress(
        user_id, step="deleting_busy_blocks",
        step_label="Deleting busy blocks from client calendars...",
        current=0, total=len(busy_blocks),
    )
    for i, block in enumerate(busy_blocks):
        client = await _get_client_for_email(block["google_account_email"])
        if not client:
            failed_mapping_ids.add(block["event_mapping_id"])
            summary["errors"].append(
                f"busy_block_token_unavailable:{block['busy_block_event_id']}"
            )
            _update_cleanup_progress(user_id, current=i + 1)
            continue

        try:
            await _throttled_delete(
                client,
                block["google_calendar_id"],
                block["busy_block_event_id"],
            )
            summary["db_busy_blocks_deleted"] += 1
        except Exception as e:
            failed_mapping_ids.add(block["event_mapping_id"])
            summary["errors"].append(
                f"busy_block_delete:{block['busy_block_event_id']}:{type(e).__name__}"
            )
            logger.warning(
                "Failed DB-driven busy block delete for user=%s event=%s: %s",
                user_id,
                block["busy_block_event_id"],
                e,
            )
        _update_cleanup_progress(user_id, current=i + 1)

    logger.info("Cleanup: Pass 1b done (%d deleted). Starting Pass 2 (prefix sweep) for user %s",
                summary["db_busy_blocks_deleted"], user_id)
    # Pass 2: Prefix sweep across visible calendars for extra coverage.
    if managed_prefix:
        seen_calendars: set[tuple[str, str]] = set()

        async def _sweep_calendar(
            calendar_id: str,
            account_email: str,
            label: str,
        ) -> None:
            calendar_key = ((account_email or "").strip().lower(), calendar_id)
            if calendar_key in seen_calendars:
                return
            seen_calendars.add(calendar_key)

            client = await _get_client_for_email(account_email)
            if not client:
                return

            try:
                events = await asyncio.wait_for(
                    asyncio.to_thread(
                        client.search_events, calendar_id, managed_prefix,
                        single_events=False,
                    ),
                    timeout=30,
                )
                summary["prefix_calendars_scanned"] += 1
            except asyncio.TimeoutError:
                summary["errors"].append(f"prefix_scan_timeout:{label}")
                logger.warning("Prefix sweep timed out for user=%s calendar=%s", user_id, calendar_id)
                return
            except Exception as e:
                summary["errors"].append(f"prefix_scan:{label}:{type(e).__name__}")
                logger.warning(
                    "Failed prefix sweep for user=%s calendar=%s label=%s: %s",
                    user_id,
                    calendar_id,
                    label,
                    e,
                )
                return

            for event in events:
                event_id = event.get("id")
                if not event_id:
                    continue
                if not _event_has_managed_prefix(event, managed_prefix):
                    continue

                summary["prefix_matches_found"] += 1
                try:
                    await _throttled_delete(client, calendar_id, event_id)
                    summary["prefix_events_deleted"] += 1
                except Exception as e:
                    summary["errors"].append(f"prefix_delete:{event_id}:{type(e).__name__}")
                    logger.warning(
                        "Failed prefix delete for user=%s calendar=%s event=%s: %s",
                        user_id,
                        calendar_id,
                        event_id,
                        e,
                    )

        # Build list of calendars to sweep so we can report progress.
        sweep_list: list[tuple[str, str, str]] = []  # (cal_id, email, label)
        if user["main_calendar_id"]:
            sweep_list.append((user["main_calendar_id"], user["email"], "main"))

        cursor = await db.execute(
            """SELECT cc.google_calendar_id, cc.display_name, ot.google_account_email
               FROM client_calendars cc
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE cc.user_id = ?""",
            (user_id,),
        )
        client_calendars = await cursor.fetchall()
        for calendar in client_calendars:
            cal_label = calendar["display_name"] or calendar["google_calendar_id"][:20]
            sweep_list.append((
                calendar["google_calendar_id"],
                calendar["google_account_email"],
                cal_label,
            ))

        for sweep_i, (cal_id, cal_email, cal_label) in enumerate(sweep_list):
            _update_cleanup_progress(
                user_id, step="sweeping_prefix",
                step_label=f"Scanning {cal_label} for leftover events...",
                current=sweep_i, total=len(sweep_list),
            )
            await _sweep_calendar(
                calendar_id=cal_id,
                account_email=cal_email,
                label=cal_label,
            )
        _update_cleanup_progress(
            user_id, step="sweeping_prefix",
            step_label="Calendar scan complete.",
            current=len(sweep_list), total=len(sweep_list),
        )
    else:
        summary["errors"].append("managed_event_prefix_not_configured")

    _update_cleanup_progress(
        user_id, step="cleaning_db",
        step_label="Cleaning up database records...",
        current=0, total=0,
    )

    # Local reset: only delete mappings whose remote events were successfully cleaned up.
    # Retain mappings that failed so they can be retried or cleaned manually.

    # Collect all mapping IDs for this user.
    cursor = await db.execute(
        "SELECT id FROM event_mappings WHERE user_id = ?",
        (user_id,),
    )
    all_mapping_ids = set(row["id"] for row in await cursor.fetchall())

    # Mappings not referenced by pass 1a (main-origin or already-deleted) are safe to remove
    # as long as their busy blocks were all cleaned (or they had none that failed).
    safe_to_delete = (all_mapping_ids - failed_mapping_ids)

    if safe_to_delete:
        placeholders = ",".join("?" * len(safe_to_delete))
        cursor = await db.execute(
            f"SELECT COUNT(*) FROM event_mappings WHERE id IN ({placeholders})",
            tuple(safe_to_delete),
        )
        summary["local_mappings_deleted"] = (await cursor.fetchone())[0]

        cursor = await db.execute(
            f"""SELECT COUNT(*) FROM busy_blocks
                WHERE event_mapping_id IN ({placeholders})""",
            tuple(safe_to_delete),
        )
        summary["local_busy_blocks_deleted"] = (await cursor.fetchone())[0]

        await db.execute(
            f"DELETE FROM event_mappings WHERE id IN ({placeholders})",
            tuple(safe_to_delete),
        )
    else:
        summary["local_mappings_deleted"] = 0
        summary["local_busy_blocks_deleted"] = 0

    await db.commit()

    if summary["errors"]:
        summary["status"] = "partial"

    logger.info("Managed cleanup completed for user %s: %s", user_id, summary)

    # Audit entry — errors list may be long; store it alongside the counts.
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'managed_cleanup', ?, ?)""",
        (
            user_id,
            summary["status"],
            json.dumps({
                **{k: v for k, v in summary.items() if k != "errors"},
                "error_count": len(summary["errors"]),
                "errors": summary["errors"],
            }),
        ),
    )
    await db.commit()

    return summary
