"""Core sync engine."""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from app.config import get_settings
from app.database import get_database, get_setting
from app.sync.google_calendar import GoogleCalendarClient
from app.sync.rules import (
    sync_client_event_to_main,
    sync_main_event_to_clients,
    handle_deleted_client_event,
    handle_deleted_main_event,
)

logger = logging.getLogger(__name__)

# Per-calendar locks to prevent concurrent syncs (e.g. webhook + periodic overlap).
_calendar_locks: dict[str, asyncio.Lock] = {}
_calendar_locks_guard = asyncio.Lock()


async def _get_calendar_lock(key: str) -> asyncio.Lock:
    """Get or create an asyncio lock for a specific calendar sync key."""
    async with _calendar_locks_guard:
        if key not in _calendar_locks:
            _calendar_locks[key] = asyncio.Lock()
        return _calendar_locks[key]


def _summary_has_prefix(summary: Optional[str], prefix: str) -> bool:
    """Check whether an event summary starts with the configured visible prefix."""
    if not summary:
        return False

    normalized_prefix = (prefix or "").strip().lower()
    if not normalized_prefix:
        return False

    return summary.strip().lower().startswith(normalized_prefix)


async def is_sync_paused() -> bool:
    """Check if sync is globally paused."""
    setting = await get_setting("sync_paused")
    return setting and setting.get("value_plain") == "true"


async def trigger_sync_for_calendar(client_calendar_id: int) -> None:
    """Trigger sync for a specific client calendar."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping calendar sync")
        return

    lock = await _get_calendar_lock(f"client:{client_calendar_id}")
    if lock.locked():
        logger.info(f"Sync already in progress for calendar {client_calendar_id}, skipping")
        return

    async with lock:
        await _sync_client_calendar(client_calendar_id)


async def _sync_client_calendar(client_calendar_id: int) -> None:
    """Internal: perform sync for a client calendar (must be called under lock)."""
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

        # Fetch events from client calendar
        sync_token = calendar["sync_token"]
        result = client.list_events(calendar["google_calendar_id"], sync_token=sync_token)

        if result.get("sync_token_expired"):
            # Need full sync
            logger.info(f"Sync token expired for calendar {client_calendar_id}, doing full sync")
            result = client.list_events(calendar["google_calendar_id"])

        events = result["events"]
        new_sync_token = result.get("next_sync_token")

        logger.info(f"Syncing {len(events)} events from client calendar {client_calendar_id}")

        # Process each event
        synced_count = 0
        deleted_count = 0

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
                    )
                    deleted_count += 1
                else:
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

            except Exception as e:
                logger.error(f"Error processing event {event.get('id')}: {e}")
                event_errors.append(f"{event.get('id')}: {e}")

        if event_errors:
            raise RuntimeError(
                f"{len(event_errors)} event(s) failed while syncing calendar {client_calendar_id}. "
                f"First error: {event_errors[0]}"
            )

        # Update sync state
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
        await db.commit()

        # Log success
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


async def trigger_sync_for_main_calendar(user_id: int) -> None:
    """Trigger sync for a user's main calendar."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping main calendar sync")
        return

    lock = await _get_calendar_lock(f"main:{user_id}")
    if lock.locked():
        logger.info(f"Main calendar sync already in progress for user {user_id}, skipping")
        return

    async with lock:
        await _sync_main_calendar(user_id)


async def _sync_main_calendar(user_id: int) -> None:
    """Internal: perform main calendar sync (must be called under lock)."""

    db = await get_database()

    # Get user info
    cursor = await db.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    )
    user = await cursor.fetchone()

    if not user or not user["main_calendar_id"]:
        logger.warning(f"User {user_id} not found or no main calendar")
        return

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

        # Fetch events
        sync_token = sync_state["sync_token"] if sync_state else None
        is_full_sync = sync_token is None
        result = main_client.list_events(main_calendar_id, sync_token=sync_token)

        if result.get("sync_token_expired"):
            logger.info(f"Main calendar sync token expired for user {user_id}, doing full sync")
            result = main_client.list_events(main_calendar_id)
            is_full_sync = True

        events = result["events"]
        new_sync_token = result.get("next_sync_token")

        logger.info(f"Processing {len(events)} events from main calendar for user {user_id}")

        # Process events
        for event in events:
            try:
                if event.get("status") == "cancelled":
                    await handle_deleted_main_event(
                        user_id,
                        event["id"],
                        recurring_event_id=event.get("recurringEventId"),
                        original_start_time=event.get("originalStartTime"),
                    )
                else:
                    await sync_main_event_to_clients(
                        main_client=main_client,
                        event=event,
                        user_id=user_id,
                        main_calendar_id=main_calendar_id,
                        user_email=user_email,
                    )
            except Exception as e:
                logger.error(f"Error processing main event {event.get('id')}: {e}")
                event_errors.append(f"{event.get('id')}: {e}")

        if event_errors:
            raise RuntimeError(
                f"{len(event_errors)} event(s) failed while syncing main calendar for user {user_id}. "
                f"First error: {event_errors[0]}"
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
                json.dumps({"events_processed": len(events), "is_full_sync": is_full_sync}),
            )
        )
        await db.commit()

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


async def trigger_sync_for_user(user_id: int) -> None:
    """Trigger sync for all of a user's calendars."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping user sync")
        return

    db = await get_database()

    # Get all active client calendars
    cursor = await db.execute(
        """SELECT id FROM client_calendars
           WHERE user_id = ? AND is_active = TRUE""",
        (user_id,)
    )
    calendars = await cursor.fetchall()

    # Sync each client calendar
    for cal in calendars:
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

                for mapping in mappings:
                    mapping_cleanup_ok = True

                    if mapping["main_event_id"]:
                        try:
                            main_client.delete_event(calendar["main_calendar_id"], mapping["main_event_id"])
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

    client_cache: dict[str, GoogleCalendarClient] = {}
    token_failures: set[str] = set()

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

    # Track which mappings/blocks were successfully cleaned remotely so we only
    # delete DB records for those -- otherwise we lose tracking for retry.
    cleaned_mapping_ids: set[int] = set()
    failed_mapping_ids: set[int] = set()

    # Pass 1a: DB-driven cleanup of client-origin event copies on the main calendar.
    cursor = await db.execute(
        """SELECT id, main_event_id
           FROM event_mappings
           WHERE user_id = ? AND origin_type = 'client'
           AND deleted_at IS NULL AND main_event_id IS NOT NULL""",
        (user_id,),
    )
    client_origin_mappings = await cursor.fetchall()
    summary["db_client_mappings_checked"] = len(client_origin_mappings)

    if client_origin_mappings and user["main_calendar_id"]:
        main_client = await _get_client_for_email(user["email"])
        if main_client:
            for mapping in client_origin_mappings:
                try:
                    main_client.delete_event(user["main_calendar_id"], mapping["main_event_id"])
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

    # Pass 1b: DB-driven cleanup of busy blocks on client calendars.
    cursor = await db.execute(
        """SELECT bb.id, bb.busy_block_event_id, cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE em.user_id = ?""",
        (user_id,),
    )
    busy_blocks = await cursor.fetchall()
    summary["db_busy_blocks_checked"] = len(busy_blocks)

    for block in busy_blocks:
        client = await _get_client_for_email(block["google_account_email"])
        if not client:
            continue

        try:
            client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
            summary["db_busy_blocks_deleted"] += 1
        except Exception as e:
            summary["errors"].append(
                f"busy_block_delete:{block['busy_block_event_id']}:{type(e).__name__}"
            )
            logger.warning(
                "Failed DB-driven busy block delete for user=%s event=%s: %s",
                user_id,
                block["busy_block_event_id"],
                e,
            )

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
                events = client.search_events(calendar_id, managed_prefix)
                summary["prefix_calendars_scanned"] += 1
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
                if not _summary_has_prefix(event.get("summary"), managed_prefix):
                    continue

                summary["prefix_matches_found"] += 1
                try:
                    client.delete_event(calendar_id, event_id)
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

        if user["main_calendar_id"]:
            await _sweep_calendar(
                calendar_id=user["main_calendar_id"],
                account_email=user["email"],
                label="main",
            )

        cursor = await db.execute(
            """SELECT cc.google_calendar_id, ot.google_account_email
               FROM client_calendars cc
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE cc.user_id = ?""",
            (user_id,),
        )
        client_calendars = await cursor.fetchall()
        for calendar in client_calendars:
            await _sweep_calendar(
                calendar_id=calendar["google_calendar_id"],
                account_email=calendar["google_account_email"],
                label=f"client:{calendar['google_calendar_id']}",
            )
    else:
        summary["errors"].append("managed_event_prefix_not_configured")

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
