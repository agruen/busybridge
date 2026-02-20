"""Core sync engine."""

import json
import logging
from datetime import datetime
from typing import Optional

from app.database import get_database, get_setting
from app.sync.google_calendar import GoogleCalendarClient
from app.sync.rules import (
    sync_client_event_to_main,
    sync_main_event_to_clients,
    handle_deleted_client_event,
    handle_deleted_main_event,
)

logger = logging.getLogger(__name__)


async def is_sync_paused() -> bool:
    """Check if sync is globally paused."""
    setting = await get_setting("sync_paused")
    return setting and setting.get("value_plain") == "true"


async def trigger_sync_for_calendar(client_calendar_id: int) -> None:
    """Trigger sync for a specific client calendar."""
    if await is_sync_paused():
        logger.info("Sync is paused, skipping calendar sync")
        return

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
    main_calendar_id = calendar["main_calendar_id"]
    user_email = calendar["user_email"]

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
        event_errors = []

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

        # Log failure
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'sync', 'failure', ?)""",
            (
                user_id,
                client_calendar_id,
                json.dumps({"error": str(e)}),
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
        event_errors = []
        for event in events:
            try:
                if event.get("status") == "cancelled":
                    await handle_deleted_main_event(user_id, event["id"])
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

    except Exception as e:
        logger.exception(f"Error during calendar cleanup: {e}")
