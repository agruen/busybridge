"""Consistency check logic."""

import logging
from datetime import datetime

from app.database import get_database
from app.sync.google_calendar import GoogleCalendarClient

logger = logging.getLogger(__name__)


async def run_consistency_check() -> dict:
    """
    Run consistency check across all users.

    Verifies:
    1. For each event_mapping, origin and synced copies still exist
    2. If origin deleted but copies remain: delete copies
    3. If copies deleted but origin remains: recreate copies

    Returns summary of actions taken.
    """
    db = await get_database()
    summary = {
        "users_checked": 0,
        "mappings_checked": 0,
        "orphaned_main_events_deleted": 0,
        "missing_copies_recreated": 0,
        "orphaned_busy_blocks_deleted": 0,
        "errors": 0,
    }

    # Get all users with main calendars
    cursor = await db.execute(
        "SELECT * FROM users WHERE main_calendar_id IS NOT NULL"
    )
    users = await cursor.fetchall()

    for user in users:
        summary["users_checked"] += 1

        try:
            await check_user_consistency(user["id"], summary)
        except Exception as e:
            logger.error(f"Consistency check failed for user {user['id']}: {e}")
            summary["errors"] += 1

    logger.info(f"Consistency check completed: {summary}")
    return summary


async def check_user_consistency(user_id: int, summary: dict) -> None:
    """Check consistency for a single user."""
    db = await get_database()

    from app.auth.google import get_valid_access_token

    # Get user info
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()

    if not user or not user["main_calendar_id"]:
        return

    try:
        main_token = await get_valid_access_token(user_id, user["email"])
        main_client = GoogleCalendarClient(main_token)
    except Exception as e:
        logger.warning(f"Cannot get main calendar access for user {user_id}: {e}")
        return

    # Check event mappings with client origins
    cursor = await db.execute(
        """SELECT em.*, cc.google_calendar_id, cc.display_name, ot.google_account_email
           FROM event_mappings em
           JOIN client_calendars cc ON em.origin_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE em.user_id = ? AND em.origin_type = 'client' AND em.deleted_at IS NULL
           AND cc.is_active = TRUE""",
        (user_id,)
    )
    client_mappings = await cursor.fetchall()

    for mapping in client_mappings:
        summary["mappings_checked"] += 1

        try:
            client_token = await get_valid_access_token(user_id, mapping["google_account_email"])
            client = GoogleCalendarClient(client_token)

            # Check if origin event still exists
            origin_event = client.get_event(mapping["google_calendar_id"], mapping["origin_event_id"])

            if not origin_event or origin_event.get("status") == "cancelled":
                # Origin deleted — clean up main copy
                if mapping["main_event_id"]:
                    try:
                        main_client.delete_event(user["main_calendar_id"], mapping["main_event_id"])
                        summary["orphaned_main_events_deleted"] += 1
                        logger.info(f"Deleted orphaned main event {mapping['main_event_id']}")
                    except Exception:
                        pass

                # Fetch busy blocks and delete them remotely before wiping DB rows.
                # Without this step the "Busy" events linger on client calendars
                # even after the source meeting is gone.
                bb_cursor = await db.execute(
                    """SELECT bb.id, bb.busy_block_event_id,
                              cc.google_calendar_id, ot.google_account_email
                       FROM busy_blocks bb
                       JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                       JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                       WHERE bb.event_mapping_id = ?""",
                    (mapping["id"],)
                )
                blocks_to_delete = await bb_cursor.fetchall()
                for block in blocks_to_delete:
                    try:
                        block_token = await get_valid_access_token(
                            user_id, block["google_account_email"]
                        )
                        block_client = GoogleCalendarClient(block_token)
                        block_client.delete_event(
                            block["google_calendar_id"], block["busy_block_event_id"]
                        )
                        await db.execute("DELETE FROM busy_blocks WHERE id = ?", (block["id"],))
                    except Exception as e:
                        logger.warning(
                            f"Failed to delete busy block {block['busy_block_event_id']}: {e}"
                        )
                        summary["errors"] += 1

                await db.execute(
                    "DELETE FROM event_mappings WHERE id = ?", (mapping["id"],)
                )
                await db.commit()
                continue

            # Check if main copy exists
            if mapping["main_event_id"]:
                main_event = main_client.get_event(user["main_calendar_id"], mapping["main_event_id"])
                if not main_event or main_event.get("status") == "cancelled":
                    # Main copy missing, recreate
                    from app.sync.google_calendar import copy_event_for_main
                    source_label = (
                        mapping["display_name"]
                        or mapping["google_calendar_id"]
                        or mapping["google_account_email"]
                    )
                    if mapping["google_account_email"] not in source_label:
                        source_label = f"{source_label} ({mapping['google_account_email']})"
                    new_event_data = copy_event_for_main(origin_event, source_label=source_label)

                    try:
                        result = main_client.create_event(user["main_calendar_id"], new_event_data)
                        await db.execute(
                            "UPDATE event_mappings SET main_event_id = ?, updated_at = ? WHERE id = ?",
                            (result["id"], datetime.utcnow().isoformat(), mapping["id"])
                        )
                        await db.commit()
                        summary["missing_copies_recreated"] += 1
                        logger.info(f"Recreated main event for mapping {mapping['id']}")
                    except Exception as e:
                        logger.error(f"Failed to recreate main event: {e}")

        except Exception as e:
            logger.error(f"Error checking mapping {mapping['id']}: {e}")
            summary["errors"] += 1

    # Check busy blocks
    cursor = await db.execute(
        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email, em.deleted_at as mapping_deleted
           FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,)
    )
    busy_blocks = await cursor.fetchall()

    for block in busy_blocks:
        if block["mapping_deleted"]:
            # Mapping was deleted — delete the remote busy block first, then the
            # DB row only if the remote delete succeeded.  Deleting the DB row
            # unconditionally (as before) would lose our handle on the event and
            # leave a permanent ghost "Busy" block on the client calendar.
            deleted_remotely = False
            try:
                client_token = await get_valid_access_token(user_id, block["google_account_email"])
                block_client = GoogleCalendarClient(client_token)
                block_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                deleted_remotely = True
                summary["orphaned_busy_blocks_deleted"] += 1
            except Exception as e:
                logger.warning(
                    f"Failed to delete orphaned busy block {block['busy_block_event_id']}: {e}"
                )
                summary["errors"] += 1

            if deleted_remotely:
                await db.execute("DELETE FROM busy_blocks WHERE id = ?", (block["id"],))
                await db.commit()
        else:
            # Block is active — verify it still exists on the client calendar.
            # If a user manually deleted it, remove the now-stale DB row so it
            # doesn't accumulate indefinitely.
            try:
                client_token = await get_valid_access_token(user_id, block["google_account_email"])
                block_client = GoogleCalendarClient(client_token)
                remote = block_client.get_event(
                    block["google_calendar_id"], block["busy_block_event_id"]
                )
                if not remote or remote.get("status") == "cancelled":
                    await db.execute("DELETE FROM busy_blocks WHERE id = ?", (block["id"],))
                    await db.commit()
                    logger.info(
                        f"Removed stale busy block DB record {block['id']} "
                        f"(event no longer exists remotely)"
                    )
            except Exception as e:
                logger.warning(
                    f"Could not verify busy block {block['busy_block_event_id']} existence: {e}"
                )


async def reconcile_calendar(client_calendar_id: int) -> dict:
    """
    Full reconciliation for a single calendar.

    Fetches all events and reconciles with database.
    """
    db = await get_database()
    summary = {
        "events_found": 0,
        "new_events_synced": 0,
        "stale_mappings_removed": 0,
        "busy_blocks_recreated": 0,
    }

    # Get calendar info
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, u.email as user_email,
                  u.main_calendar_id
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           JOIN users u ON cc.user_id = u.id
           WHERE cc.id = ? AND cc.is_active = TRUE""",
        (client_calendar_id,)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        return summary

    user_id = calendar["user_id"]

    try:
        from app.auth.google import get_valid_access_token

        client_token = await get_valid_access_token(user_id, calendar["google_account_email"])
        client = GoogleCalendarClient(client_token)

        # Get all events (no sync token)
        result = client.list_events(calendar["google_calendar_id"])
        events = result["events"]
        summary["events_found"] = len(events)

        current_event_ids = set()

        for event in events:
            if event.get("status") != "cancelled" and not client.is_our_event(event):
                current_event_ids.add(event["id"])

        # Find stale mappings
        cursor = await db.execute(
            """SELECT origin_event_id FROM event_mappings
               WHERE origin_calendar_id = ? AND deleted_at IS NULL""",
            (client_calendar_id,)
        )
        mapped_ids = set(row["origin_event_id"] for row in await cursor.fetchall())

        potentially_stale_ids = mapped_ids - current_event_ids

        # Verify each potentially-stale ID via get_event before acting on it.
        # list_events(singleEvents=False) only returns parent recurring events,
        # never individual instances.  Forked instance mappings (e.g.
        # "abc123_20260227T150000Z") would always appear absent from
        # current_event_ids and would be wrongly deleted without this check.
        stale_ids = set()
        for event_id in potentially_stale_ids:
            try:
                ev = client.get_event(calendar["google_calendar_id"], event_id)
                if not ev or ev.get("status") == "cancelled":
                    stale_ids.add(event_id)
                # else: event still exists (e.g. a forked instance) — not stale
            except Exception as e:
                logger.warning(
                    f"Could not verify event {event_id} existence, skipping: {e}"
                )
                # Skip rather than risk a false-positive deletion

        # For each stale mapping, clean up the remote main-calendar copy and busy
        # blocks BEFORE deleting the DB record. Only delete from DB if remote
        # cleanup succeeded so we retain tracking for retry on failure.
        actually_removed = 0
        for event_id in stale_ids:
            cursor = await db.execute(
                """SELECT id, main_event_id FROM event_mappings
                   WHERE origin_calendar_id = ? AND origin_event_id = ?""",
                (client_calendar_id, event_id)
            )
            mapping = await cursor.fetchone()
            if not mapping:
                continue

            cleanup_ok = True

            # Delete the main calendar copy
            if mapping["main_event_id"] and calendar["main_calendar_id"]:
                try:
                    main_token = await get_valid_access_token(user_id, calendar["user_email"])
                    main_client = GoogleCalendarClient(main_token)
                    main_client.delete_event(calendar["main_calendar_id"], mapping["main_event_id"])
                except Exception as e:
                    logger.warning(f"Failed to delete stale main event {mapping['main_event_id']}: {e}")
                    cleanup_ok = False

            # Delete associated busy blocks
            cursor = await db.execute(
                """SELECT bb.id, bb.busy_block_event_id, cc.google_calendar_id, ot.google_account_email
                   FROM busy_blocks bb
                   JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                   JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                   WHERE bb.event_mapping_id = ?""",
                (mapping["id"],)
            )
            blocks = await cursor.fetchall()
            for block in blocks:
                try:
                    block_token = await get_valid_access_token(user_id, block["google_account_email"])
                    block_client = GoogleCalendarClient(block_token)
                    block_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                except Exception as e:
                    logger.warning(f"Failed to delete stale busy block {block['busy_block_event_id']}: {e}")
                    cleanup_ok = False

            if cleanup_ok:
                await db.execute(
                    "DELETE FROM event_mappings WHERE id = ?",
                    (mapping["id"],)
                )
                actually_removed += 1
            else:
                logger.warning(
                    f"Retaining stale mapping {mapping['id']} for event {event_id} "
                    f"due to remote cleanup failures"
                )

        summary["stale_mappings_removed"] = actually_removed

        await db.commit()
        logger.info(f"Reconciliation completed for calendar {client_calendar_id}: {summary}")

    except Exception as e:
        logger.exception(f"Reconciliation failed for calendar {client_calendar_id}: {e}")

    return summary
