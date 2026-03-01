"""Sync rule implementations."""

import logging
from datetime import datetime
from typing import Optional

from googleapiclient.errors import HttpError

from app.database import get_database
from app.sync.google_calendar import (
    GoogleCalendarClient,
    create_busy_block,
    create_personal_busy_block,
    copy_event_for_main,
    should_create_busy_block,
    can_user_edit_event,
    derive_instance_event_id,
)

logger = logging.getLogger(__name__)


async def sync_client_event_to_main(
    client: GoogleCalendarClient,
    main_client: GoogleCalendarClient,
    event: dict,
    user_id: int,
    client_calendar_id: int,
    main_calendar_id: str,
    client_email: str,
    source_label: Optional[str] = None,
    color_id: Optional[str] = None,
) -> Optional[str]:
    """
    Sync a client calendar event to the main calendar.

    Returns the main event ID if created/updated.
    """
    db = await get_database()

    # Skip events we created (busy blocks)
    if client.is_our_event(event):
        logger.debug(f"Skipping our own event: {event.get('id')}")
        return None

    # Skip cancelled events (handle deletion separately)
    if event.get("status") == "cancelled":
        return None

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

    # Check if we already have a mapping for this event
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, client_calendar_id, event_id)
    )
    existing = await cursor.fetchone()

    # Prepare the event copy for main calendar
    main_event_data = copy_event_for_main(event, source_label=source_label, color_id=color_id)

    # Determine if user can edit
    user_can_edit = can_user_edit_event(event, client_email)

    # Parse event times
    start = event.get("start", {})
    end = event.get("end", {})
    is_all_day = "date" in start

    if is_all_day:
        event_start = start.get("date")
        event_end = end.get("date")
    else:
        event_start = start.get("dateTime")
        event_end = end.get("dateTime")

    is_recurring = "recurrence" in event or recurring_event_id is not None

    if existing:
        # Update existing mapping
        main_event_id = existing["main_event_id"]

        if main_event_id:
            try:
                main_client.update_event(main_calendar_id, main_event_id, main_event_data)
                logger.info(f"Updated main event {main_event_id} from client event {event_id}")
            except HttpError as e:
                if e.resp.status in (404, 410):
                    # Old event is gone -- safe to create a replacement
                    logger.warning(f"Main event {main_event_id} gone ({e.resp.status}), creating replacement")
                    result = main_client.create_event(main_calendar_id, main_event_data)
                    main_event_id = result["id"]
                else:
                    # Transient/server error -- re-raise so we don't create a duplicate
                    logger.error(f"Failed to update main event (HTTP {e.resp.status}): {e}")
                    raise
            except Exception as e:
                # Non-HTTP error (network timeout, etc.) -- re-raise to avoid orphaned duplicate
                logger.error(f"Failed to update main event: {e}")
                raise

        # Update mapping
        await db.execute(
            """UPDATE event_mappings SET
               main_event_id = ?, event_start = ?, event_end = ?,
               is_all_day = ?, is_recurring = ?, user_can_edit = ?, updated_at = ?
               WHERE id = ?""",
            (main_event_id, event_start, event_end, is_all_day, is_recurring,
             user_can_edit, datetime.utcnow().isoformat(), existing["id"])
        )
        await db.commit()

        return main_event_id

    else:
        # If this is a modified instance of a recurring series we already
        # track, cancel the old recurring occurrences at this time slot first
        # so we don't end up with duplicate busy blocks.
        if recurring_event_id:
            cursor = await db.execute(
                """SELECT * FROM event_mappings
                   WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
                (user_id, client_calendar_id, recurring_event_id)
            )
            parent_mapping = await cursor.fetchone()

            if parent_mapping:
                original_start_time = event.get("originalStartTime")
                if original_start_time:
                    # Cancel the instance on the main-calendar copy of the series.
                    if parent_mapping["main_event_id"]:
                        main_instance_id = derive_instance_event_id(
                            parent_mapping["main_event_id"], original_start_time
                        )
                        try:
                            main_client.delete_event(main_calendar_id, main_instance_id)
                            logger.info(
                                f"Cancelled series main instance {main_instance_id} "
                                "to fork modified occurrence"
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to cancel series main instance {main_instance_id}: {e}"
                            )

                    # Cancel the corresponding busy block instances.
                    cursor2 = await db.execute(
                        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
                           FROM busy_blocks bb
                           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                           WHERE bb.event_mapping_id = ?""",
                        (parent_mapping["id"],)
                    )
                    for block in await cursor2.fetchall():
                        bb_instance_id = derive_instance_event_id(
                            block["busy_block_event_id"], original_start_time
                        )
                        try:
                            from app.auth.google import get_valid_access_token
                            token = await get_valid_access_token(
                                user_id, block["google_account_email"]
                            )
                            cal_client = GoogleCalendarClient(token)
                            cal_client.delete_event(block["google_calendar_id"], bb_instance_id)
                            logger.info(f"Cancelled series busy block instance {bb_instance_id}")
                        except Exception as e:
                            logger.warning(
                                f"Failed to cancel series busy block instance {bb_instance_id}: {e}"
                            )

        # Create new standalone event on main calendar for this instance.
        try:
            result = main_client.create_event(main_calendar_id, main_event_data)
            main_event_id = result["id"]
            logger.info(f"Created main event {main_event_id} from client event {event_id}")
        except Exception as e:
            logger.error(f"Failed to create main event: {e}")
            return None

        # Create mapping
        await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                origin_recurring_event_id, main_event_id, event_start, event_end,
                is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'client', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, client_calendar_id, event_id, recurring_event_id,
             main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
        )
        await db.commit()

        return main_event_id


async def sync_main_event_to_clients(
    main_client: GoogleCalendarClient,
    event: dict,
    user_id: int,
    main_calendar_id: str,
    user_email: str,
) -> list[str]:
    """
    Sync a main calendar event to all client calendars as busy blocks.

    Returns list of busy block event IDs created.
    """
    db = await get_database()
    created_blocks = []

    # Skip events we created
    if main_client.is_our_event(event):
        logger.debug(f"Skipping our own event on main: {event.get('id')}")
        return []

    # Check if we should create busy blocks
    if not should_create_busy_block(event):
        logger.debug(f"Skipping event (no busy block needed): {event.get('id')}")
        return []

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

    # Check if this event originated from a client (skip if so - we don't double-sync)
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND main_event_id = ? AND origin_type = 'client'""",
        (user_id, event_id)
    )
    existing_client_origin = await cursor.fetchone()

    if existing_client_origin:
        # This event came from a client calendar, need to create busy blocks
        # on OTHER client calendars (not the origin)
        origin_calendar_id = existing_client_origin["origin_calendar_id"]
    else:
        # This is a native main calendar event
        origin_calendar_id = None

    # Get or create event mapping for main-origin events
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_type = 'main' AND origin_event_id = ?""",
        (user_id, event_id)
    )
    mapping = await cursor.fetchone()

    start = event.get("start", {})
    end = event.get("end", {})
    is_all_day = "date" in start

    if is_all_day:
        event_start = start.get("date")
        event_end = end.get("date")
    else:
        event_start = start.get("dateTime")
        event_end = end.get("dateTime")

    is_recurring = "recurrence" in event or recurring_event_id is not None

    if not mapping and not existing_client_origin:
        # Create mapping for main-origin event
        cursor = await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_event_id, origin_recurring_event_id,
                main_event_id, event_start, event_end, is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'main', ?, ?, ?, ?, ?, ?, ?, TRUE)
               RETURNING id""",
            (user_id, event_id, recurring_event_id, event_id,
             event_start, event_end, is_all_day, is_recurring)
        )
        mapping_row = await cursor.fetchone()
        mapping_id = mapping_row["id"]
        await db.commit()
    elif mapping:
        mapping_id = mapping["id"]
        # Update the mapping
        await db.execute(
            """UPDATE event_mappings SET
               event_start = ?, event_end = ?, is_all_day = ?, updated_at = ?
               WHERE id = ?""",
            (event_start, event_end, is_all_day, datetime.utcnow().isoformat(), mapping_id)
        )
        await db.commit()
    elif existing_client_origin:
        mapping_id = existing_client_origin["id"]

    # Get all active client calendars (exclude personal -- they are read-only)
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'""",
        (user_id,)
    )
    client_calendars = await cursor.fetchall()

    # Create busy block data
    busy_block = create_busy_block(start, end, is_all_day)

    # Copy recurrence if present
    if "recurrence" in event:
        busy_block["recurrence"] = event["recurrence"]

    for cal in client_calendars:
        # Skip the origin calendar
        if origin_calendar_id and cal["id"] == origin_calendar_id:
            continue

        # Check if busy block already exists
        cursor = await db.execute(
            """SELECT * FROM busy_blocks
               WHERE event_mapping_id = ? AND client_calendar_id = ?""",
            (mapping_id, cal["id"])
        )
        existing_block = await cursor.fetchone()

        try:
            from app.auth.google import get_valid_access_token
            client_token = await get_valid_access_token(user_id, cal["google_account_email"])
            client_calendar_client = GoogleCalendarClient(client_token)

            if existing_block:
                # Update existing busy block
                try:
                    client_calendar_client.update_event(
                        cal["google_calendar_id"],
                        existing_block["busy_block_event_id"],
                        busy_block
                    )
                    logger.info(f"Updated busy block {existing_block['busy_block_event_id']} on calendar {cal['id']}")
                except Exception as e:
                    logger.warning(f"Failed to update busy block, creating new: {e}")
                    # Fail-safe: create replacement first, then repoint DB record.
                    # If replacement creation fails, keep the original mapping so cleanup/retry is still possible.
                    try:
                        replacement = client_calendar_client.create_event(
                            cal["google_calendar_id"],
                            busy_block
                        )
                        replacement_id = replacement["id"]

                        await db.execute(
                            """UPDATE busy_blocks
                               SET busy_block_event_id = ?
                               WHERE id = ?""",
                            (replacement_id, existing_block["id"])
                        )
                        await db.commit()

                        created_blocks.append(replacement_id)
                        logger.info(
                            f"Created replacement busy block {replacement_id} on calendar {cal['id']}"
                        )

                        # Best-effort cleanup of the old block after DB tracking is updated.
                        try:
                            client_calendar_client.delete_event(
                                cal["google_calendar_id"],
                                existing_block["busy_block_event_id"]
                            )
                        except Exception as cleanup_error:
                            logger.warning(
                                f"Failed to delete old busy block {existing_block['busy_block_event_id']}: "
                                f"{cleanup_error}"
                            )

                    except Exception as create_error:
                        logger.error(
                            f"Failed to create replacement busy block on calendar {cal['id']}: {create_error}"
                        )

                    continue

            if not existing_block:
                # Create new busy block
                result = client_calendar_client.create_event(cal["google_calendar_id"], busy_block)
                block_id = result["id"]

                await db.execute(
                    """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
                       VALUES (?, ?, ?)""",
                    (mapping_id, cal["id"], block_id)
                )
                await db.commit()

                created_blocks.append(block_id)
                logger.info(f"Created busy block {block_id} on calendar {cal['id']}")

        except Exception as e:
            logger.error(f"Failed to create busy block on calendar {cal['id']}: {e}")

    return created_blocks


async def _handle_cancelled_recurring_instance(
    user_id: int,
    mapping: dict,
    original_start_time: dict,
    main_calendar_id: Optional[str],
    main_client: Optional[GoogleCalendarClient],
) -> None:
    """Cancel one instance of a recurring event's main-calendar copy and busy blocks.

    Called when a single occurrence of a tracked recurring series is cancelled.
    Derives the instance event ID for each managed event (main copy + each busy
    block) from the parent event ID and the occurrence's ``originalStartTime``,
    then issues a delete for that specific instance.  The parent recurring event
    and its DB mapping are left intact.
    """
    db = await get_database()

    # Cancel the corresponding instance on the main-calendar copy of the series.
    if main_client and main_calendar_id and mapping["main_event_id"]:
        main_instance_id = derive_instance_event_id(
            mapping["main_event_id"], original_start_time
        )
        try:
            main_client.delete_event(main_calendar_id, main_instance_id)
            logger.info(f"Cancelled main calendar instance {main_instance_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel main calendar instance {main_instance_id}: {e}")

    # Cancel the corresponding instance on each busy-block calendar.
    cursor = await db.execute(
        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE bb.event_mapping_id = ?""",
        (mapping["id"],)
    )
    busy_blocks = await cursor.fetchall()

    for block in busy_blocks:
        bb_instance_id = derive_instance_event_id(
            block["busy_block_event_id"], original_start_time
        )
        try:
            from app.auth.google import get_valid_access_token
            token = await get_valid_access_token(user_id, block["google_account_email"])
            cal_client = GoogleCalendarClient(token)
            cal_client.delete_event(block["google_calendar_id"], bb_instance_id)
            logger.info(f"Cancelled busy block instance {bb_instance_id}")
        except Exception as e:
            logger.warning(f"Failed to cancel busy block instance {bb_instance_id}: {e}")


async def handle_deleted_client_event(
    user_id: int,
    client_calendar_id: int,
    event_id: str,
    main_calendar_id: str,
    main_client: GoogleCalendarClient,
    recurring_event_id: Optional[str] = None,
    original_start_time: Optional[dict] = None,
) -> None:
    """Handle deletion of an event from a client calendar.

    When *recurring_event_id* is provided the deleted event is a single
    instance of a recurring series.  In that case we cancel just that one
    occurrence on the main-calendar copy and on every busy-block calendar,
    leaving the rest of the series intact.
    """
    db = await get_database()

    # ------------------------------------------------------------------ #
    # Single-instance cancellation of a recurring series                   #
    # ------------------------------------------------------------------ #
    if recurring_event_id:
        handled = False

        # Propagate the instance cancellation through the parent mapping.
        if original_start_time:
            cursor = await db.execute(
                """SELECT * FROM event_mappings
                   WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
                (user_id, client_calendar_id, recurring_event_id)
            )
            parent_mapping = await cursor.fetchone()

            if parent_mapping:
                await _handle_cancelled_recurring_instance(
                    user_id=user_id,
                    mapping=parent_mapping,
                    original_start_time=original_start_time,
                    main_calendar_id=main_calendar_id,
                    main_client=main_client,
                )
                handled = True
        else:
            logger.warning(
                f"Cancelled recurring instance {event_id} has no originalStartTime; "
                "cannot cancel specific occurrence"
            )

        # If we previously forked this instance (due to a prior modification),
        # clean up the standalone mapping and its events too.
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
            (user_id, client_calendar_id, event_id)
        )
        instance_mapping = await cursor.fetchone()

        if instance_mapping:
            if instance_mapping["main_event_id"]:
                try:
                    main_client.delete_event(main_calendar_id, instance_mapping["main_event_id"])
                    logger.info(
                        f"Deleted forked instance main event {instance_mapping['main_event_id']}"
                    )
                except Exception as e:
                    logger.error(f"Failed to delete forked instance main event: {e}")

            cursor = await db.execute(
                """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
                   FROM busy_blocks bb
                   JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                   JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                   WHERE bb.event_mapping_id = ?""",
                (instance_mapping["id"],)
            )
            forked_blocks = await cursor.fetchall()
            deleted_ids: set[int] = set()
            for block in forked_blocks:
                try:
                    from app.auth.google import get_valid_access_token
                    token = await get_valid_access_token(user_id, block["google_account_email"])
                    cal_client = GoogleCalendarClient(token)
                    cal_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                    deleted_ids.add(block["id"])
                except Exception as e:
                    logger.error(f"Failed to delete forked instance busy block: {e}")

            if deleted_ids:
                placeholders = ",".join("?" * len(deleted_ids))
                await db.execute(
                    f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
                    tuple(deleted_ids)
                )
            await db.execute("DELETE FROM event_mappings WHERE id = ?", (instance_mapping["id"],))
            await db.commit()
            handled = True

        if handled:
            logger.info(f"Handled cancelled recurring instance {event_id}")
        else:
            logger.debug(f"No mapping found for cancelled recurring instance {event_id}")
        return

    # ------------------------------------------------------------------ #
    # Full deletion of a non-instance event                                #
    # ------------------------------------------------------------------ #

    # Find the event mapping
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, client_calendar_id, event_id)
    )
    mapping = await cursor.fetchone()

    if not mapping:
        logger.debug(f"No mapping found for deleted event {event_id}")
        return

    # Delete the main calendar copy
    if mapping["main_event_id"]:
        try:
            main_client.delete_event(main_calendar_id, mapping["main_event_id"])
            logger.info(f"Deleted main event {mapping['main_event_id']}")
        except Exception as e:
            logger.error(f"Failed to delete main event: {e}")

    # Delete all busy blocks created for this event -- only remove DB records
    # for blocks that were successfully deleted remotely.
    cursor = await db.execute(
        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE bb.event_mapping_id = ?""",
        (mapping["id"],)
    )
    busy_blocks = await cursor.fetchall()

    deleted_block_ids: set[int] = set()
    for block in busy_blocks:
        try:
            from app.auth.google import get_valid_access_token
            token = await get_valid_access_token(user_id, block["google_account_email"])
            client = GoogleCalendarClient(token)
            client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
            deleted_block_ids.add(block["id"])
            logger.info(f"Deleted busy block {block['busy_block_event_id']}")
        except Exception as e:
            logger.error(f"Failed to delete busy block: {e}")

    # Only remove DB records for blocks confirmed deleted remotely
    if deleted_block_ids:
        placeholders = ",".join("?" * len(deleted_block_ids))
        await db.execute(
            f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
            tuple(deleted_block_ids),
        )

    # Soft delete the mapping (for recurring events) or hard delete
    if mapping["is_recurring"]:
        await db.execute(
            "UPDATE event_mappings SET deleted_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), mapping["id"])
        )
    else:
        await db.execute("DELETE FROM event_mappings WHERE id = ?", (mapping["id"],))

    await db.commit()
    logger.info(f"Cleaned up mapping for deleted event {event_id}")


async def handle_deleted_main_event(
    user_id: int,
    event_id: str,
    recurring_event_id: Optional[str] = None,
    original_start_time: Optional[dict] = None,
) -> None:
    """Handle deletion of an event from the main calendar.

    When *recurring_event_id* is provided the deleted event is a single
    instance of a recurring series.  In that case we cancel just that one
    occurrence on every busy-block calendar, leaving the rest of the series
    intact.
    """
    db = await get_database()

    # ------------------------------------------------------------------ #
    # Single-instance cancellation of a recurring series                   #
    # ------------------------------------------------------------------ #
    if recurring_event_id and original_start_time:
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND main_event_id = ?""",
            (user_id, recurring_event_id)
        )
        parent_mapping = await cursor.fetchone()

        if parent_mapping:
            # Cancel busy-block instances only (the main calendar is the source
            # of truth here, so no need to touch it again).
            await _handle_cancelled_recurring_instance(
                user_id=user_id,
                mapping=parent_mapping,
                original_start_time=original_start_time,
                main_calendar_id=None,
                main_client=None,
            )
            logger.info(f"Handled cancelled main recurring instance {event_id}")
        else:
            logger.debug(f"No mapping found for cancelled main recurring instance {event_id}")
        return

    # ------------------------------------------------------------------ #
    # Full deletion of a non-instance event                                #
    # ------------------------------------------------------------------ #

    # Find mappings for this main event
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND main_event_id = ?""",
        (user_id, event_id)
    )
    mapping = await cursor.fetchone()

    if not mapping:
        logger.debug(f"No mapping found for deleted main event {event_id}")
        return

    # If it's a client-origin event, handle appropriately
    if mapping["origin_type"] == "client" and mapping["origin_calendar_id"]:
        # User deleted the synced copy from main - need to decline/delete on client
        cursor = await db.execute(
            """SELECT cc.*, ot.google_account_email
               FROM client_calendars cc
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE cc.id = ?""",
            (mapping["origin_calendar_id"],)
        )
        cal = await cursor.fetchone()

        if cal:
            try:
                from app.auth.google import get_valid_access_token
                token = await get_valid_access_token(user_id, cal["google_account_email"])
                client = GoogleCalendarClient(token)
                client.delete_event(cal["google_calendar_id"], mapping["origin_event_id"])
                logger.info(f"Deleted client event {mapping['origin_event_id']}")
            except Exception as e:
                logger.warning(f"Failed to delete client event: {e}")

    # Delete all busy blocks for this event -- only remove DB records for
    # blocks that were successfully deleted remotely.
    cursor = await db.execute(
        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE bb.event_mapping_id = ?""",
        (mapping["id"],)
    )
    busy_blocks = await cursor.fetchall()

    deleted_block_ids: set[int] = set()
    for block in busy_blocks:
        try:
            from app.auth.google import get_valid_access_token
            token = await get_valid_access_token(user_id, block["google_account_email"])
            client = GoogleCalendarClient(token)
            client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
            deleted_block_ids.add(block["id"])
        except Exception as e:
            logger.error(f"Failed to delete busy block: {e}")

    # Only remove DB records for blocks confirmed deleted remotely
    if deleted_block_ids:
        placeholders = ",".join("?" * len(deleted_block_ids))
        await db.execute(
            f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
            tuple(deleted_block_ids),
        )

    if mapping["is_recurring"]:
        await db.execute(
            "UPDATE event_mappings SET deleted_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), mapping["id"])
        )
    else:
        await db.execute("DELETE FROM event_mappings WHERE id = ?", (mapping["id"],))

    await db.commit()


# ------------------------------------------------------------------ #
# Personal calendar sync rules                                        #
# ------------------------------------------------------------------ #


async def sync_personal_event_to_all(
    personal_client: GoogleCalendarClient,
    main_client: GoogleCalendarClient,
    event: dict,
    user_id: int,
    personal_calendar_id: int,
    main_calendar_id: str,
    user_email: str,
) -> Optional[str]:
    """
    Sync a personal calendar event to all calendars as busy blocks.

    Unlike client events which get full copies on the main calendar,
    personal events create ONLY busy blocks everywhere:
    - A busy block on the main calendar (stored as main_event_id)
    - Busy blocks on ALL client calendars (stored in busy_blocks table)

    Personal calendars never receive busy blocks -- they are read-only
    sources of availability information.

    Returns the main busy block event ID if created/updated.
    """
    db = await get_database()

    # Skip events we created (our own busy blocks)
    if personal_client.is_our_event(event):
        logger.debug(f"Skipping our own event on personal: {event.get('id')}")
        return None

    # Skip cancelled events
    if event.get("status") == "cancelled":
        return None

    # Skip events that shouldn't create busy blocks (declined, free all-day, etc.)
    if not should_create_busy_block(event):
        logger.debug(f"Skipping personal event (no busy block needed): {event.get('id')}")
        return None

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

    # Parse event times
    start = event.get("start", {})
    end = event.get("end", {})
    is_all_day = "date" in start

    if is_all_day:
        event_start = start.get("date")
        event_end = end.get("date")
    else:
        event_start = start.get("dateTime")
        event_end = end.get("dateTime")

    is_recurring = "recurrence" in event or recurring_event_id is not None

    # Check for existing mapping
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, personal_calendar_id, event_id)
    )
    existing = await cursor.fetchone()

    # Build the personal busy block for the main calendar
    busy_block_data = create_personal_busy_block(start, end, is_all_day)
    if "recurrence" in event:
        busy_block_data["recurrence"] = event["recurrence"]

    if existing:
        # Update existing mapping
        main_event_id = existing["main_event_id"]

        if main_event_id:
            try:
                main_client.update_event(main_calendar_id, main_event_id, busy_block_data)
                logger.info(f"Updated personal busy block {main_event_id} on main calendar")
            except HttpError as e:
                if e.resp.status in (404, 410):
                    logger.warning(f"Personal busy block {main_event_id} gone, recreating")
                    result = main_client.create_event(main_calendar_id, busy_block_data)
                    main_event_id = result["id"]
                else:
                    raise
            except Exception:
                raise

        await db.execute(
            """UPDATE event_mappings SET
               main_event_id = ?, event_start = ?, event_end = ?,
               is_all_day = ?, is_recurring = ?, updated_at = ?
               WHERE id = ?""",
            (main_event_id, event_start, event_end, is_all_day, is_recurring,
             datetime.utcnow().isoformat(), existing["id"])
        )
        await db.commit()

        mapping_id = existing["id"]
    else:
        # Create the busy block on the main calendar
        try:
            result = main_client.create_event(main_calendar_id, busy_block_data)
            main_event_id = result["id"]
            logger.info(f"Created personal busy block {main_event_id} on main calendar")
        except Exception as e:
            logger.error(f"Failed to create personal busy block on main: {e}")
            return None

        # Create mapping with origin_type='personal'
        cursor = await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                origin_recurring_event_id, main_event_id, event_start, event_end,
                is_all_day, is_recurring, user_can_edit)
               VALUES (?, 'personal', ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
               RETURNING id""",
            (user_id, personal_calendar_id, event_id, recurring_event_id,
             main_event_id, event_start, event_end, is_all_day, is_recurring)
        )
        row = await cursor.fetchone()
        mapping_id = row["id"]
        await db.commit()

    # Create/update busy blocks on all CLIENT calendars only (not personal calendars).
    # Personal calendars are read-only sources -- they never receive busy blocks.
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'""",
        (user_id,)
    )
    client_calendars = await cursor.fetchall()

    client_busy_block = create_personal_busy_block(start, end, is_all_day)
    if "recurrence" in event:
        client_busy_block["recurrence"] = event["recurrence"]

    for cal in client_calendars:
        cursor = await db.execute(
            """SELECT * FROM busy_blocks
               WHERE event_mapping_id = ? AND client_calendar_id = ?""",
            (mapping_id, cal["id"])
        )
        existing_block = await cursor.fetchone()

        try:
            from app.auth.google import get_valid_access_token
            client_token = await get_valid_access_token(user_id, cal["google_account_email"])
            cal_client = GoogleCalendarClient(client_token)

            if existing_block:
                try:
                    cal_client.update_event(
                        cal["google_calendar_id"],
                        existing_block["busy_block_event_id"],
                        client_busy_block
                    )
                except Exception as e:
                    logger.warning(f"Failed to update personal busy block, recreating: {e}")
                    try:
                        replacement = cal_client.create_event(
                            cal["google_calendar_id"], client_busy_block
                        )
                        await db.execute(
                            "UPDATE busy_blocks SET busy_block_event_id = ? WHERE id = ?",
                            (replacement["id"], existing_block["id"])
                        )
                        await db.commit()
                    except Exception as create_error:
                        logger.error(f"Failed to recreate personal busy block: {create_error}")
            else:
                result = cal_client.create_event(cal["google_calendar_id"], client_busy_block)
                await db.execute(
                    """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
                       VALUES (?, ?, ?)""",
                    (mapping_id, cal["id"], result["id"])
                )
                await db.commit()
                logger.info(f"Created personal busy block on client calendar {cal['id']}")

        except Exception as e:
            logger.error(f"Failed to sync personal busy block to calendar {cal['id']}: {e}")

    return main_event_id


async def handle_deleted_personal_event(
    user_id: int,
    personal_calendar_id: int,
    event_id: str,
    main_calendar_id: str,
    main_client: GoogleCalendarClient,
    recurring_event_id: Optional[str] = None,
    original_start_time: Optional[dict] = None,
) -> None:
    """Handle deletion of an event from a personal calendar.

    Deletes the corresponding busy blocks from the main calendar and all
    client calendars.
    """
    db = await get_database()

    # Single-instance cancellation of a recurring series
    if recurring_event_id:
        if original_start_time:
            cursor = await db.execute(
                """SELECT * FROM event_mappings
                   WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
                (user_id, personal_calendar_id, recurring_event_id)
            )
            parent_mapping = await cursor.fetchone()

            if parent_mapping:
                await _handle_cancelled_recurring_instance(
                    user_id=user_id,
                    mapping=parent_mapping,
                    original_start_time=original_start_time,
                    main_calendar_id=main_calendar_id,
                    main_client=main_client,
                )

        # Clean up forked instance mapping if it exists
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
            (user_id, personal_calendar_id, event_id)
        )
        instance_mapping = await cursor.fetchone()

        if instance_mapping:
            if instance_mapping["main_event_id"]:
                try:
                    main_client.delete_event(main_calendar_id, instance_mapping["main_event_id"])
                except Exception as e:
                    logger.error(f"Failed to delete forked personal busy block from main: {e}")

            cursor = await db.execute(
                """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
                   FROM busy_blocks bb
                   JOIN client_calendars cc ON bb.client_calendar_id = cc.id
                   JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
                   WHERE bb.event_mapping_id = ?""",
                (instance_mapping["id"],)
            )
            forked_blocks = await cursor.fetchall()
            deleted_ids: set[int] = set()
            for block in forked_blocks:
                try:
                    from app.auth.google import get_valid_access_token
                    token = await get_valid_access_token(user_id, block["google_account_email"])
                    cal_client = GoogleCalendarClient(token)
                    cal_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                    deleted_ids.add(block["id"])
                except Exception as e:
                    logger.error(f"Failed to delete forked personal busy block: {e}")

            if deleted_ids:
                placeholders = ",".join("?" * len(deleted_ids))
                await db.execute(
                    f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
                    tuple(deleted_ids)
                )
            await db.execute("DELETE FROM event_mappings WHERE id = ?", (instance_mapping["id"],))
            await db.commit()

        return

    # Full deletion of a non-instance event
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, personal_calendar_id, event_id)
    )
    mapping = await cursor.fetchone()

    if not mapping:
        logger.debug(f"No mapping found for deleted personal event {event_id}")
        return

    # Delete the busy block from the main calendar
    if mapping["main_event_id"]:
        try:
            main_client.delete_event(main_calendar_id, mapping["main_event_id"])
            logger.info(f"Deleted personal busy block {mapping['main_event_id']} from main")
        except Exception as e:
            logger.error(f"Failed to delete personal busy block from main: {e}")

    # Delete busy blocks from all client calendars
    cursor = await db.execute(
        """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
           FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE bb.event_mapping_id = ?""",
        (mapping["id"],)
    )
    personal_busy_blocks = await cursor.fetchall()

    deleted_block_ids: set[int] = set()
    for block in personal_busy_blocks:
        try:
            from app.auth.google import get_valid_access_token
            token = await get_valid_access_token(user_id, block["google_account_email"])
            client = GoogleCalendarClient(token)
            client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
            deleted_block_ids.add(block["id"])
        except Exception as e:
            logger.error(f"Failed to delete personal busy block: {e}")

    if deleted_block_ids:
        placeholders = ",".join("?" * len(deleted_block_ids))
        await db.execute(
            f"DELETE FROM busy_blocks WHERE id IN ({placeholders})",
            tuple(deleted_block_ids),
        )

    if mapping["is_recurring"]:
        await db.execute(
            "UPDATE event_mappings SET deleted_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), mapping["id"])
        )
    else:
        await db.execute("DELETE FROM event_mappings WHERE id = ?", (mapping["id"],))

    await db.commit()
    logger.info(f"Cleaned up mapping for deleted personal event {event_id}")
