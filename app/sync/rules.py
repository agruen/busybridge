"""Sync rule implementations."""

import logging
from datetime import datetime
from typing import Optional

from googleapiclient.errors import HttpError

from app.database import get_database
from app.sync.google_calendar import (
    AsyncGoogleCalendarClient,
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
    main_email: Optional[str] = None,
    sa_main_client: Optional[GoogleCalendarClient] = None,
) -> tuple[Optional[str], bool]:
    """
    Sync a client calendar event to the main calendar.

    Returns (main_event_id, times_changed) — the main event ID if
    created/updated, and whether the event times changed compared
    to the previously stored mapping.
    """
    db = await get_database()

    # Skip events we created (busy blocks)
    if client.is_our_event(event):
        logger.debug(f"Skipping our own event: {event.get('id')}")
        return None, False

    # Skip cancelled events (handle deletion separately)
    if event.get("status") == "cancelled":
        return None, False

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

    # Check if we already have a mapping for this event
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, client_calendar_id, event_id)
    )
    existing = await cursor.fetchone()

    # If this is a rescheduled recurring instance (_R prefix) and we don't
    # have a mapping for it, look for the original instance it replaced.
    # This prevents duplicate events when Google sends both the cancelled
    # original and the replacement.
    if not existing and recurring_event_id:
        original_start = event.get("originalStartTime", {})
        orig_dt = original_start.get("dateTime") or original_start.get("date", "")
        if orig_dt:
            cursor = await db.execute(
                """SELECT * FROM event_mappings
                   WHERE user_id = ? AND origin_calendar_id = ?
                     AND origin_event_id LIKE ? AND deleted_at IS NULL""",
                (user_id, client_calendar_id, f"{recurring_event_id}_%")
            )
            candidates = await cursor.fetchall()
            for candidate in candidates:
                cand_start = candidate["event_start"] or ""
                if orig_dt in cand_start or cand_start in orig_dt:
                    existing = candidate
                    await db.execute(
                        "UPDATE event_mappings SET origin_event_id = ?, updated_at = ? WHERE id = ?",
                        (event_id, datetime.utcnow().isoformat(), candidate["id"])
                    )
                    await db.commit()
                    logger.info(
                        f"Re-keyed mapping {candidate['id']} from "
                        f"{candidate['origin_event_id']} to {event_id}"
                    )
                    break

    # Determine if user can edit
    user_can_edit = can_user_edit_event(event, client_email)

    # Always use the user's own token for client-event copies on main.
    # This keeps events movable and enables native RSVP buttons for
    # invites.  The SA is reserved for personal busy blocks on main
    # (immovable placeholders).
    use_sa = False
    write_client = main_client

    # Determine the current RSVP status.  Prefer the live value from the
    # client event's attendees (covers "accepted from email" etc.) and fall
    # back to the DB value so we don't accidentally reset it.
    client_rsvp = None
    if event.get("attendees") and client_email:
        for att in event["attendees"]:
            if att.get("email", "").lower() == client_email.lower() or att.get("self"):
                client_rsvp = att.get("responseStatus")
                break
    current_rsvp = client_rsvp or (existing["rsvp_status"] if existing else None)
    copy_origin_props = {
        "bb_origin_id": event_id,
        "bb_origin_cal": client_calendar_id,
        "bb_type": "client_copy",
    }
    if existing:
        copy_origin_props["bb_mapping_id"] = str(existing["id"])

    main_event_data = copy_event_for_main(
        event,
        source_label=source_label,
        color_id=color_id,
        main_email=main_email,
        current_rsvp_status=current_rsvp,
        user_can_edit=user_can_edit,
        use_service_account=use_sa,
        origin_props=copy_origin_props,
    )

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

        # Track whether times changed (used by caller to skip busy block updates)
        _times_changed = (
            (existing["event_start"] or "") != event_start
            or (existing["event_end"] or "") != event_end
            or bool(existing["is_all_day"]) != is_all_day
        )

        if main_event_id:
            try:
                await write_client.update_event(main_calendar_id, main_event_id, main_event_data)
                logger.info(f"Updated main event {main_event_id} from client event {event_id}")
            except HttpError as e:
                if e.resp.status in (404, 410):
                    # Old event is gone — create replacement
                    logger.warning(f"Main event {main_event_id} gone ({e.resp.status}), creating replacement")
                    result = await write_client.create_event(main_calendar_id, main_event_data)
                    main_event_id = result["id"]
                else:
                    # Transient/server error -- re-raise so we don't create a duplicate
                    logger.error(f"Failed to update main event (HTTP {e.resp.status}): {e}")
                    raise
            except Exception as e:
                # Non-HTTP error (network timeout, etc.) -- re-raise to avoid orphaned duplicate
                logger.error(f"Failed to update main event: {e}")
                raise

        # Keep stored RSVP in sync: use client-side RSVP when available,
        # otherwise preserve the DB value (or start tracking if newly an invite).
        new_rsvp = client_rsvp or existing["rsvp_status"]
        if new_rsvp is None and event.get("attendees") and main_email:
            new_rsvp = "needsAction"

        # Update mapping
        await db.execute(
            """UPDATE event_mappings SET
               main_event_id = ?, event_start = ?, event_end = ?,
               is_all_day = ?, is_recurring = ?, user_can_edit = ?,
               rsvp_status = ?, updated_at = ?
               WHERE id = ?""",
            (main_event_id, event_start, event_end, is_all_day, is_recurring,
             user_can_edit, new_rsvp, datetime.utcnow().isoformat(), existing["id"])
        )
        await db.commit()

        return main_event_id, _times_changed

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
                            await main_client.delete_event(main_calendar_id, main_instance_id)
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
                            cal_client = AsyncGoogleCalendarClient(token)
                            await cal_client.delete_event(block["google_calendar_id"], bb_instance_id)
                            logger.info(f"Cancelled series busy block instance {bb_instance_id}")
                        except Exception as e:
                            logger.warning(
                                f"Failed to cancel series busy block instance {bb_instance_id}: {e}"
                            )

        # Create new main calendar copy.  Orphaned copies from prior crashes
        # are cleaned up by the periodic orphan scanner (every 6 hours).
        try:
            result = await write_client.create_event(main_calendar_id, main_event_data)
            main_event_id = result["id"]
            logger.info(f"Created main event {main_event_id} from client event {event_id}")
        except Exception as e:
            logger.error(f"Failed to create main event: {e}")
            return None, False

        # Track RSVP status for events with attendees (invites).
        # Use the live client RSVP (e.g. already "accepted") so we don't
        # lose an RSVP that happened before the first sync.
        initial_rsvp = current_rsvp if (event.get("attendees") and main_email) else None
        if initial_rsvp is None and event.get("attendees") and main_email:
            initial_rsvp = "needsAction"

        # Create mapping
        cursor = await db.execute(
            """INSERT INTO event_mappings
               (user_id, origin_type, origin_calendar_id, origin_event_id,
                origin_recurring_event_id, main_event_id, event_start, event_end,
                is_all_day, is_recurring, user_can_edit, rsvp_status)
               VALUES (?, 'client', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               RETURNING id""",
            (user_id, client_calendar_id, event_id, recurring_event_id,
             main_event_id, event_start, event_end, is_all_day, is_recurring,
             user_can_edit, initial_rsvp)
        )
        new_row = await cursor.fetchone()
        await db.commit()

        # Stamp the mapping ID onto the main event for full traceability
        if new_row and main_event_id:
            try:
                await write_client.patch_event(main_calendar_id, main_event_id, {
                    "extendedProperties": {"private": {"bb_mapping_id": str(new_row["id"])}},
                })
            except Exception:
                pass  # Non-critical — origin_id is the primary dedup key

        return main_event_id, True  # New event — times always "changed"


async def _propagate_cancelled_instances(
    source_client: GoogleCalendarClient,
    source_calendar_id: str,
    source_recurring_id: str,
    target_client: GoogleCalendarClient,
    target_calendar_id: str,
    target_recurring_id: str,
) -> int:
    """Cancel instances on a target recurring event that are cancelled on the source.

    After creating a recurring busy block that mirrors a source recurring
    event, the busy block blindly expands all instances — including ones
    that were cancelled on the source.  This function queries the source
    for cancelled instances and cancels the matching instances on the target.

    Returns the number of instances cancelled.
    """
    cancelled = await source_client.list_cancelled_instances(
        source_calendar_id, source_recurring_id
    )
    if not cancelled:
        return 0

    count = 0
    for inst in cancelled:
        original_start = inst.get("originalStartTime")
        if not original_start:
            continue
        try:
            instance_id = derive_instance_event_id(target_recurring_id, original_start)
            await target_client.delete_event(target_calendar_id, instance_id)
            count += 1
        except Exception:
            pass  # Instance may not exist yet or already cancelled
    if count:
        logger.info(
            f"Cancelled {count} instance(s) on busy block {target_recurring_id[:25]} "
            f"to match source {source_recurring_id[:25]}"
        )
    return count


async def sync_main_event_to_clients(
    main_client: GoogleCalendarClient,
    event: dict,
    user_id: int,
    main_calendar_id: str,
    user_email: str,
    *,
    skip_busy_block_update: bool = False,
) -> list[str]:
    """
    Sync a main calendar event to all client calendars as busy blocks.

    Returns list of busy block event IDs created.
    """
    db = await get_database()
    created_blocks = []

    # Check if we should create busy blocks
    if not should_create_busy_block(event):
        logger.debug(f"Skipping event (no busy block needed): {event.get('id')}")
        return []

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

    # Check if this event originated from a client or personal sync (skip personal entirely)
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND main_event_id = ? AND origin_type IN ('client', 'personal')""",
        (user_id, event_id)
    )
    existing_origin = await cursor.fetchone()

    if existing_origin and existing_origin["origin_type"] == "personal":
        # This event is a personal busy block we created on main — skip entirely
        # (the personal sync path already created busy blocks on client calendars)
        logger.debug(f"Skipping personal-origin event on main: {event_id}")
        return []

    if existing_origin:
        # This event came from a client calendar, need to create busy blocks
        # on OTHER client calendars (not the origin)
        origin_calendar_id = existing_origin["origin_calendar_id"]
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

    if not mapping and not existing_origin:
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
    elif existing_origin:
        mapping_id = existing_origin["id"]

    # Get all active client calendars (exclude personal calendars — they are read-only)
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'""",
        (user_id,)
    )
    client_calendars = await cursor.fetchall()

    # Check if times actually changed — skip busy block updates if they haven't.
    # Busy blocks only carry time data, so unchanged times = unchanged busy block.
    #
    # For client-origin events, the caller controls this via skip_busy_block_update.
    # We must NOT second-guess the caller by comparing against existing_origin,
    # because the mapping's stored times are already updated by the time we run.
    #
    # For main-origin events, compare against the pre-update `mapping` row.
    # ORDERING NOTE: `mapping` holds the row fetched BEFORE the UPDATE at
    # line ~390.  Do NOT re-fetch between the UPDATE and this comparison.
    times_changed = True
    if skip_busy_block_update:
        times_changed = False
    elif not existing_origin and mapping:
        # Main-origin only: compare against pre-update mapping times.
        try:
            prev_start = mapping["event_start"] or ""
            prev_end = mapping["event_end"] or ""
            times_changed = (
                prev_start != event_start
                or prev_end != event_end
                or bool(mapping["is_all_day"]) != is_all_day
            )
        except (KeyError, TypeError):
            times_changed = True

    # Create busy block data
    bb_origin_props = {
        "bb_origin_id": event.get("id", ""),
        "bb_mapping_id": str(mapping_id),
        "bb_type": "busy_block",
    }
    busy_block = create_busy_block(start, end, is_all_day, origin_props=bb_origin_props)

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
            client_calendar_client = AsyncGoogleCalendarClient(client_token)

            if existing_block:
                if not times_changed:
                    continue  # Busy block is already correct — skip the API call
                try:
                    await client_calendar_client.update_event(
                        cal["google_calendar_id"],
                        existing_block["busy_block_event_id"],
                        busy_block
                    )
                    logger.info(f"Updated busy block {existing_block['busy_block_event_id']} on calendar {cal['id']}")
                except Exception as e:
                    logger.warning(f"Failed to update busy block, creating new: {e}")
                    try:
                        replacement = await client_calendar_client.create_event(
                            cal["google_calendar_id"], busy_block,
                        )
                        await db.execute(
                            "UPDATE busy_blocks SET busy_block_event_id = ? WHERE id = ?",
                            (replacement["id"], existing_block["id"]),
                        )
                        await db.commit()
                        created_blocks.append(replacement["id"])
                        logger.info(f"Created replacement busy block {replacement['id']} on calendar {cal['id']}")
                        try:
                            await client_calendar_client.delete_event(
                                cal["google_calendar_id"],
                                existing_block["busy_block_event_id"],
                            )
                        except Exception:
                            pass
                        if "recurrence" in event:
                            await _propagate_cancelled_instances(
                                source_client=main_client,
                                source_calendar_id=main_calendar_id,
                                source_recurring_id=event_id,
                                target_client=client_calendar_client,
                                target_calendar_id=cal["google_calendar_id"],
                                target_recurring_id=replacement["id"],
                            )
                    except Exception as create_error:
                        logger.error(f"Failed to create replacement busy block on calendar {cal['id']}: {create_error}")
                    continue

            if not existing_block:
                result = await client_calendar_client.create_event(cal["google_calendar_id"], busy_block)
                block_id = result["id"]

                await db.execute(
                    """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
                       VALUES (?, ?, ?)""",
                    (mapping_id, cal["id"], block_id)
                )
                await db.commit()

                created_blocks.append(block_id)
                logger.info(f"Created busy block {block_id} on calendar {cal['id']}")

                # For recurring events, cancel instances that are cancelled
                # on the main calendar so the busy block doesn't show phantom
                # occurrences.
                if "recurrence" in event:
                    await _propagate_cancelled_instances(
                        source_client=main_client,
                        source_calendar_id=main_calendar_id,
                        source_recurring_id=event_id,
                        target_client=client_calendar_client,
                        target_calendar_id=cal["google_calendar_id"],
                        target_recurring_id=block_id,
                    )

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
            await main_client.delete_event(main_calendar_id, main_instance_id)
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
            cal_client = AsyncGoogleCalendarClient(token)
            await cal_client.delete_event(block["google_calendar_id"], bb_instance_id)
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
    sa_main_client: Optional[GoogleCalendarClient] = None,
) -> None:
    """Handle deletion of an event from a client calendar.

    When *recurring_event_id* is provided the deleted event is a single
    instance of a recurring series.  In that case we cancel just that one
    occurrence on the main-calendar copy and on every busy-block calendar,
    leaving the rest of the series intact.
    """
    db = await get_database()

    # Use SA client for main calendar writes when available
    write_client = sa_main_client if sa_main_client is not None else main_client

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
                if parent_mapping["deleted_at"]:
                    logger.debug(f"Skipping cancelled instance {event_id} — parent already deleted")
                    return

                await _handle_cancelled_recurring_instance(
                    user_id=user_id,
                    mapping=parent_mapping,
                    original_start_time=original_start_time,
                    main_calendar_id=main_calendar_id,
                    main_client=write_client,
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
                    await write_client.delete_event(main_calendar_id, instance_mapping["main_event_id"])
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
                    cal_client = AsyncGoogleCalendarClient(token)
                    await cal_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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
            await write_client.delete_event(main_calendar_id, mapping["main_event_id"])
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
            client = AsyncGoogleCalendarClient(token)
            await client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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


async def propagate_rsvp_to_client(
    user_id: int,
    main_event: dict,
    mapping: dict,
    new_rsvp_status: str,
) -> bool:
    """Propagate an RSVP response from the main calendar copy back to the client calendar.

    Patches the client event's attendee list with the new responseStatus and
    updates the stored rsvp_status in event_mappings.

    Returns True if the propagation succeeded.
    """
    db = await get_database()

    # Look up the client calendar and its OAuth credentials.
    cursor = await db.execute(
        """SELECT cc.google_calendar_id, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.id = ?""",
        (mapping["origin_calendar_id"],)
    )
    cal = await cursor.fetchone()
    if not cal:
        logger.warning(
            f"Cannot propagate RSVP: client calendar {mapping['origin_calendar_id']} not found"
        )
        return False

    client_email = cal["google_account_email"]

    try:
        from app.auth.google import get_valid_access_token
        token = await get_valid_access_token(user_id, client_email)
        client = AsyncGoogleCalendarClient(token)

        # Patch only the attendee response on the client event.
        await client.patch_event(
            cal["google_calendar_id"],
            mapping["origin_event_id"],
            {"attendees": [{"email": client_email, "responseStatus": new_rsvp_status}]},
            send_updates="none",
        )

        # Persist the new status so future syncs don't re-trigger propagation.
        await db.execute(
            "UPDATE event_mappings SET rsvp_status = ?, updated_at = ? WHERE id = ?",
            (new_rsvp_status, datetime.utcnow().isoformat(), mapping["id"]),
        )
        await db.commit()

        logger.info(
            f"Propagated RSVP '{new_rsvp_status}' to client event "
            f"{mapping['origin_event_id']} on calendar {cal['google_calendar_id']}"
        )
        return True

    except Exception as e:
        logger.error(
            f"Failed to propagate RSVP to client event {mapping['origin_event_id']}: {e}"
        )
        return False


async def propagate_time_to_client(
    user_id: int,
    main_event: dict,
    mapping: dict,
) -> bool:
    """Propagate a time/date change from the main calendar copy back to the client calendar.

    When the user moves an editable event on the main calendar, this patches
    the client origin event with the new start/end and updates the mapping DB
    so future syncs don't bounce.

    Returns True if the propagation succeeded.
    """
    db = await get_database()

    start = main_event.get("start", {})
    end = main_event.get("end", {})
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
    try:
        times_match = (
            datetime.fromisoformat(current_start) == datetime.fromisoformat(stored_start)
            and datetime.fromisoformat(current_end) == datetime.fromisoformat(stored_end)
        )
    except (ValueError, TypeError):
        times_match = current_start == stored_start and current_end == stored_end

    if times_match:
        return False

    # Look up the client calendar and its OAuth credentials.
    cursor = await db.execute(
        """SELECT cc.google_calendar_id, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.id = ?""",
        (mapping["origin_calendar_id"],)
    )
    cal = await cursor.fetchone()
    if not cal:
        logger.warning(
            f"Cannot propagate time change: client calendar {mapping['origin_calendar_id']} not found"
        )
        return False

    client_email = cal["google_account_email"]

    try:
        from app.auth.google import get_valid_access_token
        token = await get_valid_access_token(user_id, client_email)
        client = AsyncGoogleCalendarClient(token)

        # Build the patch body with the new times.
        if is_all_day:
            patch = {
                "start": {"date": current_start},
                "end": {"date": current_end},
            }
        else:
            tz = start.get("timeZone") or end.get("timeZone")
            patch_start = {"dateTime": current_start}
            patch_end = {"dateTime": current_end}
            if tz:
                patch_start["timeZone"] = tz
                patch_end["timeZone"] = tz
            patch = {"start": patch_start, "end": patch_end}

        await client.patch_event(
            cal["google_calendar_id"],
            mapping["origin_event_id"],
            patch,
            send_updates="none",
        )

        # Update stored times so future syncs don't re-trigger propagation.
        await db.execute(
            """UPDATE event_mappings
               SET event_start = ?, event_end = ?, is_all_day = ?, updated_at = ?
               WHERE id = ?""",
            (current_start, current_end, is_all_day, datetime.utcnow().isoformat(), mapping["id"]),
        )
        await db.commit()

        logger.info(
            f"Propagated time change to client event {mapping['origin_event_id']} "
            f"on calendar {cal['google_calendar_id']} "
            f"({stored_start} -> {current_start})"
        )
        return True

    except Exception as e:
        logger.error(
            f"Failed to propagate time change to client event {mapping['origin_event_id']}: {e}"
        )
        return False


async def handle_deleted_main_event(
    user_id: int,
    event_id: str,
    recurring_event_id: Optional[str] = None,
    original_start_time: Optional[dict] = None,
    is_full_sync: bool = False,
) -> None:
    """Handle deletion of an event from the main calendar.

    When *recurring_event_id* is provided the deleted event is a single
    instance of a recurring series.  In that case we cancel just that one
    occurrence on every busy-block calendar, leaving the rest of the series
    intact.

    *is_full_sync* indicates this came from a full (non-incremental) sync.
    During full syncs, "cancelled" events may appear simply because they
    weren't in the sync window — they are NOT necessarily user-deleted.
    We must never delete real client-origin events during a full sync.
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
            # If the parent series is already deleted, skip individual instances —
            # the busy block series was already deleted with the parent.
            if parent_mapping["deleted_at"]:
                logger.debug(f"Skipping cancelled instance {event_id} — parent already deleted")
                return

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

    # If it's a client-origin event, decide whether to cascade the delete
    # back to the client calendar based on how we learned about the deletion.
    if mapping["origin_type"] == "client" and mapping["origin_calendar_id"]:
        if is_full_sync:
            # During a full sync (token lost/expired), "cancelled" events may
            # just be outside the sync window — NOT actually deleted by the
            # user.  Only clean up mapping so the event re-syncs next cycle.
            logger.info(
                f"Full sync: main-calendar copy of client event "
                f"{mapping['origin_event_id']} appears cancelled — "
                f"cleaning up mapping only (client event untouched)"
            )
        else:
            # Incremental sync: user removed the event from main calendar.
            # If this was an invite (has rsvp_status), propagate "declined"
            # back to the client rather than deleting the event.
            if mapping["rsvp_status"] is not None:
                await propagate_rsvp_to_client(
                    user_id=user_id,
                    main_event={},
                    mapping=dict(mapping),
                    new_rsvp_status="declined",
                )
                logger.info(
                    f"Propagated 'declined' RSVP for removed main event "
                    f"{mapping['origin_event_id']}"
                )
            else:
                # No RSVP tracking — this was a non-invite event the user
                # created or fully owns.  Cascade delete to client.
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
                        client = AsyncGoogleCalendarClient(token)
                        await client.delete_event(cal["google_calendar_id"], mapping["origin_event_id"])
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
            client = AsyncGoogleCalendarClient(token)
            await client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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


async def sync_personal_event_to_all(
    personal_client: GoogleCalendarClient,
    main_client: GoogleCalendarClient,
    event: dict,
    user_id: int,
    personal_calendar_id: int,
    main_calendar_id: str,
    user_email: str,
    sa_main_client: Optional[GoogleCalendarClient] = None,
) -> Optional[str]:
    """
    Sync a personal calendar event to main calendar + all client calendars as busy blocks.

    Personal events produce privacy-preserving "Busy (Personal)" blocks everywhere.
    Returns the main event ID if created/updated.
    """
    db = await get_database()

    # Skip events we created
    if personal_client.is_our_event(event):
        return None

    # Skip cancelled events
    if event.get("status") == "cancelled":
        return None

    # Skip events that don't warrant a busy block
    if not should_create_busy_block(event):
        return None

    event_id = event["id"]
    recurring_event_id = event.get("recurringEventId")

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

    # Use SA client for main calendar writes when available
    use_sa = sa_main_client is not None
    write_client = sa_main_client if use_sa else main_client

    # Check for existing mapping first so we can include mapping_id in props
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, personal_calendar_id, event_id)
    )
    existing = await cursor.fetchone()

    personal_origin_props = {
        "bb_origin_id": event_id,
        "bb_origin_cal": personal_calendar_id,
        "bb_type": "personal_block",
    }
    if existing:
        personal_origin_props["bb_mapping_id"] = str(existing["id"])

    # Build the personal busy block for main calendar (SA mode makes it immovable)
    busy_block = create_personal_busy_block(
        start, end, is_all_day,
        use_service_account=use_sa,
        main_email=main_calendar_id if use_sa else None,
        origin_props=personal_origin_props,
    )
    if "recurrence" in event:
        busy_block["recurrence"] = event["recurrence"]

    # Check if times changed — skip updates if they haven't
    personal_times_changed = True
    if existing:
        try:
            personal_times_changed = (
                (existing["event_start"] or "") != event_start
                or (existing["event_end"] or "") != event_end
                or bool(existing["is_all_day"]) != is_all_day
            )
        except (KeyError, TypeError):
            personal_times_changed = True

    if existing:
        main_event_id = existing["main_event_id"]

        # Update main calendar busy block
        if main_event_id and personal_times_changed:
            try:
                await write_client.update_event(main_calendar_id, main_event_id, busy_block)
                logger.info(f"Updated personal busy block {main_event_id} on main calendar")
            except HttpError as e:
                if e.resp.status == 403 and "forbiddenForServiceAccounts" in str(e):
                    logger.warning("SA cannot add attendees (no DWD), retrying update without attendees")
                    busy_block.pop("attendees", None)
                    await write_client.update_event(main_calendar_id, main_event_id, busy_block)
                elif e.resp.status in (404, 410):
                    result = await write_client.create_event(main_calendar_id, busy_block)
                    main_event_id = result["id"]
                else:
                    logger.error(f"Failed to update personal main event: {e}")
                    raise

        # Update mapping (only if times or main_event_id changed)
        if personal_times_changed or main_event_id != existing["main_event_id"]:
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
        try:
            result = await write_client.create_event(main_calendar_id, busy_block)
            main_event_id = result["id"]
            logger.info(f"Created personal busy block {main_event_id} on main calendar")
        except HttpError as e:
            if e.resp.status == 403 and "forbiddenForServiceAccounts" in str(e):
                # Domain-Wide Delegation not configured — retry without attendees
                logger.warning("SA cannot add attendees (no DWD), retrying without attendees")
                busy_block.pop("attendees", None)
                try:
                    result = await write_client.create_event(main_calendar_id, busy_block)
                    main_event_id = result["id"]
                    logger.info(f"Created personal busy block {main_event_id} on main calendar (no attendees)")
                except Exception as e2:
                    logger.error(f"Failed to create personal main event (retry): {e2}")
                    return None
            else:
                logger.error(f"Failed to create personal main event: {e}")
                return None
        except Exception as e:
            logger.error(f"Failed to create personal main event: {e}")
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

        # Stamp the mapping ID onto the main event for full traceability
        if main_event_id:
            try:
                await write_client.patch_event(main_calendar_id, main_event_id, {
                    "extendedProperties": {"private": {"bb_mapping_id": str(mapping_id)}},
                })
            except Exception:
                pass  # Non-critical

        # For recurring personal events, cancel instances on the main-calendar
        # busy block that are cancelled on the personal calendar.
        if "recurrence" in event and main_event_id:
            cursor = await db.execute(
                "SELECT google_calendar_id FROM client_calendars WHERE id = ?",
                (personal_calendar_id,)
            )
            pcal_row = await cursor.fetchone()
            if pcal_row:
                await _propagate_cancelled_instances(
                    source_client=personal_client,
                    source_calendar_id=pcal_row["google_calendar_id"],
                    source_recurring_id=event_id,
                    target_client=write_client,
                    target_calendar_id=main_calendar_id,
                    target_recurring_id=main_event_id,
                )

    # Create/update busy blocks on all client calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
             AND cc.calendar_type = 'client'""",
        (user_id,)
    )
    client_calendars = await cursor.fetchall()

    client_busy_block = create_personal_busy_block(
        start, end, is_all_day,
        origin_props={
            "bb_origin_id": event_id,
            "bb_mapping_id": str(mapping_id),
            "bb_type": "personal_client_block",
        },
    )
    if "recurrence" in event:
        client_busy_block["recurrence"] = event["recurrence"]

    # Pre-fetch existing busy blocks for all client calendars to avoid cursor conflicts
    existing_blocks_by_cal = {}
    for cal in client_calendars:
        cursor = await db.execute(
            """SELECT * FROM busy_blocks
               WHERE event_mapping_id = ? AND client_calendar_id = ?""",
            (mapping_id, cal["id"])
        )
        existing_blocks_by_cal[cal["id"]] = await cursor.fetchone()

    for cal in client_calendars:
        existing_block = existing_blocks_by_cal[cal["id"]]

        try:
            from app.auth.google import get_valid_access_token
            client_token = await get_valid_access_token(user_id, cal["google_account_email"])
            client_calendar_client = AsyncGoogleCalendarClient(client_token)

            if existing_block:
                if not personal_times_changed:
                    continue  # Times unchanged — busy block already correct
                try:
                    await client_calendar_client.update_event(
                        cal["google_calendar_id"],
                        existing_block["busy_block_event_id"],
                        client_busy_block
                    )
                except Exception:
                    try:
                        replacement = await client_calendar_client.create_event(
                            cal["google_calendar_id"], client_busy_block
                        )
                        await db.execute(
                            "UPDATE busy_blocks SET busy_block_event_id = ? WHERE id = ?",
                            (replacement["id"], existing_block["id"])
                        )
                        if "recurrence" in event and main_event_id:
                            await _propagate_cancelled_instances(
                                source_client=write_client,
                                source_calendar_id=main_calendar_id,
                                source_recurring_id=main_event_id,
                                target_client=client_calendar_client,
                                target_calendar_id=cal["google_calendar_id"],
                                target_recurring_id=replacement["id"],
                            )
                    except Exception as ce:
                        logger.error(f"Failed to replace personal busy block on calendar {cal['id']}: {ce}")
            else:
                result = await client_calendar_client.create_event(
                    cal["google_calendar_id"], client_busy_block,
                )
                block_id = result["id"]
                await db.execute(
                    """INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id)
                       VALUES (?, ?, ?)""",
                    (mapping_id, cal["id"], block_id)
                )

                if "recurrence" in event and main_event_id:
                    await _propagate_cancelled_instances(
                        source_client=write_client,
                        source_calendar_id=main_calendar_id,
                        source_recurring_id=main_event_id,
                        target_client=client_calendar_client,
                        target_calendar_id=cal["google_calendar_id"],
                        target_recurring_id=block_id,
                    )

        except Exception as e:
            logger.error(f"Failed to sync personal busy block to calendar {cal['id']}: {e}")

    # Single commit after all client calendars processed
    await db.commit()

    return main_event_id


async def handle_deleted_personal_event(
    user_id: int,
    personal_calendar_id: int,
    event_id: str,
    main_calendar_id: str,
    main_client: GoogleCalendarClient,
    recurring_event_id: Optional[str] = None,
    original_start_time: Optional[dict] = None,
    sa_main_client: Optional[GoogleCalendarClient] = None,
) -> None:
    """Handle deletion of an event from a personal calendar.

    Deletes the main calendar busy block and all client calendar busy blocks.
    """
    db = await get_database()
    write_client = sa_main_client if sa_main_client is not None else main_client

    # Handle recurring instance cancellation
    if recurring_event_id and original_start_time:
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
            (user_id, personal_calendar_id, recurring_event_id)
        )
        parent_mapping = await cursor.fetchone()

        if parent_mapping:
            if parent_mapping["deleted_at"]:
                logger.debug(f"Skipping cancelled personal instance {event_id} — parent already deleted")
                return

            await _handle_cancelled_recurring_instance(
                user_id=user_id,
                mapping=parent_mapping,
                original_start_time=original_start_time,
                main_calendar_id=main_calendar_id,
                main_client=write_client,
            )

        # Also clean up any forked instance mapping
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
            (user_id, personal_calendar_id, event_id)
        )
        instance_mapping = await cursor.fetchone()
        if instance_mapping:
            await _cleanup_personal_mapping(db, instance_mapping, main_calendar_id, write_client, user_id)
        return

    # Full deletion
    cursor = await db.execute(
        """SELECT * FROM event_mappings
           WHERE user_id = ? AND origin_calendar_id = ? AND origin_event_id = ?""",
        (user_id, personal_calendar_id, event_id)
    )
    mapping = await cursor.fetchone()

    if not mapping:
        return

    await _cleanup_personal_mapping(db, mapping, main_calendar_id, write_client, user_id)


async def _cleanup_personal_mapping(
    db,
    mapping: dict,
    main_calendar_id: str,
    write_client: GoogleCalendarClient,
    user_id: int,
) -> None:
    """Delete main event + all busy blocks for a personal event mapping, then remove the mapping."""
    if mapping["main_event_id"]:
        try:
            await write_client.delete_event(main_calendar_id, mapping["main_event_id"])
            logger.info(f"Deleted personal main event {mapping['main_event_id']}")
        except Exception as e:
            logger.error(f"Failed to delete personal main event: {e}")

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
            client = AsyncGoogleCalendarClient(token)
            await client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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
    logger.info(f"Cleaned up personal event mapping {mapping['id']}")
