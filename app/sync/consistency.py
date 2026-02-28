"""Consistency check logic."""

import json
import logging
from datetime import datetime

from app.database import get_database
from app.sync.google_calendar import GoogleCalendarClient

logger = logging.getLogger(__name__)


async def run_consistency_check(dry_run: bool = False) -> dict:
    """
    Run consistency check across all users.

    Verifies:
    1. For each event_mapping, origin and synced copies still exist
    2. If origin deleted but copies remain: delete copies
    3. If copies deleted but origin remains: recreate copies

    When dry_run=True no changes are written; the returned summary includes a
    "planned_actions" list describing every action that would be taken.

    Returns summary of actions taken (or planned).
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
    if dry_run:
        summary["planned_actions"] = []

    # Get all users with main calendars
    cursor = await db.execute(
        "SELECT * FROM users WHERE main_calendar_id IS NOT NULL"
    )
    users = await cursor.fetchall()

    for user in users:
        summary["users_checked"] += 1

        try:
            await check_user_consistency(user["id"], summary, dry_run=dry_run)
        except Exception as e:
            logger.error(f"Consistency check failed for user {user['id']}: {e}")
            summary["errors"] += 1

    logger.info(f"Consistency check completed: {summary}")
    return summary


async def check_user_consistency(user_id: int, summary: dict, dry_run: bool = False) -> None:
    """Check consistency for a single user.

    When dry_run=True no remote calls are made and no DB rows are modified;
    instead each planned action is appended to summary["planned_actions"].
    Either way, per-user counts in *summary* are updated so callers can see
    what was (or would be) done.

    After a non-dry-run pass a sync_log entry is written recording the delta
    for this user.
    """
    db = await get_database()

    from app.auth.google import get_valid_access_token

    # Snapshot counters so we can write a per-user delta to sync_log.
    _before = {k: summary.get(k, 0) for k in (
        "mappings_checked", "orphaned_main_events_deleted",
        "missing_copies_recreated", "orphaned_busy_blocks_deleted", "errors",
    )}

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
                    if dry_run:
                        summary.setdefault("planned_actions", []).append({
                            "action": "delete_orphaned_main_event",
                            "event_mapping_id": mapping["id"],
                            "main_event_id": mapping["main_event_id"],
                            "origin_event_id": mapping["origin_event_id"],
                        })
                        summary["orphaned_main_events_deleted"] += 1
                    else:
                        try:
                            main_client.delete_event(user["main_calendar_id"], mapping["main_event_id"])
                            summary["orphaned_main_events_deleted"] += 1
                            logger.info(f"Deleted orphaned main event {mapping['main_event_id']}")
                        except Exception:
                            pass

                # Fetch busy blocks associated with this mapping.
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
                    if dry_run:
                        summary.setdefault("planned_actions", []).append({
                            "action": "delete_busy_block_for_removed_event",
                            "busy_block_id": block["id"],
                            "busy_block_event_id": block["busy_block_event_id"],
                        })
                    else:
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

                if not dry_run:
                    await db.execute(
                        "DELETE FROM event_mappings WHERE id = ?", (mapping["id"],)
                    )
                    await db.commit()
                continue

            # Check if main copy exists
            if mapping["main_event_id"]:
                main_event = main_client.get_event(user["main_calendar_id"], mapping["main_event_id"])
                if not main_event or main_event.get("status") == "cancelled":
                    # Main copy missing — recreate (or plan to)
                    from app.sync.google_calendar import copy_event_for_main
                    source_label = (
                        mapping["display_name"]
                        or mapping["google_calendar_id"]
                        or mapping["google_account_email"]
                    )
                    if mapping["google_account_email"] not in source_label:
                        source_label = f"{source_label} ({mapping['google_account_email']})"

                    if dry_run:
                        summary.setdefault("planned_actions", []).append({
                            "action": "recreate_main_event",
                            "event_mapping_id": mapping["id"],
                            "origin_event_id": mapping["origin_event_id"],
                            "missing_main_event_id": mapping["main_event_id"],
                        })
                        summary["missing_copies_recreated"] += 1
                    else:
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
            if dry_run:
                summary.setdefault("planned_actions", []).append({
                    "action": "delete_orphaned_busy_block",
                    "busy_block_id": block["id"],
                    "busy_block_event_id": block["busy_block_event_id"],
                })
                summary["orphaned_busy_blocks_deleted"] += 1
            else:
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
                    if dry_run:
                        summary.setdefault("planned_actions", []).append({
                            "action": "remove_stale_busy_block_record",
                            "busy_block_id": block["id"],
                            "busy_block_event_id": block["busy_block_event_id"],
                        })
                    else:
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

    # Write a per-user audit entry so admins can see what the consistency
    # checker did (or found) without trawling application logs.
    # Skipped for dry-run passes — those are purely informational.
    if not dry_run:
        _delta = {k: summary.get(k, 0) - _before[k] for k in _before}
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'consistency_check', ?, ?)""",
            (
                user_id,
                "warning" if _delta["errors"] > 0 else "success",
                json.dumps(_delta),
            ),
        )
        await db.commit()


async def reconcile_calendar(client_calendar_id: int, dry_run: bool = False) -> dict:
    """
    Full reconciliation for a single calendar.

    Fetches all events and reconciles with database.

    When dry_run=True no changes are written; the returned summary includes a
    "planned_actions" list describing what would be removed.
    """
    db = await get_database()
    summary = {
        "events_found": 0,
        "new_events_synced": 0,
        "stale_mappings_removed": 0,
        "busy_blocks_recreated": 0,
    }
    if dry_run:
        summary["planned_actions"] = []

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
        # blocks BEFORE deleting the DB record.
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

            if dry_run:
                summary.setdefault("planned_actions", []).append({
                    "action": "remove_stale_mapping",
                    "event_id": event_id,
                    "mapping_id": mapping["id"],
                    "main_event_id": mapping["main_event_id"],
                })
                actually_removed += 1
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

        if not dry_run:
            await db.commit()

        logger.info(f"Reconciliation completed for calendar {client_calendar_id}: {summary}")

        # Write audit entry (non-dry-run only).
        if not dry_run:
            await db.execute(
                """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
                   VALUES (?, ?, 'reconcile', 'success', ?)""",
                (
                    user_id,
                    client_calendar_id,
                    json.dumps({
                        "events_found": summary["events_found"],
                        "stale_mappings_removed": summary["stale_mappings_removed"],
                    }),
                ),
            )
            await db.commit()

    except Exception as e:
        logger.exception(f"Reconciliation failed for calendar {client_calendar_id}: {e}")

    return summary
