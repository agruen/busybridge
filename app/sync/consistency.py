"""Consistency check logic."""

import json
import logging
from datetime import datetime, timedelta, timezone

from app.database import get_database
from app.sync.google_calendar import AsyncGoogleCalendarClient, GoogleCalendarClient

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
            if not dry_run:
                await _record_check_failure(user["id"])

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
        main_client = AsyncGoogleCalendarClient(main_token)
    except Exception as e:
        logger.warning(f"Cannot get main calendar access for user {user_id}: {e}")
        return

    # Use SA client for main calendar writes when available
    sa_main_client = None
    cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
    sa_row = await cursor.fetchone()
    if sa_row and sa_row["sa_tier"] == 2:
        from app.auth.service_account import get_sa_main_client
        sa_main_client = get_sa_main_client(user["main_calendar_id"])
        if sa_main_client is None:
            logger.warning(f"SA access lost for user {user_id} during consistency check")
            await db.execute("UPDATE users SET sa_tier = 0 WHERE id = ?", (user_id,))
            await db.commit()
    write_client = sa_main_client if sa_main_client is not None else main_client

    # Check event mappings with client AND personal origins
    cursor = await db.execute(
        """SELECT em.*, cc.google_calendar_id, cc.display_name, ot.google_account_email
           FROM event_mappings em
           JOIN client_calendars cc ON em.origin_calendar_id = cc.id
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE em.user_id = ? AND em.origin_type IN ('client', 'personal')
           AND em.deleted_at IS NULL
           AND cc.is_active = TRUE""",
        (user_id,)
    )
    client_mappings = await cursor.fetchall()

    for mapping in client_mappings:
        summary["mappings_checked"] += 1

        try:
            client_token = await get_valid_access_token(user_id, mapping["google_account_email"])
            client = AsyncGoogleCalendarClient(client_token)

            # Check if origin event still exists
            origin_event = await client.get_event(mapping["google_calendar_id"], mapping["origin_event_id"])

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
                            await write_client.delete_event(user["main_calendar_id"], mapping["main_event_id"])
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
                            block_client = AsyncGoogleCalendarClient(block_token)
                            await block_client.delete_event(
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
                main_event = await main_client.get_event(user["main_calendar_id"], mapping["main_event_id"])
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
                        use_sa = sa_main_client is not None
                        new_event_data = copy_event_for_main(
                            origin_event,
                            source_label=source_label,
                            main_email=user["email"],
                            current_rsvp_status=mapping["rsvp_status"],
                            user_can_edit=mapping["user_can_edit"],
                            use_service_account=use_sa,
                        )
                        try:
                            result = await write_client.create_event(user["main_calendar_id"], new_event_data)
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
                    block_client = AsyncGoogleCalendarClient(client_token)
                    await block_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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
                block_client = AsyncGoogleCalendarClient(client_token)
                remote = await block_client.get_event(
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

        # Track integrity status
        await _record_check_result(user_id, _delta)


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
        client = AsyncGoogleCalendarClient(client_token)

        # Get all events (no sync token)
        result = await client.list_events(calendar["google_calendar_id"])
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
                ev = await client.get_event(calendar["google_calendar_id"], event_id)
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
                    main_client = AsyncGoogleCalendarClient(main_token)
                    # Use SA client when available
                    sa_del_client = None
                    cursor = await db.execute("SELECT sa_tier FROM users WHERE id = ?", (user_id,))
                    sa_row = await cursor.fetchone()
                    if sa_row and sa_row["sa_tier"] == 2:
                        from app.auth.service_account import get_sa_main_client
                        sa_del_client = get_sa_main_client(calendar["main_calendar_id"])
                    del_client = sa_del_client if sa_del_client is not None else main_client
                    await del_client.delete_event(calendar["main_calendar_id"], mapping["main_event_id"])
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
                    block_client = AsyncGoogleCalendarClient(block_token)
                    await block_client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
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


# ── Integrity status tracking ─────────────────────────────────────────


async def _record_check_result(user_id: int, delta: dict) -> None:
    """Upsert integrity_status after a successful consistency check."""
    db = await get_database()

    issues_auto_fixed = (
        delta.get("orphaned_main_events_deleted", 0)
        + delta.get("missing_copies_recreated", 0)
        + delta.get("orphaned_busy_blocks_deleted", 0)
    )
    unresolved = delta.get("errors", 0)
    issues_found = issues_auto_fixed + unresolved

    details = {
        "orphaned_main_events_deleted": delta.get("orphaned_main_events_deleted", 0),
        "missing_copies_recreated": delta.get("missing_copies_recreated", 0),
        "orphaned_busy_blocks_deleted": delta.get("orphaned_busy_blocks_deleted", 0),
        "errors": delta.get("errors", 0),
        "mappings_checked": delta.get("mappings_checked", 0),
    }

    now = datetime.utcnow().isoformat()
    await db.execute(
        """INSERT INTO integrity_status
               (user_id, last_check_at, issues_found, issues_auto_fixed,
                unresolved_issues, consecutive_check_failures, details_json, updated_at)
           VALUES (?, ?, ?, ?, ?, 0, ?, ?)
           ON CONFLICT(user_id) DO UPDATE SET
               last_check_at = excluded.last_check_at,
               issues_found = excluded.issues_found,
               issues_auto_fixed = excluded.issues_auto_fixed,
               unresolved_issues = excluded.unresolved_issues,
               consecutive_check_failures = 0,
               details_json = excluded.details_json,
               updated_at = excluded.updated_at""",
        (user_id, now, issues_found, issues_auto_fixed, unresolved,
         json.dumps(details), now),
    )
    await db.commit()

    # Alert if unresolved issues (throttled to once per 24 hours)
    if unresolved > 0:
        await _maybe_alert_integrity(user_id, unresolved, details)


async def _record_check_failure(user_id: int) -> None:
    """Increment consecutive failure count when the check itself crashes."""
    db = await get_database()
    now = datetime.utcnow().isoformat()
    await db.execute(
        """INSERT INTO integrity_status
               (user_id, last_check_at, consecutive_check_failures, updated_at)
           VALUES (?, ?, 1, ?)
           ON CONFLICT(user_id) DO UPDATE SET
               consecutive_check_failures = integrity_status.consecutive_check_failures + 1,
               last_check_at = excluded.last_check_at,
               updated_at = excluded.updated_at""",
        (user_id, now, now),
    )
    await db.commit()


async def _maybe_alert_integrity(user_id: int, unresolved: int, details: dict) -> None:
    """Queue an integrity alert if not sent in the last 24 hours."""
    db = await get_database()

    # Check if we already alerted recently
    cursor = await db.execute(
        """SELECT id FROM alert_queue
           WHERE alert_type = 'integrity_issues'
             AND recipient_email IN (SELECT email FROM users WHERE id = ?)
             AND created_at > datetime('now', '-1 day')""",
        (user_id,),
    )
    if await cursor.fetchone():
        return

    from app.alerts.email import queue_alert

    detail_lines = [f"Unresolved issues: {unresolved}"]
    if details.get("errors"):
        detail_lines.append(f"Errors during check: {details['errors']}")
    if details.get("orphaned_main_events_deleted"):
        detail_lines.append(f"Orphaned events cleaned up: {details['orphaned_main_events_deleted']}")
    if details.get("missing_copies_recreated"):
        detail_lines.append(f"Missing copies recreated: {details['missing_copies_recreated']}")

    await queue_alert(
        alert_type="integrity_issues",
        user_id=user_id,
        details="\n".join(detail_lines),
    )


# ── Google→DB orphan scan ────────────────────────────────────────────


async def scan_for_orphans(dry_run: bool = False) -> dict:
    """Scan Google Calendars for orphaned events and duplicates.

    Unlike the DB→Google consistency check, this goes the other
    direction: lists every event *we* created on each calendar, cross-
    references with the DB, and deletes anything the DB doesn't know about.

    Also checks for duplicate DB records pointing to different Google
    events for the same origin.

    Returns a summary dict of actions taken (or planned when dry_run=True).
    """
    db = await get_database()
    summary = {
        "calendars_scanned": 0,
        "our_events_found": 0,
        "orphans_found": 0,
        "orphans_deleted": 0,
        "db_duplicates_found": 0,
        "db_duplicates_fixed": 0,
        "errors": 0,
        "details": [],
    }

    # Get all users
    cursor = await db.execute("SELECT * FROM users")
    users = await cursor.fetchall()

    for user in users:
        user_id = user["id"]
        try:
            await _scan_user_orphans(user_id, summary, dry_run)
            await _scan_db_duplicates(user_id, summary, dry_run)
        except Exception as e:
            logger.error("Orphan scan failed for user %d: %s", user_id, e)
            summary["errors"] += 1

    logger.info(
        "Orphan scan complete: scanned %d calendars, found %d our events, "
        "%d orphans (%d deleted), %d DB duplicates (%d fixed), %d errors",
        summary["calendars_scanned"], summary["our_events_found"],
        summary["orphans_found"], summary["orphans_deleted"],
        summary["db_duplicates_found"], summary["db_duplicates_fixed"],
        summary["errors"],
    )
    return summary


async def _scan_user_orphans(
    user_id: int, summary: dict, dry_run: bool
) -> None:
    """Scan all calendars for a user and find orphaned events."""
    from app.auth.google import get_valid_access_token

    db = await get_database()
    now = datetime.now(timezone.utc)
    time_min = (now - timedelta(days=7)).isoformat()
    time_max = (now + timedelta(days=365)).isoformat()

    # Build the set of all Google event IDs we know about in the DB
    cursor = await db.execute(
        "SELECT main_event_id FROM event_mappings WHERE user_id = ? AND deleted_at IS NULL",
        (user_id,),
    )
    known_main_ids = {row["main_event_id"] for row in await cursor.fetchall() if row["main_event_id"]}

    cursor = await db.execute(
        """SELECT bb.busy_block_event_id
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           WHERE em.user_id = ?""",
        (user_id,),
    )
    known_block_ids = {row["busy_block_event_id"] for row in await cursor.fetchall() if row["busy_block_event_id"]}

    cursor = await db.execute(
        "SELECT origin_event_id, origin_calendar_id FROM event_mappings WHERE user_id = ? AND deleted_at IS NULL",
        (user_id,),
    )
    known_origin_ids = {row["origin_event_id"] for row in await cursor.fetchall() if row["origin_event_id"]}

    # Get main calendar
    cursor = await db.execute(
        "SELECT main_calendar_id, email FROM users WHERE id = ?", (user_id,),
    )
    user_row = await cursor.fetchone()
    if not user_row:
        return
    main_cal_id = user_row["main_calendar_id"]

    # Get all calendars to scan
    cursor = await db.execute(
        """SELECT cc.google_calendar_id, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,),
    )
    client_cals = await cursor.fetchall()

    # Scan main calendar
    try:
        token = await get_valid_access_token(user_id, user_row["email"])
        main_client = AsyncGoogleCalendarClient(token)
        await _dedup_calendar(
            main_client, main_cal_id, known_main_ids,
            time_min, time_max, summary, dry_run, "main",
        )
    except Exception as e:
        logger.warning("Orphan scan: failed to scan main calendar: %s", e)
        summary["errors"] += 1

    # Scan each client calendar
    for cal in client_cals:
        cal_id = cal["google_calendar_id"]
        email = cal["google_account_email"]
        try:
            token = await get_valid_access_token(user_id, email)
            client = AsyncGoogleCalendarClient(token)
            await _dedup_calendar(
                client, cal_id, known_block_ids | known_origin_ids,
                time_min, time_max, summary, dry_run, "client",
            )
        except Exception as e:
            logger.warning("Orphan scan: failed to scan %s: %s", cal_id[:25], e)
            summary["errors"] += 1


async def _dedup_calendar(
    client: GoogleCalendarClient,
    calendar_id: str,
    known_ids: set[str],
    time_min: str,
    time_max: str,
    summary: dict,
    dry_run: bool,
    cal_label: str,
) -> None:
    """Scan one calendar for orphans and duplicates.

    Groups events by bb_origin_id.  For each origin:
    - If the DB knows one of the event IDs → keep that, delete the rest.
    - If the DB knows none → keep the newest, delete the rest.
    - Single events unknown to the DB → orphan, delete.
    """
    from app.config import get_settings
    settings = get_settings()

    our_events = await client.list_our_events(
        calendar_id, time_min=time_min, time_max=time_max,
    )
    summary["calendars_scanned"] += 1
    summary["our_events_found"] += len(our_events)

    # Group by bb_origin_id
    by_origin: dict[str, list[dict]] = {}
    no_origin: list[dict] = []

    for event in our_events:
        eid = event["id"]
        if eid in known_ids:
            continue  # DB knows about it, it's fine

        props = event.get("extendedProperties", {}).get("private", {})
        origin_id = props.get("bb_origin_id")

        # On client calendars, skip events without our bb_type metadata
        # (they could be the user's own events, not busy blocks we created).
        if cal_label == "client":
            bb_type = props.get("bb_type", "")
            if not bb_type and settings.calendar_sync_tag not in props:
                continue  # Not our event

        if origin_id:
            by_origin.setdefault(origin_id, []).append(event)
        else:
            no_origin.append(event)

    # Handle groups with the same bb_origin_id
    for origin_id, events in by_origin.items():
        # Determine the bb_type from any event in the group
        sample_props = events[0].get("extendedProperties", {}).get("private", {})
        bb_type = sample_props.get("bb_type", "")
        is_real_copy = bb_type == "client_copy"

        db_known = [e for e in events if e["id"] in known_ids]

        if len(events) == 1 and not db_known:
            if is_real_copy:
                # Orphaned client_copy with no DB mapping — delete it.
                # sync_client_event_to_main always creates a new main event
                # (never re-links to an existing orphan), so keeping orphans
                # just accumulates zombies.  The client sync will recreate
                # a fresh copy with proper mapping if the client event still
                # exists.
                await _mark_orphan(
                    summary, events[0], calendar_id, cal_label, dry_run, client,
                )
            else:
                # Single orphaned busy block — delete it.  Busy blocks are
                # created by us, not the user.  No adoption path exists.
                await _mark_orphan(
                    summary, events[0], calendar_id, cal_label, dry_run, client,
                )
        elif len(events) > 1:
            # Multiple events for same origin — duplicates!
            if db_known:
                keep = db_known[0]
            else:
                # All orphaned (no DB mapping) — delete all of them.
                # For client_copy events: sync_client_event_to_main always
                # creates a fresh copy, so there's no re-adoption path.
                # For busy_blocks: no adoption path exists either.
                keep = None

            for event in events:
                if keep and event["id"] == keep["id"]:
                    continue
                summary["orphans_found"] += 1
                keep_note = f"keeping {keep['id'][:20]}" if keep else "deleting all"
                detail = (
                    f"{cal_label}:{calendar_id[:25]}:{event['id'][:20]} "
                    f"DUPLICATE origin={origin_id} ({keep_note})"
                )
                if not dry_run:
                    try:
                        await client.delete_event(calendar_id, event["id"])
                        summary["orphans_deleted"] += 1
                        logger.info("Orphan scan: deleted duplicate: %s", detail)
                    except Exception as e:
                        logger.warning("Orphan scan: failed to delete %s: %s", detail, e)
                        summary["errors"] += 1
                else:
                    logger.info("Orphan scan [dry-run]: would delete duplicate: %s", detail)
                summary["details"].append({
                    "calendar": calendar_id, "event_id": event["id"],
                    "origin_id": origin_id, "action": "delete_duplicate",
                })

    # Handle events with no bb_origin_id (legacy or untagged)
    for event in no_origin:
        await _mark_orphan(summary, event, calendar_id, cal_label, dry_run, client)


async def _mark_orphan(
    summary: dict,
    event: dict,
    calendar_id: str,
    cal_label: str,
    dry_run: bool,
    client: AsyncGoogleCalendarClient,
) -> None:
    """Mark a single event as orphan and optionally delete it."""
    eid = event["id"]
    props = event.get("extendedProperties", {}).get("private", {})
    origin_id = props.get("bb_origin_id", "unknown")
    bb_type = props.get("bb_type", "unknown")
    summary["orphans_found"] += 1
    detail = f"{cal_label}:{calendar_id[:25]}:{eid[:20]} type={bb_type} origin={origin_id}"

    if not dry_run:
        try:
            await client.delete_event(calendar_id, eid)
            summary["orphans_deleted"] += 1
            logger.info("Orphan scan: deleted orphan: %s", detail)
        except Exception as e:
            logger.warning("Orphan scan: failed to delete %s: %s", detail, e)
            summary["errors"] += 1
    else:
        logger.info("Orphan scan [dry-run]: would delete orphan: %s", detail)

    summary["details"].append({
        "calendar": calendar_id, "event_id": eid,
        "type": bb_type, "action": "delete",
    })


async def _scan_db_duplicates(
    user_id: int, summary: dict, dry_run: bool
) -> None:
    """Check for duplicate DB records pointing to different Google events."""
    db = await get_database()

    # Check for duplicate main_event_ids (two mappings claiming the same
    # main calendar event — shouldn't happen).
    cursor = await db.execute(
        """SELECT main_event_id, COUNT(*) as cnt
           FROM event_mappings
           WHERE user_id = ? AND deleted_at IS NULL AND main_event_id IS NOT NULL
           GROUP BY main_event_id
           HAVING cnt > 1""",
        (user_id,),
    )
    for row in await cursor.fetchall():
        summary["db_duplicates_found"] += 1
        logger.warning(
            "Orphan scan: duplicate main_event_id %s appears in %d mappings",
            row["main_event_id"], row["cnt"],
        )

    # Check for duplicate busy_block_event_ids (two DB rows claiming the
    # same Google Calendar busy block — shouldn't happen).
    cursor = await db.execute(
        """SELECT bb.busy_block_event_id, COUNT(*) as cnt
           FROM busy_blocks bb
           JOIN event_mappings em ON bb.event_mapping_id = em.id
           WHERE em.user_id = ?
           GROUP BY bb.busy_block_event_id
           HAVING cnt > 1""",
        (user_id,),
    )
    for row in await cursor.fetchall():
        summary["db_duplicates_found"] += 1
        logger.warning(
            "Orphan scan: duplicate busy_block_event_id %s appears in %d rows",
            row["busy_block_event_id"], row["cnt"],
        )
