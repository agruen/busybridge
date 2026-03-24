"""Webcal/ICS subscription sync engine."""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.database import get_database
from app.sync.google_calendar import AsyncGoogleCalendarClient
from app.sync.ics_parser import fetch_ics_feed, parse_ics_events, build_webcal_google_event

logger = logging.getLogger(__name__)


async def sync_webcal_subscription(subscription_id: int) -> None:
    """Sync a single webcal subscription.

    Creates/updates events on the user's main calendar only.
    Busy blocks on client calendars are handled by the regular
    main→client sync flow.
    """
    db = await get_database()

    # Load subscription
    cursor = await db.execute(
        """SELECT ws.*, u.email as user_email, u.main_calendar_id, u.id as uid
           FROM webcal_subscriptions ws
           JOIN users u ON ws.user_id = u.id
           WHERE ws.id = ?""",
        (subscription_id,),
    )
    sub = await cursor.fetchone()
    if not sub:
        logger.warning(f"Webcal subscription {subscription_id} not found")
        return

    if not sub["is_active"]:
        return

    user_id = sub["uid"]
    main_calendar_id = sub["main_calendar_id"]
    if not main_calendar_id:
        logger.warning(f"User {user_id} has no main calendar, skipping webcal sync")
        return

    now = datetime.utcnow().isoformat()

    try:
        # Fetch ICS feed
        ics_content, new_etag = await fetch_ics_feed(
            sub["url"], etag=sub["last_etag"]
        )

        if ics_content is None:
            # 304 Not Modified
            await db.execute(
                "UPDATE webcal_subscriptions SET last_poll_at = ? WHERE id = ?",
                (now, subscription_id),
            )
            await db.commit()
            logger.debug(f"Webcal {subscription_id} unchanged (304)")
            return

        # Parse events
        time_min = datetime.now(timezone.utc) - timedelta(days=30)
        time_max = datetime.now(timezone.utc) + timedelta(days=365)
        parsed_events = parse_ics_events(ics_content, time_min, time_max)

        logger.info(
            f"Webcal {subscription_id}: fetched {len(parsed_events)} events "
            f"from {sub['url'][:60]}"
        )

        # Get main calendar client
        from app.auth.google import get_valid_access_token
        main_token = await get_valid_access_token(user_id, sub["user_email"])
        main_client = AsyncGoogleCalendarClient(main_token)

        # Get existing mappings for this subscription
        cursor = await db.execute(
            """SELECT id, origin_event_id, main_event_id, event_start, event_end,
                      is_all_day
               FROM event_mappings
               WHERE webcal_subscription_id = ? AND deleted_at IS NULL""",
            (subscription_id,),
        )
        existing_mappings = {
            row["origin_event_id"]: dict(row)
            for row in await cursor.fetchall()
        }

        prefix = (sub["display_prefix"] or "").strip()

        seen_uids = set()
        created_count = 0
        updated_count = 0

        for parsed in parsed_events:
            ics_uid = parsed["ics_uid"]
            seen_uids.add(ics_uid)

            origin_props = {
                "bb_type": "webcal",
                "bb_webcal_sub_id": str(subscription_id),
                "bb_origin_id": ics_uid,
            }

            google_event = build_webcal_google_event(parsed, prefix, origin_props)
            existing = existing_mappings.get(ics_uid)

            event_start = parsed["start"].get("dateTime") or parsed["start"].get("date")
            event_end = parsed["end"].get("dateTime") or parsed["end"].get("date")

            if existing:
                # Check if event changed
                if (
                    (existing["event_start"] or "") == (event_start or "")
                    and (existing["event_end"] or "") == (event_end or "")
                    and bool(existing["is_all_day"]) == parsed["is_all_day"]
                ):
                    continue  # No change

                # Update existing event
                main_event_id = existing["main_event_id"]
                if main_event_id:
                    try:
                        await main_client.update_event(
                            main_calendar_id, main_event_id, google_event
                        )
                        logger.info(
                            f"Updated webcal event {main_event_id} from {ics_uid[:30]}"
                        )
                    except Exception as e:
                        if hasattr(e, 'resp') and e.resp.status in (404, 410):
                            result = await main_client.create_event(
                                main_calendar_id, google_event
                            )
                            main_event_id = result["id"]
                        else:
                            logger.error(f"Failed to update webcal event: {e}")
                            continue

                await db.execute(
                    """UPDATE event_mappings SET
                       main_event_id = ?, event_start = ?, event_end = ?,
                       is_all_day = ?, updated_at = ?
                       WHERE id = ?""",
                    (main_event_id, event_start, event_end,
                     parsed["is_all_day"], now, existing["id"]),
                )
                updated_count += 1

            else:
                # Create new event
                try:
                    result = await main_client.create_event(
                        main_calendar_id, google_event
                    )
                    main_event_id = result["id"]
                    logger.info(
                        f"Created webcal event {main_event_id} from {ics_uid[:30]}"
                    )
                except Exception as e:
                    logger.error(f"Failed to create webcal event: {e}")
                    continue

                # Insert mapping
                cursor = await db.execute(
                    """INSERT INTO event_mappings
                       (user_id, origin_type, origin_event_id, main_event_id,
                        event_start, event_end, is_all_day, is_recurring,
                        user_can_edit, webcal_subscription_id)
                       VALUES (?, 'webcal', ?, ?, ?, ?, ?, FALSE, FALSE, ?)
                       RETURNING id""",
                    (user_id, ics_uid, main_event_id, event_start, event_end,
                     parsed["is_all_day"], subscription_id),
                )
                await cursor.fetchone()
                created_count += 1

        # Removal sweep: delete events no longer in the feed
        removed_count = 0
        for uid, mapping in existing_mappings.items():
            if uid in seen_uids:
                continue

            # Delete from main calendar
            if mapping["main_event_id"]:
                try:
                    await main_client.delete_event(
                        main_calendar_id, mapping["main_event_id"]
                    )
                except Exception as e:
                    if not (hasattr(e, 'resp') and e.resp.status in (404, 410)):
                        logger.error(
                            f"Failed to delete webcal event {mapping['main_event_id']}: {e}"
                        )

            # Soft-delete mapping
            await db.execute(
                "UPDATE event_mappings SET deleted_at = ? WHERE id = ?",
                (now, mapping["id"]),
            )
            removed_count += 1

        await db.commit()

        # Update subscription status
        await db.execute(
            """UPDATE webcal_subscriptions SET
               last_poll_at = ?, last_etag = ?, last_success_at = ?,
               consecutive_failures = 0, last_error = NULL, updated_at = ?
               WHERE id = ?""",
            (now, new_etag, now, now, subscription_id),
        )
        await db.commit()

        # Log
        await db.execute(
            """INSERT INTO sync_log (user_id, action, status, details)
               VALUES (?, 'sync_webcal', 'success', ?)""",
            (user_id, json.dumps({
                "subscription_id": subscription_id,
                "created": created_count,
                "updated": updated_count,
                "removed": removed_count,
                "total_parsed": len(parsed_events),
            })),
        )
        await db.commit()

        if created_count or updated_count or removed_count:
            logger.info(
                f"Webcal {subscription_id} sync: "
                f"{created_count} created, {updated_count} updated, "
                f"{removed_count} removed"
            )

        # Feed activity
        try:
            from app.sync.engine import _log_activity
            _log_activity(
                user_id,
                f"Webcal sync: {created_count} new, {updated_count} updated, "
                f"{removed_count} removed",
            )
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Webcal {subscription_id} sync failed: {e}")

        # Increment failure counter
        await db.execute(
            """UPDATE webcal_subscriptions SET
               last_poll_at = ?, consecutive_failures = consecutive_failures + 1,
               last_error = ?, updated_at = ?
               WHERE id = ?""",
            (now, str(e)[:500], now, subscription_id),
        )
        await db.commit()


async def cleanup_webcal_subscription(subscription_id: int, user_id: int) -> None:
    """Clean up all events for a webcal subscription being disconnected."""
    db = await get_database()

    # Get user info for main calendar access
    cursor = await db.execute(
        "SELECT email, main_calendar_id FROM users WHERE id = ?",
        (user_id,),
    )
    user = await cursor.fetchone()
    if not user or not user["main_calendar_id"]:
        return

    # Get all mappings
    cursor = await db.execute(
        """SELECT id, main_event_id FROM event_mappings
           WHERE webcal_subscription_id = ? AND deleted_at IS NULL""",
        (subscription_id,),
    )
    mappings = await cursor.fetchall()

    try:
        from app.auth.google import get_valid_access_token
        main_token = await get_valid_access_token(user_id, user["email"])
        main_client = AsyncGoogleCalendarClient(main_token)
    except Exception:
        main_client = None

    now = datetime.utcnow().isoformat()

    for mapping in mappings:
        # Delete from main calendar
        if main_client and mapping["main_event_id"]:
            try:
                await main_client.delete_event(
                    user["main_calendar_id"], mapping["main_event_id"]
                )
            except Exception:
                pass

        # Clean up any busy blocks that may exist from before this fix
        cursor = await db.execute(
            """SELECT bb.*, cc.google_calendar_id, ot.google_account_email
               FROM busy_blocks bb
               JOIN client_calendars cc ON bb.client_calendar_id = cc.id
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE bb.event_mapping_id = ?""",
            (mapping["id"],),
        )
        blocks = await cursor.fetchall()
        if blocks:
            from app.auth.google import get_valid_access_token
            for block in blocks:
                try:
                    token = await get_valid_access_token(user_id, block["google_account_email"])
                    client = AsyncGoogleCalendarClient(token)
                    await client.delete_event(block["google_calendar_id"], block["busy_block_event_id"])
                except Exception:
                    pass
            await db.execute(
                "DELETE FROM busy_blocks WHERE event_mapping_id = ?",
                (mapping["id"],),
            )

        # Soft-delete mapping
        await db.execute(
            "UPDATE event_mappings SET deleted_at = ? WHERE id = ?",
            (now, mapping["id"]),
        )

    await db.commit()
    logger.info(
        f"Cleaned up {len(mappings)} events for webcal subscription {subscription_id}"
    )
