"""One-time backfill: stamp bb_origin_id, bb_type, bb_mapping_id onto existing
Google Calendar events that were created before the metadata embedding code.

Safe to run multiple times — it patches extendedProperties.private which
merges with existing properties (doesn't overwrite calendarSyncEngine tag).

Usage:
    docker compose exec calendar-sync python /app/scripts/backfill_metadata.py
    docker compose exec calendar-sync python /app/scripts/backfill_metadata.py --dry-run
"""
import asyncio
import sys
import logging

sys.path.insert(0, "/app")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def main():
    dry_run = "--dry-run" in sys.argv

    from app.database import get_database
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    db = await get_database()

    # Get all users
    cursor = await db.execute("SELECT id, email, main_calendar_id FROM users")
    users = await cursor.fetchall()

    total_patched = 0
    total_skipped = 0
    total_errors = 0

    for user in users:
        user_id = user["id"]
        main_cal_id = user["main_calendar_id"]
        logger.info("Processing user %d (%s)", user_id, user["email"])

        # Get main calendar client
        try:
            main_token = await get_valid_access_token(user_id, user["email"])
            main_client = GoogleCalendarClient(main_token)
        except Exception as e:
            logger.error("  Failed to get main calendar client: %s", e)
            total_errors += 1
            continue

        # Build client map: calendar_id -> (client, email)
        cursor = await db.execute(
            """SELECT cc.id, cc.google_calendar_id, ot.google_account_email
               FROM client_calendars cc
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE cc.user_id = ? AND cc.is_active = TRUE""",
            (user_id,),
        )
        cal_rows = await cursor.fetchall()
        client_map = {}
        for cal in cal_rows:
            try:
                token = await get_valid_access_token(user_id, cal["google_account_email"])
                client_map[cal["id"]] = {
                    "client": GoogleCalendarClient(token),
                    "google_calendar_id": cal["google_calendar_id"],
                }
            except Exception as e:
                logger.warning("  Failed to get client for calendar %s: %s",
                               cal["google_calendar_id"][:25], e)

        # Process all event mappings for this user
        cursor = await db.execute(
            """SELECT * FROM event_mappings
               WHERE user_id = ? AND deleted_at IS NULL""",
            (user_id,),
        )
        mappings = await cursor.fetchall()

        for mapping in mappings:
            mapping_id = mapping["id"]
            origin_type = mapping["origin_type"]
            origin_event_id = mapping["origin_event_id"]
            origin_calendar_id = mapping["origin_calendar_id"]
            main_event_id = mapping["main_event_id"]

            # Determine bb_type for the main calendar event
            if origin_type == "client":
                main_bb_type = "client_copy"
            elif origin_type == "personal":
                main_bb_type = "personal_block"
            elif origin_type == "main":
                main_bb_type = "main_origin"
            else:
                continue

            # Patch main calendar event
            if main_event_id:
                props = {
                    "bb_origin_id": origin_event_id,
                    "bb_mapping_id": str(mapping_id),
                    "bb_type": main_bb_type,
                }
                if origin_type in ("client", "personal"):
                    props["bb_origin_cal"] = str(origin_calendar_id)

                if dry_run:
                    logger.info("  [dry-run] Would patch main event %s with %s",
                                main_event_id[:20], props)
                    total_patched += 1
                else:
                    try:
                        main_client.patch_event(main_cal_id, main_event_id, {
                            "extendedProperties": {"private": props},
                        })
                        total_patched += 1
                    except Exception as e:
                        err = str(e)
                        if "404" in err or "410" in err:
                            total_skipped += 1
                        else:
                            logger.warning("  Failed to patch main event %s: %s",
                                           main_event_id[:20], e)
                            total_errors += 1

            # Patch busy blocks
            bb_cursor = await db.execute(
                """SELECT bb.id, bb.busy_block_event_id, bb.client_calendar_id
                   FROM busy_blocks bb
                   WHERE bb.event_mapping_id = ?""",
                (mapping_id,),
            )
            blocks = await bb_cursor.fetchall()

            for block in blocks:
                cal_info = client_map.get(block["client_calendar_id"])
                if not cal_info:
                    total_skipped += 1
                    continue

                bb_type = "personal_client_block" if origin_type == "personal" else "busy_block"
                props = {
                    "bb_origin_id": origin_event_id,
                    "bb_mapping_id": str(mapping_id),
                    "bb_type": bb_type,
                }

                if dry_run:
                    logger.info("  [dry-run] Would patch busy block %s on %s with %s",
                                block["busy_block_event_id"][:20],
                                cal_info["google_calendar_id"][:25], props)
                    total_patched += 1
                else:
                    try:
                        cal_info["client"].patch_event(
                            cal_info["google_calendar_id"],
                            block["busy_block_event_id"],
                            {"extendedProperties": {"private": props}},
                        )
                        total_patched += 1
                    except Exception as e:
                        err = str(e)
                        if "404" in err or "410" in err:
                            total_skipped += 1
                        else:
                            logger.warning("  Failed to patch busy block %s: %s",
                                           block["busy_block_event_id"][:20], e)
                            total_errors += 1

    prefix = "[dry-run] " if dry_run else ""
    logger.info(
        "%sBackfill complete: %d patched, %d skipped (gone), %d errors",
        prefix, total_patched, total_skipped, total_errors,
    )


if __name__ == "__main__":
    asyncio.run(main())
