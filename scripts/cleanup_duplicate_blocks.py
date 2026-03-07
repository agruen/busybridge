"""One-time cleanup: remove duplicate main-origin mappings and their busy blocks.

These were created because the main sync loop re-processed personal-origin
busy blocks as if they were native main-calendar events, creating a second
set of busy blocks on every client calendar.
"""
import asyncio
import sys
import logging

sys.path.insert(0, "/app")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def main():
    from app.database import get_database
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import GoogleCalendarClient

    db = await get_database()

    # Find main-origin mappings whose main_event_id also appears as a
    # personal-origin mapping's main_event_id — these are the duplicates.
    cursor = await db.execute(
        """SELECT em.id, em.main_event_id, em.origin_event_id
           FROM event_mappings em
           WHERE em.origin_type = 'main'
             AND em.main_event_id IN (
                 SELECT main_event_id FROM event_mappings WHERE origin_type = 'personal'
             )"""
    )
    duplicates = await cursor.fetchall()
    logger.info(f"Found {len(duplicates)} duplicate main-origin mappings")

    if not duplicates:
        logger.info("Nothing to clean up")
        return

    # For each duplicate, find and delete its busy blocks on Google Calendar,
    # then remove DB records.
    deleted_events = 0
    failed_deletes = 0

    for dup in duplicates:
        mapping_id = dup["id"]

        # Get busy blocks for this mapping
        cursor = await db.execute(
            """SELECT bb.id, bb.busy_block_event_id,
                      cc.google_calendar_id, ot.google_account_email
               FROM busy_blocks bb
               JOIN client_calendars cc ON bb.client_calendar_id = cc.id
               JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
               WHERE bb.event_mapping_id = ?""",
            (mapping_id,),
        )
        blocks = await cursor.fetchall()

        for block in blocks:
            try:
                token = await get_valid_access_token(1, block["google_account_email"])
                client = GoogleCalendarClient(token)
                client.delete_event(
                    block["google_calendar_id"], block["busy_block_event_id"]
                )
                deleted_events += 1
                logger.info(
                    f"  Deleted duplicate busy block {block['busy_block_event_id']} "
                    f"from {block['google_calendar_id']}"
                )
            except Exception as e:
                # 404/410 means already gone — that's fine
                if "404" in str(e) or "410" in str(e):
                    deleted_events += 1
                else:
                    failed_deletes += 1
                    logger.warning(
                        f"  Failed to delete {block['busy_block_event_id']}: {e}"
                    )

        # Delete DB records for this mapping's busy blocks
        await db.execute(
            "DELETE FROM busy_blocks WHERE event_mapping_id = ?", (mapping_id,)
        )

    # Delete all the duplicate mappings
    dup_ids = [d["id"] for d in duplicates]
    placeholders = ",".join("?" * len(dup_ids))
    await db.execute(
        f"DELETE FROM event_mappings WHERE id IN ({placeholders})", tuple(dup_ids)
    )
    await db.commit()

    logger.info(
        f"Cleanup complete: removed {len(duplicates)} duplicate mappings, "
        f"deleted {deleted_events} Google Calendar events, "
        f"{failed_deletes} failures"
    )


if __name__ == "__main__":
    asyncio.run(main())
