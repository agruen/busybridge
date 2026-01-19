"""Retention cleanup job."""

import logging
from datetime import datetime, timedelta

from app.database import get_database
from app.config import get_settings

logger = logging.getLogger(__name__)


async def run_retention_cleanup() -> dict:
    """
    Run retention cleanup according to policy.

    Retention policy:
    - Single (non-recurring) event mappings: 30 days after event end
    - Recurring event series: Kept while series exists
    - Soft-deleted recurring series: 30 days after deletion
    - Audit/sync log entries: 90 days
    - Disconnected calendar records: 30 days after disconnection
    """
    settings = get_settings()
    db = await get_database()
    now = datetime.utcnow()

    summary = {
        "expired_event_mappings": 0,
        "deleted_recurring_series": 0,
        "old_sync_logs": 0,
        "disconnected_calendars": 0,
        "old_busy_blocks": 0,
    }

    # 1. Delete single event mappings older than retention period
    event_cutoff = (now - timedelta(days=settings.event_retention_days)).isoformat()

    cursor = await db.execute(
        """DELETE FROM event_mappings
           WHERE is_recurring = FALSE AND event_end IS NOT NULL AND event_end < ?
           RETURNING id""",
        (event_cutoff,)
    )
    deleted = await cursor.fetchall()
    summary["expired_event_mappings"] = len(deleted)

    # Also delete related busy blocks
    if deleted:
        deleted_ids = [r["id"] for r in deleted]
        placeholders = ",".join("?" * len(deleted_ids))
        await db.execute(
            f"DELETE FROM busy_blocks WHERE event_mapping_id IN ({placeholders})",
            deleted_ids
        )

    # 2. Delete soft-deleted recurring series older than retention period
    recurring_cutoff = (now - timedelta(days=settings.recurring_soft_delete_days)).isoformat()

    cursor = await db.execute(
        """DELETE FROM event_mappings
           WHERE is_recurring = TRUE AND deleted_at IS NOT NULL AND deleted_at < ?
           RETURNING id""",
        (recurring_cutoff,)
    )
    deleted = await cursor.fetchall()
    summary["deleted_recurring_series"] = len(deleted)

    if deleted:
        deleted_ids = [r["id"] for r in deleted]
        placeholders = ",".join("?" * len(deleted_ids))
        await db.execute(
            f"DELETE FROM busy_blocks WHERE event_mapping_id IN ({placeholders})",
            deleted_ids
        )

    # 3. Delete old sync logs
    log_cutoff = (now - timedelta(days=settings.audit_log_retention_days)).isoformat()

    cursor = await db.execute(
        "DELETE FROM sync_log WHERE created_at < ? RETURNING id",
        (log_cutoff,)
    )
    deleted = await cursor.fetchall()
    summary["old_sync_logs"] = len(deleted)

    # 4. Delete disconnected calendar records older than retention period
    calendar_cutoff = (now - timedelta(days=settings.disconnected_calendar_retention_days)).isoformat()

    cursor = await db.execute(
        """DELETE FROM client_calendars
           WHERE is_active = FALSE AND disconnected_at IS NOT NULL AND disconnected_at < ?
           RETURNING id""",
        (calendar_cutoff,)
    )
    deleted = await cursor.fetchall()
    summary["disconnected_calendars"] = len(deleted)

    # 5. Clean up orphaned busy blocks (where mapping no longer exists)
    cursor = await db.execute(
        """DELETE FROM busy_blocks
           WHERE event_mapping_id NOT IN (SELECT id FROM event_mappings)
           RETURNING id"""
    )
    deleted = await cursor.fetchall()
    summary["old_busy_blocks"] = len(deleted)

    await db.commit()

    logger.info(f"Retention cleanup completed: {summary}")

    # Log the cleanup
    await db.execute(
        """INSERT INTO sync_log (action, status, details)
           VALUES ('retention_cleanup', 'success', ?)""",
        (str(summary),)
    )
    await db.commit()

    return summary


async def vacuum_database() -> None:
    """Run VACUUM on the database to reclaim space."""
    db = await get_database()

    logger.info("Running database VACUUM")
    await db.execute("VACUUM")

    logger.info("Database VACUUM completed")
