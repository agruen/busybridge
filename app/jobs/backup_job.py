"""Scheduled backup job."""

import logging

logger = logging.getLogger(__name__)


async def run_scheduled_backup() -> None:
    """Create a daily backup and apply the retention policy.

    Scheduled at 23:00 local time (TZ env var respected by APScheduler).
    """
    from app.sync.backup import create_backup, apply_retention_policy

    logger.info("Scheduled backup starting")
    try:
        metadata = await create_backup()
        logger.info(
            f"Scheduled backup complete: {metadata['backup_id']} "
            f"({metadata['total_events_snapshotted']} events)"
        )
    except Exception as e:
        logger.error(f"Scheduled backup failed: {e}")
        return

    try:
        retention = apply_retention_policy()
        if retention["deleted"]:
            logger.info(f"Retention: removed {len(retention['deleted'])} old backup(s)")
    except Exception as e:
        logger.error(f"Retention policy failed: {e}")
