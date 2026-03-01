"""Periodic sync job."""

import logging
from datetime import datetime, timedelta

from app.database import get_database, get_setting

logger = logging.getLogger(__name__)


async def run_periodic_sync() -> None:
    """Run periodic sync for all active calendars."""
    # Check if sync is paused
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        logger.debug("Sync is paused, skipping periodic sync")
        return

    # Acquire lock
    if not await acquire_job_lock("periodic_sync"):
        logger.debug("Periodic sync already running, skipping")
        return

    try:
        db = await get_database()

        # Get all active calendars (client + personal)
        cursor = await db.execute(
            """SELECT id, calendar_type FROM client_calendars WHERE is_active = TRUE"""
        )
        calendars = await cursor.fetchall()

        logger.info(f"Running periodic sync for {len(calendars)} calendars")

        from app.sync.engine import trigger_sync_for_calendar, trigger_sync_for_personal_calendar

        for cal in calendars:
            try:
                if cal["calendar_type"] == "personal":
                    await trigger_sync_for_personal_calendar(cal["id"])
                else:
                    await trigger_sync_for_calendar(cal["id"])
            except Exception as e:
                logger.error(f"Error syncing calendar {cal['id']}: {e}")

        # Also sync main calendars
        cursor = await db.execute(
            "SELECT id FROM users WHERE main_calendar_id IS NOT NULL"
        )
        users = await cursor.fetchall()

        from app.sync.engine import trigger_sync_for_main_calendar

        for user in users:
            try:
                await trigger_sync_for_main_calendar(user["id"])
            except Exception as e:
                logger.error(f"Error syncing main calendar for user {user['id']}: {e}")

        logger.info("Periodic sync completed")

    finally:
        await release_job_lock("periodic_sync")


async def run_consistency_check_job() -> None:
    """Run consistency check job."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return

    if not await acquire_job_lock("consistency_check"):
        logger.debug("Consistency check already running, skipping")
        return

    try:
        from app.sync.consistency import run_consistency_check
        await run_consistency_check()
    finally:
        await release_job_lock("consistency_check")


async def refresh_expiring_tokens() -> None:
    """Proactively refresh tokens that will expire soon."""
    db = await get_database()

    # Find tokens expiring within 1 hour
    threshold = (datetime.utcnow() + timedelta(hours=1)).isoformat()

    cursor = await db.execute(
        """SELECT ot.*, u.email as user_email
           FROM oauth_tokens ot
           JOIN users u ON ot.user_id = u.id
           WHERE ot.token_expiry IS NOT NULL AND ot.token_expiry < ?""",
        (threshold,)
    )
    expiring = await cursor.fetchall()

    if not expiring:
        return

    logger.info(f"Refreshing {len(expiring)} expiring tokens")

    from app.auth.google import get_valid_access_token

    for token in expiring:
        try:
            # This will automatically refresh if needed
            await get_valid_access_token(token["user_id"], token["google_account_email"])
            logger.debug(f"Refreshed token for {token['google_account_email']}")
        except Exception as e:
            logger.error(f"Failed to refresh token for {token['google_account_email']}: {e}")

            # Check if this is a permanent error
            if "invalid_grant" in str(e).lower():
                from app.alerts.email import queue_alert
                await queue_alert(
                    alert_type="token_revoked",
                    user_id=token["user_id"],
                    details=f"Token for {token['google_account_email']} has been revoked. User needs to re-authenticate."
                )


async def acquire_job_lock(job_name: str, timeout_minutes: int = 30) -> bool:
    """
    Acquire a lock for a job.

    Returns True if lock acquired, False if job is already running.
    """
    db = await get_database()
    now = datetime.utcnow()
    cutoff = (now - timedelta(minutes=timeout_minutes)).isoformat()

    # First, try to clean up stale locks
    await db.execute(
        """DELETE FROM job_locks WHERE job_name = ? AND locked_at < ?""",
        (job_name, cutoff)
    )
    await db.commit()

    # Now try to acquire the lock
    try:
        await db.execute(
            """INSERT INTO job_locks (job_name, locked_at, locked_by)
               VALUES (?, ?, ?)""",
            (job_name, now.isoformat(), "worker")
        )
        await db.commit()
        return True
    except Exception:
        # Lock already held by another process
        return False


async def release_job_lock(job_name: str) -> None:
    """Release a job lock."""
    db = await get_database()
    await db.execute("DELETE FROM job_locks WHERE job_name = ?", (job_name,))
    await db.commit()
