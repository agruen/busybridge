"""Periodic sync job."""

import logging
from datetime import datetime, timedelta

from app.database import get_database, get_setting, set_setting

logger = logging.getLogger(__name__)

# Auto-pause when every active calendar has this many consecutive failures
_CIRCUIT_BREAKER_THRESHOLD = 3


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

        # Get all active calendars
        cursor = await db.execute(
            """SELECT id, calendar_type FROM client_calendars WHERE is_active = TRUE"""
        )
        calendars = await cursor.fetchall()

        client_cals = [c for c in calendars if c["calendar_type"] != "personal"]
        personal_cals = [c for c in calendars if c["calendar_type"] == "personal"]

        logger.info(f"Running periodic sync for {len(client_cals)} client + {len(personal_cals)} personal calendars")

        from app.sync.engine import trigger_sync_for_calendar, trigger_sync_for_personal_calendar

        for cal in client_cals:
            try:
                await trigger_sync_for_calendar(cal["id"], schedule_verification=False)
            except Exception as e:
                logger.error(f"Error syncing client calendar {cal['id']}: {e}")

        for cal in personal_cals:
            try:
                await trigger_sync_for_personal_calendar(cal["id"])
            except Exception as e:
                logger.error(f"Error syncing personal calendar {cal['id']}: {e}")

        # Sync webcal subscriptions
        cursor = await db.execute(
            "SELECT id FROM webcal_subscriptions WHERE is_active = TRUE"
        )
        webcal_subs = await cursor.fetchall()
        if webcal_subs:
            logger.info(f"Running periodic sync for {len(webcal_subs)} webcal subscriptions")
            from app.sync.webcal_sync import sync_webcal_subscription
            for sub in webcal_subs:
                try:
                    await sync_webcal_subscription(sub["id"])
                except Exception as e:
                    logger.error(f"Error syncing webcal subscription {sub['id']}: {e}")

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

        # Circuit breaker: auto-pause if ALL calendars are failing
        await _check_circuit_breaker()

    finally:
        await release_job_lock("periodic_sync")


async def _check_circuit_breaker() -> None:
    """Auto-pause sync if every active calendar is consistently failing.

    Triggers when ALL calendars for a user have >= _CIRCUIT_BREAKER_THRESHOLD
    consecutive failures. Sends an alert and pauses sync to prevent
    wasting API quota and flooding logs.
    """
    db = await get_database()

    # Get all users with active calendars
    cursor = await db.execute(
        """SELECT DISTINCT user_id FROM client_calendars WHERE is_active = TRUE"""
    )
    user_ids = [row["user_id"] for row in await cursor.fetchall()]

    for user_id in user_ids:
        # Count active calendars and how many are at/above the failure threshold
        cursor = await db.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN COALESCE(css.consecutive_failures, 0) >= ? THEN 1 ELSE 0 END) as failing
               FROM client_calendars cc
               LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
               WHERE cc.user_id = ? AND cc.is_active = TRUE""",
            (_CIRCUIT_BREAKER_THRESHOLD, user_id),
        )
        row = await cursor.fetchone()
        total = row["total"] or 0
        failing = row["failing"] or 0

        if total == 0 or failing < total:
            continue  # Not all calendars are failing

        # Every calendar is failing — trip the circuit breaker
        logger.error(
            "Circuit breaker: ALL %d calendars for user %d have %d+ consecutive failures. "
            "Auto-pausing sync.",
            total, user_id, _CIRCUIT_BREAKER_THRESHOLD,
        )
        await set_setting("sync_paused", "true")

        from app.alerts.email import queue_alert
        await queue_alert(
            alert_type="circuit_breaker",
            user_id=user_id,
            details=(
                f"Sync has been automatically paused because all {total} calendars "
                f"have failed {_CIRCUIT_BREAKER_THRESHOLD}+ times consecutively. "
                f"This usually means OAuth tokens have expired or Google is having an outage. "
                f"Check your connected accounts and resume sync from the settings page."
            ),
        )
        return  # Paused globally, no need to check other users


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


async def run_orphan_scan_job() -> None:
    """Periodic scan for orphaned events on Google Calendars."""
    paused = await get_setting("sync_paused")
    if paused and paused.get("value_plain") == "true":
        return

    if not await acquire_job_lock("orphan_scan"):
        logger.debug("Orphan scan already running, skipping")
        return

    try:
        from app.sync.consistency import scan_for_orphans
        result = await scan_for_orphans(dry_run=False)
        if result.get("orphans_found") or result.get("db_duplicates_found"):
            logger.warning(
                "Orphan scan found %d orphans, %d DB duplicates",
                result["orphans_found"], result["db_duplicates_found"],
            )
    except Exception:
        logger.exception("Orphan scan job failed")
    finally:
        await release_job_lock("orphan_scan")


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
