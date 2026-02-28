"""APScheduler setup for background jobs."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.config import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def setup_scheduler() -> AsyncIOScheduler:
    """Set up and start the background scheduler."""
    global _scheduler

    settings = get_settings()

    _scheduler = AsyncIOScheduler()

    # Periodic sync job - every 5 minutes
    _scheduler.add_job(
        "app.jobs.sync_job:run_periodic_sync",
        trigger=IntervalTrigger(minutes=settings.sync_interval_minutes),
        id="periodic_sync",
        name="Periodic Calendar Sync",
        replace_existing=True,
    )

    # Webhook renewal - every 6 hours (optional)
    if settings.enable_webhooks:
        _scheduler.add_job(
            "app.jobs.webhook_renewal:renew_expiring_webhooks",
            trigger=IntervalTrigger(hours=settings.webhook_renewal_hours),
            id="webhook_renewal",
            name="Webhook Renewal",
            replace_existing=True,
        )
        # Register webhooks for all users on startup
        _scheduler.add_job(
            "app.jobs.webhook_renewal:register_all_webhooks",
            id="webhook_initial_registration",
            name="Webhook Initial Registration",
            replace_existing=True,
        )
    else:
        logger.info("Webhook renewal job disabled (ENABLE_WEBHOOKS=false)")

    # Consistency check - every hour
    _scheduler.add_job(
        "app.jobs.sync_job:run_consistency_check_job",
        trigger=IntervalTrigger(hours=settings.consistency_check_hours),
        id="consistency_check",
        name="Consistency Check",
        replace_existing=True,
    )

    # Token refresh - every 30 minutes
    _scheduler.add_job(
        "app.jobs.sync_job:refresh_expiring_tokens",
        trigger=IntervalTrigger(minutes=settings.token_refresh_minutes),
        id="token_refresh",
        name="Token Refresh",
        replace_existing=True,
    )

    # Alert queue processing - every minute
    _scheduler.add_job(
        "app.jobs.alerts:process_alert_queue",
        trigger=IntervalTrigger(minutes=settings.alert_process_minutes),
        id="alert_processing",
        name="Alert Queue Processing",
        replace_existing=True,
    )

    # Retention cleanup - daily at 3 AM
    _scheduler.add_job(
        "app.jobs.cleanup:run_retention_cleanup",
        trigger=CronTrigger(hour=3, minute=0),
        id="retention_cleanup",
        name="Retention Cleanup",
        replace_existing=True,
    )

    # Stale alert cleanup - daily at 4 AM
    _scheduler.add_job(
        "app.jobs.alerts:cleanup_stale_alerts",
        trigger=CronTrigger(hour=4, minute=0),
        id="stale_alert_cleanup",
        name="Stale Alert Cleanup",
        replace_existing=True,
    )

    # Backup - daily at 11 PM (uses system timezone, set via TZ env var)
    _scheduler.add_job(
        "app.jobs.backup_job:run_scheduled_backup",
        trigger=CronTrigger(hour=23, minute=0),
        id="daily_backup",
        name="Daily Backup",
        replace_existing=True,
    )

    _scheduler.start()
    logger.info("Background scheduler started")

    return _scheduler


def shutdown_scheduler() -> None:
    """Shutdown the scheduler."""
    global _scheduler

    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Background scheduler stopped")


def get_scheduler() -> AsyncIOScheduler | None:
    """Get the current scheduler instance."""
    return _scheduler
