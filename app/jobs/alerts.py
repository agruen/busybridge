"""Alert processing job."""

import logging
from datetime import datetime, timedelta

from app.database import get_database

logger = logging.getLogger(__name__)


async def process_alert_queue() -> int:
    """
    Process the email alert queue.

    Returns number of alerts processed.
    """
    db = await get_database()

    # Get unsent alerts, limit to batch size
    cursor = await db.execute(
        """SELECT * FROM alert_queue
           WHERE sent_at IS NULL AND attempts < 3
           ORDER BY created_at ASC
           LIMIT 10"""
    )
    alerts = await cursor.fetchall()

    if not alerts:
        return 0

    processed = 0
    from app.alerts.email import send_email

    for alert in alerts:
        try:
            await send_email(
                to_email=alert["recipient_email"],
                subject=alert["subject"],
                body=alert["body"],
            )

            # Mark as sent
            await db.execute(
                "UPDATE alert_queue SET sent_at = ? WHERE id = ?",
                (datetime.utcnow().isoformat(), alert["id"])
            )
            await db.commit()

            processed += 1
            logger.info(f"Sent alert {alert['id']} to {alert['recipient_email']}")

        except Exception as e:
            logger.error(f"Failed to send alert {alert['id']}: {e}")

            # Update attempt count and last attempt time
            backoff_minutes = 2 ** alert["attempts"]  # Exponential backoff
            await db.execute(
                """UPDATE alert_queue
                   SET attempts = attempts + 1, last_attempt = ?
                   WHERE id = ?""",
                (datetime.utcnow().isoformat(), alert["id"])
            )
            await db.commit()

    return processed


async def cleanup_stale_alerts() -> int:
    """
    Clean up old alerts.

    - Delete sent alerts older than 7 days
    - Delete failed alerts (3+ attempts) older than 7 days
    """
    db = await get_database()
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()

    cursor = await db.execute(
        """DELETE FROM alert_queue
           WHERE (sent_at IS NOT NULL AND sent_at < ?)
           OR (attempts >= 3 AND created_at < ?)
           RETURNING id""",
        (cutoff, cutoff)
    )
    deleted = await cursor.fetchall()
    await db.commit()

    count = len(deleted)
    if count:
        logger.info(f"Cleaned up {count} stale alerts")

    return count
