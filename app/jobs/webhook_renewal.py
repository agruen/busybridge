"""Webhook renewal job."""

import logging
from datetime import datetime, timedelta

from app.database import get_database

logger = logging.getLogger(__name__)


async def renew_expiring_webhooks() -> None:
    """Renew webhook channels that are expiring within 24 hours."""
    db = await get_database()

    # Find webhooks expiring within 24 hours
    threshold = (datetime.utcnow() + timedelta(hours=24)).isoformat()

    cursor = await db.execute(
        """SELECT wc.*, u.email as user_email, u.main_calendar_id,
                  cc.google_calendar_id, ot.google_account_email
           FROM webhook_channels wc
           JOIN users u ON wc.user_id = u.id
           LEFT JOIN client_calendars cc ON wc.client_calendar_id = cc.id
           LEFT JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE wc.expiration < ?""",
        (threshold,)
    )
    expiring = await cursor.fetchall()

    if not expiring:
        logger.debug("No webhooks need renewal")
        return

    logger.info(f"Renewing {len(expiring)} expiring webhooks")

    from app.auth.google import get_valid_access_token
    from app.api.webhooks import register_webhook_channel, stop_webhook_channel

    for webhook in expiring:
        try:
            # Determine which calendar this is for
            if webhook["calendar_type"] == "main":
                calendar_id = webhook["main_calendar_id"]
                email = webhook["user_email"]
            else:
                calendar_id = webhook["google_calendar_id"]
                email = webhook["google_account_email"]

            if not calendar_id or not email:
                logger.warning(f"Missing calendar info for webhook {webhook['channel_id']}")
                continue

            # Get access token
            access_token = await get_valid_access_token(webhook["user_id"], email)

            # Stop old channel
            await stop_webhook_channel(
                webhook["channel_id"],
                webhook["resource_id"],
                access_token
            )

            # Register new channel
            await register_webhook_channel(
                user_id=webhook["user_id"],
                calendar_type=webhook["calendar_type"],
                calendar_id=calendar_id,
                client_calendar_id=webhook["client_calendar_id"],
                access_token=access_token,
            )

            logger.info(f"Renewed webhook for calendar {calendar_id}")

        except Exception as e:
            logger.error(f"Failed to renew webhook {webhook['channel_id']}: {e}")

            # Queue alert if this keeps failing
            from app.alerts.email import queue_alert
            await queue_alert(
                alert_type="webhook_registration_failed",
                user_id=webhook["user_id"],
                details=f"Failed to renew webhook: {str(e)}"
            )


async def register_webhooks_for_user(user_id: int) -> None:
    """Register webhooks for all of a user's calendars."""
    db = await get_database()

    # Get user info
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()

    if not user or not user["main_calendar_id"]:
        return

    from app.auth.google import get_valid_access_token
    from app.api.webhooks import register_webhook_channel

    # Register for main calendar
    try:
        access_token = await get_valid_access_token(user_id, user["email"])
        await register_webhook_channel(
            user_id=user_id,
            calendar_type="main",
            calendar_id=user["main_calendar_id"],
            access_token=access_token,
        )
        logger.info(f"Registered webhook for main calendar of user {user_id}")
    except Exception as e:
        logger.error(f"Failed to register main calendar webhook: {e}")

    # Register for client and personal calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,)
    )
    calendars = await cursor.fetchall()

    for cal in calendars:
        try:
            cal_type = cal["calendar_type"] if "calendar_type" in cal.keys() else "client"
            access_token = await get_valid_access_token(user_id, cal["google_account_email"])
            await register_webhook_channel(
                user_id=user_id,
                calendar_type=cal_type,
                calendar_id=cal["google_calendar_id"],
                client_calendar_id=cal["id"],
                access_token=access_token,
            )
            logger.info(f"Registered webhook for {cal_type} calendar {cal['id']}")
        except Exception as e:
            logger.error(f"Failed to register webhook for calendar {cal['id']}: {e}")
