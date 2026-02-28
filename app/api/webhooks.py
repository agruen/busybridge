"""Webhook receiver for Google Calendar push notifications."""

import logging
from datetime import datetime

from fastapi import APIRouter, Header, HTTPException, Request, status

from app.database import get_database

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/google-calendar")
async def receive_google_calendar_webhook(
    request: Request,
    x_goog_channel_id: str = Header(None, alias="X-Goog-Channel-ID"),
    x_goog_channel_token: str = Header(None, alias="X-Goog-Channel-Token"),
    x_goog_resource_id: str = Header(None, alias="X-Goog-Resource-ID"),
    x_goog_resource_state: str = Header(None, alias="X-Goog-Resource-State"),
    x_goog_message_number: str = Header(None, alias="X-Goog-Message-Number"),
):
    """
    Receive push notifications from Google Calendar.

    Google sends a POST request with headers indicating what changed.
    We don't receive the actual event data - we need to fetch it ourselves.
    """
    if not x_goog_channel_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing channel ID"
        )

    logger.info(
        f"Webhook received: channel={x_goog_channel_id}, "
        f"resource={x_goog_resource_id}, state={x_goog_resource_state}"
    )

    # Handle sync message (sent when webhook is first registered)
    if x_goog_resource_state == "sync":
        logger.info(f"Sync message for channel {x_goog_channel_id}")
        return {"status": "ok"}

    # Look up the channel in our database
    db = await get_database()
    cursor = await db.execute(
        """SELECT * FROM webhook_channels WHERE channel_id = ?""",
        (x_goog_channel_id,)
    )
    channel = await cursor.fetchone()

    if not channel:
        logger.warning(f"Unknown webhook channel: {x_goog_channel_id}")
        # Don't return error - Google will keep retrying
        return {"status": "ok", "message": "Unknown channel"}

    # Verify the shared secret token to confirm the request came from Google.
    # Channels registered without a token (legacy rows with empty string) are
    # accepted so that in-flight channels survive a rolling deploy; they will
    # be replaced by the normal renewal cycle within 6 days.
    import hmac
    stored_token = channel["token"] if channel["token"] else ""
    if stored_token and not hmac.compare_digest(stored_token, x_goog_channel_token or ""):
        logger.warning(f"Webhook token mismatch for channel {x_goog_channel_id}")
        return {"status": "ok"}

    # Verify resource ID matches the channel we registered.
    # If it doesn't match, ignore the notification to avoid triggering sync
    # for a potentially spoofed or stale webhook.
    if (
        x_goog_resource_id
        and channel["resource_id"]
        and x_goog_resource_id != channel["resource_id"]
    ):
        logger.warning(
            f"Webhook resource mismatch for channel {x_goog_channel_id}: "
            f"expected={channel['resource_id']} got={x_goog_resource_id}"
        )
        return {"status": "ok", "message": "Resource mismatch"}

    # Check if channel is expired
    if channel["expiration"]:
        expiry = datetime.fromisoformat(channel["expiration"])
        if datetime.utcnow() > expiry:
            logger.warning(f"Expired webhook channel: {x_goog_channel_id}, cleaning up")
            # Delete expired channel from database
            await db.execute(
                "DELETE FROM webhook_channels WHERE channel_id = ?",
                (x_goog_channel_id,)
            )
            await db.commit()
            # TODO: Trigger webhook re-registration for this calendar
            return {"status": "ok", "message": "Channel expired and removed"}

    # Trigger sync for the affected calendar
    try:
        from app.sync.engine import trigger_sync_for_calendar, trigger_sync_for_main_calendar
        from app.utils.tasks import create_background_task

        if channel["calendar_type"] == "main":
            # Sync main calendar
            create_background_task(
                trigger_sync_for_main_calendar(channel["user_id"]),
                f"sync_main_calendar_user_{channel['user_id']}"
            )
        else:
            # Sync client calendar
            if channel["client_calendar_id"]:
                create_background_task(
                    trigger_sync_for_calendar(channel["client_calendar_id"]),
                    f"sync_calendar_{channel['client_calendar_id']}"
                )

        logger.info(
            f"Sync triggered for calendar: "
            f"type={channel['calendar_type']}, calendar_id={channel['client_calendar_id']}"
        )

    except Exception as e:
        logger.exception(f"Failed to trigger sync from webhook: {e}")
        # Still return OK to avoid Google retrying
        return {"status": "ok", "message": "Sync trigger failed"}

    return {"status": "ok"}


async def register_webhook_channel(
    user_id: int,
    calendar_type: str,
    calendar_id: str,
    client_calendar_id: int = None,
    access_token: str = None,
) -> dict:
    """
    Register a webhook channel with Google Calendar.

    Returns the channel info including expiration time.
    """
    import secrets
    import uuid
    from datetime import timedelta
    import httpx
    from app.config import get_settings

    settings = get_settings()
    channel_id = str(uuid.uuid4())
    channel_token = secrets.token_urlsafe(32)
    webhook_url = f"{settings.public_url}/api/webhooks/google-calendar"

    # Google webhooks expire after max 7 days, we'll set for 6 days
    expiration = datetime.utcnow() + timedelta(days=6)
    expiration_ms = int(expiration.timestamp() * 1000)

    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
        "expiration": str(expiration_ms),
        "token": channel_token,
    }

    # Make the watch request
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/watch",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json=body,
        )

        if response.status_code != 200:
            logger.error(f"Failed to register webhook: {response.text}")
            raise ValueError(f"Failed to register webhook: {response.text}")

        result = response.json()

    # Store the channel in database
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, token, expiration)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            user_id,
            calendar_type,
            client_calendar_id,
            channel_id,
            result["resourceId"],
            channel_token,
            expiration.isoformat(),
        )
    )
    await db.commit()

    logger.info(f"Registered webhook channel {channel_id} for calendar {calendar_id}")

    return {
        "channel_id": channel_id,
        "resource_id": result["resourceId"],
        "expiration": expiration.isoformat(),
    }


async def stop_webhook_channel(channel_id: str, resource_id: str, access_token: str) -> bool:
    """Stop (unregister) a webhook channel."""
    import httpx

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://www.googleapis.com/calendar/v3/channels/stop",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={
                    "id": channel_id,
                    "resourceId": resource_id,
                },
            )

            # 404 is OK - channel might already be stopped
            if response.status_code not in [200, 204, 404]:
                logger.warning(f"Failed to stop webhook channel: {response.text}")
                return False

        # Remove from database
        db = await get_database()
        await db.execute(
            "DELETE FROM webhook_channels WHERE channel_id = ?",
            (channel_id,)
        )
        await db.commit()

        logger.info(f"Stopped webhook channel {channel_id}")
        return True

    except Exception as e:
        logger.exception(f"Error stopping webhook channel: {e}")
        return False
