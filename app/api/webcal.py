"""Webcal/ICS subscription management API endpoints."""

import json
import logging
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.database import get_database

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webcal-subscriptions", tags=["webcal-subscriptions"])


class WebcalSubscriptionResponse(BaseModel):
    id: int
    url: str
    display_prefix: str
    is_active: bool = True
    last_poll_at: Optional[str] = None
    last_success_at: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    event_count: int = 0


class CreateWebcalRequest(BaseModel):
    url: str
    display_prefix: str = ""


class UpdateWebcalRequest(BaseModel):
    display_prefix: str


@router.get("", response_model=list[WebcalSubscriptionResponse])
async def list_webcal_subscriptions(user: User = Depends(get_current_user)):
    """List webcal subscriptions for current user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT ws.*,
                  (SELECT COUNT(*) FROM event_mappings em
                   WHERE em.webcal_subscription_id = ws.id AND em.deleted_at IS NULL) as event_count
           FROM webcal_subscriptions ws
           WHERE ws.user_id = ? AND ws.is_active = TRUE
           ORDER BY ws.created_at DESC""",
        (user.id,),
    )
    rows = await cursor.fetchall()

    results = []
    for row in rows:
        sync_status = "ok"
        if row["consecutive_failures"] and row["consecutive_failures"] >= 5:
            sync_status = "error"
        elif row["consecutive_failures"] and row["consecutive_failures"] >= 1:
            sync_status = "warning"
        elif not row["last_success_at"]:
            sync_status = "pending"

        results.append(WebcalSubscriptionResponse(
            id=row["id"],
            url=row["url"],
            display_prefix=row["display_prefix"] or "",
            is_active=bool(row["is_active"]),
            last_poll_at=row["last_poll_at"],
            last_success_at=row["last_success_at"],
            sync_status=sync_status,
            consecutive_failures=row["consecutive_failures"] or 0,
            last_error=row["last_error"],
            event_count=row["event_count"] or 0,
        ))

    return results


@router.post("", response_model=WebcalSubscriptionResponse)
async def create_webcal_subscription(
    request: CreateWebcalRequest,
    user: User = Depends(get_current_user),
):
    """Add a new webcal subscription."""
    db = await get_database()

    # Normalize URL
    url = request.url.strip()
    if url.startswith("webcal://"):
        url = "https://" + url[len("webcal://"):]

    if not url.startswith(("https://", "http://")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL must start with https://, http://, or webcal://",
        )

    # Check for duplicates
    cursor = await db.execute(
        "SELECT id FROM webcal_subscriptions WHERE user_id = ? AND url = ? AND is_active = TRUE",
        (user.id, url),
    )
    if await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This URL is already subscribed",
        )

    # Validate by trying to fetch
    from app.sync.ics_parser import fetch_ics_feed
    try:
        content, etag = await fetch_ics_feed(url)
        if content is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not fetch ICS feed from this URL",
            )
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to fetch URL: {e}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid ICS feed: {e}",
        )

    # Insert
    cursor = await db.execute(
        """INSERT INTO webcal_subscriptions (user_id, url, display_prefix)
           VALUES (?, ?, ?)
           RETURNING id""",
        (user.id, url, request.display_prefix.strip()),
    )
    row = await cursor.fetchone()
    sub_id = row["id"]
    await db.commit()

    # Log
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'connect_webcal', 'success', ?)""",
        (user.id, json.dumps({"url": url, "prefix": request.display_prefix})),
    )
    await db.commit()

    # Trigger initial sync
    from app.sync.webcal_sync import sync_webcal_subscription
    from app.utils.tasks import create_background_task
    create_background_task(
        sync_webcal_subscription(sub_id),
        f"initial_sync_webcal_{sub_id}",
    )

    return WebcalSubscriptionResponse(
        id=sub_id,
        url=url,
        display_prefix=request.display_prefix.strip(),
        is_active=True,
        sync_status="pending",
    )


@router.delete("/{subscription_id}")
async def delete_webcal_subscription(
    subscription_id: int,
    user: User = Depends(get_current_user),
):
    """Remove a webcal subscription and clean up synced events."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ? AND user_id = ? AND is_active = TRUE",
        (subscription_id, user.id),
    )
    sub = await cursor.fetchone()
    if not sub:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    # Clean up events
    from app.sync.webcal_sync import cleanup_webcal_subscription
    await cleanup_webcal_subscription(subscription_id, user.id)

    # Deactivate
    await db.execute(
        "UPDATE webcal_subscriptions SET is_active = FALSE, updated_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), subscription_id),
    )
    await db.commit()

    # Log
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'disconnect_webcal', 'success', ?)""",
        (user.id, json.dumps({"url": sub["url"]})),
    )
    await db.commit()

    return {"status": "ok", "message": "Webcal subscription removed"}


@router.post("/{subscription_id}/sync")
async def trigger_webcal_sync(
    subscription_id: int,
    user: User = Depends(get_current_user),
):
    """Trigger manual sync for a webcal subscription."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ? AND user_id = ? AND is_active = TRUE",
        (subscription_id, user.id),
    )
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    from app.sync.webcal_sync import sync_webcal_subscription
    from app.utils.tasks import create_background_task
    create_background_task(
        sync_webcal_subscription(subscription_id),
        f"manual_sync_webcal_{subscription_id}",
    )

    return {"status": "ok", "message": "Sync triggered"}


@router.patch("/{subscription_id}")
async def update_webcal_subscription(
    subscription_id: int,
    request: UpdateWebcalRequest,
    user: User = Depends(get_current_user),
):
    """Update a webcal subscription's prefix."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ? AND user_id = ? AND is_active = TRUE",
        (subscription_id, user.id),
    )
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    await db.execute(
        "UPDATE webcal_subscriptions SET display_prefix = ?, updated_at = ? WHERE id = ?",
        (request.display_prefix.strip(), datetime.utcnow().isoformat(), subscription_id),
    )
    await db.commit()

    # Trigger re-sync to update event summaries
    from app.sync.webcal_sync import sync_webcal_subscription
    from app.utils.tasks import create_background_task

    # Clear etag to force re-processing
    await db.execute(
        "UPDATE webcal_subscriptions SET last_etag = NULL WHERE id = ?",
        (subscription_id,),
    )
    await db.commit()

    create_background_task(
        sync_webcal_subscription(subscription_id),
        f"resync_webcal_{subscription_id}",
    )

    return {"status": "ok", "message": "Prefix updated, re-sync triggered"}
