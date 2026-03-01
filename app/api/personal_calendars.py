"""Personal calendar management API endpoints."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.auth.google import get_valid_access_token
from app.database import get_database

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/personal-calendars", tags=["personal-calendars"])


class PersonalCalendarResponse(BaseModel):
    """Personal calendar response model."""
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    google_account_email: str
    is_active: bool = True
    last_sync: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0


class ConnectPersonalCalendarRequest(BaseModel):
    """Request to connect a personal calendar."""
    token_id: int
    calendar_id: str
    display_name: Optional[str] = None


class PersonalCalendarStatusResponse(BaseModel):
    """Detailed personal calendar status response."""
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    is_active: bool = True
    sync_token: Optional[str] = None
    last_full_sync: Optional[str] = None
    last_incremental_sync: Optional[str] = None
    consecutive_failures: int = 0
    last_error: Optional[str] = None
    event_count: int = 0
    busy_block_count: int = 0


@router.get("", response_model=list[PersonalCalendarResponse])
async def list_personal_calendars(user: User = Depends(get_current_user)):
    """List connected personal calendars for current user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE AND cc.calendar_type = 'personal'
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )

    rows = await cursor.fetchall()
    calendars = []

    for row in rows:
        last_sync = row["last_incremental_sync"] or row["last_full_sync"]

        sync_status = "ok"
        if row["consecutive_failures"] and row["consecutive_failures"] >= 5:
            sync_status = "error"
        elif row["consecutive_failures"] and row["consecutive_failures"] >= 1:
            sync_status = "warning"
        elif not last_sync:
            sync_status = "pending"

        calendars.append(PersonalCalendarResponse(
            id=row["id"],
            google_calendar_id=row["google_calendar_id"],
            display_name=row["display_name"],
            google_account_email=row["google_account_email"],
            is_active=bool(row["is_active"]),
            last_sync=last_sync,
            sync_status=sync_status,
            consecutive_failures=row["consecutive_failures"] or 0,
        ))

    return calendars


@router.post("", response_model=PersonalCalendarResponse)
async def connect_personal_calendar(
    request: ConnectPersonalCalendarRequest,
    user: User = Depends(get_current_user)
):
    """Connect a new personal calendar."""
    db = await get_database()

    # Verify token belongs to user (accept any account_type so we can reuse
    # tokens that were originally stored for a different purpose)
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE id = ? AND user_id = ?""",
        (request.token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )

    # Verify calendar exists and is accessible
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, token["google_account_email"])
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        cal_info = service.calendars().get(calendarId=request.calendar_id).execute()
        display_name = request.display_name or cal_info.get("summary", request.calendar_id)

    except Exception as e:
        logger.error(f"Failed to verify personal calendar: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot access calendar: {str(e)}"
        )

    # Prevent connecting the same calendar as the user's main calendar
    cursor = await db.execute(
        "SELECT main_calendar_id FROM users WHERE id = ?",
        (user.id,)
    )
    user_row = await cursor.fetchone()
    if user_row and user_row["main_calendar_id"] == request.calendar_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot connect your main calendar as a personal calendar"
        )

    # Check if already connected (as personal)
    cursor = await db.execute(
        """SELECT id FROM client_calendars
           WHERE user_id = ? AND google_calendar_id = ? AND is_active = TRUE
             AND calendar_type = 'personal'""",
        (user.id, request.calendar_id)
    )
    existing = await cursor.fetchone()

    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Personal calendar already connected"
        )

    # Create the personal calendar connection
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type)
           VALUES (?, ?, ?, ?, 'personal')
           RETURNING id""",
        (user.id, request.token_id, request.calendar_id, display_name)
    )
    row = await cursor.fetchone()
    calendar_id = row["id"]

    # Create sync state entry
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id)
           VALUES (?)""",
        (calendar_id,)
    )
    await db.commit()

    # Log the connection
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'connect_personal', 'success', ?)""",
        (user.id, calendar_id, f'{{"calendar_id": "{request.calendar_id}"}}')
    )
    await db.commit()

    # Trigger initial sync (in background)
    from app.sync.engine import trigger_sync_for_personal_calendar
    from app.utils.tasks import create_background_task
    create_background_task(
        trigger_sync_for_personal_calendar(calendar_id),
        f"initial_sync_personal_{calendar_id}"
    )

    return PersonalCalendarResponse(
        id=calendar_id,
        google_calendar_id=request.calendar_id,
        display_name=display_name,
        google_account_email=token["google_account_email"],
        is_active=True,
        sync_status="pending",
    )


@router.delete("/{calendar_id}")
async def disconnect_personal_calendar(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Disconnect a personal calendar."""
    db = await get_database()

    # Verify calendar belongs to user and is personal
    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE AND calendar_type = 'personal'""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Personal calendar not found"
        )

    # Perform cleanup (reuses the same mechanism as client calendars)
    from app.sync.engine import cleanup_disconnected_calendar
    await cleanup_disconnected_calendar(calendar_id, user.id)

    # Mark calendar as inactive
    await db.execute(
        """UPDATE client_calendars
           SET is_active = FALSE, disconnected_at = ?
           WHERE id = ?""",
        (datetime.utcnow().isoformat(), calendar_id)
    )
    await db.commit()

    # Log the disconnection
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'disconnect_personal', 'success', NULL)""",
        (user.id, calendar_id)
    )
    await db.commit()

    return {"status": "ok", "message": "Personal calendar disconnected"}


@router.post("/{calendar_id}/sync")
async def trigger_personal_calendar_sync(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Trigger manual sync for a personal calendar."""
    db = await get_database()

    # Verify calendar belongs to user and is personal
    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE AND calendar_type = 'personal'""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Personal calendar not found"
        )

    from app.sync.engine import trigger_sync_for_personal_calendar
    from app.utils.tasks import create_background_task
    create_background_task(
        trigger_sync_for_personal_calendar(calendar_id),
        f"manual_sync_personal_{calendar_id}"
    )

    return {"status": "ok", "message": "Personal calendar sync triggered"}


@router.get("/{calendar_id}/status", response_model=PersonalCalendarStatusResponse)
async def get_personal_calendar_status(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Get detailed sync status for a personal calendar."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT cc.*, css.sync_token, css.last_full_sync,
                  css.last_incremental_sync, css.consecutive_failures, css.last_error
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.id = ? AND cc.user_id = ? AND cc.calendar_type = 'personal'""",
        (calendar_id, user.id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Personal calendar not found"
        )

    # Count events and busy blocks originating from this personal calendar
    cursor = await db.execute(
        """SELECT COUNT(*) FROM event_mappings
           WHERE origin_calendar_id = ? AND origin_type = 'personal' AND deleted_at IS NULL""",
        (calendar_id,)
    )
    event_count = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM busy_blocks
           JOIN event_mappings em ON busy_blocks.event_mapping_id = em.id
           WHERE em.origin_calendar_id = ? AND em.origin_type = 'personal'""",
        (calendar_id,)
    )
    busy_block_count = (await cursor.fetchone())[0]

    return PersonalCalendarStatusResponse(
        id=calendar["id"],
        google_calendar_id=calendar["google_calendar_id"],
        display_name=calendar["display_name"],
        is_active=bool(calendar["is_active"]),
        sync_token=calendar["sync_token"][:20] + "..." if calendar["sync_token"] else None,
        last_full_sync=calendar["last_full_sync"],
        last_incremental_sync=calendar["last_incremental_sync"],
        consecutive_failures=calendar["consecutive_failures"] or 0,
        last_error=calendar["last_error"],
        event_count=event_count,
        busy_block_count=busy_block_count,
    )
