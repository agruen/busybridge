"""Personal calendar management API endpoints."""

import json
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
    id: int
    google_calendar_id: str
    display_name: Optional[str] = None
    google_account_email: str
    is_active: bool = True
    last_sync: Optional[str] = None
    sync_status: str = "unknown"
    consecutive_failures: int = 0


class ConnectPersonalCalendarRequest(BaseModel):
    token_id: int
    calendars: list[dict]  # [{calendar_id, display_name}]


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


@router.post("", response_model=list[PersonalCalendarResponse])
async def connect_personal_calendars(
    request: ConnectPersonalCalendarRequest,
    user: User = Depends(get_current_user)
):
    """Connect one or more personal calendars."""
    db = await get_database()

    # Verify token belongs to user and is a personal token
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE id = ? AND user_id = ? AND account_type = 'personal'""",
        (request.token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )

    if not request.calendars:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No calendars selected"
        )

    results = []
    for cal_info in request.calendars:
        calendar_id = cal_info.get("calendar_id")
        display_name = cal_info.get("display_name", calendar_id)

        if not calendar_id:
            continue

        # Check if already connected
        cursor = await db.execute(
            """SELECT id FROM client_calendars
               WHERE user_id = ? AND google_calendar_id = ? AND is_active = TRUE""",
            (user.id, calendar_id)
        )
        if await cursor.fetchone():
            continue

        # Create the personal calendar connection (no color needed)
        cursor = await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type)
               VALUES (?, ?, ?, ?, 'personal')
               RETURNING id""",
            (user.id, request.token_id, calendar_id, display_name)
        )
        row = await cursor.fetchone()
        new_id = row["id"]

        # Create sync state
        await db.execute(
            "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)",
            (new_id,)
        )
        await db.commit()

        # Log
        await db.execute(
            """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
               VALUES (?, ?, 'connect_personal', 'success', ?)""",
            (user.id, new_id, json.dumps({"calendar_id": calendar_id}))
        )
        await db.commit()

        # Trigger initial sync
        from app.sync.engine import trigger_sync_for_personal_calendar
        from app.utils.tasks import create_background_task
        create_background_task(
            trigger_sync_for_personal_calendar(new_id),
            f"initial_sync_personal_{new_id}"
        )

        results.append(PersonalCalendarResponse(
            id=new_id,
            google_calendar_id=calendar_id,
            display_name=display_name,
            google_account_email=token["google_account_email"],
            is_active=True,
            sync_status="pending",
        ))

    return results


@router.delete("/{calendar_id}")
async def disconnect_personal_calendar(
    calendar_id: int,
    user: User = Depends(get_current_user)
):
    """Disconnect a personal calendar."""
    db = await get_database()

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

    from app.sync.engine import cleanup_disconnected_calendar
    await cleanup_disconnected_calendar(calendar_id, user.id)

    await db.execute(
        """UPDATE client_calendars
           SET is_active = FALSE, disconnected_at = ?
           WHERE id = ?""",
        (datetime.utcnow().isoformat(), calendar_id)
    )
    await db.commit()

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

    return {"status": "ok", "message": "Sync triggered"}
