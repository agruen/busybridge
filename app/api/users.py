"""User API endpoints."""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.auth.google import get_valid_access_token
from app.database import get_database, get_setting, set_setting
from app.encryption import decrypt_value, encrypt_value

logger = logging.getLogger(__name__)
router = APIRouter(tags=["users"])


class UserResponse(BaseModel):
    """User response model."""
    id: int
    email: str
    display_name: Optional[str] = None
    main_calendar_id: Optional[str] = None
    is_admin: bool = False


class CalendarInfo(BaseModel):
    """Calendar info from Google."""
    id: str
    summary: str
    primary: bool = False
    access_role: str


class SetMainCalendarRequest(BaseModel):
    """Request to set main calendar."""
    calendar_id: str


class AlertPreferences(BaseModel):
    """User alert preferences."""
    email_on_sync_failure: bool = True
    email_on_token_revoked: bool = True


@router.get("/me", response_model=UserResponse)
async def get_me(user: User = Depends(get_current_user)):
    """Get current user profile."""
    return UserResponse(
        id=user.id,
        email=user.email,
        display_name=user.display_name,
        main_calendar_id=user.main_calendar_id,
        is_admin=user.is_admin,
    )


@router.get("/me/calendars")
async def list_my_calendars(user: User = Depends(get_current_user)):
    """List user's Google calendars for selection."""
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, user.email)
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        result = service.calendarList().list().execute()
        calendars = []

        for cal in result.get("items", []):
            calendars.append(CalendarInfo(
                id=cal["id"],
                summary=cal.get("summary", cal["id"]),
                primary=cal.get("primary", False),
                access_role=cal.get("accessRole", "reader"),
            ))

        return {"calendars": calendars}

    except Exception as e:
        logger.exception(f"Failed to list calendars: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list calendars: {str(e)}"
        )


@router.put("/me/main-calendar")
async def set_main_calendar(
    request: SetMainCalendarRequest,
    user: User = Depends(get_current_user)
):
    """Set user's main calendar."""
    db = await get_database()

    # Verify the calendar exists and user has access
    try:
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, user.email)
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        # This will raise if calendar doesn't exist or no access
        service.calendars().get(calendarId=request.calendar_id).execute()

    except Exception as e:
        logger.error(f"Failed to verify calendar: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot access the specified calendar"
        )

    # Update user's main calendar
    await db.execute(
        "UPDATE users SET main_calendar_id = ? WHERE id = ?",
        (request.calendar_id, user.id)
    )
    await db.commit()

    return {"status": "ok", "main_calendar_id": request.calendar_id}


@router.get("/me/alert-preferences", response_model=AlertPreferences)
async def get_alert_preferences(user: User = Depends(get_current_user)):
    """Get user's alert preferences."""
    sync_failure = await get_setting(f"user_{user.id}_alert_sync_failure")
    token_revoked = await get_setting(f"user_{user.id}_alert_token_revoked")

    return AlertPreferences(
        email_on_sync_failure=sync_failure is None or sync_failure.get("value_plain") != "false",
        email_on_token_revoked=token_revoked is None or token_revoked.get("value_plain") != "false",
    )


@router.put("/me/alert-preferences")
async def update_alert_preferences(
    prefs: AlertPreferences,
    user: User = Depends(get_current_user)
):
    """Update user's alert preferences."""
    await set_setting(
        f"user_{user.id}_alert_sync_failure",
        "true" if prefs.email_on_sync_failure else "false"
    )
    await set_setting(
        f"user_{user.id}_alert_token_revoked",
        "true" if prefs.email_on_token_revoked else "false"
    )

    return {"status": "ok"}
