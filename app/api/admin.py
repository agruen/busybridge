"""Admin API endpoints."""

import json
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.auth.session import require_admin, User
from app.database import get_database, get_setting, set_setting
from app.config import get_settings
from app.encryption import encrypt_value

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin"])


class SystemHealth(BaseModel):
    """System health overview."""
    total_users: int
    active_users_24h: int
    total_calendars: int
    active_calendars: int
    total_events_synced: int
    total_busy_blocks: int
    sync_errors_24h: int
    webhooks_active: int
    webhooks_expiring_soon: int
    sync_paused: bool
    database_size_mb: float


class UserSummary(BaseModel):
    """User summary for admin view."""
    id: int
    email: str
    display_name: Optional[str] = None
    is_admin: bool
    calendars_connected: int
    last_login: Optional[str] = None
    created_at: str


class UserDetail(BaseModel):
    """Detailed user information."""
    id: int
    email: str
    display_name: Optional[str] = None
    main_calendar_id: Optional[str] = None
    is_admin: bool
    created_at: str
    last_login_at: Optional[str] = None
    calendars: list
    recent_sync_logs: list


class SettingsResponse(BaseModel):
    """System settings response."""
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_from_address: Optional[str] = None
    alert_emails: Optional[str] = None
    alerts_enabled: bool = False
    sync_paused: bool = False


class UpdateSettingsRequest(BaseModel):
    """Request to update settings."""
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_from_address: Optional[str] = None
    alert_emails: Optional[str] = None
    alerts_enabled: Optional[bool] = None


class FactoryResetRequest(BaseModel):
    """Factory reset request."""
    confirmation: str  # Must be "RESET"


@router.get("/health", response_model=SystemHealth)
async def get_system_health(admin: User = Depends(require_admin)):
    """Get detailed system health."""
    db = await get_database()
    settings = get_settings()

    # Total users
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    total_users = (await cursor.fetchone())[0]

    # Active users in last 24h
    cursor = await db.execute(
        """SELECT COUNT(*) FROM users
           WHERE last_login_at > datetime('now', '-1 day')"""
    )
    active_users = (await cursor.fetchone())[0]

    # Total and active calendars
    cursor = await db.execute("SELECT COUNT(*) FROM client_calendars")
    total_calendars = (await cursor.fetchone())[0]

    cursor = await db.execute(
        "SELECT COUNT(*) FROM client_calendars WHERE is_active = TRUE"
    )
    active_calendars = (await cursor.fetchone())[0]

    # Events and busy blocks
    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE deleted_at IS NULL"
    )
    total_events = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM busy_blocks")
    total_busy_blocks = (await cursor.fetchone())[0]

    # Sync errors in last 24h
    cursor = await db.execute(
        """SELECT COUNT(*) FROM sync_log
           WHERE status = 'failure' AND created_at > datetime('now', '-1 day')"""
    )
    sync_errors = (await cursor.fetchone())[0]

    # Webhooks
    cursor = await db.execute(
        "SELECT COUNT(*) FROM webhook_channels WHERE expiration > datetime('now')"
    )
    active_webhooks = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM webhook_channels
           WHERE expiration > datetime('now')
           AND expiration < datetime('now', '+1 day')"""
    )
    expiring_webhooks = (await cursor.fetchone())[0]

    # Sync paused
    paused = await get_setting("sync_paused")
    sync_paused = paused and paused.get("value_plain") == "true"

    # Database size
    db_size = 0
    if os.path.exists(settings.database_path):
        db_size = os.path.getsize(settings.database_path) / (1024 * 1024)

    return SystemHealth(
        total_users=total_users,
        active_users_24h=active_users,
        total_calendars=total_calendars,
        active_calendars=active_calendars,
        total_events_synced=total_events,
        total_busy_blocks=total_busy_blocks,
        sync_errors_24h=sync_errors,
        webhooks_active=active_webhooks,
        webhooks_expiring_soon=expiring_webhooks,
        sync_paused=sync_paused,
        database_size_mb=round(db_size, 2),
    )


@router.get("/users", response_model=list[UserSummary])
async def list_users(
    admin: User = Depends(require_admin),
    search: Optional[str] = None,
):
    """List all users."""
    db = await get_database()

    query = """
        SELECT u.*, COUNT(cc.id) as calendar_count
        FROM users u
        LEFT JOIN client_calendars cc ON u.id = cc.user_id AND cc.is_active = TRUE
    """
    params = []

    if search:
        query += " WHERE u.email LIKE ? OR u.display_name LIKE ?"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " GROUP BY u.id ORDER BY u.created_at DESC"

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()

    return [
        UserSummary(
            id=row["id"],
            email=row["email"],
            display_name=row["display_name"],
            is_admin=bool(row["is_admin"]),
            calendars_connected=row["calendar_count"],
            last_login=row["last_login_at"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


@router.get("/users/{user_id}", response_model=UserDetail)
async def get_user_detail(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Get detailed user information."""
    db = await get_database()

    # Get user
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = await cursor.fetchone()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Get calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.consecutive_failures,
                  css.last_incremental_sync
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ?
           ORDER BY cc.created_at DESC""",
        (user_id,)
    )
    calendars = [dict(row) for row in await cursor.fetchall()]

    # Get recent sync logs
    cursor = await db.execute(
        """SELECT * FROM sync_log
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 20""",
        (user_id,)
    )
    logs = [dict(row) for row in await cursor.fetchall()]

    return UserDetail(
        id=user["id"],
        email=user["email"],
        display_name=user["display_name"],
        main_calendar_id=user["main_calendar_id"],
        is_admin=bool(user["is_admin"]),
        created_at=user["created_at"],
        last_login_at=user["last_login_at"],
        calendars=calendars,
        recent_sync_logs=logs,
    )


@router.post("/users/{user_id}/sync")
async def trigger_user_sync(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Trigger sync for a user's calendars."""
    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    from app.sync.engine import trigger_sync_for_user
    import asyncio
    asyncio.create_task(trigger_sync_for_user(user_id))

    return {"status": "ok", "message": "Sync triggered"}


@router.post("/users/{user_id}/force-reauth")
async def force_user_reauth(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Force user to re-authenticate by invalidating their tokens."""
    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Delete all tokens for user
    await db.execute("DELETE FROM oauth_tokens WHERE user_id = ?", (user_id,))
    await db.commit()

    # Log action
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'force_reauth', 'success', 'Admin forced re-authentication')""",
        (user_id,)
    )
    await db.commit()

    return {"status": "ok", "message": "User tokens invalidated"}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    admin: User = Depends(require_admin),
):
    """Delete a user and all their data."""
    if user_id == admin.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete yourself"
        )

    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    # Cleanup user's calendars first
    cursor = await db.execute(
        "SELECT id FROM client_calendars WHERE user_id = ?", (user_id,)
    )
    calendars = await cursor.fetchall()

    from app.sync.engine import cleanup_disconnected_calendar
    for cal in calendars:
        try:
            await cleanup_disconnected_calendar(cal["id"], user_id)
        except Exception as e:
            logger.warning(f"Error cleaning up calendar {cal['id']}: {e}")

    # Delete user (cascades to related records)
    await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    await db.commit()

    return {"status": "ok", "message": "User deleted"}


@router.put("/users/{user_id}/admin")
async def set_user_admin(
    user_id: int,
    is_admin: bool,
    admin: User = Depends(require_admin),
):
    """Set admin status for a user."""
    db = await get_database()

    cursor = await db.execute("SELECT id FROM users WHERE id = ?", (user_id,))
    if not await cursor.fetchone():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )

    await db.execute(
        "UPDATE users SET is_admin = ? WHERE id = ?",
        (is_admin, user_id)
    )
    await db.commit()

    return {"status": "ok", "is_admin": is_admin}


@router.post("/users/{user_id}/calendars/{calendar_id}/disconnect")
async def admin_disconnect_calendar(
    user_id: int,
    calendar_id: int,
    admin: User = Depends(require_admin),
):
    """Disconnect a calendar for a user."""
    db = await get_database()

    cursor = await db.execute(
        """SELECT * FROM client_calendars
           WHERE id = ? AND user_id = ? AND is_active = TRUE""",
        (calendar_id, user_id)
    )
    calendar = await cursor.fetchone()

    if not calendar:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar not found"
        )

    from app.sync.engine import cleanup_disconnected_calendar
    await cleanup_disconnected_calendar(calendar_id, user_id)

    await db.execute(
        """UPDATE client_calendars
           SET is_active = FALSE, disconnected_at = ?
           WHERE id = ?""",
        (datetime.utcnow().isoformat(), calendar_id)
    )
    await db.commit()

    return {"status": "ok", "message": "Calendar disconnected"}


@router.get("/logs")
async def get_system_logs(
    admin: User = Depends(require_admin),
    page: int = 1,
    page_size: int = 100,
    user_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    action_filter: Optional[str] = None,
):
    """Get system-wide sync logs."""
    db = await get_database()

    query = """
        SELECT sl.*, u.email as user_email, cc.display_name as calendar_name
        FROM sync_log sl
        LEFT JOIN users u ON sl.user_id = u.id
        LEFT JOIN client_calendars cc ON sl.calendar_id = cc.id
        WHERE 1=1
    """
    params = []

    if user_id:
        query += " AND sl.user_id = ?"
        params.append(user_id)

    if status_filter:
        query += " AND sl.status = ?"
        params.append(status_filter)

    if action_filter:
        query += " AND sl.action = ?"
        params.append(action_filter)

    # Get total
    count_query = query.replace(
        "SELECT sl.*, u.email as user_email, cc.display_name as calendar_name",
        "SELECT COUNT(*)"
    )
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    # Get paginated results
    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()

    return {
        "entries": [dict(row) for row in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/sync/pause")
async def pause_sync(admin: User = Depends(require_admin)):
    """Pause all sync operations."""
    await set_setting("sync_paused", "true")
    return {"status": "ok", "sync_paused": True}


@router.post("/sync/resume")
async def resume_sync(admin: User = Depends(require_admin)):
    """Resume sync operations."""
    await set_setting("sync_paused", "false")
    return {"status": "ok", "sync_paused": False}


@router.post("/cleanup")
async def trigger_cleanup(admin: User = Depends(require_admin)):
    """Trigger manual cleanup of old records."""
    from app.jobs.cleanup import run_retention_cleanup
    import asyncio
    asyncio.create_task(run_retention_cleanup())
    return {"status": "ok", "message": "Cleanup triggered"}


@router.get("/settings", response_model=SettingsResponse)
async def get_admin_settings(admin: User = Depends(require_admin)):
    """Get system settings."""
    result = SettingsResponse()

    for key in ["smtp_host", "smtp_port", "smtp_username", "smtp_from_address", "alert_emails"]:
        setting = await get_setting(key)
        if setting:
            value = setting.get("value_plain")
            if key == "smtp_port" and value:
                value = int(value)
            setattr(result, key, value)

    alerts_enabled = await get_setting("alerts_enabled")
    result.alerts_enabled = alerts_enabled and alerts_enabled.get("value_plain") == "true"

    sync_paused = await get_setting("sync_paused")
    result.sync_paused = sync_paused and sync_paused.get("value_plain") == "true"

    return result


@router.put("/settings")
async def update_admin_settings(
    request: UpdateSettingsRequest,
    admin: User = Depends(require_admin),
):
    """Update system settings."""
    if request.smtp_host is not None:
        await set_setting("smtp_host", request.smtp_host)

    if request.smtp_port is not None:
        await set_setting("smtp_port", str(request.smtp_port))

    if request.smtp_username is not None:
        await set_setting("smtp_username", request.smtp_username)

    if request.smtp_password is not None:
        await set_setting("smtp_password", request.smtp_password, is_sensitive=True, encrypt_func=encrypt_value)

    if request.smtp_from_address is not None:
        await set_setting("smtp_from_address", request.smtp_from_address)

    if request.alert_emails is not None:
        await set_setting("alert_emails", request.alert_emails)

    if request.alerts_enabled is not None:
        await set_setting("alerts_enabled", "true" if request.alerts_enabled else "false")

    return {"status": "ok"}


@router.post("/settings/test-email")
async def send_test_email(admin: User = Depends(require_admin)):
    """Send a test email to verify SMTP configuration."""
    from app.alerts.email import send_email

    try:
        await send_email(
            to_email=admin.email,
            subject="Calendar Sync - Test Email",
            body="This is a test email from Calendar Sync Engine.\n\nIf you received this, your email configuration is working correctly.",
        )
        return {"status": "ok", "message": f"Test email sent to {admin.email}"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to send email: {str(e)}"
        )


@router.post("/factory-reset")
async def factory_reset(
    request: FactoryResetRequest,
    admin: User = Depends(require_admin),
):
    """Factory reset - delete all data and return to OOBE."""
    if request.confirmation != "RESET":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation must be 'RESET'"
        )

    db = await get_database()
    settings = get_settings()

    # Delete all data
    tables = [
        "webhook_channels", "alert_queue", "sync_log", "busy_blocks",
        "event_mappings", "calendar_sync_state", "main_calendar_sync_state",
        "client_calendars", "oauth_tokens", "users", "settings", "organization",
        "job_locks"
    ]

    for table in tables:
        await db.execute(f"DELETE FROM {table}")

    await db.commit()

    # Delete encryption key file
    if os.path.exists(settings.encryption_key_file):
        os.remove(settings.encryption_key_file)

    return {"status": "ok", "message": "Factory reset complete. Please restart the application."}


@router.get("/export")
async def export_database(admin: User = Depends(require_admin)):
    """Download database backup."""
    settings = get_settings()

    if not os.path.exists(settings.database_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database file not found"
        )

    return FileResponse(
        path=settings.database_path,
        filename=f"calendar-sync-backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.db",
        media_type="application/octet-stream",
    )
