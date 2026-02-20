"""Sync status and control API endpoints."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.database import get_database, get_setting

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])


class SyncStatusResponse(BaseModel):
    """Overall sync status for a user."""
    calendars_connected: int
    calendars_healthy: int
    calendars_warning: int
    calendars_error: int
    last_sync: Optional[str] = None
    events_synced: int
    busy_blocks_created: int
    sync_paused: bool = False


class SyncLogEntry(BaseModel):
    """Sync log entry."""
    id: int
    calendar_id: Optional[int] = None
    calendar_name: Optional[str] = None
    action: str
    status: str
    details: Optional[str] = None
    created_at: str


class SyncLogResponse(BaseModel):
    """Sync log response."""
    entries: list[SyncLogEntry]
    total: int
    page: int
    page_size: int


class ManagedCleanupResponse(BaseModel):
    """Response for manual managed-event cleanup."""
    status: str
    message: str
    managed_event_prefix: str
    db_main_events_deleted: int
    db_busy_blocks_deleted: int
    prefix_calendars_scanned: int
    prefix_events_deleted: int
    local_mappings_deleted: int
    local_busy_blocks_deleted: int
    errors: list[str]


@router.get("/status", response_model=SyncStatusResponse)
async def get_sync_status(user: User = Depends(get_current_user)):
    """Get overall sync status for current user."""
    db = await get_database()

    # Count calendars by status
    cursor = await db.execute(
        """SELECT cc.id, css.consecutive_failures, css.last_incremental_sync, css.last_full_sync
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user.id,)
    )
    calendars = await cursor.fetchall()

    total = len(calendars)
    healthy = 0
    warning = 0
    error = 0
    last_sync = None

    for cal in calendars:
        failures = cal["consecutive_failures"] or 0
        if failures >= 5:
            error += 1
        elif failures >= 1:
            warning += 1
        else:
            healthy += 1

        cal_last_sync = cal["last_incremental_sync"] or cal["last_full_sync"]
        if cal_last_sync:
            if not last_sync or cal_last_sync > last_sync:
                last_sync = cal_last_sync

    # Count events and busy blocks
    cursor = await db.execute(
        """SELECT COUNT(*) FROM event_mappings
           WHERE user_id = ? AND deleted_at IS NULL""",
        (user.id,)
    )
    events_synced = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM busy_blocks bb
           JOIN client_calendars cc ON bb.client_calendar_id = cc.id
           WHERE cc.user_id = ?""",
        (user.id,)
    )
    busy_blocks = (await cursor.fetchone())[0]

    # Check if sync is paused
    paused_setting = await get_setting("sync_paused")
    sync_paused = bool(paused_setting and paused_setting.get("value_plain") == "true")

    return SyncStatusResponse(
        calendars_connected=total,
        calendars_healthy=healthy,
        calendars_warning=warning,
        calendars_error=error,
        last_sync=last_sync,
        events_synced=events_synced,
        busy_blocks_created=busy_blocks,
        sync_paused=sync_paused,
    )


@router.get("/log", response_model=SyncLogResponse)
async def get_sync_log(
    user: User = Depends(get_current_user),
    page: int = 1,
    page_size: int = 50,
    calendar_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Get sync activity log for current user."""
    db = await get_database()

    # Build query
    query = """
        SELECT sl.*, cc.display_name as calendar_name
        FROM sync_log sl
        LEFT JOIN client_calendars cc ON sl.calendar_id = cc.id
        WHERE sl.user_id = ?
    """
    params = [user.id]

    if calendar_id:
        query += " AND sl.calendar_id = ?"
        params.append(calendar_id)

    if status_filter:
        query += " AND sl.status = ?"
        params.append(status_filter)

    # Get total count
    count_query = query.replace("SELECT sl.*, cc.display_name as calendar_name", "SELECT COUNT(*)")
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    # Get paginated results
    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()

    entries = [
        SyncLogEntry(
            id=row["id"],
            calendar_id=row["calendar_id"],
            calendar_name=row["calendar_name"],
            action=row["action"],
            status=row["status"],
            details=row["details"],
            created_at=row["created_at"],
        )
        for row in rows
    ]

    return SyncLogResponse(
        entries=entries,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/full")
async def trigger_full_resync(user: User = Depends(get_current_user)):
    """Trigger full re-sync for current user's calendars."""
    db = await get_database()

    # Clear sync tokens for all user's calendars
    await db.execute(
        """UPDATE calendar_sync_state
           SET sync_token = NULL
           WHERE client_calendar_id IN (
               SELECT id FROM client_calendars WHERE user_id = ? AND is_active = TRUE
           )""",
        (user.id,)
    )

    # Clear main calendar sync token
    await db.execute(
        """UPDATE main_calendar_sync_state
           SET sync_token = NULL
           WHERE user_id = ?""",
        (user.id,)
    )
    await db.commit()

    # Trigger sync
    from app.sync.engine import trigger_sync_for_user
    from app.utils.tasks import create_background_task
    create_background_task(
        trigger_sync_for_user(user.id),
        f"full_resync_user_{user.id}"
    )

    # Log the action
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'full_resync', 'success', 'User triggered full re-sync')""",
        (user.id,)
    )
    await db.commit()

    return {"status": "ok", "message": "Full re-sync triggered"}


@router.post("/cleanup-managed", response_model=ManagedCleanupResponse)
async def cleanup_managed_events(user: User = Depends(get_current_user)):
    """Manually clean up BusyBridge-managed events for the current user."""
    db = await get_database()

    from app.sync.engine import cleanup_managed_events_for_user

    summary = await cleanup_managed_events_for_user(user.id)
    status_value = summary.get("status") or "ok"
    message = (
        "Managed cleanup completed"
        if status_value == "ok"
        else "Managed cleanup completed with warnings"
    )

    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'managed_cleanup', ?, ?)""",
        (
            user.id,
            status_value,
            json.dumps(summary),
        ),
    )
    await db.commit()

    return ManagedCleanupResponse(
        status=status_value,
        message=message,
        managed_event_prefix=summary.get("managed_event_prefix", ""),
        db_main_events_deleted=summary.get("db_main_events_deleted", 0),
        db_busy_blocks_deleted=summary.get("db_busy_blocks_deleted", 0),
        prefix_calendars_scanned=summary.get("prefix_calendars_scanned", 0),
        prefix_events_deleted=summary.get("prefix_events_deleted", 0),
        local_mappings_deleted=summary.get("local_mappings_deleted", 0),
        local_busy_blocks_deleted=summary.get("local_busy_blocks_deleted", 0),
        errors=summary.get("errors", []),
    )
