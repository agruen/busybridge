"""Sync status and control API endpoints."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.auth.session import get_current_user, User
from app.database import get_database, get_setting, set_setting

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
    integrity_status: Optional[str] = None
    integrity_last_check: Optional[str] = None
    integrity_unresolved: int = 0


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

    # Integrity status
    integrity_status_val = None
    integrity_last_check = None
    integrity_unresolved = 0
    cursor = await db.execute(
        "SELECT * FROM integrity_status WHERE user_id = ?", (user.id,)
    )
    irow = await cursor.fetchone()
    if irow:
        integrity_last_check = irow["last_check_at"]
        integrity_unresolved = irow["unresolved_issues"] or 0
        if (irow["consecutive_check_failures"] or 0) >= 3:
            integrity_status_val = "error"
        elif integrity_unresolved > 0:
            integrity_status_val = "warning"
        elif irow["last_check_at"]:
            integrity_status_val = "ok"

    return SyncStatusResponse(
        calendars_connected=total,
        calendars_healthy=healthy,
        calendars_warning=warning,
        calendars_error=error,
        last_sync=last_sync,
        events_synced=events_synced,
        busy_blocks_created=busy_blocks,
        sync_paused=sync_paused,
        integrity_status=integrity_status_val,
        integrity_last_check=integrity_last_check,
        integrity_unresolved=integrity_unresolved,
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


@router.post("/pause")
async def pause_sync(user: User = Depends(get_current_user)):
    """Pause all sync operations."""
    await set_setting("sync_paused", "true")

    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_pause', 'success', 'User paused sync')""",
        (user.id,),
    )
    await db.commit()

    return {"status": "ok", "sync_paused": True}


@router.post("/resume")
async def resume_sync(user: User = Depends(get_current_user)):
    """Resume sync operations."""
    await set_setting("sync_paused", "false")

    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync_resume', 'success', 'User resumed sync')""",
        (user.id,),
    )
    await db.commit()

    return {"status": "ok", "sync_paused": False}


@router.get("/cleanup-progress")
async def get_cleanup_progress(user: User = Depends(get_current_user)):
    """Poll cleanup progress for the current user."""
    from app.sync.engine import get_cleanup_progress as _get_progress, _cleanup_in_progress, _cleanup_progress
    from app.utils.tasks import _background_tasks
    progress = _get_progress(user.id)
    if progress is None:
        return {
            "status": "idle",
            "_debug_in_progress": list(_cleanup_in_progress),
            "_debug_progress_keys": list(_cleanup_progress.keys()),
            "_debug_bg_tasks": len(_background_tasks),
        }
    return progress


@router.get("/integrity")
async def get_integrity_status(user: User = Depends(get_current_user)):
    """Get integrity check status for current user."""
    db = await get_database()

    cursor = await db.execute(
        "SELECT * FROM integrity_status WHERE user_id = ?", (user.id,)
    )
    row = await cursor.fetchone()

    if not row:
        return {
            "status": "unknown",
            "last_check_at": None,
            "issues_found": 0,
            "issues_auto_fixed": 0,
            "unresolved_issues": 0,
            "consecutive_check_failures": 0,
            "details": None,
        }

    if (row["consecutive_check_failures"] or 0) >= 3:
        check_status = "error"
    elif (row["unresolved_issues"] or 0) > 0:
        check_status = "warning"
    elif row["last_check_at"]:
        check_status = "ok"
    else:
        check_status = "unknown"

    details = None
    if row["details_json"]:
        details = json.loads(row["details_json"])

    return {
        "status": check_status,
        "last_check_at": row["last_check_at"],
        "issues_found": row["issues_found"] or 0,
        "issues_auto_fixed": row["issues_auto_fixed"] or 0,
        "unresolved_issues": row["unresolved_issues"] or 0,
        "consecutive_check_failures": row["consecutive_check_failures"] or 0,
        "details": details,
    }


async def _run_cleanup_and_resync(user_id: int) -> None:
    """Background task: run cleanup, log results, then trigger resync."""
    from app.sync.engine import cleanup_managed_events_for_user, trigger_sync_for_user
    from app.utils.tasks import create_background_task

    try:
        summary = await cleanup_managed_events_for_user(user_id)
    except Exception:
        logger.exception("Cleanup failed for user %s", user_id)
        return

    db = await get_database()
    status_value = summary.get("status") or "ok"
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'managed_cleanup', ?, ?)""",
        (user_id, status_value, json.dumps(summary)),
    )
    await db.commit()

    # Only trigger resync if cleanup was fully successful.
    # A partial cleanup means some Google Calendar events weren't deleted
    # (e.g. token failure).  Resyncing now would create duplicates because
    # the DB mappings were removed but the orphaned calendar events survive.
    if status_value == "partial":
        logger.warning(
            "Cleanup was partial for user %s — skipping automatic resync to avoid duplicates. "
            "Fix the underlying issue and retry cleanup, or resync manually.",
            user_id,
        )
        return

    create_background_task(
        trigger_sync_for_user(user_id),
        f"cleanup_resync_user_{user_id}",
    )


async def _run_cleanup_and_pause(user_id: int) -> None:
    """Background task: pause sync, run cleanup, leave sync paused."""
    from app.sync.engine import cleanup_managed_events_for_user

    try:
        summary = await cleanup_managed_events_for_user(user_id, resume_after=False)
    except Exception:
        logger.exception("Cleanup failed for user %s", user_id)
        return

    db = await get_database()
    status_value = summary.get("status") or "ok"
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'cleanup_and_pause', ?, ?)""",
        (user_id, status_value, json.dumps(summary)),
    )
    await db.commit()

    if status_value == "partial":
        logger.warning(
            "Cleanup-and-pause was partial for user %s — sync is paused but some Google Calendar "
            "events were not deleted. Resuming sync and resyncing may create duplicates. "
            "Fix the underlying issue (e.g. broken OAuth token) and re-run cleanup before resuming.",
            user_id,
        )


@router.get("/check-connections")
async def check_connections(user: User = Depends(get_current_user)):
    """Check if all OAuth tokens are valid and can be refreshed."""
    from app.auth.google import get_valid_access_token

    db = await get_database()
    cursor = await db.execute(
        """SELECT id, google_account_email, account_type, token_expiry
           FROM oauth_tokens WHERE user_id = ?""",
        (user.id,)
    )
    tokens = await cursor.fetchall()

    accounts = []
    all_ok = True
    for tok in tokens:
        email = tok["google_account_email"]
        account_type = tok["account_type"]
        try:
            await get_valid_access_token(user.id, email)
            accounts.append({
                "email": email,
                "account_type": account_type,
                "status": "ok",
            })
        except Exception as e:
            all_ok = False
            error_str = str(e).lower()
            if "invalid_grant" in error_str:
                fix = "reconnect"
                message = "Token has been revoked. Please reconnect this account."
            elif "no token found" in error_str:
                fix = "reconnect"
                message = "No token on file. Please reconnect this account."
            else:
                fix = "retry"
                message = f"Token refresh failed: {type(e).__name__}"
            accounts.append({
                "email": email,
                "account_type": account_type,
                "status": "error",
                "message": message,
                "fix": fix,
            })

    return {"status": "ok" if all_ok else "error", "accounts": accounts}


@router.post("/cleanup-managed")
async def cleanup_managed_events(user: User = Depends(get_current_user)):
    """Kick off cleanup + resync in the background. Returns immediately."""
    from app.sync.engine import get_cleanup_progress as _get_progress
    from app.utils.tasks import create_background_task

    # Prevent double-start
    progress = _get_progress(user.id)
    if progress and progress.get("status") == "running":
        return {"status": "already_running", "message": "Cleanup is already in progress."}

    create_background_task(
        _run_cleanup_and_resync(user.id),
        f"cleanup_managed_user_{user.id}",
    )

    return {"status": "started", "message": "Cleanup started. Events will re-sync automatically."}


@router.post("/cleanup-and-pause")
async def cleanup_and_pause(user: User = Depends(get_current_user)):
    """Kick off pause + cleanup in the background. Returns immediately."""
    from app.sync.engine import get_cleanup_progress as _get_progress
    from app.utils.tasks import create_background_task

    progress = _get_progress(user.id)
    if progress and progress.get("status") == "running":
        return {"status": "already_running", "message": "Cleanup is already in progress."}

    create_background_task(
        _run_cleanup_and_pause(user.id),
        f"cleanup_pause_user_{user.id}",
    )

    return {"status": "started", "message": "Cleanup started. Sync will be paused."}
