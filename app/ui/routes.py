"""UI page routes."""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.auth.session import get_current_user, get_current_user_optional, User
from app.config import get_settings, get_test_mode_home_allowlist
from app.database import get_database, is_oobe_completed

logger = logging.getLogger(__name__)
router = APIRouter(tags=["ui"])

templates = Jinja2Templates(directory="app/ui/templates")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Root route - redirect to app or setup."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)


@router.get("/app", response_class=HTMLResponse)
async def dashboard(request: Request, error: Optional[str] = None):
    """Main dashboard page."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get connected calendars
    cursor = await db.execute(
        """SELECT cc.*, ot.google_account_email, css.last_incremental_sync,
                  css.last_full_sync, css.consecutive_failures
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE
           ORDER BY cc.created_at DESC""",
        (user.id,)
    )
    calendars = await cursor.fetchall()

    # Get sync status
    cursor = await db.execute(
        """SELECT COUNT(*) as total,
                  SUM(CASE WHEN css.consecutive_failures >= 5 THEN 1 ELSE 0 END) as errors,
                  SUM(CASE WHEN css.consecutive_failures BETWEEN 1 AND 4 THEN 1 ELSE 0 END) as warnings
           FROM client_calendars cc
           LEFT JOIN calendar_sync_state css ON cc.id = css.client_calendar_id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user.id,)
    )
    status_row = await cursor.fetchone()

    # Get event count
    cursor = await db.execute(
        "SELECT COUNT(*) FROM event_mappings WHERE user_id = ? AND deleted_at IS NULL",
        (user.id,)
    )
    event_count = (await cursor.fetchone())[0]
    managed_event_prefix = (get_settings().managed_event_prefix or "").strip()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "calendars": calendars,
        "status": status_row,
        "event_count": event_count,
        "managed_event_prefix": managed_event_prefix,
        "error": error,
    })


@router.get("/app/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    error: Optional[str] = None,
    domain: Optional[str] = None,
):
    """Login page."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    user = await get_current_user_optional(request)
    if user:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    settings = get_settings()
    required_domain = None
    allowed_home_emails = sorted(get_test_mode_home_allowlist()) if settings.test_mode else []

    if not settings.test_mode:
        # Get organization domain for display
        db = await get_database()
        cursor = await db.execute("SELECT google_workspace_domain FROM organization LIMIT 1")
        org = await cursor.fetchone()
        required_domain = domain or (org["google_workspace_domain"] if org else None)

    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
        "required_domain": required_domain,
        "test_mode": settings.test_mode,
        "allowed_home_emails": allowed_home_emails,
    })


@router.get("/app/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """User settings page."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get user's calendars from Google
    calendars = []
    try:
        from app.auth.google import get_valid_access_token
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, user.email)
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        result = service.calendarList().list().execute()
        calendars = result.get("items", [])
    except Exception as e:
        logger.error(f"Failed to get calendars: {e}")

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "user": user,
        "calendars": calendars,
    })


@router.get("/app/logs", response_class=HTMLResponse)
async def logs_page(
    request: Request,
    page: int = 1,
    calendar_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Sync logs page."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()
    page_size = 50

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

    # Get total
    count_query = query.replace("SELECT sl.*, cc.display_name as calendar_name", "SELECT COUNT(*)")
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    # Get paginated results
    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    logs = await cursor.fetchall()

    # Get user's calendars for filter dropdown
    cursor = await db.execute(
        "SELECT id, display_name FROM client_calendars WHERE user_id = ?",
        (user.id,)
    )
    calendars = await cursor.fetchall()

    total_pages = (total + page_size - 1) // page_size

    return templates.TemplateResponse("logs.html", {
        "request": request,
        "user": user,
        "logs": logs,
        "calendars": calendars,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "calendar_id": calendar_id,
        "status_filter": status_filter,
    })


@router.get("/app/calendars/select", response_class=HTMLResponse)
async def select_calendar_page(
    request: Request,
    token_id: int,
    email: str,
):
    """Calendar selection page after OAuth."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Verify token belongs to user
    cursor = await db.execute(
        "SELECT * FROM oauth_tokens WHERE id = ? AND user_id = ?",
        (token_id, user.id)
    )
    token = await cursor.fetchone()

    if not token:
        return RedirectResponse(url="/app?error=invalid_token", status_code=status.HTTP_302_FOUND)

    # Get calendars from the client account
    calendars = []
    try:
        from app.auth.google import get_valid_access_token
        from googleapiclient.discovery import build
        from google.oauth2.credentials import Credentials

        access_token = await get_valid_access_token(user.id, email)
        credentials = Credentials(token=access_token)
        service = build("calendar", "v3", credentials=credentials)

        result = service.calendarList().list().execute()
        # Filter to calendars where user has write access
        for cal in result.get("items", []):
            if cal.get("accessRole") in ["owner", "writer"]:
                calendars.append(cal)

    except Exception as e:
        logger.error(f"Failed to get calendars: {e}")
        return RedirectResponse(url="/app?error=calendar_fetch_failed", status_code=status.HTTP_302_FOUND)

    return templates.TemplateResponse("select_calendar.html", {
        "request": request,
        "user": user,
        "token_id": token_id,
        "email": email,
        "calendars": calendars,
    })


# Admin routes
@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    """Admin dashboard."""
    user = await get_current_user_optional(request)
    if not user:
        return RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)

    if not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get system stats
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    total_users = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM client_calendars WHERE is_active = TRUE")
    active_calendars = (await cursor.fetchone())[0]

    cursor = await db.execute("SELECT COUNT(*) FROM event_mappings WHERE deleted_at IS NULL")
    total_events = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COUNT(*) FROM sync_log
           WHERE status = 'failure' AND created_at > datetime('now', '-1 day')"""
    )
    errors_24h = (await cursor.fetchone())[0]

    # Recent alerts
    cursor = await db.execute(
        """SELECT * FROM alert_queue
           ORDER BY created_at DESC LIMIT 10"""
    )
    recent_alerts = await cursor.fetchall()

    return templates.TemplateResponse("admin/dashboard.html", {
        "request": request,
        "user": user,
        "total_users": total_users,
        "active_calendars": active_calendars,
        "total_events": total_events,
        "errors_24h": errors_24h,
        "recent_alerts": recent_alerts,
    })


@router.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, search: Optional[str] = None):
    """Admin user management page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

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
    users = await cursor.fetchall()

    return templates.TemplateResponse("admin/users.html", {
        "request": request,
        "user": user,
        "users": users,
        "search": search,
    })


@router.get("/admin/users/{user_id}", response_class=HTMLResponse)
async def admin_user_detail(request: Request, user_id: int):
    """Admin user detail page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()

    # Get target user
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    target_user = await cursor.fetchone()

    if not target_user:
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

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
    calendars = await cursor.fetchall()

    # Get recent logs
    cursor = await db.execute(
        """SELECT * FROM sync_log
           WHERE user_id = ?
           ORDER BY created_at DESC LIMIT 20""",
        (user_id,)
    )
    logs = await cursor.fetchall()

    return templates.TemplateResponse("admin/user_detail.html", {
        "request": request,
        "user": user,
        "target_user": target_user,
        "calendars": calendars,
        "logs": logs,
    })


@router.get("/admin/logs", response_class=HTMLResponse)
async def admin_logs(
    request: Request,
    page: int = 1,
    user_id: Optional[int] = None,
    status_filter: Optional[str] = None,
):
    """Admin system logs page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    db = await get_database()
    page_size = 100

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

    # Get total
    count_query = query.replace(
        "SELECT sl.*, u.email as user_email, cc.display_name as calendar_name",
        "SELECT COUNT(*)"
    )
    cursor = await db.execute(count_query, params)
    total = (await cursor.fetchone())[0]

    query += " ORDER BY sl.created_at DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    cursor = await db.execute(query, params)
    logs = await cursor.fetchall()

    total_pages = (total + page_size - 1) // page_size

    return templates.TemplateResponse("admin/logs.html", {
        "request": request,
        "user": user,
        "logs": logs,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "user_id": user_id,
        "status_filter": status_filter,
    })


@router.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    """Admin settings page."""
    user = await get_current_user_optional(request)
    if not user or not user.is_admin:
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    from app.database import get_setting

    settings = {}
    for key in ["smtp_host", "smtp_port", "smtp_username", "smtp_from_address", "alert_emails", "alerts_enabled", "sync_paused"]:
        setting = await get_setting(key)
        if setting:
            settings[key] = setting.get("value_plain", "")

    return templates.TemplateResponse("admin/settings.html", {
        "request": request,
        "user": user,
        "settings": settings,
    })
