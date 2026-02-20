"""Additional branch coverage tests for ui.routes."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.database import get_database


def _request(path: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
    }
    return Request(scope)


async def _insert_user(
    email: str,
    google_user_id: str,
    is_admin: bool = False,
    main_calendar_id: str | None = None,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], is_admin, main_calendar_id, datetime.utcnow().isoformat()),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, email: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?)
           RETURNING id""",
        (user_id, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _user(id_: int, email: str, is_admin: bool = False, main_calendar_id: str | None = None):
    return SimpleNamespace(
        id=id_,
        email=email,
        is_admin=is_admin,
        display_name=email.split("@")[0],
        main_calendar_id=main_calendar_id,
    )


@pytest.mark.asyncio
async def test_index_oobe_completed_redirects_to_app(test_db, monkeypatch):
    """Index route should redirect to /app when OOBE is complete."""
    from app.ui.routes import index

    async def oobe_true():
        return True

    monkeypatch.setattr("app.ui.routes.is_oobe_completed", oobe_true)
    response = await index(_request("/"))
    assert response.status_code == 302
    assert response.headers["location"] == "/app"


@pytest.mark.asyncio
async def test_logs_page_calendar_and_status_filters(test_db, monkeypatch):
    """Logs page should apply optional calendar and status filters."""
    from app.ui.routes import logs_page

    user_id = await _insert_user("ui-filter@example.com", "ui-filter-google", main_calendar_id="main")
    token_id = await _insert_token(user_id, "ui-filter-client@example.com")
    cal_1 = await _insert_calendar(user_id, token_id, "ui-filter-cal-1")
    cal_2 = await _insert_calendar(user_id, token_id, "ui-filter-cal-2")
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'success', '{}')""",
        (user_id, cal_1),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'failure', '{}')""",
        (user_id, cal_2),
    )
    await db.commit()

    async def auth_user(_request):
        return _user(user_id, "ui-filter@example.com", main_calendar_id="main")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", auth_user)

    response = await logs_page(
        _request("/app/logs"),
        page=1,
        calendar_id=cal_2,
        status_filter="failure",
    )
    assert response.status_code == 200
    assert response.context["total"] == 1
    assert response.context["calendar_id"] == cal_2
    assert response.context["status_filter"] == "failure"


@pytest.mark.asyncio
async def test_admin_page_auth_redirects_and_user_filter(test_db, monkeypatch):
    """Admin pages should redirect non-admin users and apply user filter in admin logs."""
    from app.ui.routes import admin_logs, admin_settings, admin_user_detail, admin_users

    admin_id = await _insert_user("ui-admin@example.com", "ui-admin-google", is_admin=True, main_calendar_id="main")
    user_id = await _insert_user("ui-normal@example.com", "ui-normal-google", is_admin=False, main_calendar_id="main")
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync', 'failure', '{}')""",
        (user_id,),
    )
    await db.commit()

    async def normal_user(_request):
        return _user(user_id, "ui-normal@example.com", is_admin=False, main_calendar_id="main")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", normal_user)
    assert (await admin_users(_request("/admin/users"))).headers["location"] == "/app"
    assert (await admin_user_detail(_request("/admin/users/1"), user_id=1)).headers["location"] == "/app"
    assert (await admin_logs(_request("/admin/logs"), user_id=user_id)).headers["location"] == "/app"
    assert (await admin_settings(_request("/admin/settings"))).headers["location"] == "/app"

    async def admin_user(_request):
        return _user(admin_id, "ui-admin@example.com", is_admin=True, main_calendar_id="main")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", admin_user)
    filtered = await admin_logs(_request("/admin/logs"), user_id=user_id, page=1)
    assert filtered.status_code == 200
    assert filtered.context["user_id"] == user_id
