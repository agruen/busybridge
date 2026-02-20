"""Tests for UI page routes."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.database import get_database, set_setting


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
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id, created_at, last_login_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           RETURNING id""",
        (
            email,
            google_user_id,
            email.split("@")[0],
            is_admin,
            main_calendar_id,
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat(),
        ),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, account_type: str, email: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(user_id: int, token_id: int, calendar_id: str, is_active: bool = True) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id, is_active),
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
async def test_index_dashboard_and_login_page_paths(test_db, monkeypatch):
    """Index/dashboard/login routes should redirect correctly and render when authenticated."""
    from app.ui.routes import dashboard, index, login_page

    async def oobe_false():
        return False

    async def oobe_true():
        return True

    monkeypatch.setattr("app.ui.routes.is_oobe_completed", oobe_false)
    idx = await index(_request("/"))
    assert idx.status_code == 302
    assert idx.headers["location"] == "/setup"

    dash_setup = await dashboard(_request("/app"))
    assert dash_setup.headers["location"] == "/setup"

    login_setup = await login_page(_request("/app/login"))
    assert login_setup.headers["location"] == "/setup"

    monkeypatch.setattr("app.ui.routes.is_oobe_completed", oobe_true)

    async def no_user(_request):
        return None

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", no_user)
    dash_login = await dashboard(_request("/app"))
    assert dash_login.headers["location"] == "/app/login"

    # Authenticated dashboard render
    user_id = await _insert_user("ui-user@example.com", "ui-google", main_calendar_id="main")
    db = await get_database()
    token_id = await _insert_token(user_id, "client", "ui-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "ui-cal")
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_id, 0),
    )
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'e1', 'm1', FALSE, TRUE)""",
        (user_id, cal_id),
    )
    await db.execute(
        "INSERT INTO organization (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted) VALUES (?, ?, ?)",
        ("example.com", b"x", b"y"),
    )
    await db.commit()

    async def auth_user(_request):
        return _user(user_id, "ui-user@example.com", main_calendar_id="main")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", auth_user)
    dash_render = await dashboard(_request("/app"))
    assert dash_render.status_code == 200
    assert dash_render.context["event_count"] >= 1
    assert "managed_event_prefix" in dash_render.context

    # login page with authenticated user redirects to /app
    login_auth = await login_page(_request("/app/login"))
    assert login_auth.headers["location"] == "/app"

    # login page unauthenticated shows org domain
    monkeypatch.setattr("app.ui.routes.get_current_user_optional", no_user)
    login_render = await login_page(_request("/app/login"))
    assert login_render.status_code == 200
    assert login_render.context["required_domain"] == "example.com"

    monkeypatch.setattr("app.ui.routes.get_settings", lambda: SimpleNamespace(test_mode=True))
    monkeypatch.setattr(
        "app.ui.routes.get_test_mode_home_allowlist",
        lambda: {"test-a@gmail.com", "test-b@gmail.com"},
    )
    login_test_mode = await login_page(_request("/app/login"))
    assert login_test_mode.status_code == 200
    assert login_test_mode.context["test_mode"] is True
    assert login_test_mode.context["required_domain"] is None
    assert login_test_mode.context["allowed_home_emails"] == [
        "test-a@gmail.com",
        "test-b@gmail.com",
    ]


@pytest.mark.asyncio
async def test_settings_logs_and_calendar_select_paths(test_db, monkeypatch):
    """Settings/log/select-calendar routes should handle auth and external API outcomes."""
    from app.ui.routes import logs_page, select_calendar_page, settings_page

    user_id = await _insert_user("pages@example.com", "pages-google", main_calendar_id="main")
    user_obj = _user(user_id, "pages@example.com", main_calendar_id="main")
    db = await get_database()

    async def no_user(_request):
        return None

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", no_user)
    assert (await settings_page(_request("/app/settings"))).headers["location"] == "/app/login"
    assert (await logs_page(_request("/app/logs"))).headers["location"] == "/app/login"
    assert (await select_calendar_page(_request("/app/calendars/select"), token_id=1, email="x")).headers["location"] == "/app/login"

    async def auth_user(_request):
        return user_obj

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", auth_user)

    # settings page success
    async def fake_get_valid_access_token(_user_id: int, _email: str):
        return "token"

    class FakeService:
        def calendarList(self):
            return SimpleNamespace(
                list=lambda: SimpleNamespace(
                    execute=lambda: {
                        "items": [
                            {"id": "c1", "summary": "Cal 1", "accessRole": "owner"},
                            {"id": "c2", "summary": "Cal 2", "accessRole": "reader"},
                        ]
                    }
                )
            )

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("googleapiclient.discovery.build", lambda *_args, **_kwargs: FakeService())
    settings_ok = await settings_page(_request("/app/settings"))
    assert settings_ok.status_code == 200
    assert len(settings_ok.context["calendars"]) == 2

    # settings page failure should still render
    async def failing_token(_user_id: int, _email: str):
        raise RuntimeError("no token")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_token)
    settings_fail = await settings_page(_request("/app/settings"))
    assert settings_fail.status_code == 200
    assert settings_fail.context["calendars"] == []

    # logs page render
    token_id = await _insert_token(user_id, "client", "pages-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "pages-cal")
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'success', '{}')""",
        (user_id, cal_id),
    )
    await db.commit()

    logs = await logs_page(_request("/app/logs"), page=1)
    assert logs.status_code == 200
    assert logs.context["total"] >= 1

    # calendar select invalid token
    invalid_select = await select_calendar_page(_request("/app/calendars/select"), token_id=999, email="pages-client@example.com")
    assert "invalid_token" in invalid_select.headers["location"]

    # valid token and calendar fetch/filter
    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    select_ok = await select_calendar_page(
        _request("/app/calendars/select"),
        token_id=token_id,
        email="pages-client@example.com",
    )
    assert select_ok.status_code == 200
    # Filtered to owner/writer only.
    assert len(select_ok.context["calendars"]) == 1

    monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_token)
    select_fail = await select_calendar_page(
        _request("/app/calendars/select"),
        token_id=token_id,
        email="pages-client@example.com",
    )
    assert "calendar_fetch_failed" in select_fail.headers["location"]


@pytest.mark.asyncio
async def test_admin_ui_pages(test_db, monkeypatch):
    """Admin UI pages should enforce auth/admin and render expected data."""
    from app.ui.routes import (
        admin_dashboard,
        admin_logs,
        admin_settings,
        admin_user_detail,
        admin_users,
    )

    admin_id = await _insert_user("admin-ui@example.com", "admin-ui-google", is_admin=True, main_calendar_id="main-admin")
    user_id = await _insert_user("normal-ui@example.com", "normal-ui-google", is_admin=False, main_calendar_id="main-user")
    db = await get_database()
    token_id = await _insert_token(user_id, "client", "normal-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "normal-cal")
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_id, 1),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'failure', '{}')""",
        (user_id, cal_id),
    )
    await db.execute(
        """INSERT INTO alert_queue (alert_type, recipient_email, subject, body)
           VALUES ('sync_failures', 'ops@example.com', 'subj', 'body')"""
    )
    await set_setting("smtp_host", "smtp.example.com")
    await set_setting("sync_paused", "true")
    await db.commit()

    async def no_user(_request):
        return None

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", no_user)
    assert (await admin_dashboard(_request("/admin"))).headers["location"] == "/app/login"

    async def normal_user(_request):
        return _user(user_id, "normal-ui@example.com", is_admin=False, main_calendar_id="main-user")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", normal_user)
    assert (await admin_dashboard(_request("/admin"))).headers["location"] == "/app"

    async def admin_user(_request):
        return _user(admin_id, "admin-ui@example.com", is_admin=True, main_calendar_id="main-admin")

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", admin_user)
    dashboard = await admin_dashboard(_request("/admin"))
    assert dashboard.status_code == 200
    assert dashboard.context["total_users"] >= 2

    users_page = await admin_users(_request("/admin/users"), search="normal")
    assert users_page.status_code == 200
    assert users_page.context["search"] == "normal"

    detail = await admin_user_detail(_request(f"/admin/users/{user_id}"), user_id=user_id)
    assert detail.status_code == 200
    assert detail.context["target_user"]["email"] == "normal-ui@example.com"

    missing_detail = await admin_user_detail(_request("/admin/users/999"), user_id=999)
    assert missing_detail.headers["location"] == "/admin/users"

    logs = await admin_logs(_request("/admin/logs"), page=1, status_filter="failure")
    assert logs.status_code == 200
    assert logs.context["total"] >= 1

    settings = await admin_settings(_request("/admin/settings"))
    assert settings.status_code == 200
    assert settings.context["settings"]["smtp_host"] == "smtp.example.com"
