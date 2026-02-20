"""Coverage tests for oauth_callback main-calendar initialization branch."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.database import get_database


def _request(path: str = "/auth/callback") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_oauth_callback_initializes_main_calendar_when_missing(test_db, monkeypatch):
    """OAuth callback should set main calendar from primary calendar when user has none."""
    from app.auth.routes import oauth_callback

    db = await get_database()
    await db.execute(
        """INSERT INTO users (id, email, google_user_id, display_name, main_calendar_id)
           VALUES (22, 'init-main@example.com', 'init-main-google', 'Init Main', NULL)"""
    )
    await db.commit()

    async def fake_get_oauth_state(_state):
        return {"type": "login", "next": "/app", "user_id": None}

    async def fake_exchange(_code, _redirect_uri):
        return {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}

    async def fake_get_user_info(_access_token):
        return {"email": "init-main@example.com", "id": "init-main-google", "name": "Init Main"}

    async def fake_get_org():
        return None

    async def fake_create_or_update_user(**_kwargs):
        return SimpleNamespace(id=22, email="init-main@example.com", is_admin=False, main_calendar_id=None)

    async def fake_store_oauth_tokens(**_kwargs):
        return 1

    async def fake_update_user_last_login(_user_id: int):
        return None

    class FakeGoogleCalendarClient:
        def __init__(self, _access_token: str, _settings):
            pass

        def list_calendars(self):
            return [{"id": "primary-22", "primary": True}, {"id": "other", "primary": False}]

    monkeypatch.setattr("app.auth.routes.get_oauth_state", fake_get_oauth_state)
    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr("app.auth.routes.get_user_info", fake_get_user_info)
    monkeypatch.setattr("app.auth.routes.get_organization", fake_get_org)
    monkeypatch.setattr("app.auth.routes.create_or_update_user", fake_create_or_update_user)
    monkeypatch.setattr("app.auth.routes.store_oauth_tokens", fake_store_oauth_tokens)
    monkeypatch.setattr("app.auth.routes.update_user_last_login", fake_update_user_last_login)
    monkeypatch.setattr("app.auth.routes.create_session_token", lambda **_kwargs: "session-token")
    monkeypatch.setattr("app.sync.google_calendar.GoogleCalendarClient", FakeGoogleCalendarClient)

    response = await oauth_callback(_request(), code="code", state="state")
    assert response.status_code == 302
    assert response.headers["location"] == "/app"

    cursor = await db.execute("SELECT main_calendar_id FROM users WHERE id = 22")
    assert (await cursor.fetchone())["main_calendar_id"] == "primary-22"


@pytest.mark.asyncio
async def test_oauth_callback_main_calendar_init_failure_is_non_fatal(test_db, monkeypatch):
    """OAuth callback should continue login even if main-calendar initialization fails."""
    from app.auth.routes import oauth_callback

    db = await get_database()
    await db.execute(
        """INSERT INTO users (id, email, google_user_id, display_name, main_calendar_id)
           VALUES (23, 'init-fail@example.com', 'init-fail-google', 'Init Fail', NULL)"""
    )
    await db.commit()

    async def fake_get_oauth_state(_state):
        return {"type": "login", "next": "/app", "user_id": None}

    async def fake_exchange(_code, _redirect_uri):
        return {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}

    async def fake_get_user_info(_access_token):
        return {"email": "init-fail@example.com", "id": "init-fail-google", "name": "Init Fail"}

    async def fake_get_org():
        return None

    async def fake_create_or_update_user(**_kwargs):
        return SimpleNamespace(id=23, email="init-fail@example.com", is_admin=False, main_calendar_id=None)

    async def fake_store_oauth_tokens(**_kwargs):
        return 1

    async def fake_update_user_last_login(_user_id: int):
        return None

    class ExplodingGoogleCalendarClient:
        def __init__(self, _access_token: str, _settings):
            pass

        def list_calendars(self):
            raise RuntimeError("calendar list failed")

    monkeypatch.setattr("app.auth.routes.get_oauth_state", fake_get_oauth_state)
    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr("app.auth.routes.get_user_info", fake_get_user_info)
    monkeypatch.setattr("app.auth.routes.get_organization", fake_get_org)
    monkeypatch.setattr("app.auth.routes.create_or_update_user", fake_create_or_update_user)
    monkeypatch.setattr("app.auth.routes.store_oauth_tokens", fake_store_oauth_tokens)
    monkeypatch.setattr("app.auth.routes.update_user_last_login", fake_update_user_last_login)
    monkeypatch.setattr("app.auth.routes.create_session_token", lambda **_kwargs: "session-token")
    monkeypatch.setattr("app.sync.google_calendar.GoogleCalendarClient", ExplodingGoogleCalendarClient)

    response = await oauth_callback(_request(), code="code", state="state")
    assert response.status_code == 302
    assert response.headers["location"] == "/app"

    # Since initialization failed, value should remain unset.
    cursor = await db.execute("SELECT main_calendar_id FROM users WHERE id = 23")
    assert (await cursor.fetchone())["main_calendar_id"] is None
