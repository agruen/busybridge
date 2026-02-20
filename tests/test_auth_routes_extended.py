"""Extended tests for auth route helpers and callback flows."""

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
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
async def test_oauth_state_helpers_and_redirect_uri(test_db):
    """OAuth state helpers should store/retrieve one-time state and clean up expired rows."""
    from app.auth.routes import cleanup_expired_oauth_states, get_oauth_state, get_redirect_uri, store_oauth_state

    await store_oauth_state("state-1", "login", user_id=10, next_url="/next", ttl_minutes=10)
    state = await get_oauth_state("state-1")
    assert state == {"type": "login", "user_id": 10, "next": "/next"}
    # One-time use: second lookup should be missing.
    assert await get_oauth_state("state-1") is None

    db = await get_database()
    await db.execute(
        """INSERT INTO oauth_states (state, state_type, user_id, next_url, expires_at)
           VALUES (?, ?, ?, ?, ?)""",
        ("expired-state", "login", None, None, (datetime.utcnow() - timedelta(minutes=1)).isoformat()),
    )
    await db.commit()
    await cleanup_expired_oauth_states()
    cursor = await db.execute("SELECT COUNT(*) FROM oauth_states WHERE state = 'expired-state'")
    assert (await cursor.fetchone())[0] == 0

    assert get_redirect_uri(_request(), "/auth/callback").endswith("/auth/callback")


@pytest.mark.asyncio
async def test_login_route_paths(test_db, monkeypatch):
    """Login route should redirect to setup, error on missing creds, and redirect to auth URL."""
    from app.auth.routes import login

    # OOBE not complete -> setup redirect
    monkeypatch.setattr("app.auth.routes.is_oobe_completed", lambda: SimpleNamespace(__await__=lambda self: iter([False])))

    async def oobe_false():
        return False

    monkeypatch.setattr("app.auth.routes.is_oobe_completed", oobe_false)
    response = await login(_request("/auth/login"), next="/app")
    assert response.status_code == 302
    assert response.headers["location"] == "/setup"

    async def oobe_true():
        return True

    monkeypatch.setattr("app.auth.routes.is_oobe_completed", oobe_true)

    async def missing_creds():
        raise ValueError("missing")

    monkeypatch.setattr("app.auth.routes.get_oauth_credentials", missing_creds)
    with pytest.raises(HTTPException):
        await login(_request("/auth/login"), next="/app")

    async def ok_creds():
        return ("client-id.apps.googleusercontent.com", "secret")

    async def fake_store_state(state: str, state_type: str, user_id=None, next_url=None, ttl_minutes: int = 10):
        return None

    async def fake_cleanup_states():
        return None

    monkeypatch.setattr("app.auth.routes.get_oauth_credentials", ok_creds)
    monkeypatch.setattr("app.auth.routes.store_oauth_state", fake_store_state)
    monkeypatch.setattr("app.auth.routes.cleanup_expired_oauth_states", fake_cleanup_states)
    monkeypatch.setattr("app.auth.routes.build_auth_url", lambda **_kwargs: "https://accounts.google.com/o/oauth2/v2/auth?x=1")
    response = await login(_request("/auth/login"), next="/app")
    assert response.status_code == 302
    assert "accounts.google.com" in response.headers["location"]


@pytest.mark.asyncio
async def test_oauth_callback_paths(test_db, monkeypatch):
    """OAuth callback should handle explicit errors, invalid state, mismatch, and success."""
    from app.auth.routes import oauth_callback

    # explicit error from provider
    error_redirect = await oauth_callback(_request(), error="access_denied")
    assert error_redirect.status_code == 302
    assert "error=access_denied" in error_redirect.headers["location"]

    async def no_state(_state):
        return None

    monkeypatch.setattr("app.auth.routes.get_oauth_state", no_state)
    with pytest.raises(HTTPException):
        await oauth_callback(_request(), code="x", state="missing")

    # Domain mismatch path
    async def good_state(_state):
        return {"type": "login", "next": "/app", "user_id": None}

    async def fake_exchange(_code, _redirect_uri):
        return {"access_token": "access", "refresh_token": "refresh", "expires_in": 3600}

    async def fake_user_info(_access):
        return {"email": "person@outside.com", "id": "g123", "name": "Outside"}

    async def fake_org():
        return {"google_workspace_domain": "inside.com"}

    monkeypatch.setattr("app.auth.routes.get_oauth_state", good_state)
    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)
    monkeypatch.setattr("app.auth.routes.get_organization", fake_org)
    mismatch = await oauth_callback(_request(), code="c", state="s")
    assert mismatch.status_code == 302
    assert "domain_mismatch" in mismatch.headers["location"]

    # Success path with session cookie
    async def fake_user_info_ok(_access):
        return {"email": "person@inside.com", "id": "g123", "name": "Inside"}

    async def fake_org_none():
        return None

    async def fake_create_or_update_user(**_kwargs):
        return SimpleNamespace(id=7, email="person@inside.com", is_admin=False, main_calendar_id="main")

    async def fake_store_tokens(**_kwargs):
        return 1

    async def fake_update_last_login(_user_id: int):
        return None

    monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info_ok)
    monkeypatch.setattr("app.auth.routes.get_organization", fake_org_none)
    monkeypatch.setattr("app.auth.routes.create_or_update_user", fake_create_or_update_user)
    monkeypatch.setattr("app.auth.routes.store_oauth_tokens", fake_store_tokens)
    monkeypatch.setattr("app.auth.routes.update_user_last_login", fake_update_last_login)
    monkeypatch.setattr("app.auth.routes.create_session_token", lambda **_kwargs: "session-token")

    success = await oauth_callback(_request(), code="c", state="s")
    assert success.status_code == 302
    assert success.headers["location"] == "/app"
    assert "session=" in success.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_oauth_callback_failure_and_client_connect_paths(test_db, monkeypatch):
    """Callback and client-connect routes should handle all key error and success branches."""
    from app.auth.routes import connect_client, connect_client_callback, oauth_callback

    async def good_state(_state):
        return {"type": "login", "next": "/app", "user_id": None}

    async def exploding_exchange(_code, _redirect_uri):
        raise RuntimeError("exchange failed")

    monkeypatch.setattr("app.auth.routes.get_oauth_state", good_state)
    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", exploding_exchange)
    callback_failed = await oauth_callback(_request(), code="x", state="s")
    assert callback_failed.status_code == 302
    assert "callback_failed" in callback_failed.headers["location"]

    # connect-client start
    async def fake_current_user(_request):
        return SimpleNamespace(id=1, email="user@example.com", is_admin=False)

    async def fake_get_oauth_credentials():
        return ("client-id.apps.googleusercontent.com", "secret")

    async def fake_store_state(state: str, state_type: str, user_id=None, next_url=None, ttl_minutes: int = 10):
        return None

    async def fake_cleanup():
        return None

    monkeypatch.setattr("app.auth.routes.get_current_user", fake_current_user)
    monkeypatch.setattr("app.auth.routes.get_oauth_credentials", fake_get_oauth_credentials)
    monkeypatch.setattr("app.auth.routes.store_oauth_state", fake_store_state)
    monkeypatch.setattr("app.auth.routes.cleanup_expired_oauth_states", fake_cleanup)
    monkeypatch.setattr("app.auth.routes.build_auth_url", lambda **_kwargs: "https://accounts.google.com/o/oauth2/v2/auth?y=1")
    connect_start = await connect_client(_request("/auth/connect-client"))
    assert connect_start.status_code == 302
    assert "accounts.google.com" in connect_start.headers["location"]

    async def missing_creds():
        raise ValueError("missing")

    monkeypatch.setattr("app.auth.routes.get_oauth_credentials", missing_creds)
    with pytest.raises(HTTPException):
        await connect_client(_request("/auth/connect-client"))

    # connect-client callback paths
    error_cb = await connect_client_callback(_request(), error="access_denied")
    assert "client_connect_failed" in error_cb.headers["location"]

    async def no_state(_state):
        return None

    monkeypatch.setattr("app.auth.routes.get_oauth_state", no_state)
    with pytest.raises(HTTPException):
        await connect_client_callback(_request(), code="x", state="missing")

    async def wrong_type_state(_state):
        return {"type": "login", "user_id": 1}

    monkeypatch.setattr("app.auth.routes.get_oauth_state", wrong_type_state)
    with pytest.raises(HTTPException):
        await connect_client_callback(_request(), code="x", state="bad-type")

    async def client_state(_state):
        return {"type": "client", "user_id": 7}

    async def exchange_no_refresh(_code, _redirect_uri):
        return {"access_token": "a", "expires_in": 3600}

    monkeypatch.setattr("app.auth.routes.get_oauth_state", client_state)
    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", exchange_no_refresh)
    no_refresh = await connect_client_callback(_request(), code="x", state="ok")
    assert "no_refresh_token" in no_refresh.headers["location"]

    async def exchange_ok(_code, _redirect_uri):
        return {"access_token": "a", "refresh_token": "r", "expires_in": 3600}

    async def fake_user_info(_access):
        return {"email": "client@example.com"}

    async def fake_store_tokens(**_kwargs):
        return 99

    monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", exchange_ok)
    monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)
    monkeypatch.setattr("app.auth.routes.store_oauth_tokens", fake_store_tokens)
    ok = await connect_client_callback(_request(), code="x", state="ok")
    assert ok.status_code == 302
    assert "token_id=99" in ok.headers["location"]

    async def failing_store_tokens(**_kwargs):
        raise RuntimeError("store failed")

    monkeypatch.setattr("app.auth.routes.store_oauth_tokens", failing_store_tokens)
    failed = await connect_client_callback(_request(), code="x", state="ok")
    assert "client_callback_failed" in failed.headers["location"]


@pytest.mark.asyncio
async def test_logout_routes():
    """Logout routes should redirect to login and clear cookie."""
    from app.auth.routes import logout, logout_get

    response = await logout(None)
    assert response.status_code == 302
    assert response.headers["location"] == "/app/login"
    assert "session=\"\"" in response.headers.get("set-cookie", "")

    response_get = await logout_get(None)
    assert response_get.status_code == 302
    assert response_get.headers["location"] == "/app/login"

