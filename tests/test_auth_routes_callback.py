"""Tests for app/auth/routes.py — oauth_callback and connect_client_callback flows."""

from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_user(
    email: str,
    google_user_id: str,
    main_calendar_id: Optional[str] = None,
    is_admin: bool = False,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], is_admin, main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_org(domain: str = "example.com") -> None:
    db = await get_database()
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        (domain, b"enc-id", b"enc-secret"),
    )
    await db.commit()


async def _store_state(state: str, state_type: str, user_id: Optional[int] = None, next_url: str = "/app") -> None:
    from app.auth.routes import store_oauth_state
    await store_oauth_state(state, state_type, user_id=user_id, next_url=next_url)


def _fake_request(path: str = "/auth/callback") -> MagicMock:
    """Build a minimal Request-like object for routes that call get_redirect_uri."""
    req = MagicMock()
    req.cookies = {}
    return req


# ---------------------------------------------------------------------------
# oauth_callback — error parameter path
# ---------------------------------------------------------------------------


class TestOAuthCallbackErrorParam:
    @pytest.mark.asyncio
    async def test_oauth_error_param_redirects_to_login_error(self, test_db):
        from app.auth.routes import oauth_callback
        from starlette.responses import RedirectResponse

        req = _fake_request()
        resp = await oauth_callback(
            request=req,
            error="access_denied",
            error_description="User denied access",
        )
        assert isinstance(resp, RedirectResponse)
        assert "access_denied" in resp.headers["location"]


# ---------------------------------------------------------------------------
# oauth_callback — invalid / expired state
# ---------------------------------------------------------------------------


class TestOAuthCallbackInvalidState:
    @pytest.mark.asyncio
    async def test_missing_state_raises_400(self, test_db):
        from app.auth.routes import oauth_callback
        from fastapi import HTTPException

        req = _fake_request()
        with pytest.raises(HTTPException) as exc_info:
            await oauth_callback(request=req, code="somecode", state="nonexistent-state")
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# oauth_callback — domain mismatch
# ---------------------------------------------------------------------------


class TestOAuthCallbackDomainMismatch:
    @pytest.mark.asyncio
    async def test_wrong_domain_redirects_with_domain_mismatch(self, test_db, monkeypatch):
        from app.auth.routes import oauth_callback
        from starlette.responses import RedirectResponse

        await _insert_org(domain="company.com")
        state = "test-domain-state"
        await _store_state(state, "login", next_url="/app")

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "user@wrong-domain.com", "id": "gid-domain", "name": "User"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=False, public_url="http://localhost:3000", session_expire_days=7),
        )
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))

        req = _fake_request()
        resp = await oauth_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "domain_mismatch" in resp.headers["location"]


# ---------------------------------------------------------------------------
# oauth_callback — test_mode allowlist
# ---------------------------------------------------------------------------


class TestOAuthCallbackTestMode:
    @pytest.mark.asyncio
    async def test_email_not_in_allowlist_redirects_with_error(self, test_db, monkeypatch):
        from app.auth.routes import oauth_callback
        from starlette.responses import RedirectResponse

        state = "test-mode-state"
        await _store_state(state, "login", next_url="/app")

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "notallowed@example.com", "id": "gid-notallowed", "name": "User"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=True, public_url="http://localhost:3000", session_expire_days=7),
        )
        monkeypatch.setattr("app.auth.routes.get_test_mode_home_allowlist", lambda: ["allowed@example.com"])
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))

        req = _fake_request()
        resp = await oauth_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "email_not_allowed" in resp.headers["location"]

    @pytest.mark.asyncio
    async def test_empty_allowlist_redirects_with_no_allowlist_error(self, test_db, monkeypatch):
        from app.auth.routes import oauth_callback
        from starlette.responses import RedirectResponse

        state = "test-mode-no-list-state"
        await _store_state(state, "login", next_url="/app")

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "someone@example.com", "id": "gid-someone", "name": "User"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=True, public_url="http://localhost:3000", session_expire_days=7),
        )
        monkeypatch.setattr("app.auth.routes.get_test_mode_home_allowlist", lambda: [])
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))

        req = _fake_request()
        resp = await oauth_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "test_mode_no_home_allowlist" in resp.headers["location"]


# ---------------------------------------------------------------------------
# oauth_callback — refresh token preservation
# ---------------------------------------------------------------------------


class TestOAuthCallbackRefreshTokenPreservation:
    @pytest.mark.asyncio
    async def test_existing_refresh_token_preserved_when_google_omits_it(
        self, test_db, monkeypatch
    ):
        from app.auth.routes import oauth_callback

        user_id = await _insert_user("returning@example.com", "gid-returning", main_calendar_id="main-cal")
        state = "refresh-preserve-state"
        await _store_state(state, "login", next_url="/app")

        # Google does NOT return a refresh token this time
        async def fake_exchange(code, redirect_uri):
            return {"access_token": "new-access-tok", "expires_in": 3600}  # no refresh_token

        async def fake_user_info(token):
            return {"email": "returning@example.com", "id": "gid-returning", "name": "Returning User"}

        existing_refresh = "existing-refresh-token"

        # Simulate a stored token being found with encrypted bytes;
        # mock decrypt_value so we never need a real key file.
        async def fake_get_oauth_token(uid, email):
            return {"refresh_token_encrypted": b"enc-data"}

        def fake_decrypt_value(data):
            return existing_refresh

        stored_tokens = {}

        async def capturing_store_tokens(**kwargs):
            stored_tokens.update(kwargs)
            return 1

        from types import SimpleNamespace
        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)
        monkeypatch.setattr("app.auth.routes.get_oauth_token", fake_get_oauth_token)
        monkeypatch.setattr("app.auth.routes.decrypt_value", fake_decrypt_value)
        monkeypatch.setattr("app.auth.routes.store_oauth_tokens", capturing_store_tokens)
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=False, public_url="http://localhost:3000", session_expire_days=7),
        )
        monkeypatch.setattr("app.auth.routes.get_organization", AsyncMock(return_value=None))
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))
        monkeypatch.setattr("app.auth.routes.update_user_last_login", AsyncMock())

        req = _fake_request()
        await oauth_callback(request=req, code="code", state=state)

        # The stored refresh token should be the existing one, not empty
        assert stored_tokens.get("refresh_token") == existing_refresh


# ---------------------------------------------------------------------------
# oauth_callback — main calendar init on first login
# ---------------------------------------------------------------------------


class TestOAuthCallbackMainCalendarInit:
    @pytest.mark.asyncio
    async def test_sets_main_calendar_on_first_login(self, test_db, monkeypatch):
        from app.auth.routes import oauth_callback

        state = "first-login-state"
        await _store_state(state, "login", next_url="/app")

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "new@example.com", "id": "gid-new", "name": "New User"}

        # No main_calendar_id on the newly created user
        from app.auth.session import User as SessionUser

        async def fake_create_or_update_user(**kwargs):
            uid = await _insert_user("new@example.com", "gid-new", main_calendar_id=None)
            return SessionUser(id=uid, email="new@example.com", google_user_id="gid-new", is_admin=False)

        fake_cal_client = MagicMock()
        fake_cal_client.list_calendars.return_value = [
            {"id": "primary@example.com", "primary": True}
        ]

        from types import SimpleNamespace
        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)
        monkeypatch.setattr("app.auth.routes.create_or_update_user", fake_create_or_update_user)
        monkeypatch.setattr("app.auth.routes.store_oauth_tokens", AsyncMock(return_value=1))
        monkeypatch.setattr("app.auth.routes.update_user_last_login", AsyncMock())
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=False, public_url="http://localhost:3000", session_expire_days=7),
        )
        monkeypatch.setattr("app.auth.routes.get_organization", AsyncMock(return_value=None))
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))
        # GoogleCalendarClient is lazily imported inside the route handler;
        # patch it where it is actually defined.
        monkeypatch.setattr(
            "app.sync.google_calendar.GoogleCalendarClient",
            lambda token, settings=None: fake_cal_client,
        )

        req = _fake_request()
        await oauth_callback(request=req, code="code", state=state)

        # Check that main_calendar_id was written to the DB
        db = await get_database()
        cursor = await db.execute("SELECT main_calendar_id FROM users WHERE email = 'new@example.com'")
        row = await cursor.fetchone()
        assert row is not None
        assert row["main_calendar_id"] == "primary@example.com"


# ---------------------------------------------------------------------------
# oauth_callback — exception during token exchange → fallback redirect
# ---------------------------------------------------------------------------


class TestOAuthCallbackExceptionFallback:
    @pytest.mark.asyncio
    async def test_exception_during_exchange_redirects_to_callback_failed(self, test_db, monkeypatch):
        from app.auth.routes import oauth_callback
        from starlette.responses import RedirectResponse

        state = "exception-state"
        await _store_state(state, "login", next_url="/app")

        async def failing_exchange(code, redirect_uri):
            raise RuntimeError("network error")

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", failing_exchange)
        monkeypatch.setattr("app.auth.routes.is_oobe_completed", AsyncMock(return_value=True))

        req = _fake_request()
        resp = await oauth_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "callback_failed" in resp.headers["location"]


# ---------------------------------------------------------------------------
# connect_client_callback — error parameter
# ---------------------------------------------------------------------------


class TestConnectClientCallbackErrorParam:
    @pytest.mark.asyncio
    async def test_error_param_redirects_with_reason(self, test_db):
        from app.auth.routes import connect_client_callback
        from starlette.responses import RedirectResponse

        req = _fake_request()
        resp = await connect_client_callback(
            request=req,
            error="access_denied",
            error_description="Denied",
        )
        assert isinstance(resp, RedirectResponse)
        assert "client_connect_failed" in resp.headers["location"]
        assert "access_denied" in resp.headers["location"]


# ---------------------------------------------------------------------------
# connect_client_callback — invalid state
# ---------------------------------------------------------------------------


class TestConnectClientCallbackInvalidState:
    @pytest.mark.asyncio
    async def test_missing_state_raises_400(self, test_db):
        from app.auth.routes import connect_client_callback
        from fastapi import HTTPException

        req = _fake_request()
        with pytest.raises(HTTPException) as exc_info:
            await connect_client_callback(request=req, code="code", state="nonexistent")
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_wrong_state_type_raises_400(self, test_db):
        from app.auth.routes import connect_client_callback
        from fastapi import HTTPException

        state = "wrong-type-state"
        await _store_state(state, "login")  # "login" type, not "client"

        req = _fake_request()
        with pytest.raises(HTTPException) as exc_info:
            await connect_client_callback(request=req, code="code", state=state)
        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# connect_client_callback — no refresh token
# ---------------------------------------------------------------------------


class TestConnectClientCallbackNoRefreshToken:
    @pytest.mark.asyncio
    async def test_missing_refresh_token_redirects_with_error(self, test_db, monkeypatch):
        from app.auth.routes import connect_client_callback
        from starlette.responses import RedirectResponse

        user_id = await _insert_user("owner@example.com", "gid-owner", main_calendar_id="main-cal")
        state = "no-refresh-state"
        await _store_state(state, "client", user_id=user_id)

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "expires_in": 3600}  # no refresh_token

        async def fake_user_info(token):
            return {"email": "client@external.com", "id": "gid-client"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=False, public_url="http://localhost:3000"),
        )

        req = _fake_request()
        resp = await connect_client_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "no_refresh_token" in resp.headers["location"]


# ---------------------------------------------------------------------------
# connect_client_callback — test_mode client allowlist
# ---------------------------------------------------------------------------


class TestConnectClientCallbackTestMode:
    @pytest.mark.asyncio
    async def test_client_email_not_in_allowlist_redirects(self, test_db, monkeypatch):
        from app.auth.routes import connect_client_callback
        from starlette.responses import RedirectResponse

        user_id = await _insert_user("owner2@example.com", "gid-owner2", main_calendar_id="main-cal")
        state = "client-test-mode-state"
        await _store_state(state, "client", user_id=user_id)

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "blocked@external.com", "id": "gid-blocked"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=True, public_url="http://localhost:3000"),
        )
        monkeypatch.setattr("app.auth.routes.get_test_mode_client_allowlist", lambda: ["allowed@external.com"])

        req = _fake_request()
        resp = await connect_client_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "client_email_not_allowed" in resp.headers["location"]

    @pytest.mark.asyncio
    async def test_empty_client_allowlist_redirects_with_no_allowlist_error(self, test_db, monkeypatch):
        from app.auth.routes import connect_client_callback
        from starlette.responses import RedirectResponse

        user_id = await _insert_user("owner3@example.com", "gid-owner3", main_calendar_id="main-cal")
        state = "client-no-list-state"
        await _store_state(state, "client", user_id=user_id)

        async def fake_exchange(code, redirect_uri):
            return {"access_token": "tok", "refresh_token": "ref", "expires_in": 3600}

        async def fake_user_info(token):
            return {"email": "someone@external.com", "id": "gid-ext"}

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", fake_exchange)
        monkeypatch.setattr("app.auth.routes.get_user_info", fake_user_info)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=True, public_url="http://localhost:3000"),
        )
        monkeypatch.setattr("app.auth.routes.get_test_mode_client_allowlist", lambda: [])

        req = _fake_request()
        resp = await connect_client_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "test_mode_no_client_allowlist" in resp.headers["location"]


# ---------------------------------------------------------------------------
# connect_client_callback — exception fallback
# ---------------------------------------------------------------------------


class TestConnectClientCallbackExceptionFallback:
    @pytest.mark.asyncio
    async def test_exception_during_exchange_redirects_to_callback_failed(self, test_db, monkeypatch):
        from app.auth.routes import connect_client_callback
        from starlette.responses import RedirectResponse

        user_id = await _insert_user("owner4@example.com", "gid-owner4", main_calendar_id="main-cal")
        state = "client-exception-state"
        await _store_state(state, "client", user_id=user_id)

        async def failing_exchange(code, redirect_uri):
            raise RuntimeError("timeout")

        monkeypatch.setattr("app.auth.routes.exchange_code_for_tokens", failing_exchange)

        from types import SimpleNamespace
        monkeypatch.setattr(
            "app.auth.routes.get_settings",
            lambda: SimpleNamespace(test_mode=False, public_url="http://localhost:3000"),
        )

        req = _fake_request()
        resp = await connect_client_callback(request=req, code="code", state=state)

        assert isinstance(resp, RedirectResponse)
        assert "client_callback_failed" in resp.headers["location"]
