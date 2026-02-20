"""Additional branch coverage tests for auth.google."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from app.database import get_database
from app.encryption import encrypt_value, init_encryption_manager


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_get_oauth_credentials_missing_org_raises(test_db):
    """Credential lookup should raise when organization is not configured."""
    from app.auth.google import get_oauth_credentials

    with pytest.raises(ValueError):
        await get_oauth_credentials()


@pytest.mark.asyncio
async def test_exchange_and_refresh_paths_with_db_credentials(test_db, test_encryption_key, monkeypatch):
    """Token exchange/refresh should use DB credentials and handle non-200 + success cases."""
    from app.auth.google import exchange_code_for_tokens, refresh_access_token

    init_encryption_manager(test_encryption_key)
    db = await get_database()
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        (
            "example.com",
            encrypt_value("client.apps.googleusercontent.com"),
            encrypt_value("super-secret-value"),
        ),
    )
    await db.commit()

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = text

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, response: FakeResponse):
            self.response = response

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, *_args, **_kwargs):
            return self.response

    # Exchange failure branch (non-200) with implicit DB credentials.
    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(FakeResponse(400, text="bad exchange")),
    )
    with pytest.raises(ValueError):
        await exchange_code_for_tokens("code", "https://example.com/callback")

    # Refresh success branch with implicit DB credentials.
    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(FakeResponse(200, payload={"access_token": "new-access", "expires_in": 3600})),
    )
    refreshed = await refresh_access_token("refresh-token")
    assert refreshed["access_token"] == "new-access"


@pytest.mark.asyncio
async def test_get_valid_access_token_raises_after_retry_exhaustion(test_db, test_encryption_key, monkeypatch):
    """Token refresh should re-raise after all transient-retry attempts fail."""
    from app.auth.google import get_valid_access_token, store_oauth_tokens

    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user("retryfail@example.com", "retryfail-google")
    token_id = await store_oauth_tokens(
        user_id=user_id,
        account_type="home",
        email="retryfail@example.com",
        access_token="access-old",
        refresh_token="refresh-old",
        expires_in=3600,
    )

    db = await get_database()
    await db.execute(
        "UPDATE oauth_tokens SET token_expiry = ? WHERE id = ?",
        ((datetime.utcnow() - timedelta(minutes=10)).isoformat(), token_id),
    )
    await db.commit()

    async def always_fail_refresh(_refresh_token: str, client_id=None, client_secret=None):
        raise RuntimeError("temporary network failure")

    async def fake_sleep(_seconds: float):
        return None

    monkeypatch.setattr("app.auth.google.refresh_access_token", always_fail_refresh)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(RuntimeError):
        await get_valid_access_token(user_id, "retryfail@example.com")


@pytest.mark.asyncio
async def test_calendar_service_helpers(test_db, monkeypatch):
    """Calendar service helper wrappers should delegate correctly."""
    from app.auth.google import get_calendar_service, get_calendar_service_for_user

    calls = {"build": 0, "token": 0}

    def fake_build(_service: str, _version: str, credentials):
        calls["build"] += 1
        return {"service": "calendar", "credentials": credentials}

    monkeypatch.setattr("app.auth.google.build", fake_build)

    class DummyCreds:
        pass

    creds = DummyCreds()
    service = get_calendar_service(creds)
    assert service["service"] == "calendar"
    assert service["credentials"] is creds

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        calls["token"] += 1
        return "access-token"

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)

    wrapper_calls = {"count": 0}

    def fake_get_calendar_service(credentials):
        wrapper_calls["count"] += 1
        return {"wrapped": credentials.token}

    monkeypatch.setattr("app.auth.google.get_calendar_service", fake_get_calendar_service)

    wrapped = await get_calendar_service_for_user(1, "user@example.com")
    assert calls["token"] == 1
    assert wrapper_calls["count"] == 1
    assert wrapped == {"wrapped": "access-token"}
