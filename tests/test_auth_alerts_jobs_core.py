"""Core unit tests for auth, alerts, and background jobs."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.database import get_database, set_setting
from app.encryption import encrypt_value, init_encryption_manager


async def _insert_user(
    email: str = "user@example.com",
    google_user_id: str = "google-user-1",
    is_admin: bool = False,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", is_admin),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _request_with_cookie(name: str, value: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"cookie", f"{name}={value}".encode("utf-8"))],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_auth_session_token_roundtrip_and_invalid(test_db, test_encryption_key):
    """Session tokens should decode correctly and reject invalid input."""
    from app.auth.session import create_session_token, verify_session_token

    init_encryption_manager(test_encryption_key)
    token = create_session_token(123, "user@example.com", is_admin=True)
    session = verify_session_token(token)
    assert session is not None
    assert session.user_id == 123
    assert session.email == "user@example.com"
    assert session.is_admin is True

    assert verify_session_token("not-a-valid-token") is None


@pytest.mark.asyncio
async def test_auth_session_current_user_helpers(test_db, test_encryption_key):
    """Current-user helpers should handle cookie and auth failures correctly."""
    from app.auth.session import (
        SESSION_COOKIE_NAME,
        create_session_token,
        get_current_user,
        get_current_user_optional,
    )

    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user()
    token = create_session_token(user_id, "user@example.com")

    # No cookie -> optional returns None
    request_no_cookie = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    assert await get_current_user_optional(request_no_cookie) is None

    # Invalid token -> optional returns None
    request_bad_cookie = _request_with_cookie(SESSION_COOKIE_NAME, "bad-token")
    assert await get_current_user_optional(request_bad_cookie) is None

    # Valid token -> returns user
    request_ok = _request_with_cookie(SESSION_COOKIE_NAME, token)
    user = await get_current_user_optional(request_ok)
    assert user is not None
    assert user.id == user_id

    # Required user should raise when missing
    with pytest.raises(HTTPException) as exc:
        await get_current_user(request_no_cookie)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_auth_session_require_admin_create_update_and_login_timestamp(test_db):
    """Session helpers should enforce admin and support user create/update."""
    from app.auth.session import User, create_or_update_user, require_admin, update_user_last_login

    with pytest.raises(HTTPException) as exc:
        await require_admin(
            User(
                id=1,
                email="not-admin@example.com",
                google_user_id="g1",
                is_admin=False,
            )
        )
    assert exc.value.status_code == 403

    created = await create_or_update_user(
        email="person@example.com",
        google_user_id="google-person",
        display_name="Person A",
        is_admin=True,
    )
    assert created.email == "person@example.com"
    assert created.is_admin is True

    updated = await create_or_update_user(
        email="person2@example.com",
        google_user_id="google-person",
        display_name="Person B",
        is_admin=False,  # should not override existing is_admin for existing user
    )
    assert updated.id == created.id
    assert updated.email == "person2@example.com"
    assert updated.display_name == "Person B"
    assert updated.is_admin is True

    await update_user_last_login(created.id)
    db = await get_database()
    cursor = await db.execute("SELECT last_login_at FROM users WHERE id = ?", (created.id,))
    row = await cursor.fetchone()
    assert row["last_login_at"] is not None


@pytest.mark.asyncio
async def test_google_auth_build_url_and_oauth_credential_validation():
    """Google auth helpers should build predictable OAuth URLs and basic validation."""
    from app.auth.google import build_auth_url, test_oauth_credentials

    url = build_auth_url(
        client_id="client-id.apps.googleusercontent.com",
        redirect_uri="https://example.com/auth/callback",
        scopes=["openid", "email"],
        state="state123",
        login_hint="user@example.com",
        prompt="select_account",
    )
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert query["client_id"] == ["client-id.apps.googleusercontent.com"]
    assert query["state"] == ["state123"]
    assert query["login_hint"] == ["user@example.com"]
    assert "openid email" in query["scope"][0]

    assert await test_oauth_credentials("", "secret") is False
    assert await test_oauth_credentials("bad-client-id", "long-secret") is False
    assert await test_oauth_credentials("ok.apps.googleusercontent.com", "short") is False
    assert await test_oauth_credentials("ok.apps.googleusercontent.com", "long-enough-secret") is True


@pytest.mark.asyncio
async def test_google_auth_http_helpers_success_and_failure(test_db, monkeypatch):
    """Token exchange, refresh, and user info helpers should handle HTTP statuses."""
    from app.auth.google import exchange_code_for_tokens, get_user_info, refresh_access_token

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict, text: str = ""):
            self.status_code = status_code
            self._payload = payload
            self.text = text or str(payload)

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, post_response: FakeResponse | None = None, get_response: FakeResponse | None = None):
            self._post_response = post_response
            self._get_response = get_response

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, *_args, **_kwargs):
            return self._post_response

        async def get(self, *_args, **_kwargs):
            return self._get_response

    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(post_response=FakeResponse(200, {"access_token": "a1"})),
    )
    result = await exchange_code_for_tokens(
        "code",
        "https://example.com/callback",
        client_id="id.apps.googleusercontent.com",
        client_secret="secret",
    )
    assert result["access_token"] == "a1"

    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(post_response=FakeResponse(400, {}, "bad request")),
    )
    with pytest.raises(ValueError):
        await refresh_access_token("refresh", client_id="id.apps.googleusercontent.com", client_secret="secret")

    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(get_response=FakeResponse(200, {"email": "u@example.com"})),
    )
    info = await get_user_info("access")
    assert info["email"] == "u@example.com"

    monkeypatch.setattr(
        "app.auth.google.httpx.AsyncClient",
        lambda: FakeClient(get_response=FakeResponse(401, {}, "unauthorized")),
    )
    with pytest.raises(ValueError):
        await get_user_info("bad")


@pytest.mark.asyncio
async def test_google_token_storage_and_refresh_paths(test_db, test_encryption_key, monkeypatch):
    """OAuth token storage should encrypt values and refresh paths should work."""
    from app.auth.google import (
        get_oauth_token,
        get_valid_access_token,
        refresh_access_token,
        store_oauth_tokens,
    )

    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user(email="oauth@example.com", google_user_id="oauth-google-1")

    token_id = await store_oauth_tokens(
        user_id=user_id,
        account_type="home",
        email="oauth@example.com",
        access_token="access-old",
        refresh_token="refresh-old",
        expires_in=3600,
    )
    assert isinstance(token_id, int)
    token_data = await get_oauth_token(user_id, "oauth@example.com")
    assert token_data is not None

    # Non-expired token should be returned without refresh.
    db = await get_database()
    await db.execute(
        "UPDATE oauth_tokens SET token_expiry = ? WHERE id = ?",
        ((datetime.utcnow() + timedelta(days=1)).isoformat(), token_id),
    )
    await db.commit()
    assert await get_valid_access_token(user_id, "oauth@example.com") == "access-old"

    # Expired token should refresh and store new token.
    await db.execute(
        "UPDATE oauth_tokens SET token_expiry = ? WHERE id = ?",
        ((datetime.utcnow() - timedelta(minutes=1)).isoformat(), token_id),
    )
    await db.commit()

    async def fake_refresh(_refresh_token: str, client_id=None, client_secret=None):
        return {"access_token": "access-new", "expires_in": 1200}

    monkeypatch.setattr("app.auth.google.refresh_access_token", fake_refresh)
    refreshed = await get_valid_access_token(user_id, "oauth@example.com")
    assert refreshed == "access-new"

    # invalid_grant should bubble up.
    await db.execute(
        "UPDATE oauth_tokens SET token_expiry = ? WHERE id = ?",
        ((datetime.utcnow() - timedelta(minutes=1)).isoformat(), token_id),
    )
    await db.commit()

    async def invalid_grant_refresh(_refresh_token: str, client_id=None, client_secret=None):
        raise ValueError("invalid_grant")

    monkeypatch.setattr("app.auth.google.refresh_access_token", invalid_grant_refresh)
    with pytest.raises(ValueError):
        await get_valid_access_token(user_id, "oauth@example.com")

    # Retry should recover from transient failure.
    await db.execute(
        "UPDATE oauth_tokens SET token_expiry = ? WHERE id = ?",
        ((datetime.utcnow() - timedelta(minutes=1)).isoformat(), token_id),
    )
    await db.commit()

    attempts = {"count": 0}

    async def flaky_refresh(_refresh_token: str, client_id=None, client_secret=None):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("transient timeout")
        return {"access_token": "access-retried", "expires_in": 300}

    async def fake_sleep(_seconds: float):
        return None

    monkeypatch.setattr("app.auth.google.refresh_access_token", flaky_refresh)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    retried = await get_valid_access_token(user_id, "oauth@example.com")
    assert retried == "access-retried"

    with pytest.raises(ValueError):
        await get_valid_access_token(999, "missing@example.com")


@pytest.mark.asyncio
async def test_get_oauth_credentials_reads_encrypted_org_values(test_db, test_encryption_key):
    """OAuth credentials should decrypt values from the organization table."""
    from app.auth.google import get_oauth_credentials

    init_encryption_manager(test_encryption_key)
    db = await get_database()
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        (
            "example.com",
            encrypt_value("client.apps.googleusercontent.com"),
            encrypt_value("super-secret"),
        ),
    )
    await db.commit()

    client_id, client_secret = await get_oauth_credentials()
    assert client_id == "client.apps.googleusercontent.com"
    assert client_secret == "super-secret"


@pytest.mark.asyncio
async def test_email_config_send_queue_and_test_helpers(test_db, test_encryption_key, monkeypatch):
    """Email helpers should support config retrieval, queuing, dedupe, and test email."""
    from app.alerts.email import (
        generate_alert_content,
        get_smtp_config,
        queue_alert,
        send_email,
        send_test_email_to,
    )

    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user(email="alert-user@example.com", google_user_id="alert-google-user")

    # Not configured host should raise when sending.
    with pytest.raises(ValueError):
        await send_email("to@example.com", "subject", "body")

    await set_setting("smtp_host", "smtp.example.com")
    await set_setting("smtp_port", "2525")
    await set_setting("smtp_username", "smtp-user")
    await set_setting("smtp_from_address", "noreply@example.com")
    await set_setting("smtp_password", "smtp-pass", is_sensitive=True, encrypt_func=encrypt_value)

    config = await get_smtp_config()
    assert config["host"] == "smtp.example.com"
    assert config["port"] == 2525
    assert config["password"] == "smtp-pass"

    sent = {"count": 0}

    async def fake_send(*_args, **_kwargs):
        sent["count"] += 1
        return None

    monkeypatch.setattr("app.alerts.email.aiosmtplib.send", fake_send)
    await send_email(
        to_email="person@example.com",
        subject="Hello",
        body="text body",
        html_body="<p>html</p>",
    )
    assert sent["count"] == 1

    subject, body = generate_alert_content("sync_failures", "things broke", calendar_id=123)
    assert "Sync Failures" in subject
    assert "Calendar ID: 123" in body

    # Queue behavior with dedupe
    await set_setting("alerts_enabled", "true")
    await set_setting("alert_emails", "admin@example.com")
    await queue_alert("sync_failures", user_id=user_id, calendar_id=55, details="x")
    await queue_alert("sync_failures", user_id=user_id, calendar_id=55, details="x")

    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
    # 2 recipients (user + admin), dedupe prevents second insert
    assert (await cursor.fetchone())[0] == 2

    # send_test_email_to uses send_email internally
    assert await send_test_email_to("person@example.com") is True

    async def fake_send_fail(*_args, **_kwargs):
        raise RuntimeError("smtp down")

    monkeypatch.setattr("app.alerts.email.send_email", fake_send_fail)
    assert await send_test_email_to("person@example.com") is False


@pytest.mark.asyncio
async def test_process_alert_queue_and_cleanup_stale_alerts(test_db, monkeypatch):
    """Alert jobs should process send success/failure and clean stale records."""
    from app.jobs.alerts import cleanup_stale_alerts, process_alert_queue

    db = await get_database()

    # Empty queue
    assert await process_alert_queue() == 0

    # One successful send
    await db.execute(
        """INSERT INTO alert_queue (alert_type, recipient_email, subject, body, attempts)
           VALUES ('sync_failures', 'a@example.com', 'subj', 'body', 0)"""
    )
    await db.commit()

    async def fake_send_success(**_kwargs):
        return None

    monkeypatch.setattr("app.alerts.email.send_email", fake_send_success)
    processed = await process_alert_queue()
    assert processed == 1

    cursor = await db.execute("SELECT sent_at FROM alert_queue WHERE recipient_email = 'a@example.com'")
    assert (await cursor.fetchone())["sent_at"] is not None

    # One failed send increments attempts
    await db.execute(
        """INSERT INTO alert_queue (alert_type, recipient_email, subject, body, attempts)
           VALUES ('sync_failures', 'b@example.com', 'subj', 'body', 1)"""
    )
    await db.commit()

    async def fake_send_fail(**_kwargs):
        raise RuntimeError("smtp failure")

    monkeypatch.setattr("app.alerts.email.send_email", fake_send_fail)
    processed = await process_alert_queue()
    assert processed == 0

    cursor = await db.execute("SELECT attempts, last_attempt FROM alert_queue WHERE recipient_email = 'b@example.com'")
    row = await cursor.fetchone()
    assert row["attempts"] == 2
    assert row["last_attempt"] is not None

    # Cleanup stale alerts
    old = (datetime.utcnow() - timedelta(days=8)).isoformat()
    await db.execute(
        """INSERT INTO alert_queue (alert_type, recipient_email, subject, body, attempts, sent_at, created_at)
           VALUES ('sync_failures', 'old1@example.com', 's', 'b', 0, ?, ?)""",
        (old, old),
    )
    await db.execute(
        """INSERT INTO alert_queue (alert_type, recipient_email, subject, body, attempts, created_at)
           VALUES ('sync_failures', 'old2@example.com', 's', 'b', 3, ?)""",
        (old,),
    )
    await db.commit()

    deleted = await cleanup_stale_alerts()
    assert deleted >= 2


@pytest.mark.asyncio
async def test_webhook_renewal_and_user_registration_jobs(test_db, monkeypatch):
    """Webhook jobs should renew expiring channels and register per-user channels."""
    from app.jobs.webhook_renewal import register_webhooks_for_user, renew_expiring_webhooks

    user_id = await _insert_user(email="wh@example.com", google_user_id="wh-google-user")
    db = await get_database()
    await db.execute("UPDATE users SET main_calendar_id = ? WHERE id = ?", ("main-cal", user_id))
    token_id = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?)
           RETURNING id""",
        (user_id, "client-cal@example.com", b"a", b"r"),
    )
    token_row = await token_id.fetchone()
    client_token_id = token_row["id"]
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, client_token_id, "client-cal-id", "Client Cal"),
    )
    client_calendar_id = (await cursor.fetchone())["id"]

    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, ?, ?, ?)""",
        (
            user_id,
            client_calendar_id,
            "chan-1",
            "resource-1",
            (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        ),
    )
    # Missing calendar info record
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', NULL, ?, ?, ?)""",
        (
            user_id,
            "chan-2",
            "resource-2",
            (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        ),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    renewed = {"stop": 0, "register": 0, "alerts": 0}

    async def fake_stop(*_args, **_kwargs):
        renewed["stop"] += 1
        return True

    async def fake_register(**_kwargs):
        renewed["register"] += 1
        return {"channel_id": "new-channel"}

    async def fake_queue_alert(**_kwargs):
        renewed["alerts"] += 1

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.api.webhooks.stop_webhook_channel", fake_stop)
    monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)
    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)

    await renew_expiring_webhooks()
    assert renewed["stop"] == 1
    assert renewed["register"] >= 1
    assert renewed["alerts"] == 0

    # Failure branch should queue alert
    async def failing_get_token(_user_id: int, _email: str) -> str:
        raise RuntimeError("boom")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_get_token)
    await renew_expiring_webhooks()
    assert renewed["alerts"] >= 1

    # Register all user webhooks
    registered_for_user = {"count": 0}

    async def get_token_for_user(_user_id: int, _email: str) -> str:
        return "token"

    async def register_for_user(**_kwargs):
        registered_for_user["count"] += 1
        return {"channel_id": "x"}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", get_token_for_user)
    monkeypatch.setattr("app.api.webhooks.register_webhook_channel", register_for_user)
    await register_webhooks_for_user(user_id)
    # one main + one active client
    assert registered_for_user["count"] == 2

    # User with no main calendar should no-op
    user_without_main = await _insert_user(email="no-main@example.com", google_user_id="no-main-google-user")
    await register_webhooks_for_user(user_without_main)
