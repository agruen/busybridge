"""Tests for OOBE setup routes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.database import get_database


def _request(path: str = "/setup") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
    }
    return Request(scope)


class FakeFormRequest:
    """Minimal request object with async form() for route unit tests."""

    def __init__(self, form_data: dict):
        self._form_data = form_data
        self.url = SimpleNamespace(path="/setup")

    async def form(self):
        return self._form_data


@pytest.mark.asyncio
async def test_setup_wizard_and_step2_paths(test_db, monkeypatch):
    """Setup wizard should redirect when complete and validate step-2 credentials."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_2, setup_wizard

    setup_module._oobe_data.clear()

    async def oobe_complete():
        return True

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_complete)
    response = await setup_wizard(_request("/setup"), step=1)
    assert response.status_code == 302
    assert response.headers["location"] == "/app"

    async def oobe_incomplete():
        return False

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_incomplete)
    step5 = await setup_wizard(_request("/setup"), step=5)
    assert step5.status_code == 200
    assert "encryption_key_b64" in setup_module._oobe_data

    # Step 2 validation errors.
    missing = await setup_step_2(FakeFormRequest({"client_id": "", "client_secret": ""}))
    assert missing.status_code == 200
    assert "required" in missing.context["error"].lower()

    bad_client = await setup_step_2(
        FakeFormRequest({"client_id": "not-google-id", "client_secret": "secret"})
    )
    assert bad_client.status_code == 200
    assert "invalid client id format" in bad_client.context["error"].lower()

    good = await setup_step_2(
        FakeFormRequest(
            {
                "client_id": "good.apps.googleusercontent.com",
                "client_secret": "secret",
            }
        )
    )
    assert good.status_code == 302
    assert good.headers["location"] == "/setup?step=3"
    assert setup_module._oobe_data["client_id"] == "good.apps.googleusercontent.com"


@pytest.mark.asyncio
async def test_setup_step3_paths_and_test_credentials(test_db, monkeypatch):
    """Step-3 auth callback should handle error, mismatch, success, and failure branches."""
    from app.ui import setup as setup_module
    from app.ui.setup import step_3_auth, step_3_callback, step_3_confirm, test_credentials

    setup_module._oobe_data.clear()

    async def fake_test_oauth_credentials(client_id: str, client_secret: str) -> bool:
        return client_id.startswith("good") and bool(client_secret)

    monkeypatch.setattr("app.auth.google.test_oauth_credentials", fake_test_oauth_credentials)
    tested = await test_credentials(
        FakeFormRequest({"client_id": "good.apps.googleusercontent.com", "client_secret": "s"})
    )
    assert tested["valid"] is True

    # Missing client id in OOBE data -> back to step 2.
    start = await step_3_auth(_request("/setup/step/3/auth"))
    assert start.status_code == 302
    assert start.headers["location"] == "/setup?step=2"

    setup_module._oobe_data["client_id"] = "good.apps.googleusercontent.com"
    setup_module._oobe_data["client_secret"] = "secret"
    monkeypatch.setattr("app.auth.google.build_auth_url", lambda **_kwargs: "https://accounts.google.com/o/oauth2/v2/auth?z=1")
    start_ok = await step_3_auth(_request("/setup/step/3/auth"))
    assert start_ok.status_code == 302
    assert "accounts.google.com" in start_ok.headers["location"]
    assert "oauth_state" in setup_module._oobe_data

    error_cb = await step_3_callback(_request("/setup/step/3/callback"), error="access_denied")
    assert "error=access_denied" in error_cb.headers["location"]

    invalid_state = await step_3_callback(
        _request("/setup/step/3/callback"),
        state="wrong-state",
    )
    assert "invalid_state" in invalid_state.headers["location"]

    async def fake_exchange(_code, _redirect_uri, _client_id, _client_secret):
        return {"access_token": "a", "refresh_token": "r", "expires_in": 300}

    async def fake_user_info(_access):
        return {"email": "admin@example.com", "name": "Admin", "id": "g-admin"}

    monkeypatch.setattr("app.auth.google.exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr("app.auth.google.get_user_info", fake_user_info)

    success = await step_3_callback(
        _request("/setup/step/3/callback"),
        code="auth-code",
        state=setup_module._oobe_data["oauth_state"],
    )
    assert success.status_code == 302
    assert success.headers["location"] == "/setup?step=3"
    assert setup_module._oobe_data["admin_email"] == "admin@example.com"
    assert setup_module._oobe_data["domain"] == "example.com"

    async def exploding_exchange(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.auth.google.exchange_code_for_tokens", exploding_exchange)
    failed = await step_3_callback(
        _request("/setup/step/3/callback"),
        code="auth-code",
        state=setup_module._oobe_data["oauth_state"],
    )
    assert "oauth_failed" in failed.headers["location"]

    setup_module._oobe_data.pop("admin_email", None)
    confirm_missing = await step_3_confirm(FakeFormRequest({}))
    assert confirm_missing.headers["location"] == "/setup?step=3"
    setup_module._oobe_data["admin_email"] = "admin@example.com"
    confirm_ok = await step_3_confirm(FakeFormRequest({}))
    assert confirm_ok.headers["location"] == "/setup?step=4"


@pytest.mark.asyncio
async def test_setup_step4_and_step5_completion_flow(test_db, monkeypatch, tmp_path):
    """Steps 4 and 5 should persist OOBE data, complete setup, and clear temporary state."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_complete, setup_step_4, setup_step_5, test_email

    setup_module._oobe_data.clear()

    step4_disabled = await setup_step_4(FakeFormRequest({"enabled": ""}))
    assert step4_disabled.status_code == 302
    assert setup_module._oobe_data["smtp_enabled"] is False

    step4_enabled = await setup_step_4(
        FakeFormRequest(
            {
                "enabled": "on",
                "smtp_host": "smtp.example.com",
                "smtp_port": "2525",
                "smtp_username": "user",
                "smtp_password": "pass",
                "from_address": "noreply@example.com",
                "alert_emails": "ops@example.com",
            }
        )
    )
    assert step4_enabled.status_code == 302
    assert setup_module._oobe_data["smtp_enabled"] is True
    assert setup_module._oobe_data["smtp_port"] == 2525

    assert (await test_email(FakeFormRequest({})))["success"] is True

    # Step 5 requires confirmation.
    setup_module._oobe_data["encryption_key_b64"] = "abc"
    missing_confirm = await setup_step_5(FakeFormRequest({"confirmed": ""}))
    assert missing_confirm.status_code == 200
    assert "must confirm" in missing_confirm.context["error"].lower()

    # Complete setup path.
    key_path = tmp_path / "enc.key"
    setup_module._oobe_data.update(
        {
            "encryption_key": b"0" * 32,
            "client_id": "good.apps.googleusercontent.com",
            "client_secret": "secret",
            "domain": "example.com",
            "admin_email": "admin@example.com",
            "admin_google_id": "g-admin",
            "admin_name": "Admin",
            "admin_access_token": "access",
            "admin_refresh_token": "refresh",
            "admin_token_expiry": 3600,
            "smtp_enabled": True,
            "smtp_host": "smtp.example.com",
            "smtp_port": 2525,
            "smtp_username": "user",
            "smtp_password": "pass",
            "smtp_from_address": "noreply@example.com",
            "alert_emails": "ops@example.com",
        }
    )

    monkeypatch.setattr("app.ui.setup.get_settings", lambda: SimpleNamespace(encryption_key_file=str(key_path)))

    completed = await setup_step_5(FakeFormRequest({"confirmed": "on"}))
    assert completed.status_code == 302
    assert completed.headers["location"] == "/setup?step=6"
    assert key_path.exists()
    assert setup_module._oobe_data == {}

    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM organization")
    assert (await cursor.fetchone())[0] == 1
    cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_admin = TRUE")
    assert (await cursor.fetchone())[0] == 1

    done = await setup_complete(_request("/setup/complete"))
    assert done.status_code == 302
    assert done.headers["location"] == "/app"
