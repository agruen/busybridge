"""Additional branch coverage tests for ui.setup."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
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
async def test_setup_step5_existing_key_context_and_step2_completed_guard(test_db, monkeypatch):
    """Setup wizard should reuse existing key context and step 2 should reject completed setup."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_2, setup_wizard

    setup_module._oobe_data.clear()
    setup_module._oobe_data["encryption_key"] = b"2" * 32
    setup_module._oobe_data["encryption_key_b64"] = "existing-key-b64"

    async def oobe_incomplete():
        return False

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_incomplete)
    rendered = await setup_wizard(_request("/setup"), step=5)
    assert rendered.status_code == 200
    assert rendered.context["encryption_key_b64"] == "existing-key-b64"

    async def oobe_complete():
        return True

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_complete)
    with pytest.raises(HTTPException) as exc:
        await setup_step_2(
            FakeFormRequest(
                {
                    "client_id": "good.apps.googleusercontent.com",
                    "client_secret": "secret",
                }
            )
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_setup_step5_generates_key_creates_directory_and_sets_alerts_disabled(test_db, monkeypatch, tmp_path):
    """Step 5 completion should generate key when missing, create key dir, and disable alerts when SMTP off."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_5

    setup_module._oobe_data.clear()
    setup_module._oobe_data.update(
        {
            "client_id": "good.apps.googleusercontent.com",
            "client_secret": "secret",
            "domain": "example.com",
            "admin_email": "admin2@example.com",
            "admin_google_id": "g-admin-2",
            "admin_name": "Admin Two",
            "admin_access_token": "access",
            "admin_refresh_token": "refresh",
            "admin_token_expiry": 1200,
            "smtp_enabled": False,
        }
    )

    nested_dir = tmp_path / "nested" / "keys"
    key_path = nested_dir / "enc.key"
    monkeypatch.setattr("app.ui.setup.get_settings", lambda: SimpleNamespace(encryption_key_file=str(key_path)))
    monkeypatch.setattr("app.ui.setup.generate_encryption_key", lambda: b"1" * 32)

    response = await setup_step_5(FakeFormRequest({"confirmed": "on"}))
    assert response.status_code == 302
    assert response.headers["location"] == "/setup?step=6"
    assert key_path.exists()
    assert setup_module._oobe_data == {}

    db = await get_database()
    cursor = await db.execute("SELECT value_plain FROM settings WHERE key = 'alerts_enabled'")
    row = await cursor.fetchone()
    assert row["value_plain"] == "false"
