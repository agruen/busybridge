"""Additional coverage tests for jobs.webhook_renewal."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.database import get_database


async def _insert_user(email: str, google_user_id: str, main_calendar_id: str | None = "main-cal") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_renew_expiring_webhooks_main_calendar_branch(test_db, monkeypatch):
    """Renewal job should use main calendar/user email for main webhook channels."""
    from app.jobs.webhook_renewal import renew_expiring_webhooks

    user_id = await _insert_user("renew-main@example.com", "renew-main-google", main_calendar_id="main-renew-cal")
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'main', NULL, 'main-renew-channel', 'main-renew-resource', ?)""",
        (user_id, (datetime.utcnow() + timedelta(hours=1)).isoformat()),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    async def fake_stop_webhook_channel(_channel_id: str, _resource_id: str, _access_token: str):
        return True

    calls: list[dict] = []

    async def fake_register_webhook_channel(**kwargs):
        calls.append(kwargs)
        return {"channel_id": "new", "resource_id": "res", "expiration": datetime.utcnow().isoformat()}

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.api.webhooks.stop_webhook_channel", fake_stop_webhook_channel)
    monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register_webhook_channel)

    await renew_expiring_webhooks()

    assert len(calls) == 1
    assert calls[0]["calendar_type"] == "main"
    assert calls[0]["calendar_id"] == "main-renew-cal"
