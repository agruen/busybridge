"""Additional coverage tests for webhook API branches."""

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
           (user_id, oauth_token_id, google_calendar_id, display_name)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


@pytest.mark.asyncio
async def test_receive_webhook_triggers_main_sync_task_path(test_db, monkeypatch):
    """Webhook receiver should dispatch main-calendar task for main channels."""
    from app.api.webhooks import receive_google_calendar_webhook

    user_id = await _insert_user("main-wh@example.com", "main-wh-google", main_calendar_id="main-wh")
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'main', NULL, ?, ?, ?)""",
        (
            user_id,
            "main-channel",
            "main-resource",
            (datetime.utcnow() + timedelta(days=1)).isoformat(),
        ),
    )
    await db.commit()

    calls: list[str] = []

    async def fake_trigger_sync_for_main_calendar(_user_id: int):
        return None

    def fake_create_background_task(coro, task_name: str = "task"):
        calls.append(task_name)
        coro.close()

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_sync_for_main_calendar)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_create_background_task)

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="main-channel",
        x_goog_resource_id="main-resource",
        x_goog_resource_state="exists",
        x_goog_message_number="2",
    )
    assert result == {"status": "ok"}
    assert calls == [f"sync_main_calendar_user_{user_id}"]


@pytest.mark.asyncio
async def test_receive_webhook_returns_ok_when_trigger_fails(test_db, monkeypatch):
    """Webhook receiver should fail safe and return OK when trigger dispatch errors."""
    from app.api.webhooks import receive_google_calendar_webhook

    user_id = await _insert_user("client-wh@example.com", "client-wh-google", main_calendar_id="main")
    token_id = await _insert_token(user_id, "client-wh-token@example.com")
    calendar_id = await _insert_calendar(user_id, token_id, "client-wh-cal")
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, ?, ?, ?)""",
        (
            user_id,
            calendar_id,
            "client-channel",
            "client-resource",
            (datetime.utcnow() + timedelta(days=1)).isoformat(),
        ),
    )
    await db.commit()

    async def fake_trigger_sync_for_calendar(_calendar_id: int):
        return None

    def exploding_background_task(coro, _task_name: str = "task"):
        coro.close()
        raise RuntimeError("dispatch failed")

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.utils.tasks.create_background_task", exploding_background_task)

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="client-channel",
        x_goog_resource_id="client-resource",
        x_goog_resource_state="exists",
        x_goog_message_number="3",
    )
    assert result["status"] == "ok"
    assert result["message"] == "Sync trigger failed"


@pytest.mark.asyncio
async def test_register_webhook_channel_failure_status_raises(test_db, monkeypatch):
    """Register helper should raise ValueError when Google returns non-200."""
    from app.api.webhooks import register_webhook_channel

    user_id = await _insert_user("reg-fail@example.com", "reg-fail-google")

    class FakeResp:
        status_code = 500
        text = "upstream failed"

        def json(self):
            return {}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, *_args, **_kwargs):
            return FakeResp()

    monkeypatch.setattr("httpx.AsyncClient", lambda: FakeClient())

    with pytest.raises(ValueError):
        await register_webhook_channel(
            user_id=user_id,
            calendar_type="main",
            calendar_id="main-cal",
            access_token="token",
        )


@pytest.mark.asyncio
async def test_stop_webhook_channel_non_success_and_exception_paths(test_db, monkeypatch):
    """Stop helper should return False for bad status and exceptions."""
    from app.api.webhooks import stop_webhook_channel

    class BadStatusResp:
        status_code = 500
        text = "bad stop"

    class BadStatusClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, *_args, **_kwargs):
            return BadStatusResp()

    monkeypatch.setattr("httpx.AsyncClient", lambda: BadStatusClient())
    assert await stop_webhook_channel("bad-status", "res", "token") is False

    class ExplodingClient:
        async def __aenter__(self):
            raise RuntimeError("network down")

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr("httpx.AsyncClient", lambda: ExplodingClient())
    assert await stop_webhook_channel("explode", "res", "token") is False
