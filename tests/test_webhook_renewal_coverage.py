"""Tests for app/jobs/webhook_renewal.py — previously uncovered branches."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_user(
    email: str,
    google_user_id: str,
    main_calendar_id: Optional[str] = "main-cal",
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], main_calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_oauth_token(user_id: int, email: str, account_type: str = "client") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"enc-access", b"enc-refresh"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_client_calendar(user_id: int, token_id: int, google_cal_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, google_cal_id, google_cal_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_webhook(
    user_id: int,
    calendar_type: str,
    client_calendar_id: Optional[int],
    channel_id: str,
    resource_id: str,
    expiration: datetime,
) -> None:
    db = await get_database()
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration.isoformat()),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# renew_expiring_webhooks — no expiring webhooks (early return)
# ---------------------------------------------------------------------------


class TestRenewExpiringWebhooksEmpty:
    @pytest.mark.asyncio
    async def test_no_webhooks_returns_early_without_error(self, test_db):
        from app.jobs.webhook_renewal import renew_expiring_webhooks

        # No webhooks in DB at all — should return silently
        await renew_expiring_webhooks()


    @pytest.mark.asyncio
    async def test_webhook_not_expiring_soon_is_not_renewed(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import renew_expiring_webhooks

        user_id = await _insert_user("no-renew@example.com", "gid-no-renew")
        # Expiration is 48 hours away — outside the 24-hour threshold
        future = datetime.utcnow() + timedelta(hours=48)
        await _insert_webhook(user_id, "main", None, "ch-far", "res-far", future)

        calls = []

        async def fake_register(**kwargs):
            calls.append(kwargs)
            return {"channel_id": "new", "resource_id": "r", "expiration": datetime.utcnow().isoformat()}

        monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)
        await renew_expiring_webhooks()

        assert calls == []


# ---------------------------------------------------------------------------
# renew_expiring_webhooks — client calendar branch
# ---------------------------------------------------------------------------


class TestRenewExpiringWebhooksClientBranch:
    @pytest.mark.asyncio
    async def test_client_calendar_webhook_renewal_uses_correct_calendar_info(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import renew_expiring_webhooks

        user_id = await _insert_user("client-owner@example.com", "gid-client-owner")
        token_id = await _insert_oauth_token(user_id, "client-account@external.com")
        cal_id = await _insert_client_calendar(user_id, token_id, "client-cal@group.calendar.google.com")

        # Expiring within 24 hours
        soon = datetime.utcnow() + timedelta(hours=1)
        await _insert_webhook(user_id, "client", cal_id, "ch-client", "res-client", soon)

        async def fake_get_token(_user_id, _email):
            return "access-token"

        async def fake_stop(channel_id, resource_id, token):
            pass

        calls = []

        async def fake_register(**kwargs):
            calls.append(kwargs)
            return {"channel_id": "new-ch", "resource_id": "new-res", "expiration": datetime.utcnow().isoformat()}

        monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_token)
        monkeypatch.setattr("app.api.webhooks.stop_webhook_channel", fake_stop)
        monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)

        await renew_expiring_webhooks()

        assert len(calls) == 1
        assert calls[0]["calendar_type"] == "client"
        assert calls[0]["calendar_id"] == "client-cal@group.calendar.google.com"
        assert calls[0]["client_calendar_id"] == cal_id


# ---------------------------------------------------------------------------
# renew_expiring_webhooks — missing calendar info warning branch
# ---------------------------------------------------------------------------


class TestRenewExpiringWebhooksMissingInfo:
    @pytest.mark.asyncio
    async def test_webhook_with_no_calendar_id_skipped(self, test_db, monkeypatch):
        """When a user has no main_calendar_id, the webhook renewal should be skipped."""
        from app.jobs.webhook_renewal import renew_expiring_webhooks

        # User with no main_calendar_id
        user_id = await _insert_user("no-cal@example.com", "gid-no-cal", main_calendar_id=None)
        soon = datetime.utcnow() + timedelta(hours=1)
        await _insert_webhook(user_id, "main", None, "ch-noinfo", "res-noinfo", soon)

        calls = []

        async def fake_register(**kwargs):
            calls.append(kwargs)
            return {"channel_id": "new", "resource_id": "r", "expiration": datetime.utcnow().isoformat()}

        monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)

        await renew_expiring_webhooks()
        assert calls == []


# ---------------------------------------------------------------------------
# renew_expiring_webhooks — failure triggers alert
# ---------------------------------------------------------------------------


class TestRenewExpiringWebhooksFailureAlert:
    @pytest.mark.asyncio
    async def test_failed_renewal_queues_alert(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import renew_expiring_webhooks

        user_id = await _insert_user("alert-user@example.com", "gid-alert")
        soon = datetime.utcnow() + timedelta(hours=1)
        await _insert_webhook(user_id, "main", None, "ch-fail", "res-fail", soon)

        async def fake_get_token(_user_id, _email):
            return "token"

        async def failing_stop(channel_id, resource_id, token):
            raise RuntimeError("stop failed")

        queued_alerts = []

        async def fake_queue_alert(**kwargs):
            queued_alerts.append(kwargs)

        monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_token)
        monkeypatch.setattr("app.api.webhooks.stop_webhook_channel", failing_stop)
        monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)

        await renew_expiring_webhooks()

        assert len(queued_alerts) == 1
        assert queued_alerts[0]["alert_type"] == "webhook_registration_failed"
        assert queued_alerts[0]["user_id"] == user_id


# ---------------------------------------------------------------------------
# register_webhooks_for_user
# ---------------------------------------------------------------------------


class TestRegisterWebhooksForUser:
    @pytest.mark.asyncio
    async def test_no_op_when_user_not_found(self, test_db):
        from app.jobs.webhook_renewal import register_webhooks_for_user

        # Should not raise
        await register_webhooks_for_user(user_id=99999)

    @pytest.mark.asyncio
    async def test_no_op_when_user_has_no_main_calendar(self, test_db):
        from app.jobs.webhook_renewal import register_webhooks_for_user

        user_id = await _insert_user("no-main@example.com", "gid-no-main", main_calendar_id=None)
        # Should not raise
        await register_webhooks_for_user(user_id=user_id)

    @pytest.mark.asyncio
    async def test_registers_main_calendar_webhook(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import register_webhooks_for_user

        user_id = await _insert_user("reg-user@example.com", "gid-reg-user")

        async def fake_get_token(_user_id, _email):
            return "token"

        calls = []

        async def fake_register(**kwargs):
            calls.append(kwargs)
            return {"channel_id": "ch", "resource_id": "res", "expiration": datetime.utcnow().isoformat()}

        monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_token)
        monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)

        await register_webhooks_for_user(user_id=user_id)

        main_calls = [c for c in calls if c["calendar_type"] == "main"]
        assert len(main_calls) == 1
        assert main_calls[0]["calendar_id"] == "main-cal"

    @pytest.mark.asyncio
    async def test_registers_client_calendar_webhooks(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import register_webhooks_for_user

        user_id = await _insert_user("reg-client@example.com", "gid-reg-client")
        token_id = await _insert_oauth_token(user_id, "cli@external.com")
        cal_id = await _insert_client_calendar(user_id, token_id, "cli-cal@group.calendar.google.com")

        async def fake_get_token(_user_id, _email):
            return "token"

        calls = []

        async def fake_register(**kwargs):
            calls.append(kwargs)
            return {"channel_id": "ch", "resource_id": "res", "expiration": datetime.utcnow().isoformat()}

        monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_token)
        monkeypatch.setattr("app.api.webhooks.register_webhook_channel", fake_register)

        await register_webhooks_for_user(user_id=user_id)

        client_calls = [c for c in calls if c["calendar_type"] == "client"]
        assert len(client_calls) == 1
        assert client_calls[0]["calendar_id"] == "cli-cal@group.calendar.google.com"
        assert client_calls[0]["client_calendar_id"] == cal_id

    @pytest.mark.asyncio
    async def test_continues_after_main_calendar_registration_failure(self, test_db, monkeypatch):
        from app.jobs.webhook_renewal import register_webhooks_for_user

        user_id = await _insert_user("fail-reg@example.com", "gid-fail-reg")

        async def failing_get_token(_user_id, _email):
            raise RuntimeError("token error")

        monkeypatch.setattr("app.auth.google.get_valid_access_token", failing_get_token)

        # Should not raise; error is logged and swallowed
        await register_webhooks_for_user(user_id=user_id)
