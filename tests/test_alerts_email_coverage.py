"""Tests for app/alerts/email.py — queue_alert, deduplication, send_email, and helpers."""

from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.database import get_database, set_setting


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0]),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _enable_alerts() -> None:
    await set_setting("alerts_enabled", "true")


async def _disable_alerts() -> None:
    await set_setting("alerts_enabled", "false")


async def _set_admin_alert_email(email: str) -> None:
    await set_setting("alert_emails", email)


# ---------------------------------------------------------------------------
# generate_alert_content
# ---------------------------------------------------------------------------


class TestGenerateAlertContent:
    def test_known_alert_type_returns_correct_subject(self):
        from app.alerts.email import generate_alert_content

        subject, body = generate_alert_content("token_revoked", "Details here")
        assert subject == "Calendar Sync - Authentication Required"
        assert "token_revoked" in body
        assert "Details here" in body

    def test_unknown_alert_type_uses_generic_subject(self):
        from app.alerts.email import generate_alert_content

        subject, body = generate_alert_content("custom_alert_type", "Some info")
        assert "custom_alert_type" in subject

    def test_calendar_id_included_when_provided(self):
        from app.alerts.email import generate_alert_content

        _, body = generate_alert_content("sync_failures", "error log", calendar_id=42)
        assert "42" in body

    def test_calendar_id_omitted_when_none(self):
        from app.alerts.email import generate_alert_content

        _, body = generate_alert_content("sync_failures", "error log", calendar_id=None)
        assert "Calendar ID" not in body

    def test_all_known_alert_types_have_subjects(self):
        from app.alerts.email import generate_alert_content

        known_types = [
            "token_revoked",
            "calendar_inaccessible",
            "sync_failures",
            "webhook_registration_failed",
            "system_error",
        ]
        for alert_type in known_types:
            subject, _ = generate_alert_content(alert_type, "")
            assert subject  # non-empty


# ---------------------------------------------------------------------------
# queue_alert — alerts disabled
# ---------------------------------------------------------------------------


class TestQueueAlertDisabled:
    @pytest.mark.asyncio
    async def test_does_nothing_when_alerts_disabled(self, test_db):
        from app.alerts.email import queue_alert

        await _disable_alerts()
        await queue_alert("sync_failures", details="test")

        db = await get_database()
        cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
        count = (await cursor.fetchone())[0]
        assert count == 0

    @pytest.mark.asyncio
    async def test_does_nothing_when_alerts_setting_absent(self, test_db):
        from app.alerts.email import queue_alert

        # No setting at all — treated as disabled
        await queue_alert("sync_failures", details="test")

        db = await get_database()
        cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
        count = (await cursor.fetchone())[0]
        assert count == 0


# ---------------------------------------------------------------------------
# queue_alert — no recipients
# ---------------------------------------------------------------------------


class TestQueueAlertNoRecipients:
    @pytest.mark.asyncio
    async def test_nothing_queued_when_no_recipients(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        # No user_id, no alert_emails setting
        await queue_alert("sync_failures", details="no one to tell")

        db = await get_database()
        cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
        count = (await cursor.fetchone())[0]
        assert count == 0


# ---------------------------------------------------------------------------
# queue_alert — user recipient
# ---------------------------------------------------------------------------


class TestQueueAlertUserRecipient:
    @pytest.mark.asyncio
    async def test_alert_queued_for_user(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        user_id = await _insert_user("notify@example.com", "gid-notify")

        await queue_alert("token_revoked", user_id=user_id, details="Token gone")

        db = await get_database()
        cursor = await db.execute(
            "SELECT * FROM alert_queue WHERE alert_type = 'token_revoked'"
        )
        rows = await cursor.fetchall()
        assert len(rows) == 1
        assert rows[0]["recipient_email"] == "notify@example.com"


# ---------------------------------------------------------------------------
# queue_alert — admin email recipient
# ---------------------------------------------------------------------------


class TestQueueAlertAdminRecipient:
    @pytest.mark.asyncio
    async def test_alert_queued_for_admin_email(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        await _set_admin_alert_email("admin@ops.com")

        await queue_alert("system_error", details="crash")

        db = await get_database()
        cursor = await db.execute("SELECT * FROM alert_queue WHERE alert_type = 'system_error'")
        rows = await cursor.fetchall()
        assert len(rows) == 1
        assert rows[0]["recipient_email"] == "admin@ops.com"

    @pytest.mark.asyncio
    async def test_alert_queued_for_multiple_admin_emails(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        await _set_admin_alert_email("admin1@ops.com,admin2@ops.com")

        await queue_alert("system_error", details="crash")

        db = await get_database()
        cursor = await db.execute("SELECT recipient_email FROM alert_queue WHERE alert_type = 'system_error'")
        rows = await cursor.fetchall()
        emails = {r["recipient_email"] for r in rows}
        assert "admin1@ops.com" in emails
        assert "admin2@ops.com" in emails


# ---------------------------------------------------------------------------
# queue_alert — deduplication
# ---------------------------------------------------------------------------


class TestQueueAlertDeduplication:
    @pytest.mark.asyncio
    async def test_duplicate_alert_within_one_hour_is_skipped(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        await _set_admin_alert_email("dedup@ops.com")

        await queue_alert("sync_failures", details="first")
        await queue_alert("sync_failures", details="second")  # duplicate

        db = await get_database()
        cursor = await db.execute(
            "SELECT COUNT(*) FROM alert_queue WHERE alert_type = 'sync_failures'"
        )
        count = (await cursor.fetchone())[0]
        # Only one should be stored
        assert count == 1

    @pytest.mark.asyncio
    async def test_different_alert_types_are_not_deduplicated(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        await _set_admin_alert_email("dedup2@ops.com")

        await queue_alert("sync_failures", details="sync issue")
        await queue_alert("token_revoked", details="token issue")

        db = await get_database()
        cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
        count = (await cursor.fetchone())[0]
        assert count == 2


# ---------------------------------------------------------------------------
# queue_alert — user not in DB
# ---------------------------------------------------------------------------


class TestQueueAlertUserNotFound:
    @pytest.mark.asyncio
    async def test_nonexistent_user_id_still_uses_admin_email(self, test_db):
        from app.alerts.email import queue_alert

        await _enable_alerts()
        await _set_admin_alert_email("fallback@ops.com")

        # user_id 99999 does not exist — admin email should still receive alert
        await queue_alert("system_error", user_id=99999, details="error")

        db = await get_database()
        cursor = await db.execute("SELECT recipient_email FROM alert_queue WHERE alert_type = 'system_error'")
        rows = await cursor.fetchall()
        emails = {r["recipient_email"] for r in rows}
        assert "fallback@ops.com" in emails


# ---------------------------------------------------------------------------
# send_email
# ---------------------------------------------------------------------------


class TestSendEmail:
    @pytest.mark.asyncio
    async def test_raises_when_smtp_not_configured(self, test_db):
        from app.alerts.email import send_email

        # No SMTP settings in the database
        with pytest.raises(ValueError, match="SMTP not configured"):
            await send_email("to@example.com", "Subject", "Body")

    @pytest.mark.asyncio
    async def test_sends_plain_text_email(self, test_db):
        from app.alerts.email import send_email

        await set_setting("smtp_host", "smtp.example.com")
        await set_setting("smtp_port", "587")
        await set_setting("smtp_from_address", "noreply@example.com")

        sent_messages = []

        async def fake_send(msg, **kwargs):
            sent_messages.append(msg)

        with patch("app.alerts.email.aiosmtplib.send", new=fake_send):
            await send_email("dest@example.com", "Hello", "Plain body")

        assert len(sent_messages) == 1
        assert sent_messages[0]["Subject"] == "Hello"
        assert sent_messages[0]["To"] == "dest@example.com"

    @pytest.mark.asyncio
    async def test_sends_multipart_email_with_html_body(self, test_db):
        from email.mime.multipart import MIMEMultipart
        from app.alerts.email import send_email

        await set_setting("smtp_host", "smtp.example.com")
        await set_setting("smtp_from_address", "noreply@example.com")

        sent_messages = []

        async def fake_send(msg, **kwargs):
            sent_messages.append(msg)

        with patch("app.alerts.email.aiosmtplib.send", new=fake_send):
            await send_email("dest@example.com", "Hello", "Plain", html_body="<b>Bold</b>")

        assert isinstance(sent_messages[0], MIMEMultipart)

    @pytest.mark.asyncio
    async def test_send_failure_reraises(self, test_db):
        from app.alerts.email import send_email

        await set_setting("smtp_host", "smtp.example.com")

        async def failing_send(msg, **kwargs):
            raise ConnectionRefusedError("cannot connect")

        with patch("app.alerts.email.aiosmtplib.send", new=failing_send):
            with pytest.raises(ConnectionRefusedError):
                await send_email("dest@example.com", "Subj", "Body")


# ---------------------------------------------------------------------------
# send_test_email_to
# ---------------------------------------------------------------------------


class TestSendTestEmailTo:
    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, test_db):
        from app.alerts.email import send_test_email_to

        await set_setting("smtp_host", "smtp.example.com")

        async def fake_send(msg, **kwargs):
            pass

        with patch("app.alerts.email.aiosmtplib.send", new=fake_send):
            result = await send_test_email_to("admin@example.com")

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_failure(self, test_db):
        from app.alerts.email import send_test_email_to

        # SMTP not configured → send_email raises ValueError → returns False
        result = await send_test_email_to("admin@example.com")
        assert result is False
