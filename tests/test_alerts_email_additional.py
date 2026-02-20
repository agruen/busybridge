"""Additional coverage tests for alerts.email branches."""

from __future__ import annotations

import pytest

from app.database import get_database, set_setting


@pytest.mark.asyncio
async def test_send_email_error_path_raises_when_transport_fails(test_db, monkeypatch):
    """send_email should log and re-raise when SMTP transport fails."""
    from app.alerts.email import send_email

    await set_setting("smtp_host", "smtp.example.com")
    await set_setting("smtp_port", "587")
    await set_setting("smtp_username", "smtp-user")
    await set_setting("smtp_from_address", "noreply@example.com")

    async def exploding_send(*_args, **_kwargs):
        raise RuntimeError("smtp down")

    monkeypatch.setattr("app.alerts.email.aiosmtplib.send", exploding_send)

    with pytest.raises(RuntimeError):
        await send_email(
            to_email="recipient@example.com",
            subject="x",
            body="y",
        )


@pytest.mark.asyncio
async def test_queue_alert_disabled_and_no_recipient_paths(test_db):
    """queue_alert should no-op when disabled and when no recipients resolve."""
    from app.alerts.email import queue_alert

    db = await get_database()

    await set_setting("alerts_enabled", "false")
    await queue_alert(alert_type="system_error", details="disabled")

    cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
    assert (await cursor.fetchone())[0] == 0

    await set_setting("alerts_enabled", "true")
    # No user_id and no alert_emails configured -> no recipients branch.
    await queue_alert(alert_type="system_error", details="no recipients")

    cursor = await db.execute("SELECT COUNT(*) FROM alert_queue")
    assert (await cursor.fetchone())[0] == 0
