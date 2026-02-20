"""Extended tests for API endpoint modules."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.auth.session import User
from app.database import get_database, set_setting
from app.encryption import encrypt_value, init_encryption_manager


async def _insert_user(
    email: str,
    google_user_id: str,
    is_admin: bool = False,
    main_calendar_id: str | None = None,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id, last_login_at)
           VALUES (?, ?, ?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], is_admin, main_calendar_id, datetime.utcnow().isoformat()),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(
    user_id: int,
    account_type: str,
    email: str,
) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_calendar(user_id: int, token_id: int, google_calendar_id: str, is_active: bool = True) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, token_id, google_calendar_id, google_calendar_id, is_active),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _user_model(user_id: int, email: str, is_admin: bool = False, main_calendar_id: str | None = None) -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"g-{user_id}",
        display_name=email.split("@")[0],
        is_admin=is_admin,
        main_calendar_id=main_calendar_id,
    )


@pytest.mark.asyncio
async def test_users_api_endpoints(test_db, monkeypatch, test_encryption_key):
    """Users API should support profile, calendar listing, main calendar update, and alert prefs."""
    from app.api.users import (
        AlertPreferences,
        SetMainCalendarRequest,
        get_alert_preferences,
        get_me,
        list_my_calendars,
        set_main_calendar,
        update_alert_preferences,
    )

    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user("user@example.com", "google-user", main_calendar_id="primary")
    user = _user_model(user_id, "user@example.com", main_calendar_id="primary")

    me = await get_me(user=user)
    assert me.id == user_id
    assert me.email == "user@example.com"

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeService:
        def calendarList(self):
            return SimpleNamespace(
                list=lambda: SimpleNamespace(
                    execute=lambda: {
                        "items": [
                            {"id": "primary", "summary": "Primary", "primary": True, "accessRole": "owner"},
                            {"id": "other", "summary": "Other", "accessRole": "reader"},
                        ]
                    }
                )
            )

        def calendars(self):
            return SimpleNamespace(get=lambda **_kwargs: SimpleNamespace(execute=lambda: {"id": "primary"}))

    monkeypatch.setattr("app.api.users.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("googleapiclient.discovery.build", lambda *_args, **_kwargs: FakeService())

    calendars = await list_my_calendars(user=user)
    assert len(calendars["calendars"]) == 2

    updated = await set_main_calendar(SetMainCalendarRequest(calendar_id="primary"), user=user)
    assert updated["main_calendar_id"] == "primary"

    prefs = await get_alert_preferences(user=user)
    assert prefs.email_on_sync_failure is True

    await update_alert_preferences(AlertPreferences(email_on_sync_failure=False, email_on_token_revoked=False), user=user)
    prefs_after = await get_alert_preferences(user=user)
    assert prefs_after.email_on_sync_failure is False
    assert prefs_after.email_on_token_revoked is False

    class FailingService:
        def calendars(self):
            return SimpleNamespace(get=lambda **_kwargs: SimpleNamespace(execute=lambda: (_ for _ in ()).throw(RuntimeError("no access"))))

    monkeypatch.setattr("googleapiclient.discovery.build", lambda *_args, **_kwargs: FailingService())
    with pytest.raises(HTTPException) as exc:
        await set_main_calendar(SetMainCalendarRequest(calendar_id="bad"), user=user)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_calendars_api_endpoints(test_db, monkeypatch):
    """Calendars API should connect/list/status/sync/disconnect safely."""
    from app.api.calendars import (
        ConnectCalendarRequest,
        connect_client_calendar,
        disconnect_client_calendar,
        get_calendar_status,
        list_client_calendars,
        trigger_calendar_sync,
    )

    user_id = await _insert_user("cal-user@example.com", "cal-user-google", main_calendar_id="main-cal")
    user = _user_model(user_id, "cal-user@example.com", main_calendar_id="main-cal")
    token_id = await _insert_token(user_id, "client", "client-acct@example.com")

    async def fake_get_valid_access_token(_user_id: int, _email: str) -> str:
        return "token"

    class FakeService:
        def calendars(self):
            return SimpleNamespace(get=lambda **_kwargs: SimpleNamespace(execute=lambda: {"summary": "Client One"}))

    triggered_tasks = []

    async def fake_trigger_sync_for_calendar(_calendar_id: int):
        return None

    def fake_create_background_task(coro, task_name: str = "task"):
        triggered_tasks.append(task_name)
        coro.close()

    async def fake_cleanup_disconnected_calendar(_calendar_id: int, _user_id: int):
        return None

    monkeypatch.setattr("app.api.calendars.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("googleapiclient.discovery.build", lambda *_args, **_kwargs: FakeService())
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_create_background_task)
    monkeypatch.setattr("app.sync.engine.cleanup_disconnected_calendar", fake_cleanup_disconnected_calendar)

    connected = await connect_client_calendar(
        ConnectCalendarRequest(token_id=token_id, calendar_id="client-cal-1"),
        user=user,
    )
    assert connected.google_calendar_id == "client-cal-1"
    assert connected.display_name == "Client One"
    assert any(name.startswith("initial_sync_calendar_") for name in triggered_tasks)

    listed = await list_client_calendars(user=user)
    assert len(listed) == 1
    assert listed[0].sync_status in {"pending", "ok", "warning", "error"}

    with pytest.raises(HTTPException) as exc_dup:
        await connect_client_calendar(
            ConnectCalendarRequest(token_id=token_id, calendar_id="client-cal-1"),
            user=user,
        )
    assert exc_dup.value.status_code == 400

    calendar_id = listed[0].id
    db = await get_database()
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'e1', 'm1', FALSE, TRUE)""",
        (user_id, calendar_id),
    )
    cursor = await db.execute("SELECT id FROM event_mappings WHERE user_id = ? AND origin_event_id = 'e1'", (user_id,))
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, calendar_id, "b1"),
    )
    await db.commit()

    status = await get_calendar_status(calendar_id=calendar_id, user=user)
    assert status.event_count >= 1
    assert status.busy_block_count >= 1

    sync_response = await trigger_calendar_sync(calendar_id=calendar_id, user=user)
    assert sync_response["status"] == "ok"

    disconnected = await disconnect_client_calendar(calendar_id=calendar_id, user=user)
    assert disconnected["status"] == "ok"

    with pytest.raises(HTTPException):
        await disconnect_client_calendar(calendar_id=calendar_id, user=user)


@pytest.mark.asyncio
async def test_sync_api_endpoints(test_db, monkeypatch):
    """Sync API should report status/log and support full resync."""
    from app.api.sync import get_sync_log, get_sync_status, trigger_full_resync

    user_id = await _insert_user("sync-user@example.com", "sync-user-google", main_calendar_id="main")
    user = _user_model(user_id, "sync-user@example.com", main_calendar_id="main")
    token_id = await _insert_token(user_id, "client", "sync-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "sync-cal")
    db = await get_database()

    await db.execute(
        """INSERT INTO calendar_sync_state
           (client_calendar_id, sync_token, consecutive_failures, last_incremental_sync)
           VALUES (?, ?, ?, ?)""",
        (cal_id, "token-1", 2, datetime.utcnow().isoformat()),
    )
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'e1', 'm1', FALSE, TRUE)""",
        (user_id, cal_id),
    )
    cursor = await db.execute("SELECT id FROM event_mappings WHERE user_id = ? AND origin_event_id = 'e1'", (user_id,))
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_id, "b1"),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'success', '{}')""",
        (user_id, cal_id),
    )
    await db.execute("INSERT INTO main_calendar_sync_state (user_id, sync_token) VALUES (?, ?)", (user_id, "main-token"))
    await db.commit()

    await set_setting("sync_paused", "true")
    status = await get_sync_status(user=user)
    assert status.calendars_connected == 1
    assert status.calendars_warning == 1
    assert status.sync_paused is True

    log = await get_sync_log(user=user, page=1, page_size=10, calendar_id=cal_id)
    assert log.total >= 1
    assert len(log.entries) >= 1

    triggered = {"count": 0}

    async def fake_trigger_sync_for_user(_user_id: int):
        return None

    def fake_create_background_task(coro, task_name: str = "task"):
        triggered["count"] += 1
        coro.close()

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_user", fake_trigger_sync_for_user)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_create_background_task)

    result = await trigger_full_resync(user=user)
    assert result["status"] == "ok"
    assert triggered["count"] == 1

    cursor = await db.execute("SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id = ?", (cal_id,))
    assert (await cursor.fetchone())["sync_token"] is None

    cursor = await db.execute("SELECT sync_token FROM main_calendar_sync_state WHERE user_id = ?", (user_id,))
    assert (await cursor.fetchone())["sync_token"] is None


@pytest.mark.asyncio
async def test_webhooks_api_functions(test_db, monkeypatch):
    """Webhook API helpers should register/stop channels and trigger sync tasks."""
    from app.api.webhooks import (
        receive_google_calendar_webhook,
        register_webhook_channel,
        stop_webhook_channel,
    )

    user_id = await _insert_user("wh-user@example.com", "wh-user-google", main_calendar_id="main-wh")
    token_id = await _insert_token(user_id, "client", "wh-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "wh-cal")
    db = await get_database()

    # Known channel should trigger task.
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, ?, ?, ?)""",
        (user_id, cal_id, "ch-1", "res-1", (datetime.utcnow() + timedelta(days=1)).isoformat()),
    )
    await db.commit()

    triggered = {"count": 0}

    async def fake_trigger_sync_for_calendar(_cal_id: int):
        return None

    async def fake_trigger_sync_for_main_calendar(_user_id: int):
        return None

    def fake_create_background_task(coro, task_name: str = "task"):
        triggered["count"] += 1
        coro.close()

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_calendar", fake_trigger_sync_for_calendar)
    monkeypatch.setattr("app.sync.engine.trigger_sync_for_main_calendar", fake_trigger_sync_for_main_calendar)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_create_background_task)

    result = await receive_google_calendar_webhook(
        request=None,
        x_goog_channel_id="ch-1",
        x_goog_resource_id="res-1",
        x_goog_resource_state="exists",
        x_goog_message_number="2",
    )
    assert result["status"] == "ok"
    assert triggered["count"] == 1

    # Register/stop channel network behavior.
    class FakeResp:
        def __init__(self, status: int, payload: dict | None = None, text: str = ""):
            self.status_code = status
            self._payload = payload or {}
            self.text = text

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, responses):
            self._responses = responses
            self._idx = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, *_args, **_kwargs):
            resp = self._responses[self._idx]
            self._idx += 1
            return resp

    monkeypatch.setattr("httpx.AsyncClient", lambda: FakeClient([FakeResp(200, {"resourceId": "resource-new"})]))
    registered = await register_webhook_channel(
        user_id=user_id,
        calendar_type="main",
        calendar_id="main-wh",
        access_token="token",
    )
    assert registered["resource_id"] == "resource-new"

    monkeypatch.setattr("httpx.AsyncClient", lambda: FakeClient([FakeResp(204)]))
    stopped = await stop_webhook_channel("ch-stop", "res-stop", "token")
    assert stopped is True


@pytest.mark.asyncio
async def test_admin_api_endpoints(test_db, monkeypatch, test_encryption_key, tmp_path):
    """Admin API should handle health, user/admin actions, settings, and reset/export safely."""
    from app.api.admin import (
        FactoryResetRequest,
        UpdateSettingsRequest,
        admin_disconnect_calendar,
        delete_user,
        export_database,
        factory_reset,
        force_user_reauth,
        get_admin_settings,
        get_system_health,
        get_system_logs,
        get_user_detail,
        list_users,
        pause_sync,
        resume_sync,
        send_test_email,
        set_user_admin,
        trigger_cleanup,
        trigger_user_sync,
        update_admin_settings,
    )

    init_encryption_manager(test_encryption_key)
    admin_id = await _insert_user("admin@example.com", "admin-google", is_admin=True, main_calendar_id="main-admin")
    user_id = await _insert_user("normal@example.com", "normal-google", is_admin=False, main_calendar_id="main-normal")
    admin = _user_model(admin_id, "admin@example.com", is_admin=True, main_calendar_id="main-admin")
    token_id = await _insert_token(user_id, "client", "normal-client@example.com")
    cal_id = await _insert_calendar(user_id, token_id, "normal-cal")
    db = await get_database()
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id, consecutive_failures) VALUES (?, ?)",
        (cal_id, 6),
    )
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_calendar_id, origin_event_id, main_event_id, is_recurring, user_can_edit)
           VALUES (?, 'client', ?, 'e1', 'm1', FALSE, TRUE)""",
        (user_id, cal_id),
    )
    cursor = await db.execute("SELECT id FROM event_mappings WHERE user_id = ? AND origin_event_id = 'e1'", (user_id,))
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, ?)",
        (mapping_id, cal_id, "busy1"),
    )
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', ?, 'wh-admin', 'r1', ?)""",
        (user_id, cal_id, (datetime.utcnow() + timedelta(hours=12)).isoformat()),
    )
    await db.execute(
        """INSERT INTO sync_log (user_id, calendar_id, action, status, details)
           VALUES (?, ?, 'sync', 'failure', '{}')""",
        (user_id, cal_id),
    )
    await db.commit()

    health = await get_system_health(admin=admin)
    assert health.total_users >= 2
    assert health.total_calendars >= 1
    assert health.sync_errors_24h >= 1

    users = await list_users(admin=admin)
    assert len(users) >= 2
    users_filtered = await list_users(admin=admin, search="normal@")
    assert any(u.email == "normal@example.com" for u in users_filtered)

    detail = await get_user_detail(user_id=user_id, admin=admin)
    assert detail.email == "normal@example.com"
    assert len(detail.calendars) >= 1

    sync_called = {"count": 0}

    async def fake_trigger_sync_for_user(_user_id: int):
        return None

    def fake_background_task(coro, task_name: str = "task"):
        sync_called["count"] += 1
        coro.close()

    monkeypatch.setattr("app.sync.engine.trigger_sync_for_user", fake_trigger_sync_for_user)
    monkeypatch.setattr("app.utils.tasks.create_background_task", fake_background_task)
    response = await trigger_user_sync(user_id=user_id, admin=admin)
    assert response["status"] == "ok"
    assert sync_called["count"] == 1

    with pytest.raises(HTTPException):
        await trigger_user_sync(user_id=9999, admin=admin)

    reauth = await force_user_reauth(user_id=user_id, admin=admin)
    assert reauth["status"] == "ok"

    # Recreate token/calendar for remaining admin tests.
    token_id_2 = await _insert_token(user_id, "client", "normal-client2@example.com")
    cal_id_2 = await _insert_calendar(user_id, token_id_2, "normal-cal-2")

    toggled = await set_user_admin(user_id=user_id, is_admin=True, admin=admin)
    assert toggled["is_admin"] is True

    async def fake_cleanup_disconnected_calendar(_calendar_id: int, _user_id: int):
        return None

    monkeypatch.setattr("app.sync.engine.cleanup_disconnected_calendar", fake_cleanup_disconnected_calendar)
    disconnect_result = await admin_disconnect_calendar(user_id=user_id, calendar_id=cal_id_2, admin=admin)
    assert disconnect_result["status"] == "ok"

    logs = await get_system_logs(admin=admin, page=1, page_size=50, status_filter="failure")
    assert logs["total"] >= 0
    assert isinstance(logs["entries"], list)

    assert (await pause_sync(admin=admin))["sync_paused"] is True
    assert (await resume_sync(admin=admin))["sync_paused"] is False

    cleanup_triggered = await trigger_cleanup(admin=admin)
    assert cleanup_triggered["status"] == "ok"

    await update_admin_settings(
        UpdateSettingsRequest(
            smtp_host="smtp.example.com",
            smtp_port=2525,
            smtp_username="smtp-user",
            smtp_password="smtp-pass",
            smtp_from_address="noreply@example.com",
            alert_emails="ops@example.com",
            alerts_enabled=True,
        ),
        admin=admin,
    )
    settings_resp = await get_admin_settings(admin=admin)
    assert settings_resp.smtp_host == "smtp.example.com"
    assert settings_resp.alerts_enabled is True

    async def fake_send_email(**_kwargs):
        return None

    monkeypatch.setattr("app.alerts.email.send_email", fake_send_email)
    assert (await send_test_email(admin=admin))["status"] == "ok"

    async def fail_send_email(**_kwargs):
        raise RuntimeError("smtp down")

    monkeypatch.setattr("app.alerts.email.send_email", fail_send_email)
    with pytest.raises(HTTPException):
        await send_test_email(admin=admin)

    # Delete user branches (self-delete forbidden, user delete success)
    user_to_delete = await _insert_user("delete-me@example.com", "delete-google", is_admin=False, main_calendar_id="main-del")
    with pytest.raises(HTTPException):
        await delete_user(user_id=admin_id, admin=admin)
    assert (await delete_user(user_id=user_to_delete, admin=admin))["status"] == "ok"

    with pytest.raises(HTTPException):
        await factory_reset(FactoryResetRequest(confirmation="WRONG"), admin=admin)

    # Ensure key file exists so factory reset exercises remove branch.
    key_file = os.environ["ENCRYPTION_KEY_FILE"]
    with open(key_file, "wb") as f:
        f.write(test_encryption_key)

    reset_result = await factory_reset(FactoryResetRequest(confirmation="RESET"), admin=admin)
    assert reset_result["status"] == "ok"

    with pytest.raises(HTTPException):
        await export_database(admin=admin)
