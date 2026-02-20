"""Coverage-focused tests for core utility/config/main modules."""

from __future__ import annotations

import os
import runpy
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from app.database import get_database


@pytest.mark.asyncio
async def test_utils_create_background_task_handles_success_and_failure():
    """Background-task wrapper should run coroutines and swallow/log exceptions."""
    from app.utils.tasks import create_background_task

    state = {"ok": 0, "failed": 0}

    async def ok_coro():
        state["ok"] += 1

    async def fail_coro():
        state["failed"] += 1
        raise RuntimeError("boom")

    task_ok = create_background_task(ok_coro(), "ok")
    await task_ok
    assert state["ok"] == 1

    task_fail = create_background_task(fail_coro(), "fail")
    await task_fail
    assert state["failed"] == 1


def test_config_get_encryption_key_and_session_secret_paths(tmp_path, monkeypatch):
    """Config helpers should validate key files and derive/fallback session secrets."""
    import app.config as config

    missing_path = tmp_path / "missing.key"
    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(encryption_key_file=str(missing_path), session_secret_key=None))
    with pytest.raises(RuntimeError):
        config.get_encryption_key()

    short_key = tmp_path / "short.key"
    short_key.write_bytes(b"short")
    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(encryption_key_file=str(short_key), session_secret_key=None))
    with pytest.raises(RuntimeError):
        config.get_encryption_key()

    good_key = tmp_path / "good.key"
    good_key.write_bytes(b"x" * 32 + b"\n\r")
    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(encryption_key_file=str(good_key), session_secret_key=None))
    key = config.get_encryption_key()
    assert len(key) == 32

    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(encryption_key_file=str(good_key), session_secret_key="explicit-secret"))
    assert config.get_session_secret() == "explicit-secret"

    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(encryption_key_file=str(good_key), session_secret_key=None))
    derived = config.get_session_secret()
    assert isinstance(derived, str)
    assert len(derived) == 64

    config._oobe_session_secret = None

    def missing_key():
        raise RuntimeError("missing key")

    monkeypatch.setattr(config, "get_encryption_key", missing_key)
    fallback_1 = config.get_session_secret()
    fallback_2 = config.get_session_secret()
    assert fallback_1 == fallback_2
    assert len(fallback_1) > 10


@pytest.mark.asyncio
async def test_encryption_manager_global_async_and_sync_paths(monkeypatch):
    """Encryption globals should initialize via both async and sync accessors."""
    import app.encryption as enc

    key = b"1" * 32
    enc._encryption_manager = None
    enc._encryption_manager_lock = None

    monkeypatch.setattr("app.config.get_encryption_key", lambda: key)

    mgr_async_1 = await enc.get_encryption_manager_async()
    mgr_async_2 = await enc.get_encryption_manager_async()
    assert mgr_async_1 is mgr_async_2

    enc._encryption_manager = None
    mgr_sync = enc.get_encryption_manager()
    assert mgr_sync is not None

    enc.init_encryption_manager(key)
    cipher = enc.encrypt_value("secret")
    plain = enc.decrypt_value(cipher)
    assert plain == "secret"

    # Ensure lock helper path executes.
    enc._encryption_manager_lock = None
    assert enc._get_lock() is not None


@pytest.mark.asyncio
async def test_main_health_exception_handler_favicon_and_lifespan(monkeypatch, tmp_path):
    """Main module handlers and lifespan should cover success/failure branches."""
    import app.main as main

    # Health failure path
    async def failing_db():
        raise RuntimeError("db down")

    monkeypatch.setattr(main, "get_database", failing_db)
    unhealthy = await main.health_check()
    assert unhealthy.status_code == 503

    # Exception handler paths
    api_req = Request({"type": "http", "method": "GET", "path": "/api/x", "headers": []})
    page_req = Request({"type": "http", "method": "GET", "path": "/app", "headers": []})
    api_resp = await main.global_exception_handler(api_req, RuntimeError("x"))
    page_resp = await main.global_exception_handler(page_req, RuntimeError("x"))
    assert api_resp.status_code == 500
    assert page_resp.status_code == 500

    fav = await main.favicon()
    assert fav.status_code == 204

    # Lifespan success
    calls = {"db": 0, "close": 0, "setup_sched": 0, "shutdown_sched": 0}
    key_file = tmp_path / "enc.key"
    key_file.write_bytes(b"2" * 32)

    async def ok_db():
        calls["db"] += 1
        return await get_database()

    async def close_db():
        calls["close"] += 1

    monkeypatch.setattr(main, "get_settings", lambda: SimpleNamespace(public_url="http://localhost:3000", database_path=":memory:", encryption_key_file=str(key_file)))
    monkeypatch.setattr(main, "get_database", ok_db)
    monkeypatch.setattr(main, "close_database", close_db)
    monkeypatch.setattr("app.config.get_encryption_key", lambda: b"2" * 32)
    monkeypatch.setattr("app.encryption.init_encryption_manager", lambda _key: None)
    monkeypatch.setattr("app.jobs.scheduler.setup_scheduler", lambda: calls.__setitem__("setup_sched", calls["setup_sched"] + 1))
    monkeypatch.setattr("app.jobs.scheduler.shutdown_scheduler", lambda: calls.__setitem__("shutdown_sched", calls["shutdown_sched"] + 1))

    async with main.lifespan(main.app):
        pass

    assert calls["db"] == 1
    assert calls["close"] == 1
    assert calls["setup_sched"] == 1
    assert calls["shutdown_sched"] == 1

    # Lifespan failure paths (encryption init and scheduler startup/shutdown failures)
    monkeypatch.setattr("app.config.get_encryption_key", lambda: (_ for _ in ()).throw(RuntimeError("no key")))
    monkeypatch.setattr("app.jobs.scheduler.setup_scheduler", lambda: (_ for _ in ()).throw(RuntimeError("sched fail")))
    monkeypatch.setattr("app.jobs.scheduler.shutdown_scheduler", lambda: (_ for _ in ()).throw(RuntimeError("shutdown fail")))
    async with main.lifespan(main.app):
        pass


def test_main_module_main_block_runs_with_stubbed_uvicorn(tmp_path, monkeypatch):
    """Running app.main as __main__ should invoke uvicorn.run with expected args."""
    calls = {"ran": False}

    def fake_run(*args, **kwargs):
        calls["ran"] = True
        calls["args"] = args
        calls["kwargs"] = kwargs

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", SimpleNamespace(run=fake_run))

    # Create static dir so import-time mount branch executes in the __main__ module run.
    static_dir = Path("app/static")
    created = False
    if not static_dir.exists():
        static_dir.mkdir(parents=True, exist_ok=True)
        created = True

    try:
        runpy.run_module("app.main", run_name="__main__")
    finally:
        if created:
            shutil.rmtree(static_dir, ignore_errors=True)

    assert calls["ran"] is True
    assert calls["kwargs"]["port"] == 3000


@pytest.mark.asyncio
async def test_cleanup_recurring_branch_and_vacuum(test_db, monkeypatch):
    """Cleanup should handle recurring-series deletion and vacuum call."""
    from app.jobs.cleanup import run_retention_cleanup, vacuum_database

    db = await get_database()
    # Create recurring mapping soft-deleted in the past.
    await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES ('cleanup-rec@example.com', 'cleanup-rec-google', 'x')"""
    )
    cursor = await db.execute("SELECT id FROM users WHERE email = 'cleanup-rec@example.com'")
    user_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO event_mappings
           (user_id, origin_type, origin_event_id, main_event_id, is_recurring, deleted_at, user_can_edit)
           VALUES (?, 'main', 'orig-rec', 'main-rec', TRUE, ?, TRUE)""",
        (user_id, (datetime.utcnow() - timedelta(days=40)).isoformat()),
    )
    await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'cleanup-client@example.com', ?, ?)""",
        (user_id, b"a", b"r"),
    )
    cursor = await db.execute(
        """SELECT id FROM oauth_tokens
           WHERE user_id = ? AND google_account_email = 'cleanup-client@example.com'""",
        (user_id,),
    )
    oauth_token_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO client_calendars (user_id, oauth_token_id, google_calendar_id, display_name)
           VALUES (?, ?, 'cleanup-client-cal', 'Cleanup Client')""",
        (user_id, oauth_token_id),
    )
    cursor = await db.execute(
        "SELECT id FROM client_calendars WHERE google_calendar_id = 'cleanup-client-cal'"
    )
    client_calendar_id = (await cursor.fetchone())["id"]
    cursor = await db.execute("SELECT id FROM event_mappings WHERE origin_event_id = 'orig-rec'")
    mapping_id = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO busy_blocks (event_mapping_id, client_calendar_id, busy_block_event_id) VALUES (?, ?, 'bb-rec')",
        (mapping_id, client_calendar_id),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["deleted_recurring_series"] >= 1

    # Vacuum path
    executed = {"vacuum": 0}

    class FakeDB:
        async def execute(self, sql, *args, **kwargs):
            if sql == "VACUUM":
                executed["vacuum"] += 1
            return None

    async def fake_get_database():
        return FakeDB()

    monkeypatch.setattr("app.jobs.cleanup.get_database", fake_get_database)
    await vacuum_database()
    assert executed["vacuum"] == 1


@pytest.mark.asyncio
async def test_webhook_renewal_missing_and_failure_branches(test_db, monkeypatch):
    """Webhook renewal module should cover no-op and registration failure paths."""
    from app.jobs.webhook_renewal import register_webhooks_for_user, renew_expiring_webhooks

    # No expiring webhooks
    await renew_expiring_webhooks()

    db = await get_database()
    await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES ('renew@example.com', 'renew-google', 'Renew', 'main-renew')"""
    )
    cursor = await db.execute("SELECT id FROM users WHERE email = 'renew@example.com'")
    user_id = (await cursor.fetchone())["id"]

    # Expiring webhook with missing joined calendar info.
    await db.execute(
        """INSERT INTO webhook_channels
           (user_id, calendar_type, client_calendar_id, channel_id, resource_id, expiration)
           VALUES (?, 'client', NULL, 'missing-joined', 'r', ?)""",
        (user_id, (datetime.utcnow() + timedelta(hours=1)).isoformat()),
    )
    await db.commit()
    await renew_expiring_webhooks()

    # Register webhooks failure branches for main and client calendars.
    await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'client-renew@example.com', ?, ?)""",
        (user_id, b"a", b"r"),
    )
    cursor = await db.execute(
        """SELECT id FROM oauth_tokens
           WHERE user_id = ? AND google_account_email = 'client-renew@example.com'""",
        (user_id,),
    )
    token_id = (await cursor.fetchone())["id"]
    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, 'client-renew-cal', 'Client Renew', TRUE)""",
        (user_id, token_id),
    )
    await db.commit()

    async def fake_get_valid_access_token(_user_id: int, _email: str):
        return "token"

    async def failing_register_webhook_channel(**_kwargs):
        raise RuntimeError("register failed")

    monkeypatch.setattr("app.auth.google.get_valid_access_token", fake_get_valid_access_token)
    monkeypatch.setattr("app.api.webhooks.register_webhook_channel", failing_register_webhook_channel)
    await register_webhooks_for_user(user_id)
