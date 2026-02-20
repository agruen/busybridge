"""Additional coverage tests for api.admin remaining branches."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.auth.session import User
from app.database import get_database


async def _insert_user(email: str, google_user_id: str, is_admin: bool = False) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], is_admin, "main-cal"),
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
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, ?, ?, TRUE)
           RETURNING id""",
        (user_id, token_id, calendar_id, calendar_id),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _admin_user(user_id: int, email: str = "admin@example.com") -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="Admin",
        main_calendar_id="main-cal",
        is_admin=True,
    )


@pytest.mark.asyncio
async def test_get_system_health_database_size_branch(test_db, monkeypatch, tmp_path):
    """Health endpoint should compute DB size when configured file exists."""
    from app.api.admin import get_system_health

    admin_id = await _insert_user("health-admin@example.com", "health-admin-google", is_admin=True)
    admin = _admin_user(admin_id, "health-admin@example.com")

    db_file = tmp_path / "health.db"
    db_file.write_bytes(b"x" * (1024 * 1024))

    monkeypatch.setattr(
        "app.api.admin.get_settings",
        lambda: SimpleNamespace(database_path=str(db_file), encryption_key_file=str(tmp_path / "unused.key")),
    )

    health = await get_system_health(admin=admin)
    assert health.database_size_mb > 0


@pytest.mark.asyncio
async def test_admin_not_found_guards(test_db):
    """Admin endpoints should return 404 for missing users/calendars."""
    from app.api.admin import (
        admin_disconnect_calendar,
        delete_user,
        force_user_reauth,
        get_user_detail,
        set_user_admin,
    )

    admin_id = await _insert_user("nf-admin@example.com", "nf-admin-google", is_admin=True)
    admin = _admin_user(admin_id, "nf-admin@example.com")

    with pytest.raises(HTTPException) as e1:
        await get_user_detail(user_id=999999, admin=admin)
    assert e1.value.status_code == 404

    with pytest.raises(HTTPException) as e2:
        await force_user_reauth(user_id=999999, admin=admin)
    assert e2.value.status_code == 404

    with pytest.raises(HTTPException) as e3:
        await delete_user(user_id=999999, admin=admin)
    assert e3.value.status_code == 404

    with pytest.raises(HTTPException) as e4:
        await set_user_admin(user_id=999999, is_admin=True, admin=admin)
    assert e4.value.status_code == 404

    with pytest.raises(HTTPException) as e5:
        await admin_disconnect_calendar(user_id=999999, calendar_id=999999, admin=admin)
    assert e5.value.status_code == 404


@pytest.mark.asyncio
async def test_delete_user_cleanup_exception_and_log_filters(test_db, monkeypatch):
    """Delete user should continue after cleanup exceptions; logs endpoint should apply user/action filters."""
    from app.api.admin import delete_user, get_system_logs

    admin_id = await _insert_user("ops-admin@example.com", "ops-admin-google", is_admin=True)
    target_id = await _insert_user("ops-user@example.com", "ops-user-google", is_admin=False)
    other_id = await _insert_user("ops-other@example.com", "ops-other-google", is_admin=False)
    admin = _admin_user(admin_id, "ops-admin@example.com")

    token_id = await _insert_token(target_id, "ops-user-client@example.com")
    cal_id = await _insert_calendar(target_id, token_id, "ops-cal")
    db = await get_database()
    await db.execute(
        """INSERT INTO sync_log (user_id, action, status, details)
           VALUES (?, 'sync', 'failure', '{}')""",
        (other_id,),
    )
    await db.commit()

    async def exploding_cleanup(calendar_id: int, cleanup_user_id: int):
        # Remove dependent records so delete_user can proceed, then raise to hit warning path.
        cleanup_db = await get_database()
        await cleanup_db.execute("DELETE FROM client_calendars WHERE id = ?", (calendar_id,))
        await cleanup_db.execute("DELETE FROM oauth_tokens WHERE user_id = ?", (cleanup_user_id,))
        await cleanup_db.commit()
        raise RuntimeError("cleanup exploded")

    monkeypatch.setattr("app.sync.engine.cleanup_disconnected_calendar", exploding_cleanup)
    result = await delete_user(user_id=target_id, admin=admin)
    assert result["status"] == "ok"

    cursor = await db.execute("SELECT COUNT(*) FROM users WHERE id = ?", (target_id,))
    assert (await cursor.fetchone())[0] == 0

    logs = await get_system_logs(admin=admin, user_id=other_id, action_filter="sync")
    assert logs["total"] == 1
    assert logs["entries"][0]["user_id"] == other_id
    assert logs["entries"][0]["action"] == "sync"


@pytest.mark.asyncio
async def test_factory_reset_key_removal_and_export_success(test_db, monkeypatch, tmp_path):
    """Factory reset should remove key file and export endpoint should return file response when DB exists."""
    from app.api.admin import FactoryResetRequest, export_database, factory_reset

    admin_id = await _insert_user("reset-admin@example.com", "reset-admin-google", is_admin=True)
    admin = _admin_user(admin_id, "reset-admin@example.com")

    db_file = tmp_path / "export.db"
    db_file.write_bytes(b"sqlite-data")
    key_file = tmp_path / "enc.key"
    key_file.write_bytes(b"k" * 32)

    monkeypatch.setattr(
        "app.api.admin.get_settings",
        lambda: SimpleNamespace(database_path=str(db_file), encryption_key_file=str(key_file)),
    )

    export_response = await export_database(admin=admin)
    assert export_response.path == str(db_file)

    reset_response = await factory_reset(FactoryResetRequest(confirmation="RESET"), admin=admin)
    assert reset_response["status"] == "ok"
    assert not Path(key_file).exists()
