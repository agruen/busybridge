"""Tests for app/api/backup.py — backup/restore API endpoints."""

from __future__ import annotations

import io
import json
import zipfile
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.auth.session import User
from app.database import get_database


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _admin_user(user_id: int = 1, email: str = "admin@example.com") -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="Admin",
        main_calendar_id="main-cal",
        is_admin=True,
    )


async def _insert_admin(email: str = "admin@example.com") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin, main_calendar_id)
           VALUES (?, ?, ?, TRUE, ?)
           RETURNING id""",
        (email, f"{email}-google", "Admin", "main-cal"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _make_zip_bytes(metadata: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("metadata.json", json.dumps(metadata))
        zf.writestr("database.db", b"SQLite format 3")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# list_backups_endpoint
# ---------------------------------------------------------------------------


class TestListBackupsEndpoint:
    @pytest.mark.asyncio
    async def test_returns_empty_list(self, test_db):
        from app.api.backup import list_backups_endpoint

        admin = _admin_user(await _insert_admin())

        with patch("app.sync.backup.list_backups", return_value=[]):
            result = await list_backups_endpoint(admin=admin)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_backup_list(self, test_db):
        from app.api.backup import list_backups_endpoint

        admin = _admin_user(await _insert_admin())
        backups = [
            {
                "backup_id": "backup-20240101-120000-daily",
                "backup_type": "daily",
                "created_at": "2024-01-01T12:00:00",
                "file_size_bytes": 1024,
            }
        ]

        with patch("app.sync.backup.list_backups", return_value=backups):
            result = await list_backups_endpoint(admin=admin)

        assert len(result) == 1
        assert result[0]["backup_id"] == "backup-20240101-120000-daily"


# ---------------------------------------------------------------------------
# create_backup_endpoint
# ---------------------------------------------------------------------------


class TestCreateBackupEndpoint:
    @pytest.mark.asyncio
    async def test_creates_backup_successfully(self, test_db):
        from app.api.backup import CreateBackupRequest, create_backup_endpoint

        admin = _admin_user(await _insert_admin())
        expected_meta = {
            "backup_id": "backup-20240101-120000-daily",
            "backup_type": "daily",
            "created_at": "2024-01-01T12:00:00",
            "file_size_bytes": 512,
        }

        async def fake_create_backup(user_ids=None):
            return expected_meta

        with patch("app.sync.backup.create_backup", new=fake_create_backup):
            result = await create_backup_endpoint(
                request=CreateBackupRequest(), admin=admin
            )

        assert result["backup_id"] == "backup-20240101-120000-daily"

    @pytest.mark.asyncio
    async def test_create_backup_exception_raises_http_500(self, test_db):
        from app.api.backup import CreateBackupRequest, create_backup_endpoint

        admin = _admin_user(await _insert_admin())

        async def failing_create_backup(user_ids=None):
            raise RuntimeError("disk full")

        with patch("app.sync.backup.create_backup", new=failing_create_backup):
            with pytest.raises(HTTPException) as exc_info:
                await create_backup_endpoint(
                    request=CreateBackupRequest(), admin=admin
                )

        assert exc_info.value.status_code == 500
        assert "disk full" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_create_backup_with_user_ids(self, test_db):
        from app.api.backup import CreateBackupRequest, create_backup_endpoint

        admin = _admin_user(await _insert_admin())
        captured = {}

        async def capturing_create_backup(user_ids=None):
            captured["user_ids"] = user_ids
            return {"backup_id": "b1", "backup_type": "daily", "created_at": "2024-01-01T00:00:00", "file_size_bytes": 100}

        with patch("app.sync.backup.create_backup", new=capturing_create_backup):
            await create_backup_endpoint(
                request=CreateBackupRequest(user_ids=[42, 99]), admin=admin
            )

        assert captured["user_ids"] == [42, 99]


# ---------------------------------------------------------------------------
# download_backup
# ---------------------------------------------------------------------------


class TestDownloadBackupEndpoint:
    @pytest.mark.asyncio
    async def test_returns_404_when_backup_not_found(self, test_db, tmp_path, monkeypatch):
        from app.api.backup import download_backup

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        admin = _admin_user(await _insert_admin())

        with patch("app.sync.backup.get_backup_dir", return_value=str(tmp_path)):
            with pytest.raises(HTTPException) as exc_info:
                await download_backup("backup-nonexistent", admin=admin)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_file_response_when_exists(self, test_db, tmp_path, monkeypatch):
        from app.api.backup import download_backup
        from fastapi.responses import FileResponse

        bid = "backup-20240101-120000-daily"
        zip_path = tmp_path / f"{bid}.zip"
        zip_path.write_bytes(b"fake zip content")

        admin = _admin_user(await _insert_admin())

        with patch("app.sync.backup.get_backup_dir", return_value=str(tmp_path)):
            response = await download_backup(bid, admin=admin)

        assert isinstance(response, FileResponse)
        assert response.path == str(zip_path)


# ---------------------------------------------------------------------------
# delete_backup_endpoint
# ---------------------------------------------------------------------------


class TestDeleteBackupEndpoint:
    @pytest.mark.asyncio
    async def test_delete_returns_ok(self, test_db):
        from app.api.backup import delete_backup_endpoint

        admin = _admin_user(await _insert_admin())
        bid = "backup-20240101-120000-daily"

        with patch("app.sync.backup.delete_backup", return_value=True):
            result = await delete_backup_endpoint(bid, admin=admin)

        assert result["status"] == "ok"
        assert result["backup_id"] == bid

    @pytest.mark.asyncio
    async def test_delete_returns_404_when_not_found(self, test_db):
        from app.api.backup import delete_backup_endpoint

        admin = _admin_user(await _insert_admin())

        with patch("app.sync.backup.delete_backup", return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                await delete_backup_endpoint("backup-nonexistent", admin=admin)

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# restore_backup_endpoint
# ---------------------------------------------------------------------------


class TestRestoreBackupEndpoint:
    @pytest.mark.asyncio
    async def test_restore_success(self, test_db):
        from app.api.backup import RestoreRequest, restore_backup_endpoint

        admin = _admin_user(await _insert_admin())
        restore_result = {
            "backup_id": "backup-20240101-120000-daily",
            "dry_run": False,
            "users_restored": [1],
            "db_restored": True,
            "events_deleted": 0,
            "events_created": 2,
            "events_updated": 0,
            "errors": [],
            "planned_actions": None,
        }

        async def fake_restore(**kwargs):
            return restore_result

        with patch("app.sync.backup.restore_from_backup", new=fake_restore):
            result = await restore_backup_endpoint(
                request=RestoreRequest(backup_id="backup-20240101-120000-daily"),
                admin=admin,
            )

        assert result.db_restored is True
        assert result.events_created == 2

    @pytest.mark.asyncio
    async def test_restore_file_not_found_raises_404(self, test_db):
        from app.api.backup import RestoreRequest, restore_backup_endpoint

        admin = _admin_user(await _insert_admin())

        async def not_found_restore(**kwargs):
            raise FileNotFoundError("Backup not found: backup-missing")

        with patch("app.sync.backup.restore_from_backup", new=not_found_restore):
            with pytest.raises(HTTPException) as exc_info:
                await restore_backup_endpoint(
                    request=RestoreRequest(backup_id="backup-missing"),
                    admin=admin,
                )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_restore_unexpected_error_raises_500(self, test_db):
        from app.api.backup import RestoreRequest, restore_backup_endpoint

        admin = _admin_user(await _insert_admin())

        async def error_restore(**kwargs):
            raise RuntimeError("something went wrong")

        with patch("app.sync.backup.restore_from_backup", new=error_restore):
            with pytest.raises(HTTPException) as exc_info:
                await restore_backup_endpoint(
                    request=RestoreRequest(backup_id="backup-20240101-120000-daily"),
                    admin=admin,
                )

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_dry_run_restore_returns_planned_actions(self, test_db):
        from app.api.backup import RestoreRequest, restore_backup_endpoint

        admin = _admin_user(await _insert_admin())
        restore_result = {
            "backup_id": "backup-20240101-120000-daily",
            "dry_run": True,
            "users_restored": [],
            "db_restored": False,
            "events_deleted": 0,
            "events_created": 0,
            "events_updated": 0,
            "errors": [],
            "planned_actions": [{"action": "create", "event_id": "evt1", "summary": "Meeting"}],
        }

        async def fake_restore(**kwargs):
            return restore_result

        with patch("app.sync.backup.restore_from_backup", new=fake_restore):
            result = await restore_backup_endpoint(
                request=RestoreRequest(backup_id="backup-20240101-120000-daily", dry_run=True),
                admin=admin,
            )

        assert result.dry_run is True
        assert len(result.planned_actions) == 1
