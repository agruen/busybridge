"""Backup and restore API endpoints."""

import logging
import os
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.auth.session import require_admin, User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/backup", tags=["backup"])

_SAFE_ID_RE = re.compile(r'^[a-zA-Z0-9._-]+$')


def _validate_backup_id(backup_id: str) -> None:
    """Reject backup IDs with path traversal characters."""
    if not _SAFE_ID_RE.match(backup_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Invalid backup ID")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class BackupMeta(BaseModel):
    backup_id: str
    backup_type: Optional[str] = None
    created_at: Optional[str] = None
    user_ids_snapshotted: Optional[list[int]] = None
    total_events_snapshotted: Optional[int] = None
    file_size_bytes: Optional[int] = None
    snapshot_errors: Optional[list[str]] = None


class CreateBackupRequest(BaseModel):
    """Optionally limit which users are included in the calendar snapshot."""
    user_ids: Optional[list[int]] = None


class RestoreRequest(BaseModel):
    backup_id: str
    user_ids: Optional[list[int]] = None   # None = all users in the backup
    restore_db: bool = True
    restore_calendars: bool = True
    dry_run: bool = False


class RestoreResponse(BaseModel):
    backup_id: str
    dry_run: bool
    users_restored: list[int]
    db_restored: bool
    events_deleted: int
    events_created: int
    events_updated: int
    errors: list[str]
    planned_actions: Optional[list[dict]] = None


# ---------------------------------------------------------------------------
# ICS export endpoints (must be before {backup_id} catch-all routes)
# ---------------------------------------------------------------------------

@router.get("/ics", response_model=list[dict])
async def list_ics_backups_endpoint(admin: User = Depends(require_admin)):
    """List all ICS backup pairs, newest first."""
    from app.sync.ics_export import list_ics_backups
    return list_ics_backups()


@router.post("/ics")
async def create_ics_backup_endpoint(admin: User = Depends(require_admin)):
    """Create an ICS backup on demand."""
    from app.sync.ics_export import create_ics_backup
    try:
        metadata = await create_ics_backup()
        return metadata
    except Exception as e:
        logger.error(f"ICS backup creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"ICS backup failed: {e}",
        )


@router.get("/ics/{backup_id}/download")
async def download_ics_backup(
    backup_id: str,
    admin: User = Depends(require_admin),
):
    """Download an ICS backup ZIP (full or clean)."""
    _validate_backup_id(backup_id)
    from app.sync.ics_export import get_ics_backup_dir
    path = os.path.join(get_ics_backup_dir(), f"{backup_id}.zip")
    if not os.path.exists(path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="ICS backup not found")
    return FileResponse(
        path=path,
        filename=f"{backup_id}.zip",
        media_type="application/zip",
    )


@router.delete("/ics/{timestamp}")
async def delete_ics_backup_endpoint(
    timestamp: str,
    admin: User = Depends(require_admin),
):
    """Delete an ICS backup pair by timestamp."""
    _validate_backup_id(timestamp)
    from app.sync.ics_export import delete_ics_backup
    deleted = delete_ics_backup(timestamp)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="ICS backup not found")
    return {"status": "ok", "timestamp": timestamp}


# ---------------------------------------------------------------------------
# Core backup endpoints
# ---------------------------------------------------------------------------

@router.get("", response_model=list[BackupMeta])
async def list_backups_endpoint(admin: User = Depends(require_admin)):
    """List all available backups, newest first."""
    from app.sync.backup import list_backups
    return list_backups()


@router.post("", response_model=BackupMeta)
async def create_backup_endpoint(
    request: CreateBackupRequest = CreateBackupRequest(),
    admin: User = Depends(require_admin),
):
    """Create a new backup immediately (on-demand)."""
    from app.sync.backup import create_backup
    try:
        metadata = await create_backup(user_ids=request.user_ids)
        return metadata
    except Exception as e:
        logger.error(f"Backup creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backup failed: {e}",
        )


@router.post("/restore", response_model=RestoreResponse)
async def restore_backup_endpoint(
    request: RestoreRequest,
    admin: User = Depends(require_admin),
):
    """Restore from a backup.

    Pass dry_run=true to preview what would change without applying anything.
    Pass user_ids to restore only specific users; omit to restore all users
    in the backup.
    """
    _validate_backup_id(request.backup_id)
    from app.sync.backup import restore_from_backup
    try:
        result = await restore_from_backup(
            backup_id=request.backup_id,
            user_ids=request.user_ids,
            restore_db=request.restore_db,
            restore_calendars=request.restore_calendars,
            dry_run=request.dry_run,
        )
        return RestoreResponse(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Restore failed: {e}",
        )


@router.get("/{backup_id}/download")
async def download_backup(
    backup_id: str,
    admin: User = Depends(require_admin),
):
    """Download a backup ZIP file."""
    _validate_backup_id(backup_id)
    from app.sync.backup import get_backup_dir
    path = os.path.join(get_backup_dir(), f"{backup_id}.zip")
    if not os.path.exists(path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Backup not found")
    return FileResponse(
        path=path,
        filename=f"{backup_id}.zip",
        media_type="application/zip",
    )


@router.delete("/{backup_id}")
async def delete_backup_endpoint(
    backup_id: str,
    admin: User = Depends(require_admin),
):
    """Delete a backup by ID."""
    _validate_backup_id(backup_id)
    from app.sync.backup import delete_backup
    deleted = delete_backup(backup_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Backup not found")
    return {"status": "ok", "backup_id": backup_id}
