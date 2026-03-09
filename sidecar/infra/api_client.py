"""HTTP client wrapper for the BusyBridge API."""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)


class APIClient:
    """Authenticated HTTP client for the BusyBridge REST API."""

    def __init__(self, base_url: str, session_token: str):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            cookies={"session": session_token},
            timeout=60.0,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def health(self) -> dict:
        resp = await self._client.get("/health")
        resp.raise_for_status()
        return resp.json()

    # ── Sync triggers ─────────────────────────────────────────────

    async def trigger_user_sync(self, user_id: int) -> dict:
        resp = await self._client.post(f"/api/admin/users/{user_id}/sync")
        resp.raise_for_status()
        return resp.json()

    async def trigger_calendar_sync(self, calendar_id: int) -> dict:
        resp = await self._client.post(
            f"/api/client-calendars/{calendar_id}/sync"
        )
        resp.raise_for_status()
        return resp.json()

    async def trigger_full_sync(self) -> dict:
        resp = await self._client.post("/api/sync/full")
        resp.raise_for_status()
        return resp.json()

    async def cleanup_managed(self) -> dict:
        resp = await self._client.post("/api/sync/cleanup-managed")
        resp.raise_for_status()
        return resp.json()

    async def cleanup_and_pause(self) -> dict:
        resp = await self._client.post("/api/sync/cleanup-and-pause")
        resp.raise_for_status()
        return resp.json()

    # ── Sync control ──────────────────────────────────────────────

    async def pause_sync(self) -> dict:
        resp = await self._client.post("/api/admin/sync/pause")
        resp.raise_for_status()
        return resp.json()

    async def resume_sync(self) -> dict:
        resp = await self._client.post("/api/admin/sync/resume")
        resp.raise_for_status()
        return resp.json()

    # ── Generic ───────────────────────────────────────────────────

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        resp = await self._client.get(path, **kwargs)
        resp.raise_for_status()
        return resp

    async def post(
        self, path: str, json: Optional[dict] = None, **kwargs: Any
    ) -> httpx.Response:
        resp = await self._client.post(path, json=json, **kwargs)
        resp.raise_for_status()
        return resp
