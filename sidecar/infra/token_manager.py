"""Decrypt OAuth tokens and build Google API credentials."""

from __future__ import annotations

import logging
import time
from typing import Optional

import httpx
from google.oauth2.credentials import Credentials

from sidecar.infra.db_reader import DBReader
from sidecar.infra.encryption import EncryptionManager

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPES = ["https://www.googleapis.com/auth/calendar"]


class TokenManager:
    """Manage OAuth tokens: decrypt from DB, refresh when expired."""

    def __init__(self, db: DBReader, encryption: EncryptionManager):
        self._db = db
        self._enc = encryption
        self._cache: dict[int, _CachedToken] = {}  # token_id -> cached
        self._client_id: Optional[str] = None
        self._client_secret: Optional[str] = None

    async def init(self) -> None:
        """Load Google client credentials from organization table."""
        org = await self._db.get_organization()
        if not org:
            raise RuntimeError("No organization found in database")
        self._client_id = self._enc.decrypt(org["google_client_id_encrypted"])
        self._client_secret = self._enc.decrypt(
            org["google_client_secret_encrypted"]
        )
        logger.info("Loaded Google client credentials from organization")

    async def get_credentials(self, token_id: int) -> Credentials:
        """Get valid Google credentials for a token ID."""
        cached = self._cache.get(token_id)
        if cached and not cached.is_expired:
            return cached.credentials

        tokens = await self._db.get_oauth_tokens()
        token_row = next((t for t in tokens if t["id"] == token_id), None)
        if not token_row:
            raise ValueError(f"Token ID {token_id} not found")

        access_token = self._enc.decrypt(token_row["access_token_encrypted"])
        refresh_token = self._enc.decrypt(token_row["refresh_token_encrypted"])

        # Check if expired — token_expiry is an ISO timestamp string in the DB
        expiry_str = token_row.get("token_expiry")
        if expiry_str:
            from datetime import datetime, timezone
            try:
                expiry_dt = datetime.fromisoformat(expiry_str)
                expiry = expiry_dt.timestamp()
            except (ValueError, TypeError):
                expiry = None
        else:
            expiry = None
        needs_refresh = not expiry or expiry < time.time()

        if needs_refresh and refresh_token:
            access_token, new_expiry = await self._refresh_token(refresh_token)
            expiry = new_expiry
        elif needs_refresh:
            logger.warning("Token %d expired and no refresh token", token_id)

        creds = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=GOOGLE_TOKEN_URL,
            client_id=self._client_id,
            client_secret=self._client_secret,
            scopes=SCOPES,
        )

        self._cache[token_id] = _CachedToken(
            credentials=creds,
            expires_at=expiry or (time.time() + 3600),
        )
        return creds

    async def _refresh_token(
        self, refresh_token: str
    ) -> tuple[str, float]:
        """Refresh an access token via Google's token endpoint."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        new_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)
        new_expiry = time.time() + expires_in
        logger.info("Refreshed token (expires in %ds)", expires_in)
        return new_token, new_expiry

    def invalidate(self, token_id: int) -> None:
        """Remove a cached token so it's re-fetched next time."""
        self._cache.pop(token_id, None)


class _CachedToken:
    def __init__(self, credentials: Credentials, expires_at: float):
        self.credentials = credentials
        self.expires_at = expires_at

    @property
    def is_expired(self) -> bool:
        return time.time() >= (self.expires_at - 60)  # 60s buffer
