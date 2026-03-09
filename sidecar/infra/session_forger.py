"""Forge JWT session cookies for authenticating with the BusyBridge API."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

from jose import jwt

logger = logging.getLogger(__name__)

ALGORITHM = "HS256"


def derive_session_secret(encryption_key: bytes) -> str:
    """Derive the session secret the same way the app does."""
    return hashlib.sha256(encryption_key + b"session_secret").hexdigest()


def forge_session_token(
    user_id: int,
    email: str,
    session_secret: str,
    *,
    is_admin: bool = True,
    expire_days: int = 7,
) -> str:
    """Create a valid JWT session token."""
    payload = {
        "user_id": user_id,
        "email": email,
        "is_admin": is_admin,
        "exp": datetime.now(timezone.utc) + timedelta(days=expire_days),
    }
    token = jwt.encode(payload, session_secret, algorithm=ALGORITHM)
    logger.info("Forged session token for user %d (%s)", user_id, email)
    return token
