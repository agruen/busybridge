"""Additional coverage tests for auth.session branches."""

from __future__ import annotations

import pytest
from starlette.requests import Request

from app.auth.session import (
    SESSION_COOKIE_NAME,
    User,
    create_session_token,
    get_current_user,
    get_user_by_id,
    require_admin,
)
from app.database import get_database


def _request_with_cookie(token: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_get_user_by_id_missing_returns_none(test_db):
    """Missing user lookup should return None."""
    assert await get_user_by_id(999999) is None


@pytest.mark.asyncio
async def test_get_current_user_success_and_require_admin_success(test_db):
    """Current-user resolver and admin guard should return user objects on success."""
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin)
           VALUES ('session-ok@example.com', 'session-ok-google', 'Session User', FALSE)
           RETURNING id"""
    )
    user_id = (await cursor.fetchone())["id"]
    await db.commit()

    token = create_session_token(user_id=user_id, email="session-ok@example.com", is_admin=False)
    resolved = await get_current_user(_request_with_cookie(token))
    assert resolved.id == user_id

    admin_user = User(
        id=1,
        email="admin@example.com",
        google_user_id="admin-google",
        display_name="Admin",
        main_calendar_id="main",
        is_admin=True,
    )
    assert await require_admin(admin_user) is admin_user
