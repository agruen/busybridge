"""Session management using JWT tokens."""

import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from jose import JWTError, jwt
from pydantic import BaseModel

from app.config import get_session_secret, get_settings
from app.database import get_database

logger = logging.getLogger(__name__)

ALGORITHM = "HS256"
SESSION_COOKIE_NAME = "session"


class SessionData(BaseModel):
    """Session data stored in JWT."""
    user_id: int
    email: str
    is_admin: bool = False
    exp: datetime


class User(BaseModel):
    """User model for authenticated requests."""
    id: int
    email: str
    google_user_id: str
    display_name: Optional[str] = None
    main_calendar_id: Optional[str] = None
    is_admin: bool = False
    created_at: Optional[datetime] = None
    last_login_at: Optional[datetime] = None


def create_session_token(user_id: int, email: str, is_admin: bool = False) -> str:
    """Create a JWT session token."""
    settings = get_settings()
    secret = get_session_secret()

    expire = datetime.utcnow() + timedelta(days=settings.session_expire_days)
    data = {
        "user_id": user_id,
        "email": email,
        "is_admin": is_admin,
        "exp": expire,
    }

    return jwt.encode(data, secret, algorithm=ALGORITHM)


def verify_session_token(token: str) -> Optional[SessionData]:
    """Verify and decode a session token."""
    try:
        secret = get_session_secret()
        payload = jwt.decode(token, secret, algorithms=[ALGORITHM])
        return SessionData(**payload)
    except JWTError as e:
        logger.warning(f"Invalid session token: {e}")
        return None


async def get_user_by_id(user_id: int) -> Optional[User]:
    """Get user from database by ID."""
    db = await get_database()
    cursor = await db.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    )
    row = await cursor.fetchone()

    if row:
        return User(
            id=row["id"],
            email=row["email"],
            google_user_id=row["google_user_id"],
            display_name=row["display_name"],
            main_calendar_id=row["main_calendar_id"],
            is_admin=bool(row["is_admin"]),
            created_at=row["created_at"],
            last_login_at=row["last_login_at"],
        )
    return None


async def get_current_user_optional(request: Request) -> Optional[User]:
    """Get current user from session, returns None if not authenticated."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None

    session = verify_session_token(token)
    if not session:
        return None

    user = await get_user_by_id(session.user_id)
    return user


async def get_current_user(request: Request) -> User:
    """Get current user from session, raises 401 if not authenticated."""
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def require_admin(user: User = Depends(get_current_user)) -> User:
    """Require admin privileges."""
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return user


async def create_or_update_user(
    email: str,
    google_user_id: str,
    display_name: Optional[str] = None,
    is_admin: bool = False
) -> User:
    """Create or update a user in the database."""
    db = await get_database()
    now = datetime.utcnow().isoformat()

    # Check if user exists
    cursor = await db.execute(
        "SELECT id FROM users WHERE google_user_id = ?", (google_user_id,)
    )
    existing = await cursor.fetchone()

    if existing:
        # Update existing user
        await db.execute(
            """UPDATE users SET
               email = ?, display_name = ?, last_login_at = ?
               WHERE id = ?""",
            (email, display_name, now, existing["id"])
        )
        await db.commit()
        return await get_user_by_id(existing["id"])
    else:
        # Create new user
        cursor = await db.execute(
            """INSERT INTO users
               (email, google_user_id, display_name, is_admin, last_login_at)
               VALUES (?, ?, ?, ?, ?)
               RETURNING id""",
            (email, google_user_id, display_name, is_admin, now)
        )
        row = await cursor.fetchone()
        await db.commit()
        return await get_user_by_id(row["id"])


async def update_user_last_login(user_id: int) -> None:
    """Update user's last login timestamp."""
    db = await get_database()
    await db.execute(
        "UPDATE users SET last_login_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), user_id)
    )
    await db.commit()
