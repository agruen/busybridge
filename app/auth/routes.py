"""Authentication routes."""

import logging
import secrets
from typing import Optional
from urllib.parse import urljoin

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from fastapi.responses import RedirectResponse

from app.auth.google import (
    HOME_SCOPES,
    CLIENT_SCOPES,
    build_auth_url,
    exchange_code_for_tokens,
    get_oauth_credentials,
    get_user_info,
    store_oauth_tokens,
)
from app.auth.session import (
    SESSION_COOKIE_NAME,
    create_or_update_user,
    create_session_token,
    get_current_user,
    update_user_last_login,
    User,
)
from app.config import get_settings
from app.database import get_database, get_organization, is_oobe_completed

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


async def store_oauth_state(state: str, state_type: str, user_id: Optional[int] = None, next_url: Optional[str] = None, ttl_minutes: int = 10) -> None:
    """Store OAuth state in database with TTL."""
    from datetime import datetime, timedelta
    db = await get_database()
    expires_at = (datetime.utcnow() + timedelta(minutes=ttl_minutes)).isoformat()
    await db.execute(
        """INSERT INTO oauth_states (state, state_type, user_id, next_url, expires_at)
           VALUES (?, ?, ?, ?, ?)""",
        (state, state_type, user_id, next_url, expires_at)
    )
    await db.commit()


async def get_oauth_state(state: str) -> Optional[dict]:
    """Retrieve and delete OAuth state from database."""
    from datetime import datetime
    db = await get_database()

    # Get state if not expired
    cursor = await db.execute(
        """SELECT state_type, user_id, next_url FROM oauth_states
           WHERE state = ? AND expires_at > ?""",
        (state, datetime.utcnow().isoformat())
    )
    row = await cursor.fetchone()

    if row:
        # Delete the state (one-time use)
        await db.execute("DELETE FROM oauth_states WHERE state = ?", (state,))
        await db.commit()
        return {
            "type": row[0],
            "user_id": row[1],
            "next": row[2]
        }
    return None


async def cleanup_expired_oauth_states() -> None:
    """Clean up expired OAuth states."""
    from datetime import datetime
    db = await get_database()
    await db.execute(
        "DELETE FROM oauth_states WHERE expires_at < ?",
        (datetime.utcnow().isoformat(),)
    )
    await db.commit()


def get_redirect_uri(request: Request, path: str) -> str:
    """Build absolute redirect URI."""
    settings = get_settings()
    return urljoin(settings.public_url, path)


@router.get("/login")
async def login(request: Request, next: Optional[str] = None):
    """Initiate Google OAuth login for home org."""
    if not await is_oobe_completed():
        return RedirectResponse(url="/setup", status_code=status.HTTP_302_FOUND)

    try:
        client_id, _ = await get_oauth_credentials()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="OAuth credentials not configured"
        )

    # Generate state token
    state = secrets.token_urlsafe(32)
    await store_oauth_state(state, "login", next_url=next or "/app")
    await cleanup_expired_oauth_states()  # Clean up old states

    redirect_uri = get_redirect_uri(request, "/auth/callback")
    auth_url = build_auth_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=HOME_SCOPES,
        state=state,
        prompt="select_account"
    )

    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@router.get("/callback")
async def oauth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None
):
    """Handle OAuth callback."""
    if error:
        logger.error(f"OAuth error: {error} - {error_description}")
        return RedirectResponse(
            url=f"/app/login?error={error}",
            status_code=status.HTTP_302_FOUND
        )

    state_data = await get_oauth_state(state)
    if not state_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired state parameter"
        )

    redirect_uri = get_redirect_uri(request, "/auth/callback")

    try:
        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(code, redirect_uri)
        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token", "")

        # Get user info
        user_info = await get_user_info(access_token)
        email = user_info["email"]
        google_user_id = user_info["id"]
        display_name = user_info.get("name", email.split("@")[0])

        # Verify domain
        org = await get_organization()
        if org:
            domain = email.split("@")[1]
            if domain != org["google_workspace_domain"]:
                logger.warning(f"Login attempt from wrong domain: {email}")
                return RedirectResponse(
                    url=f"/app/login?error=domain_mismatch&domain={org['google_workspace_domain']}",
                    status_code=status.HTTP_302_FOUND
                )

        # Create or update user
        user = await create_or_update_user(
            email=email,
            google_user_id=google_user_id,
            display_name=display_name
        )

        # Store tokens
        await store_oauth_tokens(
            user_id=user.id,
            account_type="home",
            email=email,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=tokens.get("expires_in")
        )

        # Initialize main calendar if not set
        if not user.main_calendar_id:
            try:
                from app.sync.google_calendar import GoogleCalendarClient
                from app.config import get_settings
                client_settings = get_settings()
                cal_client = GoogleCalendarClient(access_token, client_settings)
                calendars = cal_client.list_calendars()
                # Find the primary calendar
                primary_cal = next((c for c in calendars if c.get("primary")), None)
                if primary_cal:
                    db = await get_database()
                    await db.execute(
                        "UPDATE users SET main_calendar_id = ? WHERE id = ?",
                        (primary_cal["id"], user.id)
                    )
                    await db.commit()
                    logger.info(f"Set main calendar for user {user.id}: {primary_cal['id']}")
            except Exception as e:
                logger.warning(f"Could not initialize main calendar for user {user.id}: {e}")

        # Update last login
        await update_user_last_login(user.id)

        # Create session
        session_token = create_session_token(
            user_id=user.id,
            email=user.email,
            is_admin=user.is_admin
        )

        # Set cookie and redirect
        next_url = state_data.get("next", "/app")
        response = RedirectResponse(url=next_url, status_code=status.HTTP_302_FOUND)
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_token,
            httponly=True,
            secure=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 7  # 7 days
        )

        return response

    except Exception as e:
        logger.exception(f"OAuth callback error: {e}")
        return RedirectResponse(
            url=f"/app/login?error=callback_failed",
            status_code=status.HTTP_302_FOUND
        )


@router.get("/connect-client")
async def connect_client(request: Request):
    """Initiate OAuth for connecting a client calendar."""
    user = await get_current_user(request)

    try:
        client_id, _ = await get_oauth_credentials()
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="OAuth credentials not configured"
        )

    # Generate state token
    state = secrets.token_urlsafe(32)
    await store_oauth_state(state, "client", user_id=user.id)
    await cleanup_expired_oauth_states()  # Clean up old states

    redirect_uri = get_redirect_uri(request, "/auth/connect-client/callback")
    auth_url = build_auth_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=CLIENT_SCOPES,
        state=state,
        prompt="select_account consent"
    )

    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@router.get("/connect-client/callback")
async def connect_client_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None
):
    """Handle OAuth callback for client calendar connection."""
    if error:
        logger.error(f"Client OAuth error: {error} - {error_description}")
        return RedirectResponse(
            url=f"/app?error=client_connect_failed&reason={error}",
            status_code=status.HTTP_302_FOUND
        )

    state_data = await get_oauth_state(state)
    if not state_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired state parameter"
        )

    if state_data.get("type") != "client":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid state type"
        )

    user_id = state_data["user_id"]
    redirect_uri = get_redirect_uri(request, "/auth/connect-client/callback")

    try:
        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(code, redirect_uri)
        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token", "")

        if not refresh_token:
            logger.warning("No refresh token received for client account")
            return RedirectResponse(
                url="/app?error=no_refresh_token",
                status_code=status.HTTP_302_FOUND
            )

        # Get user info for the client account
        user_info = await get_user_info(access_token)
        client_email = user_info["email"]

        # Store tokens
        token_id = await store_oauth_tokens(
            user_id=user_id,
            account_type="client",
            email=client_email,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=tokens.get("expires_in")
        )

        # Redirect to calendar selection
        return RedirectResponse(
            url=f"/app/calendars/select?token_id={token_id}&email={client_email}",
            status_code=status.HTTP_302_FOUND
        )

    except Exception as e:
        logger.exception(f"Client OAuth callback error: {e}")
        return RedirectResponse(
            url="/app?error=client_callback_failed",
            status_code=status.HTTP_302_FOUND
        )


@router.post("/logout")
async def logout(response: Response):
    """Log out the current user."""
    response = RedirectResponse(url="/app/login", status_code=status.HTTP_302_FOUND)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@router.get("/logout")
async def logout_get(response: Response):
    """Log out the current user (GET for convenience)."""
    return await logout(response)
