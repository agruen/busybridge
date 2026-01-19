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

# Store OAuth states temporarily (in production, use Redis or similar)
_oauth_states: dict[str, dict] = {}


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
    _oauth_states[state] = {"type": "login", "next": next or "/app"}

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

    if not state or state not in _oauth_states:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid state parameter"
        )

    state_data = _oauth_states.pop(state)
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
async def connect_client(request: Request, user: User = None):
    """Initiate OAuth for connecting a client calendar."""
    if user is None:
        from fastapi import Depends
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
    _oauth_states[state] = {"type": "client", "user_id": user.id}

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

    if not state or state not in _oauth_states:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid state parameter"
        )

    state_data = _oauth_states.pop(state)

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
