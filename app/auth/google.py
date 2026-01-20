"""Google OAuth helpers."""

import logging
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

import httpx
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from app.config import get_settings
from app.database import get_database
from app.encryption import encrypt_value, decrypt_value

logger = logging.getLogger(__name__)

# Google OAuth endpoints
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

# Required scopes
HOME_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar",
]

CLIENT_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar",
]


async def get_oauth_credentials() -> tuple[str, str]:
    """Get OAuth credentials from database."""
    from app.encryption import get_encryption_manager

    db = await get_database()
    cursor = await db.execute("SELECT * FROM organization LIMIT 1")
    org = await cursor.fetchone()

    if not org:
        raise ValueError("Organization not configured")

    enc = get_encryption_manager()
    client_id = enc.decrypt(org["google_client_id_encrypted"])
    client_secret = enc.decrypt(org["google_client_secret_encrypted"])

    return client_id, client_secret


def build_auth_url(
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    state: str,
    login_hint: Optional[str] = None,
    prompt: str = "consent"
) -> str:
    """Build Google OAuth authorization URL."""
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        "access_type": "offline",
        "state": state,
        "prompt": prompt,
    }

    if login_hint:
        params["login_hint"] = login_hint

    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


async def exchange_code_for_tokens(
    code: str,
    redirect_uri: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None
) -> dict:
    """Exchange authorization code for tokens."""
    if not client_id or not client_secret:
        client_id, client_secret = await get_oauth_credentials()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

        if response.status_code != 200:
            logger.error(f"Token exchange failed: {response.text}")
            raise ValueError(f"Token exchange failed: {response.text}")

        return response.json()


async def refresh_access_token(
    refresh_token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None
) -> dict:
    """Refresh an access token."""
    if not client_id or not client_secret:
        client_id, client_secret = await get_oauth_credentials()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
            },
        )

        if response.status_code != 200:
            logger.error(f"Token refresh failed: {response.text}")
            raise ValueError(f"Token refresh failed: {response.text}")

        return response.json()


async def get_user_info(access_token: str) -> dict:
    """Get user info from Google."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if response.status_code != 200:
            logger.error(f"Failed to get user info: {response.text}")
            raise ValueError(f"Failed to get user info: {response.text}")

        return response.json()


async def store_oauth_tokens(
    user_id: int,
    account_type: str,
    email: str,
    access_token: str,
    refresh_token: str,
    expires_in: Optional[int] = None
) -> int:
    """Store OAuth tokens in database."""
    db = await get_database()
    now = datetime.utcnow()

    expiry = None
    if expires_in:
        expiry = (now + timedelta(seconds=expires_in)).isoformat()

    access_encrypted = encrypt_value(access_token)
    refresh_encrypted = encrypt_value(refresh_token)

    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id, google_account_email) DO UPDATE SET
           access_token_encrypted = excluded.access_token_encrypted,
           refresh_token_encrypted = excluded.refresh_token_encrypted,
           token_expiry = excluded.token_expiry,
           updated_at = excluded.updated_at
           RETURNING id""",
        (user_id, account_type, email, access_encrypted, refresh_encrypted, expiry, now.isoformat())
    )
    row = await cursor.fetchone()
    await db.commit()

    return row["id"]


async def get_oauth_token(user_id: int, email: str) -> Optional[dict]:
    """Get OAuth token for a user's account."""
    db = await get_database()
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE user_id = ? AND google_account_email = ?""",
        (user_id, email)
    )
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


async def get_valid_access_token(user_id: int, email: str) -> str:
    """Get a valid access token, refreshing if needed."""
    token_data = await get_oauth_token(user_id, email)
    if not token_data:
        raise ValueError(f"No token found for user {user_id}, email {email}")

    access_token = decrypt_value(token_data["access_token_encrypted"])
    refresh_token = decrypt_value(token_data["refresh_token_encrypted"])

    # Check if token is expired or will expire soon
    expiry = token_data.get("token_expiry")
    if expiry:
        expiry_dt = datetime.fromisoformat(expiry)
        if datetime.utcnow() >= expiry_dt - timedelta(minutes=5):
            # Token expired or expiring soon, refresh it
            logger.info(f"Refreshing token for user {user_id}, email {email}")

            # Retry logic for transient network errors
            max_retries = 3
            last_error = None
            for attempt in range(max_retries):
                try:
                    new_tokens = await refresh_access_token(refresh_token)
                    access_token = new_tokens["access_token"]

                    # Store the new tokens
                    new_refresh = new_tokens.get("refresh_token", refresh_token)
                    await store_oauth_tokens(
                        user_id=user_id,
                        account_type=token_data["account_type"],
                        email=email,
                        access_token=access_token,
                        refresh_token=new_refresh,
                        expires_in=new_tokens.get("expires_in")
                    )
                    break  # Success, exit retry loop
                except ValueError as e:
                    # Permanent errors (invalid_grant, etc.) - don't retry
                    if "invalid_grant" in str(e).lower():
                        logger.error(f"Token refresh failed with permanent error: {e}")
                        raise
                    # Network/transient errors - retry
                    last_error = e
                    if attempt < max_retries - 1:
                        import asyncio
                        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(f"Token refresh attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Failed to refresh token after {max_retries} attempts: {e}")
                        raise

    return access_token


def get_calendar_service(credentials: Credentials):
    """Build Google Calendar API service."""
    return build("calendar", "v3", credentials=credentials)


async def get_calendar_service_for_user(user_id: int, email: str):
    """Get Calendar service for a user's account."""
    access_token = await get_valid_access_token(user_id, email)
    credentials = Credentials(token=access_token)
    return get_calendar_service(credentials)


async def test_oauth_credentials(client_id: str, client_secret: str) -> bool:
    """Test if OAuth credentials are valid by making a simple API call."""
    # We can't fully validate credentials without a token exchange
    # But we can at least check they're in the right format
    if not client_id or not client_secret:
        return False

    if not client_id.endswith(".apps.googleusercontent.com"):
        return False

    if len(client_secret) < 10:
        return False

    return True
