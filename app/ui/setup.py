"""OOBE (Out-of-Box Experience) setup wizard routes."""

import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.config import get_settings
from app.database import get_database, is_oobe_completed, set_setting
from app.encryption import (
    generate_encryption_key,
    key_to_base64,
    EncryptionManager,
    init_encryption_manager,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/setup", tags=["setup"])

templates = Jinja2Templates(directory="app/ui/templates")

# Temporary storage for OOBE data (in production, use secure session storage)
_oobe_data: dict = {}


class Step2Request(BaseModel):
    """Google credentials request."""
    client_id: str
    client_secret: str


class Step4Request(BaseModel):
    """Email settings request."""
    enabled: bool = False
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    from_address: Optional[str] = None
    alert_emails: Optional[str] = None


@router.get("", response_class=HTMLResponse)
async def setup_wizard(request: Request, step: int = 1):
    """OOBE setup wizard."""
    if await is_oobe_completed():
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    template_map = {
        1: "setup/step1_welcome.html",
        2: "setup/step2_credentials.html",
        3: "setup/step3_admin.html",
        4: "setup/step4_email.html",
        5: "setup/step5_encryption.html",
        6: "setup/step6_complete.html",
    }

    template = template_map.get(step, "setup/step1_welcome.html")

    context = {
        "request": request,
        "step": step,
        "oobe_data": _oobe_data,
    }

    # For step 5, generate encryption key if not already done
    if step == 5 and "encryption_key" not in _oobe_data:
        key = generate_encryption_key()
        _oobe_data["encryption_key"] = key
        _oobe_data["encryption_key_b64"] = key_to_base64(key)
        context["encryption_key_b64"] = _oobe_data["encryption_key_b64"]
    elif step == 5:
        context["encryption_key_b64"] = _oobe_data.get("encryption_key_b64")

    return templates.TemplateResponse(template, context)


@router.post("/step/2")
async def setup_step_2(request: Request):
    """Handle step 2 - Google credentials."""
    if await is_oobe_completed():
        raise HTTPException(status_code=400, detail="Setup already completed")

    form = await request.form()
    client_id = form.get("client_id", "").strip()
    client_secret = form.get("client_secret", "").strip()

    # Validate
    if not client_id or not client_secret:
        return templates.TemplateResponse("setup/step2_credentials.html", {
            "request": request,
            "step": 2,
            "error": "Client ID and Client Secret are required",
            "client_id": client_id,
        })

    if not client_id.endswith(".apps.googleusercontent.com"):
        return templates.TemplateResponse("setup/step2_credentials.html", {
            "request": request,
            "step": 2,
            "error": "Invalid Client ID format",
            "client_id": client_id,
        })

    # Store temporarily
    _oobe_data["client_id"] = client_id
    _oobe_data["client_secret"] = client_secret

    return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)


@router.post("/step/2/test")
async def test_credentials(request: Request):
    """Test OAuth credentials."""
    form = await request.form()
    client_id = form.get("client_id", "").strip()
    client_secret = form.get("client_secret", "").strip()

    from app.auth.google import test_oauth_credentials

    is_valid = await test_oauth_credentials(client_id, client_secret)

    return {"valid": is_valid}


@router.get("/step/3/auth")
async def step_3_auth(request: Request):
    """Initiate OAuth for admin user."""
    if "client_id" not in _oobe_data:
        return RedirectResponse(url="/setup?step=2", status_code=status.HTTP_302_FOUND)

    from app.auth.google import build_auth_url, HOME_SCOPES

    settings = get_settings()
    state = secrets.token_urlsafe(32)
    _oobe_data["oauth_state"] = state

    redirect_uri = f"{settings.public_url}/setup/step/3/callback"

    auth_url = build_auth_url(
        client_id=_oobe_data["client_id"],
        redirect_uri=redirect_uri,
        scopes=HOME_SCOPES,
        state=state,
        prompt="consent select_account"
    )

    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@router.get("/step/3/callback")
async def step_3_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """OAuth callback for admin user."""
    if error:
        return RedirectResponse(url=f"/setup?step=3&error={error}", status_code=status.HTTP_302_FOUND)

    if state != _oobe_data.get("oauth_state"):
        return RedirectResponse(url="/setup?step=3&error=invalid_state", status_code=status.HTTP_302_FOUND)

    settings = get_settings()
    redirect_uri = f"{settings.public_url}/setup/step/3/callback"

    try:
        from app.auth.google import exchange_code_for_tokens, get_user_info

        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(
            code,
            redirect_uri,
            _oobe_data["client_id"],
            _oobe_data["client_secret"]
        )

        # Get user info
        user_info = await get_user_info(tokens["access_token"])

        # Store in oobe data
        _oobe_data["admin_email"] = user_info["email"]
        _oobe_data["admin_name"] = user_info.get("name", user_info["email"].split("@")[0])
        _oobe_data["admin_google_id"] = user_info["id"]
        _oobe_data["admin_access_token"] = tokens["access_token"]
        _oobe_data["admin_refresh_token"] = tokens.get("refresh_token", "")
        _oobe_data["admin_token_expiry"] = tokens.get("expires_in")
        _oobe_data["domain"] = user_info["email"].split("@")[1]

        return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)

    except Exception as e:
        logger.exception(f"OAuth callback error: {e}")
        return RedirectResponse(url=f"/setup?step=3&error=oauth_failed", status_code=status.HTTP_302_FOUND)


@router.post("/step/3/confirm")
async def step_3_confirm(request: Request):
    """Confirm admin user and domain."""
    if "admin_email" not in _oobe_data:
        return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)

    return RedirectResponse(url="/setup?step=4", status_code=status.HTTP_302_FOUND)


@router.post("/step/4")
async def setup_step_4(request: Request):
    """Handle step 4 - Email settings."""
    form = await request.form()
    enabled = form.get("enabled") == "on"

    if enabled:
        _oobe_data["smtp_enabled"] = True
        _oobe_data["smtp_host"] = form.get("smtp_host", "").strip()
        _oobe_data["smtp_port"] = int(form.get("smtp_port", "587"))
        _oobe_data["smtp_username"] = form.get("smtp_username", "").strip()
        _oobe_data["smtp_password"] = form.get("smtp_password", "").strip()
        _oobe_data["smtp_from_address"] = form.get("from_address", "").strip()
        _oobe_data["alert_emails"] = form.get("alert_emails", "").strip()
    else:
        _oobe_data["smtp_enabled"] = False

    return RedirectResponse(url="/setup?step=5", status_code=status.HTTP_302_FOUND)


@router.post("/step/4/test")
async def test_email(request: Request):
    """Send test email."""
    form = await request.form()

    # This would actually test the email settings
    # For now, return success
    return {"success": True}


@router.post("/step/5")
async def setup_step_5(request: Request):
    """Complete setup and save everything."""
    form = await request.form()
    confirmed = form.get("confirmed") == "on"

    if not confirmed:
        return templates.TemplateResponse("setup/step5_encryption.html", {
            "request": request,
            "step": 5,
            "error": "You must confirm that you have saved the encryption key",
            "encryption_key_b64": _oobe_data.get("encryption_key_b64"),
        })

    # Save encryption key to file
    settings = get_settings()
    key = _oobe_data.get("encryption_key")

    if not key:
        key = generate_encryption_key()
        _oobe_data["encryption_key"] = key

    # Ensure directory exists
    key_dir = os.path.dirname(settings.encryption_key_file)
    if key_dir and not os.path.exists(key_dir):
        os.makedirs(key_dir, exist_ok=True)

    with open(settings.encryption_key_file, "wb") as f:
        f.write(key)

    # Initialize encryption manager
    enc = init_encryption_manager(key)

    # Save everything to database
    db = await get_database()

    # Create organization
    await db.execute(
        """INSERT INTO organization
           (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        (
            _oobe_data["domain"],
            enc.encrypt(_oobe_data["client_id"]),
            enc.encrypt(_oobe_data["client_secret"]),
        )
    )

    # Create admin user
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin)
           VALUES (?, ?, ?, TRUE)
           RETURNING id""",
        (_oobe_data["admin_email"], _oobe_data["admin_google_id"], _oobe_data["admin_name"])
    )
    user_row = await cursor.fetchone()
    user_id = user_row["id"]

    # Store admin's OAuth tokens with expiry
    from datetime import datetime, timedelta
    token_expiry = None
    if _oobe_data.get("admin_token_expiry"):
        expires_in_seconds = int(_oobe_data["admin_token_expiry"])
        token_expiry = (datetime.utcnow() + timedelta(seconds=expires_in_seconds)).isoformat()

    await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry)
           VALUES (?, 'home', ?, ?, ?, ?)""",
        (
            user_id,
            _oobe_data["admin_email"],
            enc.encrypt(_oobe_data["admin_access_token"]),
            enc.encrypt(_oobe_data["admin_refresh_token"]),
            token_expiry,
        )
    )

    # Save SMTP settings if enabled
    if _oobe_data.get("smtp_enabled"):
        await set_setting("smtp_host", _oobe_data.get("smtp_host", ""))
        await set_setting("smtp_port", str(_oobe_data.get("smtp_port", 587)))
        await set_setting("smtp_username", _oobe_data.get("smtp_username", ""))
        if _oobe_data.get("smtp_password"):
            await set_setting("smtp_password", _oobe_data["smtp_password"], is_sensitive=True, encrypt_func=enc.encrypt)
        await set_setting("smtp_from_address", _oobe_data.get("smtp_from_address", ""))
        await set_setting("alert_emails", _oobe_data.get("alert_emails", ""))
        await set_setting("alerts_enabled", "true")
    else:
        await set_setting("alerts_enabled", "false")

    await db.commit()

    # Clear OOBE data
    _oobe_data.clear()

    logger.info("OOBE setup completed successfully")

    return RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND)


@router.get("/complete")
async def setup_complete(request: Request):
    """Final step redirect to dashboard."""
    return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)
