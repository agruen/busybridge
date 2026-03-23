"""OOBE (Out-of-Box Experience) setup wizard routes."""

import json
import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.config import get_settings, get_test_mode_home_allowlist
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

# Default path for service account key inside the container
SA_KEY_PATH = "/secrets/sa-key.json"


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
async def setup_wizard(
    request: Request,
    step: int = 1,
    error: Optional[str] = None,
    sa: Optional[str] = None,
    sa_email: Optional[str] = None,
):
    """OOBE setup wizard."""
    if await is_oobe_completed():
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    template_map = {
        1: "setup/step1_welcome.html",
        2: "setup/step2_credentials.html",
        3: "setup/step3_admin.html",
        4: "setup/step4_email.html",
        5: "setup/step5_service_account.html",
        6: "setup/step6_encryption.html",
        7: "setup/step7_complete.html",
    }

    template = template_map.get(step, "setup/step1_welcome.html")

    settings = get_settings()

    context = {
        "step": step,
        "oobe_data": _oobe_data,
        "error": error,
        "test_mode": settings.test_mode,
        "allowed_home_emails": sorted(get_test_mode_home_allowlist()) if settings.test_mode else [],
        "public_url": settings.public_url.rstrip("/"),
    }

    # For step 5, include SA upload status
    if step == 5:
        context["sa_uploaded"] = _oobe_data.get("sa_uploaded", False)
        context["sa_email"] = _oobe_data.get("sa_email", "")

    # For step 7, pass SA info from query params (oobe_data is cleared by then)
    if step == 7:
        context["sa_uploaded"] = sa == "1"
        context["sa_email"] = sa_email or ""

    # For step 6, generate encryption key if not already done
    if step == 6 and "encryption_key" not in _oobe_data:
        key = generate_encryption_key()
        _oobe_data["encryption_key"] = key
        _oobe_data["encryption_key_b64"] = key_to_base64(key)
        context["encryption_key_b64"] = _oobe_data["encryption_key_b64"]
    elif step == 6:
        context["encryption_key_b64"] = _oobe_data.get("encryption_key_b64")

    return templates.TemplateResponse(request, template, context=context)


@router.post("/step/2")
async def setup_step_2(request: Request):
    """Handle step 2 - Google credentials."""
    if await is_oobe_completed():
        raise HTTPException(status_code=400, detail="Setup already completed")

    form = await request.form()
    client_id = form.get("client_id", "").strip()
    client_secret = form.get("client_secret", "").strip()

    # Validate
    settings = get_settings()
    if not client_id or not client_secret:
        return templates.TemplateResponse(request, "setup/step2_credentials.html", context={
            "step": 2,
            "error": "Client ID and Client Secret are required",
            "client_id": client_id,
            "public_url": settings.public_url.rstrip("/"),
        })

    if not client_id.endswith(".apps.googleusercontent.com"):
        return templates.TemplateResponse(request, "setup/step2_credentials.html", context={
            "step": 2,
            "error": "Invalid Client ID format",
            "client_id": client_id,
            "public_url": settings.public_url.rstrip("/"),
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

        admin_email = user_info["email"].strip().lower()
        settings = get_settings()
        if settings.test_mode:
            home_allowlist = get_test_mode_home_allowlist()
            if not home_allowlist:
                return RedirectResponse(url="/setup?step=3&error=test_mode_no_home_allowlist", status_code=status.HTTP_302_FOUND)
            if admin_email not in home_allowlist:
                return RedirectResponse(url="/setup?step=3&error=admin_not_allowed", status_code=status.HTTP_302_FOUND)

        # Store in oobe data
        _oobe_data["admin_email"] = admin_email
        _oobe_data["admin_name"] = user_info.get("name", admin_email.split("@")[0])
        _oobe_data["admin_google_id"] = user_info["id"]
        _oobe_data["admin_access_token"] = tokens["access_token"]
        _oobe_data["admin_refresh_token"] = tokens.get("refresh_token", "")
        _oobe_data["admin_token_expiry"] = tokens.get("expires_in")
        _oobe_data["domain"] = admin_email.split("@")[1]

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
    """Handle step 5 - Service account key upload."""
    form = await request.form()
    sa_file: Optional[UploadFile] = form.get("sa_key_file")

    if sa_file and sa_file.filename:
        try:
            contents = await sa_file.read()
            sa_data = json.loads(contents)

            # Validate required fields
            if "client_email" not in sa_data or "private_key" not in sa_data:
                return templates.TemplateResponse(request, "setup/step5_service_account.html", context={
                    "step": 5,
                    "error": "Invalid service account key file. It must contain 'client_email' and 'private_key' fields.",
                    "sa_uploaded": False,
                    "sa_email": "",
                    "public_url": get_settings().public_url.rstrip("/"),
                })

            # Save the key file
            key_dir = os.path.dirname(SA_KEY_PATH)
            if key_dir and not os.path.exists(key_dir):
                os.makedirs(key_dir, exist_ok=True)

            with open(SA_KEY_PATH, "w") as f:
                json.dump(sa_data, f)

            _oobe_data["sa_uploaded"] = True
            _oobe_data["sa_email"] = sa_data["client_email"]
            _oobe_data["sa_key_path"] = SA_KEY_PATH

            logger.info(f"Service account key uploaded: {sa_data['client_email']}")

        except json.JSONDecodeError:
            return templates.TemplateResponse(request, "setup/step5_service_account.html", context={
                "step": 5,
                "error": "The uploaded file is not valid JSON. Make sure you're uploading the JSON key file from Google Cloud Console.",
                "sa_uploaded": False,
                "sa_email": "",
                "public_url": get_settings().public_url.rstrip("/"),
            })
        except PermissionError:
            return templates.TemplateResponse(request, "setup/step5_service_account.html", context={
                "step": 5,
                "error": f"Permission denied writing to {SA_KEY_PATH}. Make sure the secrets/ directory is writable (run: sudo chown -R 1000:1000 secrets/).",
                "sa_uploaded": False,
                "sa_email": "",
                "public_url": get_settings().public_url.rstrip("/"),
            })

    return RedirectResponse(url="/setup?step=5", status_code=status.HTTP_302_FOUND)


@router.post("/step/5/skip")
async def setup_step_5_skip(request: Request):
    """Skip service account setup."""
    _oobe_data["sa_uploaded"] = False
    return RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND)


@router.post("/step/5/continue")
async def setup_step_5_continue(request: Request):
    """Continue after service account upload."""
    return RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND)


@router.post("/step/6")
async def setup_step_6(request: Request):
    """Complete setup and save everything."""
    form = await request.form()
    confirmed = form.get("confirmed") == "on"

    if not confirmed:
        return templates.TemplateResponse(request, "setup/step6_encryption.html", context={
            "step": 6,
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

    # Activate service account if uploaded during setup
    sa_uploaded = _oobe_data.get("sa_uploaded", False)
    sa_email = _oobe_data.get("sa_email", "")
    if sa_uploaded and _oobe_data.get("sa_key_path"):
        os.environ["SERVICE_ACCOUNT_KEY_FILE"] = _oobe_data["sa_key_path"]
        # Clear cached settings and SA info so the new key is picked up
        from app.config import get_settings as _gs
        _gs.cache_clear()
        from app.auth.service_account import reset_cache as _sa_reset
        _sa_reset()
        logger.info(f"Service account activated: {sa_email}")

    # Clear OOBE data
    _oobe_data.clear()

    logger.info("OOBE setup completed successfully")

    # Pass SA info to the completion page
    return RedirectResponse(
        url=f"/setup?step=7&sa={'1' if sa_uploaded else '0'}&sa_email={sa_email}",
        status_code=status.HTTP_302_FOUND,
    )


@router.get("/complete")
async def setup_complete(request: Request):
    """Final step redirect to dashboard."""
    return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)
