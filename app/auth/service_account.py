"""Service account credential management for immovable main-calendar events."""

import json
import logging
from typing import Optional

from google.oauth2 import service_account as sa_module

from app.config import get_settings

logger = logging.getLogger(__name__)

_sa_info: Optional[dict] = None
_sa_info_loaded = False

CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar"


def _load_sa_info() -> Optional[dict]:
    """Load and cache the service account key JSON from disk."""
    global _sa_info, _sa_info_loaded

    if _sa_info_loaded:
        return _sa_info

    settings = get_settings()
    key_file = settings.service_account_key_file

    if not key_file:
        _sa_info_loaded = True
        return None

    try:
        with open(key_file, "r") as f:
            _sa_info = json.load(f)
        _sa_info_loaded = True
        logger.info("Service account key loaded from %s", key_file)
        return _sa_info
    except FileNotFoundError:
        logger.warning("Service account key file not found: %s", key_file)
        _sa_info_loaded = True
        return None
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load service account key from %s: %s", key_file, e)
        _sa_info_loaded = True
        return None


def is_sa_configured() -> bool:
    """Return True if a service account key file is configured and loadable."""
    return _load_sa_info() is not None


def get_sa_credentials() -> Optional[sa_module.Credentials]:
    """Return service account Credentials with calendar scope, or None."""
    info = _load_sa_info()
    if not info:
        return None

    return sa_module.Credentials.from_service_account_info(
        info, scopes=[CALENDAR_SCOPE]
    )


def get_sa_email() -> Optional[str]:
    """Return the service account email address, or None."""
    info = _load_sa_info()
    if not info:
        return None
    return info.get("client_email")


def get_sa_main_client(main_calendar_id: str):
    """Create an AsyncGoogleCalendarClient backed by SA credentials.

    Returns None if SA is not configured or access validation fails.
    The validation call is synchronous (blocking) but this is only called
    once per sync cycle, not in the hot path.
    """
    from app.sync.google_calendar import GoogleCalendarClient, AsyncGoogleCalendarClient

    creds = get_sa_credentials()
    if not creds:
        return None

    try:
        # Validate access with a sync client (one-time blocking call)
        sync_client = GoogleCalendarClient(credentials=creds)
        sync_client.get_calendar(main_calendar_id)
        # Return async wrapper for use in async contexts
        return AsyncGoogleCalendarClient(credentials=creds)
    except Exception as e:
        logger.warning(
            "Service account cannot access calendar %s: %s", main_calendar_id, e
        )
        return None


def reset_cache() -> None:
    """Clear the cached SA info (useful after config changes)."""
    global _sa_info, _sa_info_loaded
    _sa_info = None
    _sa_info_loaded = False
