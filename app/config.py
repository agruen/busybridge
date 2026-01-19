"""Application configuration management."""

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_path: str = "/data/calendar-sync.db"

    # Encryption
    encryption_key_file: str = "/secrets/encryption.key"

    # Server
    public_url: str = "http://localhost:3000"
    log_level: str = "info"

    # Session
    session_secret_key: Optional[str] = None  # Derived from encryption key if not set
    session_expire_days: int = 7

    # Rate limiting
    rate_limit_per_minute: int = 60
    webhook_rate_limit_per_minute: int = 120

    # Sync settings
    sync_interval_minutes: int = 5
    webhook_renewal_hours: int = 6
    consistency_check_hours: int = 1
    token_refresh_minutes: int = 30
    alert_process_minutes: int = 1

    # Retention settings (days)
    event_retention_days: int = 30
    recurring_soft_delete_days: int = 30
    audit_log_retention_days: int = 90
    disconnected_calendar_retention_days: int = 30

    # Google Calendar
    calendar_sync_tag: str = "calendarSyncEngine"
    busy_block_title: str = "Busy"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def get_encryption_key() -> bytes:
    """Load encryption key from file."""
    settings = get_settings()
    key_file = settings.encryption_key_file

    if not os.path.exists(key_file):
        raise RuntimeError(
            f"Encryption key file not found at {key_file}. "
            "Complete the setup wizard first."
        )

    with open(key_file, "rb") as f:
        key = f.read().strip()

    if len(key) < 32:
        raise RuntimeError("Invalid encryption key: must be at least 32 bytes")

    return key


def get_session_secret() -> str:
    """Get session secret key, derived from encryption key if not set."""
    settings = get_settings()
    if settings.session_secret_key:
        return settings.session_secret_key

    try:
        key = get_encryption_key()
        import hashlib
        return hashlib.sha256(key + b"session_secret").hexdigest()
    except RuntimeError:
        # During OOBE, no encryption key exists yet
        # Use a temporary secret (sessions won't persist across restarts)
        return "temporary_oobe_secret_not_for_production"
