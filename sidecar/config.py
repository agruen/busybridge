"""Sidecar configuration from environment variables."""

import os


class Config:
    APP_BASE_URL: str = os.environ.get("APP_BASE_URL", "http://calendar-sync:3000")
    DATABASE_PATH: str = os.environ.get("DATABASE_PATH", "/data/calendar-sync.db")
    ENCRYPTION_KEY_FILE: str = os.environ.get(
        "ENCRYPTION_KEY_FILE", "/secrets/encryption.key"
    )
    TEST_LOG_DIR: str = os.environ.get("TEST_LOG_DIR", "/data/test-logs")
    DASHBOARD_PORT: int = int(os.environ.get("DASHBOARD_PORT", "8100"))
    SOAK_MIN_DELAY: int = int(os.environ.get("SOAK_MIN_DELAY", "10"))
    SOAK_MAX_DELAY: int = int(os.environ.get("SOAK_MAX_DELAY", "300"))
    SYNC_WAIT_TIMEOUT: int = int(os.environ.get("SYNC_WAIT_TIMEOUT", "120"))
    POLL_INTERVAL: int = int(os.environ.get("POLL_INTERVAL", "3"))
