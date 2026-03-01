"""E2E test configuration."""

import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
E2E_DIR = Path(__file__).parent
AUTH_DIR = E2E_DIR / "auth"
AUTH_STATE_FILE = AUTH_DIR / ".auth_state.json"
CALENDAR_TOKENS_FILE = AUTH_DIR / ".calendar_tokens.json"

# ── BusyBridge instance ──────────────────────────────────────────────────────
BASE_URL = os.environ.get("E2E_BASE_URL", "https://busybridge.workingpaper.co")

# ── Test accounts ─────────────────────────────────────────────────────────────
# The "home" (main) account that logs into BusyBridge
MAIN_ACCOUNT_EMAIL = os.environ.get(
    "E2E_MAIN_EMAIL", "bbmaincalendar@gmail.com"
)

# Client accounts connected to BusyBridge
CLIENT1_EMAIL = os.environ.get("E2E_CLIENT1_EMAIL", "andrew.gruen@gmail.com")
CLIENT2_EMAIL = os.environ.get("E2E_CLIENT2_EMAIL", "agyttv@gmail.com")

# All accounts (for token iteration)
ALL_EMAILS = [MAIN_ACCOUNT_EMAIL, CLIENT1_EMAIL, CLIENT2_EMAIL]

# ── Timeouts (seconds) ───────────────────────────────────────────────────────
WEBHOOK_SYNC_TIMEOUT = 30      # Max wait for webhook-triggered sync
PERIODIC_SYNC_TIMEOUT = 120    # Max wait for periodic (cron) sync
POLL_INTERVAL = 2              # Seconds between API polls

# ── Test event naming ─────────────────────────────────────────────────────────
TEST_EVENT_PREFIX = "[E2E-TEST]"

# ── Google API ────────────────────────────────────────────────────────────────
# Path to the OAuth client credentials file used by the auth helper scripts.
# This is the same client_id / client_secret that BusyBridge itself uses.
GOOGLE_CLIENT_SECRETS_FILE = os.environ.get(
    "E2E_GOOGLE_CLIENT_SECRETS",
    str(E2E_DIR / "auth" / "client_secret.json"),
)

SCOPES = ["https://www.googleapis.com/auth/calendar"]
