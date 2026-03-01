"""
One-time helper: runs the Google OAuth installed-app flow for each of the
three test accounts and saves their refresh tokens to a JSON file.

Usage:
    python -m e2e.auth.get_calendar_tokens

Prerequisites:
  - Place a Google OAuth client_secret.json in e2e/auth/ (same project that
    BusyBridge uses, or a separate "Desktop" client).
  - The client must have the Calendar API enabled.

For each account the script will:
  1. Open a browser tab for the Google consent screen.
  2. You log in with the correct account and grant calendar access.
  3. The refresh token is captured and stored.

Tokens are saved to e2e/auth/.calendar_tokens.json.
"""

import json
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from e2e.config import ALL_EMAILS, CALENDAR_TOKENS_FILE, GOOGLE_CLIENT_SECRETS_FILE, SCOPES


def authorize_account(email: str) -> dict:
    """Run OAuth flow for one account, return credentials dict."""
    print(f"\n{'='*60}")
    print(f"  Authorizing: {email}")
    print(f"  A browser window will open. Sign in with THIS account.")
    print(f"{'='*60}\n")

    flow = InstalledAppFlow.from_client_secrets_file(
        GOOGLE_CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri="http://localhost:8085",
    )

    # Force consent to ensure we always get a refresh_token
    credentials = flow.run_local_server(
        port=8085,
        prompt="consent",
        login_hint=email,
        access_type="offline",
    )

    if not credentials.refresh_token:
        raise RuntimeError(
            f"No refresh token received for {email}. "
            "Revoke access at https://myaccount.google.com/permissions and retry."
        )

    return {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes or SCOPES),
    }


def main():
    print("Google Calendar token setup for E2E tests")
    print(f"Client secrets: {GOOGLE_CLIENT_SECRETS_FILE}\n")

    if not Path(GOOGLE_CLIENT_SECRETS_FILE).exists():
        print(f"ERROR: {GOOGLE_CLIENT_SECRETS_FILE} not found.")
        print("Download it from the Google Cloud Console (APIs & Services > Credentials).")
        sys.exit(1)

    # Load existing tokens if any
    tokens = {}
    if CALENDAR_TOKENS_FILE.exists():
        tokens = json.loads(CALENDAR_TOKENS_FILE.read_text())
        print(f"Loaded existing tokens for: {list(tokens.keys())}")

    for email in ALL_EMAILS:
        if email in tokens:
            answer = input(f"\nToken for {email} already exists. Re-authorize? [y/N] ").strip()
            if answer.lower() != "y":
                continue

        tokens[email] = authorize_account(email)
        # Save after each account in case something fails later
        CALENDAR_TOKENS_FILE.write_text(json.dumps(tokens, indent=2))
        print(f"Saved token for {email}")

    print(f"\nAll tokens saved to {CALENDAR_TOKENS_FILE}")
    print("These tokens do not expire as long as the refresh_token remains valid.")


if __name__ == "__main__":
    main()
