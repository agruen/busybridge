"""
One-time helper: opens a Chromium browser so you can manually log in to
BusyBridge, then saves the session cookies/localStorage to a JSON file that
Playwright tests will re-use.

Usage:
    python -m e2e.auth.save_auth_state

After the browser opens:
  1. Log in via the Google OAuth flow on BusyBridge.
  2. Wait until you land on the /app dashboard.
  3. Close the browser (or press Ctrl-C in the terminal).

The session state is saved to e2e/auth/.auth_state.json and will be valid
for ~7 days (BusyBridge session TTL).
"""

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

# Add parent to path so we can import config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from e2e.config import AUTH_STATE_FILE, BASE_URL


def main():
    print(f"Opening browser to {BASE_URL}/app/login ...")
    print("Log in with the home account, then close the browser when done.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto(f"{BASE_URL}/app/login")

        # Wait until the user reaches the dashboard (proof of successful login)
        print("Waiting for you to log in and reach /app ...")
        try:
            page.wait_for_url(f"{BASE_URL}/app", timeout=300_000)  # 5 min
            print("Login detected! Saving session state ...")
        except Exception:
            print("Saving whatever state we have (you may not have completed login).")

        context.storage_state(path=str(AUTH_STATE_FILE))
        browser.close()

    print(f"\nSession state saved to {AUTH_STATE_FILE}")
    print("Re-run this script when the session expires (~7 days).")


if __name__ == "__main__":
    main()
