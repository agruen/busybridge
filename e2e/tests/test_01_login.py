"""Test 1: Auth state works, unauthenticated redirects."""

import pytest
from playwright.sync_api import Page, expect

from e2e.config import AUTH_STATE_FILE, BASE_URL

pytestmark = pytest.mark.browser


def test_unauthenticated_redirects_to_login(browser):
    """Visiting /app without a session redirects to /app/login."""
    # Use a fresh context (no saved state) to simulate unauthenticated user
    context = browser.new_context()
    page = context.new_page()
    page.goto(f"{BASE_URL}/app")
    page.wait_for_url(f"**/app/login**")
    expect(page).to_have_url(f"{BASE_URL}/app/login")
    context.close()


@pytest.mark.skipif(
    not AUTH_STATE_FILE.exists(),
    reason="No saved auth state — run: python -m e2e.auth.save_auth_state",
)
def test_authenticated_session_reaches_dashboard(page: Page):
    """With saved auth state, /app loads the dashboard (no redirect to login)."""
    page.goto(f"{BASE_URL}/app")
    # Should stay on /app (not redirect to /app/login)
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(f"{BASE_URL}/app")


@pytest.mark.skipif(
    not AUTH_STATE_FILE.exists(),
    reason="No saved auth state",
)
def test_login_page_redirects_authenticated_user(page: Page):
    """An already-authenticated user visiting /app/login gets redirected to /app."""
    page.goto(f"{BASE_URL}/app/login")
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(f"{BASE_URL}/app")
