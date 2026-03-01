"""Test 9: Settings page + full re-sync trigger."""

import pytest
from playwright.sync_api import Page, expect

from e2e.config import AUTH_STATE_FILE, BASE_URL

pytestmark = [
    pytest.mark.browser,
    pytest.mark.skipif(
        not AUTH_STATE_FILE.exists(),
        reason="No saved auth state",
    ),
]


def test_settings_page_loads(page: Page):
    """Settings page loads and shows calendar options."""
    page.goto(f"{BASE_URL}/app/settings")
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(f"{BASE_URL}/app/settings")
    body_text = page.locator("body").inner_text().lower()
    assert "calendar" in body_text or "settings" in body_text


def test_trigger_full_resync(page: Page):
    """Trigger a full re-sync via the API and verify no errors."""
    # Use the API endpoint directly (the UI calls this via JS)
    response = page.request.post(f"{BASE_URL}/api/sync/full")
    assert response.ok, f"Full re-sync failed: {response.status} {response.text()}"

    data = response.json()
    # The response should indicate the sync was triggered
    assert data is not None
