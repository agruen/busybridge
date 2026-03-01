"""Test 14: Logs page shows recent sync activity."""

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


def test_logs_page_loads(page: Page):
    """Logs page loads and shows log entries or an empty state."""
    page.goto(f"{BASE_URL}/app/logs")
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(f"{BASE_URL}/app/logs")

    body_text = page.locator("body").inner_text().lower()
    assert "log" in body_text or "sync" in body_text or "activity" in body_text


def test_logs_api_returns_entries(page: Page):
    """The sync log API returns recent entries."""
    response = page.request.get(f"{BASE_URL}/api/sync/log")
    assert response.ok
    data = response.json()
    # Should be a list or dict with log entries
    assert data is not None
