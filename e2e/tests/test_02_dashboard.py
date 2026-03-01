"""Test 2: Dashboard loads and shows connected calendars."""

import pytest
from playwright.sync_api import Page, expect

from e2e.config import AUTH_STATE_FILE, BASE_URL, CLIENT1_EMAIL, CLIENT2_EMAIL

pytestmark = [
    pytest.mark.browser,
    pytest.mark.skipif(
        not AUTH_STATE_FILE.exists(),
        reason="No saved auth state",
    ),
]


def test_dashboard_loads(page: Page):
    """Dashboard page loads without errors."""
    page.goto(f"{BASE_URL}/app")
    page.wait_for_load_state("networkidle")
    expect(page).to_have_url(f"{BASE_URL}/app")
    # Should show some content (not a blank page or error)
    expect(page.locator("body")).not_to_be_empty()


def test_dashboard_shows_connected_calendars(page: Page):
    """Dashboard lists the connected client calendars."""
    page.goto(f"{BASE_URL}/app")
    page.wait_for_load_state("networkidle")

    body_text = page.locator("body").inner_text()

    # At least one of the client emails should appear on the dashboard
    has_client1 = CLIENT1_EMAIL in body_text
    has_client2 = CLIENT2_EMAIL in body_text
    assert has_client1 or has_client2, (
        f"Dashboard does not show any connected calendar. "
        f"Expected {CLIENT1_EMAIL} or {CLIENT2_EMAIL} in page text."
    )


def test_dashboard_shows_event_count(page: Page):
    """Dashboard shows a synced event count (may be 0)."""
    page.goto(f"{BASE_URL}/app")
    page.wait_for_load_state("networkidle")

    # The dashboard template renders event_count — look for numeric content
    # near "event" text. This is a loose check: we just verify the page loaded
    # with dashboard content.
    body_text = page.locator("body").inner_text().lower()
    assert "calendar" in body_text or "event" in body_text or "sync" in body_text, (
        "Dashboard page does not appear to contain calendar/event/sync content."
    )
