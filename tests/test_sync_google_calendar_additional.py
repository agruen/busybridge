"""Additional coverage tests for sync.google_calendar."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_list_events_network_error_branch():
    """list_events should log and re-raise non-HttpError exceptions."""
    from app.sync.google_calendar import GoogleCalendarClient

    class ExplodingEvents:
        def list(self, **_kwargs):
            def _raise():
                raise RuntimeError("network timeout")

            return SimpleNamespace(execute=_raise)

    client = object.__new__(GoogleCalendarClient)
    client.settings = SimpleNamespace(calendar_sync_tag="syncTag", busy_block_title="Busy")
    client.service = SimpleNamespace(events=lambda: ExplodingEvents())

    with pytest.raises(RuntimeError):
        client.list_events("cal-1")


def test_can_user_edit_event_creator_self_branch():
    """can_user_edit_event should allow edits when creator.self is true."""
    from app.sync.google_calendar import can_user_edit_event

    event = {
        "creator": {"self": True},
        "organizer": {"email": "other@example.com"},
    }
    assert can_user_edit_event(event, "person@example.com") is True
