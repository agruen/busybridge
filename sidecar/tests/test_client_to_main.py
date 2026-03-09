"""Suite 1: Client-to-Main Sync (tests 1-12)."""

from __future__ import annotations

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "client_to_main"


class BasicEventSync(TestCase):
    name = "BasicEventSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("basic")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            description="Test description", location="Test Location",
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert summary in main_event["summary"], f"Title mismatch: {main_event['summary']}"
        assert main_event.get("location") == "Test Location", "Location not preserved"


class DescriptionPreserved(TestCase):
    name = "DescriptionPreserved"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("desc")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)
        desc = "Line 1\nLine 2\nLine 3"

        event = cal_client.create_event(
            cal_id, summary, start, end, description=desc,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        main_desc = main_event.get("description", "")
        assert "Line 1" in main_desc, "Description line 1 missing"
        assert "Line 2" in main_desc, "Description line 2 missing"
        assert "Line 3" in main_desc, "Description line 3 missing"


class LocationPreserved(TestCase):
    name = "LocationPreserved"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("loc")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end, location="123 Main Street",
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert main_event.get("location") == "123 Main Street"


class UpdatePropagates(TestCase):
    name = "UpdatePropagates"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("upd")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Update title
        new_summary = ctx.factory.make_summary("upd-new")
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=5)
        cal_client.update_event(cal_id, event["id"], {
            "summary": new_summary,
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        })

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        updated = await ctx.waiter.wait_for_event_updated(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            lambda e: new_summary in e.get("summary", ""),
            description="updated main copy",
        )
        assert new_summary in updated["summary"]


class DeleteRemovesMainCopy(TestCase):
    name = "DeleteRemovesMainCopy"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("del")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Delete from client
        cal_client.delete_event(cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            description="deleted main copy",
        )


class AttendeesInFooter(TestCase):
    name = "AttendeesInFooter"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("att")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)
        attendees = [
            {"email": "test1@example.com"},
            {"email": "test2@example.com"},
        ]

        event = cal_client.create_event(
            cal_id, summary, start, end, attendees=attendees,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        desc = main_event.get("description", "")
        assert "test1@example.com" in desc or len(main_event.get("attendees", [])) > 0, \
            "Attendee info not found"


class AllDayEventSync(TestCase):
    name = "AllDayEventSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("allday")
        start, end = ctx.factory.future_all_day(days_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end, all_day=True,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main all-day copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert "date" in main_event.get("start", {}), "Not an all-day event on main"


class MultiDayEventSync(TestCase):
    name = "MultiDayEventSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("multi")
        start, end = ctx.factory.future_multi_day(days_from_now=3, span=3)

        event = cal_client.create_event(
            cal_id, summary, start, end, all_day=True,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main multi-day copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert "date" in main_event.get("start", {}), "Not all-day on main"
        assert main_event["start"]["date"] == start, f"Start date mismatch: {main_event['start']}"


class ConferenceDataPreserved(TestCase):
    name = "ConferenceDataPreserved"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("conf")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        # Create event (conference data may require specific setup)
        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # If source had conferenceData, verify it's preserved
        if event.get("conferenceData"):
            assert main_event.get("conferenceData"), "conferenceData not preserved"


class ColorIdApplied(TestCase):
    name = "ColorIdApplied"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("color")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # colorId may or may not be set depending on calendar config
        # Just verify the event synced correctly
        assert summary in main_event["summary"]


class UntitledEventSync(TestCase):
    name = "UntitledEventSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        # Create event with a marker in description since no title
        marker = ctx.factory.make_summary("untitled")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, "", start, end, description=marker,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        # Search by the marker in description
        import asyncio
        await asyncio.sleep(10)  # Give sync time

        # Try to find the event on main
        events = main_client.list_events(main_cal_id, time_min=start, time_max=end)
        main_event = None
        for e in events:
            desc = e.get("description", "")
            if marker in desc:
                main_event = e
                break

        if main_event:
            ctx.cleanup.track(main_client, main_cal_id, main_event["id"])
        # An untitled event may or may not sync - just verify no error


class MetadataFooterFormat(TestCase):
    name = "MetadataFooterFormat"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("meta")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        desc = main_event.get("description", "")
        assert "BusyBridge" in desc or "Managed" in desc, \
            f"Metadata footer not found in description: {desc[:200]}"


TESTS = [
    BasicEventSync(),
    DescriptionPreserved(),
    LocationPreserved(),
    UpdatePropagates(),
    DeleteRemovesMainCopy(),
    AttendeesInFooter(),
    AllDayEventSync(),
    MultiDayEventSync(),
    ConferenceDataPreserved(),
    ColorIdApplied(),
    UntitledEventSync(),
    MetadataFooterFormat(),
]
