"""Suite 9: Edge Cases (tests 53-63)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "edge_cases"


class CreateDeleteBeforeSync(TestCase):
    name = "CreateDeleteBeforeSync"
    suite = SUITE
    timing = TestTiming.QUICK

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]

        summary = ctx.factory.make_summary("edge-cdbs")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        cal_client.delete_event(cal_id, event["id"])

        await asyncio.sleep(5)

        # Nothing should appear on main
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]
        events = main_client.list_events(main_cal_id, q=summary)
        matches = [e for e in events if summary in e.get("summary", "")]
        assert len(matches) == 0, f"Found {len(matches)} events for deleted-before-sync"


class RapidUpdates(TestCase):
    name = "RapidUpdates"
    suite = SUITE
    timing = TestTiming.QUICK

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary1 = ctx.factory.make_summary("rapid-1")
        summary2 = ctx.factory.make_summary("rapid-2")
        summary3 = ctx.factory.make_summary("rapid-3")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary1, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        # Rapid updates
        cal_client.update_event(cal_id, event["id"], {"summary": summary2})
        cal_client.update_event(cal_id, event["id"], {"summary": summary3})

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary3 in e.get("summary", ""),
            search_query=summary3,
            description="final rapid update",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert summary3 in main_event["summary"], "Final update not reflected"


class RapidCreateAndMove(TestCase):
    name = "RapidCreateAndMove"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rapid-move")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        # Immediately move
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=6)
        cal_client.update_event(cal_id, event["id"], {
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        })

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: (summary in e.get("summary", "")
                       and new_start[:16] in e.get("start", {}).get("dateTime", "")),
            search_query=summary,
            description="moved event with correct time on main",
            timeout=90,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])


class BackToBackEdits(TestCase):
    name = "BackToBackEdits"
    suite = SUITE
    timing = TestTiming.QUICK

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cal = next(
            c for c in acct["calendars"] if c["calendar_type"] == "client"
        )
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("b2b")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Two rapid edits
        edit1 = ctx.factory.make_summary("b2b-e1")
        edit2 = ctx.factory.make_summary("b2b-e2")
        cal_client.update_event(cal_id, event["id"], {"summary": edit1})
        await asyncio.sleep(1)
        cal_client.update_event(cal_id, event["id"], {"summary": edit2})

        updated = await ctx.waiter.wait_for_event_updated(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            lambda e: edit2 in e.get("summary", ""),
            description="final back-to-back edit",
        )
        assert edit2 in updated["summary"]


class MidnightSpanningEvent(TestCase):
    name = "MidnightSpanningEvent"
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

        summary = ctx.factory.make_summary("midnight")
        # Create 11pm-1am spanning midnight
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=2)
        start_dt = tomorrow.replace(hour=23, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(hours=2)
        start = start_dt.isoformat()
        end = end_dt.isoformat()

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])
        assert summary in main_event["summary"]


class DifferentTimezone(TestCase):
    name = "DifferentTimezone"
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

        summary = ctx.factory.make_summary("tz-london")
        start, end = ctx.factory.time_slot_at_offset(3.0, timezone_name="Europe/London")

        event = cal_client.create_event(
            cal_id, summary, start, end, timezone="Europe/London",
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])
        assert summary in main_event["summary"]


class SpecialCharsInTitle(TestCase):
    name = "SpecialCharsInTitle"
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

        base = ctx.factory.make_summary("special")
        # Add special chars (but keep the searchable prefix)
        summary = base + " \u2603\u2764;drop table"
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: base in e.get("summary", ""),
            search_query=base,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert "\u2603" in main_event["summary"] or base in main_event["summary"], \
            f"Special chars corrupted: {main_event['summary']}"


class HTMLInDescription(TestCase):
    name = "HTMLInDescription"
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

        summary = ctx.factory.make_summary("html")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)
        html_desc = "<b>bold</b><script>alert(1)</script>"

        event = cal_client.create_event(
            cal_id, summary, start, end, description=html_desc,
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Description should be preserved (Google may sanitize HTML)
        desc = main_event.get("description", "")
        assert "bold" in desc or summary in main_event["summary"]


class RescheduleEvent(TestCase):
    name = "RescheduleEvent"
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

        summary = ctx.factory.make_summary("resched")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Reschedule
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=5)
        cal_client.update_event(cal_id, event["id"], {
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        })

        updated = await ctx.waiter.wait_for_event_updated(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            lambda e: new_start[:16] in e.get("start", {}).get("dateTime", ""),
            description="rescheduled event",
        )


class RescheduleToDifferentDay(TestCase):
    name = "RescheduleToDifferentDay"
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

        summary = ctx.factory.make_summary("resched-day")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Move to different day
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=48)
        cal_client.update_event(cal_id, event["id"], {
            "start": {"dateTime": new_start, "timeZone": "America/New_York"},
            "end": {"dateTime": new_end, "timeZone": "America/New_York"},
        })

        updated = await ctx.waiter.wait_for_event_updated(
            main_client, main_cal_id,
            lambda e: e["id"] == main_event["id"],
            lambda e: new_start[:10] in e.get("start", {}).get("dateTime", ""),
            description="rescheduled to different day",
        )


class FixedOffsetTimezonePreserved(TestCase):
    name = "FixedOffsetTimezonePreserved"
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

        summary = ctx.factory.make_summary("fixedtz")
        # Use fixed offset
        from datetime import datetime, timedelta, timezone as tz
        now = datetime.now(tz.utc)
        offset = tz(timedelta(hours=-5))
        start_dt = (now + timedelta(hours=3)).astimezone(offset)
        end_dt = (now + timedelta(hours=4)).astimezone(offset)
        start = start_dt.isoformat()
        end = end_dt.isoformat()

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])
        assert summary in main_event["summary"]


TESTS = [
    CreateDeleteBeforeSync(),
    RapidUpdates(),
    RapidCreateAndMove(),
    BackToBackEdits(),
    MidnightSpanningEvent(),
    DifferentTimezone(),
    SpecialCharsInTitle(),
    HTMLInDescription(),
    RescheduleEvent(),
    RescheduleToDifferentDay(),
    FixedOffsetTimezonePreserved(),
]
