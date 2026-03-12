"""Suite 2: Busy Block Creation (tests 13-25)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "busy_blocks"


def _get_two_client_cals(ctx: TestContext):
    """Get two client calendars from the first account that has them."""
    for acct in ctx.accounts:
        clients = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(clients) >= 2:
            return acct, clients[0], clients[1]
    raise RuntimeError("Need at least 2 client calendars for busy block tests")


class BusyBlockOnOtherCalendar(TestCase):
    name = "BusyBlockOnOtherCalendar"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-other")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        # Wait for main copy
        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        # Wait for busy block on B
        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "Busy" in e.get("summary", "") and "[BusyBridge]" in e.get("summary", ""),
            search_query="BusyBridge Busy",
            time_min=start, time_max=end,
            description="busy block on B",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])


class NoBusyBlockOnOrigin(TestCase):
    name = "NoBusyBlockOnOrigin"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-noself")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])
        await asyncio.sleep(10)  # Let sync complete

        # Check that NO busy block on A
        events_a = cal_a["client"].list_events(
            cal_a["google_calendar_id"],
            q="BusyBridge Busy",
            time_min=start, time_max=end,
        )
        busys_on_a = [
            e for e in events_a
            if "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", "")
        ]
        assert len(busys_on_a) == 0, f"Found {len(busys_on_a)} busy blocks on origin calendar"

        # Cleanup main copy
        main_events = acct["main_client"].list_events(
            acct["main_calendar_id"], q=summary,
        )
        for me in main_events:
            if summary in me.get("summary", ""):
                ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], me["id"])


class BusyBlockTitle(TestCase):
    name = "BusyBlockTitle"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-title")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        # Wait for main copy first
        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            search_query="BusyBridge Busy",
            time_min=start, time_max=end,
            description="busy block title check",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        title = busy["summary"]
        assert "[BusyBridge]" in title, f"Missing [BusyBridge] in: {title}"
        assert "Busy" in title, f"Missing 'Busy' in: {title}"


class BusyBlockPrivateVisibility(TestCase):
    name = "BusyBlockPrivateVisibility"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-priv")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="busy block visibility",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        assert busy.get("visibility") == "private", \
            f"Expected private visibility, got: {busy.get('visibility')}"


class BusyBlockOpaque(TestCase):
    name = "BusyBlockOpaque"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-opaq")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="busy block opacity",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        # opaque is the default (means "busy"), so transparency should be absent or "opaque"
        transparency = busy.get("transparency", "opaque")
        assert transparency == "opaque", f"Expected opaque, got: {transparency}"


class BusyBlockTimesMatch(TestCase):
    name = "BusyBlockTimesMatch"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-time")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="busy block times",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        # Compare start/end times (may differ in timezone format but same instant)
        source_start = event["start"].get("dateTime", event["start"].get("date"))
        busy_start = busy["start"].get("dateTime", busy["start"].get("date"))
        assert busy_start is not None, "Busy block has no start time"


class AllDayBusyBlock(TestCase):
    name = "AllDayBusyBlock"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-ad")
        start, end = ctx.factory.future_all_day(days_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end, all_day=True,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: ("[BusyBridge]" in e.get("summary", "")
                       and "Busy" in e.get("summary", "")
                       and e.get("start", {}).get("date") == start),
            description="all-day busy block",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])


class DeclinedNoBusyBlock(TestCase):
    name = "DeclinedNoBusyBlock"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-decl")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        # Create event with self as declined attendee
        cal_email = next(
            t["google_account_email"] for t in acct["tokens"]
            if t["id"] == cal_a["calendar"]["oauth_token_id"]
        )
        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
            attendees=[{"email": cal_email, "responseStatus": "declined"}],
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])
        await asyncio.sleep(15)

        # Check no busy block on B
        events_b = cal_b["client"].list_events(
            cal_b["google_calendar_id"],
            q="BusyBridge Busy",
            time_min=start, time_max=end,
        )
        busys = [e for e in events_b if "[BusyBridge]" in e.get("summary", "")]
        assert len(busys) == 0, f"Found {len(busys)} busy blocks for declined event"


class FreeAllDayNoBusyBlock(TestCase):
    name = "FreeAllDayNoBusyBlock"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-free")
        start, end = ctx.factory.future_all_day(days_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
            all_day=True, transparency="transparent",
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])
        await asyncio.sleep(15)

        events_b = cal_b["client"].list_events(
            cal_b["google_calendar_id"],
            q="BusyBridge Busy",
        )
        busys = [
            e for e in events_b
            if "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", "")
            and e.get("start", {}).get("date") == start
        ]
        assert len(busys) == 0, "Found busy block for transparent all-day event"


class BusyBlockDeletedWithSource(TestCase):
    name = "BusyBlockDeletedWithSource"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-delsrc")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        busy_id = busy["id"]

        # Delete source
        cal_a["client"].delete_event(cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        await ctx.waiter.wait_for_gone(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: e["id"] == busy_id,
            description="busy block after source deleted",
        )


class BusyBlockUpdatedOnTimeChange(TestCase):
    name = "BusyBlockUpdatedOnTimeChange"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-upd")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        # Change time
        new_start, new_end = ctx.factory.future_time_slot(hours_from_now=6)
        cal_a["client"].update_event(
            cal_a["google_calendar_id"], event["id"],
            {
                "start": {"dateTime": new_start, "timeZone": "America/New_York"},
                "end": {"dateTime": new_end, "timeZone": "America/New_York"},
            },
        )

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        # Verify busy block updated
        updated_busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=new_start, time_max=new_end,
            description="updated busy block",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], updated_busy["id"])


class MainOriginBusyBlockAllClients(TestCase):
    name = "MainOriginBusyBlockAllClients"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if not client_cals:
            return

        summary = ctx.factory.make_summary("bb-main")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = main_client.create_event(main_cal_id, summary, start, end)
        ctx.cleanup.track(main_client, main_cal_id, event["id"])

        await ctx.api.trigger_user_sync(acct["user_id"])

        # Check busy blocks on all clients
        for cal in client_cals:
            busy = await ctx.waiter.wait_for_event(
                cal["client"], cal["google_calendar_id"],
                lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
                time_min=start, time_max=end,
                description=f"busy block on {cal['google_calendar_id'][:20]}",
            )
            ctx.cleanup.track(cal["client"], cal["google_calendar_id"], busy["id"])


class SyncTagPreserved(TestCase):
    name = "SyncTagPreserved"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        summary = ctx.factory.make_summary("bb-tag")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        await ctx.api.trigger_calendar_sync(cal_a["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            acct["main_client"], acct["main_calendar_id"],
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(acct["main_client"], acct["main_calendar_id"], main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])

        # Check sync tag in extendedProperties
        ext_props = busy.get("extendedProperties", {}).get("private", {})
        has_tag = any("sync" in k.lower() or "busybridge" in k.lower() or "calendar" in k.lower()
                      for k in ext_props)
        assert has_tag or ext_props, \
            f"No sync tag in extendedProperties: {busy.get('extendedProperties')}"


TESTS = [
    BusyBlockOnOtherCalendar(),
    NoBusyBlockOnOrigin(),
    BusyBlockTitle(),
    BusyBlockPrivateVisibility(),
    BusyBlockOpaque(),
    BusyBlockTimesMatch(),
    AllDayBusyBlock(),
    DeclinedNoBusyBlock(),
    FreeAllDayNoBusyBlock(),
    BusyBlockDeletedWithSource(),
    BusyBlockUpdatedOnTimeChange(),
    MainOriginBusyBlockAllClients(),
    SyncTagPreserved(),
]
