"""Suite 10: Multi-Calendar (tests 64-65)."""

from __future__ import annotations

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "multi_calendar"


def _get_two_client_cals(ctx: TestContext):
    for acct in ctx.accounts:
        clients = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(clients) >= 2:
            return acct, clients[0], clients[1]
    raise RuntimeError("Need 2 client calendars for multi-calendar tests")


class CrossCalendarBusyBlocks(TestCase):
    name = "CrossCalendarBusyBlocks"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary_a = ctx.factory.make_summary("cross-a")
        summary_b = ctx.factory.make_summary("cross-b")
        start_a, end_a = ctx.factory.future_time_slot(hours_from_now=3)
        start_b, end_b = ctx.factory.future_time_slot(hours_from_now=5)

        # Create on A
        event_a = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary_a, start_a, end_a,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event_a["id"])

        # Create on B
        event_b = cal_b["client"].create_event(
            cal_b["google_calendar_id"], summary_b, start_b, end_b,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], event_b["id"])

        # A's event should produce busy block on B
        busy_on_b = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start_a, time_max=end_a,
            description="busy block from A on B",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy_on_b["id"])

        # B's event should produce busy block on A
        busy_on_a = await ctx.waiter.wait_for_event(
            cal_a["client"], cal_a["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start_b, time_max=end_b,
            description="busy block from B on A",
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], busy_on_a["id"])

        # Cleanup main copies
        for s in [summary_a, summary_b]:
            events = main_client.list_events(main_cal_id, q=s)
            for e in events:
                if s in e.get("summary", ""):
                    ctx.cleanup.track(main_client, main_cal_id, e["id"])


class SimultaneousOverlapping(TestCase):
    name = "SimultaneousOverlapping"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, cal_a, cal_b = _get_two_client_cals(ctx)
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        # Overlapping time slot
        start, end = ctx.factory.future_time_slot(hours_from_now=3, duration_hours=2)

        summary_a = ctx.factory.make_summary("overlap-a")
        summary_b = ctx.factory.make_summary("overlap-b")

        event_a = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary_a, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event_a["id"])

        event_b = cal_b["client"].create_event(
            cal_b["google_calendar_id"], summary_b, start, end,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], event_b["id"])

        # Both should appear on main
        main_a = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary_a in e.get("summary", ""),
            search_query=summary_a,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_a["id"])

        main_b = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary_b in e.get("summary", ""),
            search_query=summary_b,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_b["id"])


TESTS = [
    CrossCalendarBusyBlocks(),
    SimultaneousOverlapping(),
]
