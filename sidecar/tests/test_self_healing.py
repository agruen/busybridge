"""Suite 8: Self-Healing (tests 50-52)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "self_healing"


class MissingBusyBlockRecreated(TestCase):
    name = "MissingBusyBlockRecreated"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("heal-miss")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )

        # Delete busy block directly via Google API
        cal_b["client"].delete_event(cal_b["google_calendar_id"], busy["id"])

        # Wait for busy block to be recreated
        new_busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="recreated busy block",
            timeout=180,
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], new_busy["id"])


class ManuallyDeletedBlockRecreated(TestCase):
    name = "ManuallyDeletedBlockRecreated"
    suite = SUITE
    timing = TestTiming.SLOW

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("heal-manual")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
        )
        ctx.cleanup.track(cal_a["client"], cal_a["google_calendar_id"], event["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
        )

        # Delete busy block
        cal_b["client"].delete_event(cal_b["google_calendar_id"], busy["id"])

        # Wait for periodic sync to recreate (longer timeout)
        new_busy = await ctx.waiter.wait_for_event(
            cal_b["client"], cal_b["google_calendar_id"],
            lambda e: "[BusyBridge]" in e.get("summary", "") and "Busy" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="auto-recreated busy block",
            timeout=600,  # Up to 10 minutes for periodic sync
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], new_busy["id"])


class IdempotentSyncNoDuplicates(TestCase):
    name = "IdempotentSyncNoDuplicates"
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

        summary = ctx.factory.make_summary("idem")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        # Sync twice
        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Sync again
        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
        await asyncio.sleep(10)

        # Count copies on main
        events = main_client.list_events(main_cal_id, q=summary)
        copies = [e for e in events if summary in e.get("summary", "")]
        for e in copies:
            ctx.cleanup.track(main_client, main_cal_id, e["id"])

        assert len(copies) == 1, f"Found {len(copies)} copies instead of 1 (duplicates!)"


TESTS = [
    MissingBusyBlockRecreated(),
    ManuallyDeletedBlockRecreated(),
    IdempotentSyncNoDuplicates(),
]
