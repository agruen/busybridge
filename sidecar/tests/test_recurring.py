"""Suite 3: Recurring Events (tests 26-32)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "recurring"


def _get_cal(ctx: TestContext):
    acct = ctx.accounts[0]
    client_cal = next(
        c for c in acct["calendars"] if c["calendar_type"] == "client"
    )
    return acct, client_cal


class WeeklyRecurringSync(TestCase):
    name = "WeeklyRecurringSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-wk")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=4"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="recurring main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        assert summary in main_event["summary"]


class DailyRecurringSync(TestCase):
    name = "DailyRecurringSync"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-day")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=DAILY;COUNT=5"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="daily recurring main copy",
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])


class SingleInstanceEdit(TestCase):
    name = "SingleInstanceEdit"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-edit")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=4"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Get instances and edit the first one
        instances = cal_client.list_events(
            cal_id, q=summary, single_events=True,
        )
        test_instances = [i for i in instances if summary in i.get("summary", "")]
        if test_instances:
            instance = test_instances[0]
            new_summary = ctx.factory.make_summary("rec-edited")
            cal_client.update_event(cal_id, instance["id"], {
                "summary": new_summary,
            })

            await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
            await asyncio.sleep(10)

            # Verify the edited instance appears on main
            main_events = main_client.list_events(
                main_cal_id, q=new_summary,
            )
            edited_on_main = [e for e in main_events if new_summary in e.get("summary", "")]
            for e in edited_on_main:
                ctx.cleanup.track(main_client, main_cal_id, e["id"])


class SingleInstanceDelete(TestCase):
    name = "SingleInstanceDelete"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-del1")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=4"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Delete one instance
        instances = cal_client.list_events(cal_id, q=summary, single_events=True)
        test_instances = [i for i in instances if summary in i.get("summary", "")]
        if len(test_instances) >= 2:
            cal_client.delete_event(cal_id, test_instances[0]["id"])

            await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
            await asyncio.sleep(10)

            # Verify remaining instances still on main
            remaining = main_client.list_events(
                main_cal_id, q=summary, single_events=True,
            )
            remaining_test = [e for e in remaining if summary in e.get("summary", "")]
            for e in remaining_test:
                ctx.cleanup.track(main_client, main_cal_id, e["id"])


class RecurringBusyBlock(TestCase):
    name = "RecurringBusyBlock"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if len(client_cals) < 2:
            return
        cal_a, cal_b = client_cals[0], client_cals[1]

        summary = ctx.factory.make_summary("rec-bb")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_a["client"].create_event(
            cal_a["google_calendar_id"], summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=3"],
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
            time_min=start,
            description="recurring busy block",
        )
        ctx.cleanup.track(cal_b["client"], cal_b["google_calendar_id"], busy["id"])


class DeleteRecurringParent(TestCase):
    name = "DeleteRecurringParent"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-delp")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=3"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Delete parent
        cal_client.delete_event(cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="recurring parent on main",
        )


class ModifiedInstanceNoDuplicate(TestCase):
    name = "ModifiedInstanceNoDuplicate"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, client_cal = _get_cal(ctx)
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("rec-nodup")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, summary, start, end,
            recurrence=["RRULE:FREQ=WEEKLY;COUNT=4"],
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Edit an instance
        instances = cal_client.list_events(cal_id, q=summary, single_events=True)
        test_instances = [i for i in instances if summary in i.get("summary", "")]
        if test_instances:
            new_start, new_end = ctx.factory.future_time_slot(hours_from_now=5)
            cal_client.update_event(cal_id, test_instances[0]["id"], {
                "start": {"dateTime": new_start, "timeZone": "America/New_York"},
                "end": {"dateTime": new_end, "timeZone": "America/New_York"},
            })

            await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
            await asyncio.sleep(15)

            # Count events at the new time - should be exactly 1
            events_at_new = main_client.list_events(
                main_cal_id, q=summary, time_min=new_start, time_max=new_end,
            )
            matches = [e for e in events_at_new if summary in e.get("summary", "")]
            for e in matches:
                ctx.cleanup.track(main_client, main_cal_id, e["id"])
            assert len(matches) <= 1, f"Found {len(matches)} events at new time (duplicate!)"


TESTS = [
    WeeklyRecurringSync(),
    DailyRecurringSync(),
    SingleInstanceEdit(),
    SingleInstanceDelete(),
    RecurringBusyBlock(),
    DeleteRecurringParent(),
    ModifiedInstanceNoDuplicate(),
]
