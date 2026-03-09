"""Suite 11: Sync Control (tests 66-71)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "sync_control"


class PausePreventsSync(TestCase):
    name = "PausePreventsSync"
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

        try:
            # Pause sync
            await ctx.api.pause_sync()

            summary = ctx.factory.make_summary("pause-test")
            start, end = ctx.factory.future_time_slot(hours_from_now=3)

            event = cal_client.create_event(cal_id, summary, start, end)
            ctx.cleanup.track(cal_client, cal_id, event["id"])

            # Try to sync (should be paused)
            try:
                await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
            except Exception:
                pass  # May return error when paused

            await asyncio.sleep(10)

            # Nothing should appear on main
            events = main_client.list_events(main_cal_id, q=summary)
            matches = [e for e in events if summary in e.get("summary", "")]
            assert len(matches) == 0, f"Event synced while paused: {len(matches)} found"

        finally:
            # Always resume
            await ctx.api.resume_sync()


class ResumeProcessesPending(TestCase):
    name = "ResumeProcessesPending"
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

        summary = ctx.factory.make_summary("resume-test")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        try:
            await ctx.api.pause_sync()

            event = cal_client.create_event(cal_id, summary, start, end)
            ctx.cleanup.track(cal_client, cal_id, event["id"])

            await asyncio.sleep(5)

            # Resume
            await ctx.api.resume_sync()

            # Trigger sync
            await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

            main_event = await ctx.waiter.wait_for_event(
                main_client, main_cal_id,
                lambda e: summary in e.get("summary", ""),
                search_query=summary,
                description="event after resume",
            )
            ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        finally:
            await ctx.api.resume_sync()


class CleanupAndResync(TestCase):
    name = "CleanupAndResync"
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

        summary = ctx.factory.make_summary("cleanup-rs")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )

        # Cleanup managed events
        await ctx.api.cleanup_managed()

        # Wait for cleanup to take effect
        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="cleaned up event",
            timeout=60,
        )

        # Trigger full re-sync
        await ctx.api.trigger_full_sync()

        # Event should reappear
        main_event2 = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
            description="re-synced event",
            timeout=180,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event2["id"])


class CleanupAndPause(TestCase):
    name = "CleanupAndPause"
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

        summary1 = ctx.factory.make_summary("clnp-1")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary1, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary1 in e.get("summary", ""),
            search_query=summary1,
        )

        try:
            # Cleanup and pause
            await ctx.api.cleanup_and_pause()

            await ctx.waiter.wait_for_gone(
                main_client, main_cal_id,
                lambda e: summary1 in e.get("summary", ""),
                search_query=summary1,
                description="cleaned up event",
                timeout=60,
            )

            # New event should NOT sync while paused
            summary2 = ctx.factory.make_summary("clnp-2")
            event2 = cal_client.create_event(cal_id, summary2, start, end)
            ctx.cleanup.track(cal_client, cal_id, event2["id"])

            try:
                await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])
            except Exception:
                pass

            await asyncio.sleep(10)
            events = main_client.list_events(main_cal_id, q=summary2)
            matches = [e for e in events if summary2 in e.get("summary", "")]
            assert len(matches) == 0, "Event synced while paused after cleanup"

        finally:
            await ctx.api.resume_sync()

        # After resume, trigger sync
        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        # Both events should now sync
        main_event_new = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary1 in e.get("summary", ""),
            search_query=summary1,
            description="re-synced after resume",
            timeout=180,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event_new["id"])


class FullResyncReprocesses(TestCase):
    name = "FullResyncReprocesses"
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

        summary = ctx.factory.make_summary("full-resync")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])

        # Full re-sync
        result = await ctx.api.trigger_full_sync()
        await asyncio.sleep(15)

        # Verify no duplicates
        events = main_client.list_events(main_cal_id, q=summary)
        copies = [e for e in events if summary in e.get("summary", "")]
        for e in copies:
            ctx.cleanup.track(main_client, main_cal_id, e["id"])
        assert len(copies) == 1, f"Full resync created {len(copies)} copies (expected 1)"


class ManualSyncForCalendar(TestCase):
    name = "ManualSyncForCalendar"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct = ctx.accounts[0]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if not client_cals:
            return
        client_cal = client_cals[0]
        cal_client = client_cal["client"]
        cal_id = client_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("manual-sync")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        # Sync only this calendar
        await ctx.api.trigger_calendar_sync(client_cal["calendar"]["id"])

        main_event = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: summary in e.get("summary", ""),
            search_query=summary,
        )
        ctx.cleanup.track(main_client, main_cal_id, main_event["id"])


TESTS = [
    PausePreventsSync(),
    ResumeProcessesPending(),
    CleanupAndResync(),
    CleanupAndPause(),
    FullResyncReprocesses(),
    ManualSyncForCalendar(),
]
