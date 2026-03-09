"""Suite 4: Personal Calendar Sync (tests 33-37)."""

from __future__ import annotations

import asyncio

from sidecar.framework.base import TestCase, TestContext, TestTiming

SUITE = "personal"


def _get_personal_cal(ctx: TestContext):
    """Find an account with a personal calendar."""
    for acct in ctx.accounts:
        personal_cals = [
            c for c in acct["calendars"] if c["calendar_type"] == "personal"
        ]
        if personal_cals:
            return acct, personal_cals[0]
    raise RuntimeError("No personal calendar found")


class PersonalBlockOnMain(TestCase):
    name = "PersonalBlockOnMain"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, personal_cal = _get_personal_cal(ctx)
        cal_client = personal_cal["client"]
        cal_id = personal_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("pers-main")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(personal_cal["calendar"]["id"])

        block = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: "Personal" in e.get("summary", "") and "BusyBridge" in e.get("summary", ""),
            search_query="BusyBridge Busy Personal",
            time_min=start, time_max=end,
            description="personal block on main",
        )
        ctx.cleanup.track(main_client, main_cal_id, block["id"])

        assert "Personal" in block["summary"]


class PersonalBlockOnAllClients(TestCase):
    name = "PersonalBlockOnAllClients"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, personal_cal = _get_personal_cal(ctx)
        cal_client = personal_cal["client"]
        cal_id = personal_cal["google_calendar_id"]
        client_cals = [c for c in acct["calendars"] if c["calendar_type"] == "client"]
        if not client_cals:
            return

        summary = ctx.factory.make_summary("pers-all")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_user_sync(acct["user_id"])

        for cal in client_cals:
            block = await ctx.waiter.wait_for_event(
                cal["client"], cal["google_calendar_id"],
                lambda e: "Personal" in e.get("summary", "") and "BusyBridge" in e.get("summary", ""),
                time_min=start, time_max=end,
                description=f"personal block on {cal['google_calendar_id'][:20]}",
            )
            ctx.cleanup.track(cal["client"], cal["google_calendar_id"], block["id"])

        # Also cleanup main
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]
        main_blocks = main_client.list_events(
            main_cal_id, q="BusyBridge Busy Personal",
            time_min=start, time_max=end,
        )
        for b in main_blocks:
            if "Personal" in b.get("summary", ""):
                ctx.cleanup.track(main_client, main_cal_id, b["id"])


class PersonalPrivacy(TestCase):
    name = "PersonalPrivacy"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, personal_cal = _get_personal_cal(ctx)
        cal_client = personal_cal["client"]
        cal_id = personal_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        original_title = ctx.factory.make_summary("pers-secret")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(
            cal_id, original_title, start, end,
            description="Super secret description",
        )
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(personal_cal["calendar"]["id"])

        block = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: "Personal" in e.get("summary", "") and "BusyBridge" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="personal block privacy check",
        )
        ctx.cleanup.track(main_client, main_cal_id, block["id"])

        # Original title should NOT appear
        block_summary = block.get("summary", "")
        block_desc = block.get("description", "")
        assert original_title not in block_summary, "Original title leaked"
        assert "secret" not in block_desc.lower(), "Description leaked"


class PersonalBlockLocked(TestCase):
    name = "PersonalBlockLocked"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, personal_cal = _get_personal_cal(ctx)
        cal_client = personal_cal["client"]
        cal_id = personal_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("pers-lock")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(personal_cal["calendar"]["id"])

        block = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: "Personal" in e.get("summary", "") and "BusyBridge" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="personal block locked",
        )
        ctx.cleanup.track(main_client, main_cal_id, block["id"])

        # Should have lock emoji
        assert "\U0001f512" in block["summary"] or "lock" in block["summary"].lower(), \
            f"No lock indicator in: {block['summary']}"


class PersonalDeleteCleansUp(TestCase):
    name = "PersonalDeleteCleansUp"
    suite = SUITE
    timing = TestTiming.NORMAL

    async def run(self, ctx: TestContext) -> None:
        acct, personal_cal = _get_personal_cal(ctx)
        cal_client = personal_cal["client"]
        cal_id = personal_cal["google_calendar_id"]
        main_client = acct["main_client"]
        main_cal_id = acct["main_calendar_id"]

        summary = ctx.factory.make_summary("pers-del")
        start, end = ctx.factory.future_time_slot(hours_from_now=3)

        event = cal_client.create_event(cal_id, summary, start, end)
        ctx.cleanup.track(cal_client, cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(personal_cal["calendar"]["id"])

        block = await ctx.waiter.wait_for_event(
            main_client, main_cal_id,
            lambda e: "Personal" in e.get("summary", "") and "BusyBridge" in e.get("summary", ""),
            time_min=start, time_max=end,
            description="personal block to delete",
        )
        block_id = block["id"]

        # Delete personal event
        cal_client.delete_event(cal_id, event["id"])

        await ctx.api.trigger_calendar_sync(personal_cal["calendar"]["id"])

        await ctx.waiter.wait_for_gone(
            main_client, main_cal_id,
            lambda e: e["id"] == block_id,
            description="personal block after delete",
        )


TESTS = [
    PersonalBlockOnMain(),
    PersonalBlockOnAllClients(),
    PersonalPrivacy(),
    PersonalBlockLocked(),
    PersonalDeleteCleansUp(),
]
