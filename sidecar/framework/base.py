"""Base test case and result models."""

from __future__ import annotations

import logging
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sidecar.framework.cleanup import CleanupManager
    from sidecar.framework.event_factory import EventFactory
    from sidecar.framework.sync_waiter import SyncWaiter
    from sidecar.infra.api_client import APIClient
    from sidecar.infra.calendar_client import CalendarTestClient
    from sidecar.infra.db_reader import DBReader

logger = logging.getLogger(__name__)


class TestStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class TestTiming(str, Enum):
    QUICK = "quick"      # 10-30s delay after
    NORMAL = "normal"    # 30s-2min delay after
    SLOW = "slow"        # 2-5min delay after


@dataclass
class TestResult:
    test_name: str
    suite: str
    status: TestStatus
    duration: float
    run_id: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error: Optional[str] = None
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "test_name": self.test_name,
            "suite": self.suite,
            "status": self.status.value,
            "duration": round(self.duration, 2),
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "error": self.error,
            "details": self.details,
        }


@dataclass
class TestContext:
    """Shared context passed to every test."""
    api: APIClient
    db: DBReader
    waiter: SyncWaiter
    factory: EventFactory
    cleanup: CleanupManager
    run_id: str
    # Per-user test accounts: list of {user, tokens, calendars, clients}
    accounts: list[dict]


class TestCase:
    """Base class for all soak tests."""

    name: str = ""
    suite: str = ""
    timing: TestTiming = TestTiming.NORMAL

    async def setup(self, ctx: TestContext) -> None:
        """Optional per-test setup."""
        pass

    async def run(self, ctx: TestContext) -> None:
        """Test logic. Raise AssertionError on failure."""
        raise NotImplementedError

    async def teardown(self, ctx: TestContext) -> None:
        """Optional per-test cleanup."""
        pass

    async def execute(self, ctx: TestContext) -> TestResult:
        """Run the full test lifecycle and return a result."""
        run_id = f"{ctx.run_id}-{uuid.uuid4().hex[:6]}"
        start = time.monotonic()
        status = TestStatus.PASSED
        error = None
        details: dict = {}

        try:
            await self.setup(ctx)
            await self.run(ctx)
        except AssertionError as e:
            status = TestStatus.FAILED
            error = str(e) or "Assertion failed"
            logger.warning("FAILED %s: %s", self.name, error)
        except Exception as e:
            status = TestStatus.ERROR
            error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.error("ERROR %s: %s", self.name, error)
        finally:
            try:
                await self.teardown(ctx)
            except Exception as e:
                logger.error("Teardown error in %s: %s", self.name, e)
            # Always run cleanup for tracked events
            try:
                await ctx.cleanup.cleanup_tracked()
            except Exception as e:
                logger.error("Cleanup error in %s: %s", self.name, e)

        duration = time.monotonic() - start
        result = TestResult(
            test_name=self.name,
            suite=self.suite,
            status=status,
            duration=duration,
            run_id=run_id,
            error=error,
            details=details,
        )
        log_level = logging.INFO if status == TestStatus.PASSED else logging.WARNING
        logger.log(log_level, "%s %s (%.1fs)", status.value.upper(), self.name, duration)
        return result
