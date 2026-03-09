"""Soak test runner: pick random test, run, delay, repeat."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Optional

from sidecar.framework.base import TestCase, TestContext, TestResult, TestTiming

logger = logging.getLogger(__name__)

# Delay ranges per timing category (seconds)
TIMING_DELAYS = {
    TestTiming.QUICK: (10, 30),
    TestTiming.NORMAL: (30, 120),
    TestTiming.SLOW: (120, 300),
}


class SoakRunner:
    """Continuously run random tests with variable delays."""

    def __init__(
        self,
        tests: list[TestCase],
        ctx: TestContext,
        *,
        min_delay: int = 10,
        max_delay: int = 300,
        on_result: Optional[callable] = None,
    ):
        self.tests = tests
        self.ctx = ctx
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.on_result = on_result
        self._shutdown = False
        self._current_test: Optional[str] = None
        self._results: list[TestResult] = []

    @property
    def current_test(self) -> Optional[str]:
        return self._current_test

    @property
    def results(self) -> list[TestResult]:
        return self._results

    def request_shutdown(self) -> None:
        self._shutdown = True

    async def run(self) -> None:
        """Main soak loop."""
        logger.info("Soak runner started with %d tests", len(self.tests))

        while not self._shutdown:
            test = random.choice(self.tests)
            self._current_test = test.name

            logger.info("Running: %s (%s)", test.name, test.timing.value)
            result = await test.execute(self.ctx)
            self._results.append(result)

            if self.on_result:
                try:
                    await self.on_result(result)
                except Exception as e:
                    logger.error("on_result callback error: %s", e)

            if self._shutdown:
                break

            # Delay based on test timing category
            delay_range = TIMING_DELAYS.get(
                test.timing, (self.min_delay, self.max_delay)
            )
            # Clamp to configured min/max
            lo = max(delay_range[0], self.min_delay)
            hi = min(delay_range[1], self.max_delay)
            delay = random.uniform(lo, hi)
            logger.info("Next test in %.0fs", delay)

            # Sleep in small increments so shutdown is responsive
            elapsed = 0.0
            while elapsed < delay and not self._shutdown:
                await asyncio.sleep(min(1.0, delay - elapsed))
                elapsed += 1.0

        self._current_test = None
        logger.info("Soak runner stopped (%d results)", len(self._results))
