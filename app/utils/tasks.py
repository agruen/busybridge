"""Utilities for managing background tasks."""

import asyncio
import logging
from typing import Coroutine, Any

logger = logging.getLogger(__name__)


def create_background_task(coro: Coroutine[Any, Any, Any], task_name: str = "background_task") -> asyncio.Task:
    """
    Create a background task with proper error handling.

    Exceptions in the task will be logged instead of silently lost.
    """
    async def _wrapped_task():
        try:
            await coro
        except Exception as e:
            logger.exception(f"Error in background task '{task_name}': {e}")

    task = asyncio.create_task(_wrapped_task())
    return task
