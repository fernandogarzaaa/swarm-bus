from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from app.detector import DeadlockLoopInterceptor
from app.locker import DistributedAgentLocker


@pytest.mark.asyncio
async def test_race_condition_defense_allows_only_one_task_claim() -> None:
    redis_mock = AsyncMock()
    redis_mock.set.side_effect = [True, None]
    locker = DistributedAgentLocker(redis_client=redis_mock)

    first_claim, second_claim = await asyncio.gather(
        locker.acquire_task_lock("task-123", "agent-alpha"),
        locker.acquire_task_lock("task-123", "agent-beta"),
    )

    assert sorted([first_claim, second_claim]) == [False, True]
    assert redis_mock.set.await_count == 2


def test_loop_interception_blocks_cyclic_communication_history() -> None:
    interceptor = DeadlockLoopInterceptor()
    cyclic_history = [
        {"from": "planner", "to": "coder"},
        {"from": "coder", "to": "critic"},
        {"from": "critic", "to": "coder"},
    ]

    assert interceptor.validate_trajectory(cyclic_history) is False
