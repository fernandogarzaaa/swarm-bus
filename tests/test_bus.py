from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError

from app import main as main_module
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


@pytest.mark.asyncio
async def test_health_check_reports_ok_when_redis_is_reachable() -> None:
    redis_mock = AsyncMock()
    redis_mock.ping.return_value = True

    with patch.object(main_module.broker, "redis", redis_mock):
        response = await main_module.health_check()

    assert response.status == "ok"
    redis_mock.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_health_check_returns_503_when_redis_is_unreachable() -> None:
    redis_mock = AsyncMock()
    redis_mock.ping.side_effect = RedisError("connection refused")

    with patch.object(main_module.broker, "redis", redis_mock):
        with pytest.raises(HTTPException) as exc_info:
            await main_module.health_check()

    detail = exc_info.value.detail
    assert isinstance(detail, dict)
    assert exc_info.value.status_code == 503
    assert detail["status"] == "degraded"
