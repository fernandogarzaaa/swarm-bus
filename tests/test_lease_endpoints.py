from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from redis.exceptions import RedisError

from app import main as main_module
from app.config import settings
from app.locker import DistributedAgentLocker


@pytest.mark.asyncio
async def test_renew_extends_only_for_owner() -> None:
    redis_mock = AsyncMock()
    redis_mock.eval.side_effect = [1, 0]
    locker = DistributedAgentLocker(redis_client=redis_mock)

    assert await locker.renew_task_lock("t1", "agent-a") is True
    assert await locker.renew_task_lock("t1", "agent-b") is False
    args = redis_mock.eval.await_args_list[0].args
    assert args[1:] == (1, "swarmbus:lock:t1", "agent-a", str(settings.LOCK_TTL_MS))
    assert "PEXPIRE" in args[0]


@pytest.mark.asyncio
async def test_renew_rejects_blank_ids() -> None:
    locker = DistributedAgentLocker(redis_client=AsyncMock())
    with pytest.raises(ValueError):
        await locker.renew_task_lock("  ", "agent-a")
    with pytest.raises(ValueError):
        await locker.renew_task_lock("t1", " ")


@pytest.mark.asyncio
async def test_release_endpoint_reports_owner_result() -> None:
    redis_mock = AsyncMock()
    redis_mock.eval.side_effect = [1, 0]
    with patch.object(main_module.locker, "redis", redis_mock):
        ok = await main_module.release_task(main_module.LeaseRequest(task_id="t1", agent_id="a"))
        not_owner = await main_module.release_task(
            main_module.LeaseRequest(task_id="t1", agent_id="b")
        )
    assert ok.lease_released is True
    assert not_owner.lease_released is False


@pytest.mark.asyncio
async def test_renew_endpoint_returns_ttl_and_maps_redis_errors_to_503() -> None:
    redis_mock = AsyncMock()
    redis_mock.eval.side_effect = [1, RedisError("down")]
    with patch.object(main_module.locker, "redis", redis_mock):
        r = await main_module.renew_task(main_module.LeaseRequest(task_id="t1", agent_id="a"))
        assert r.lease_renewed is True and r.lock_ttl_ms == settings.LOCK_TTL_MS
        with pytest.raises(HTTPException) as exc:
            await main_module.renew_task(main_module.LeaseRequest(task_id="t1", agent_id="a"))
    assert exc.value.status_code == 503
