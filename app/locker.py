"""Distributed task locking for SwarmBus agents."""

from __future__ import annotations

import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import settings

logger = logging.getLogger(__name__)


class DistributedAgentLocker:
    """Redis-backed lease manager that protects tasks from concurrent execution."""

    _RELEASE_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    end
    return 0
    """

    def __init__(self, redis_client: Redis | None = None) -> None:
        self.redis: Redis = redis_client or Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            health_check_interval=30,
        )

    @staticmethod
    def _lock_key(task_id: str) -> str:
        clean_task_id = task_id.strip()
        if not clean_task_id:
            raise ValueError("task_id must be a non-empty string")
        return f"swarmbus:lock:{clean_task_id}"

    @staticmethod
    def _owner_token(agent_id: str) -> str:
        clean_agent_id = agent_id.strip()
        if not clean_agent_id:
            raise ValueError("agent_id must be a non-empty string")
        return clean_agent_id

    async def acquire_task_lock(self, task_id: str, agent_id: str) -> bool:
        """Acquire an atomic task lease for exactly one competing agent."""

        lock_key = self._lock_key(task_id)
        owner_token = self._owner_token(agent_id)
        try:
            acquired = await self.redis.set(
                lock_key,
                owner_token,
                nx=True,
                px=settings.LOCK_TTL_MS,
            )
            lease_acquired = bool(acquired)
            logger.info(
                "Task lock acquisition attempted",
                extra={
                    "task_id": task_id,
                    "agent_id": agent_id,
                    "lease_acquired": lease_acquired,
                    "lock_ttl_ms": settings.LOCK_TTL_MS,
                },
            )
            return lease_acquired
        except RedisError:
            logger.exception(
                "Redis task lock acquisition failed",
                extra={"task_id": task_id, "agent_id": agent_id},
            )
            raise

    async def release_task_lock(self, task_id: str, agent_id: str) -> bool:
        """Release a task lease only when the requesting agent owns it."""

        lock_key = self._lock_key(task_id)
        owner_token = self._owner_token(agent_id)
        try:
            released = await self.redis.eval(self._RELEASE_SCRIPT, 1, lock_key, owner_token)
            lease_released = int(released) == 1
            logger.info(
                "Task lock release attempted",
                extra={
                    "task_id": task_id,
                    "agent_id": agent_id,
                    "lease_released": lease_released,
                },
            )
            return lease_released
        except RedisError:
            logger.exception(
                "Redis task lock release failed",
                extra={"task_id": task_id, "agent_id": agent_id},
            )
            raise
