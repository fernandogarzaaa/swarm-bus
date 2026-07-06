"""FastAPI ingress for the SwarmBus coordination fabric."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

from app.broker import StreamBroker
from app.config import settings
from app.detector import DeadlockLoopInterceptor
from app.locker import DistributedAgentLocker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SwarmBus",
    description="Event-driven multi-agent message broker and coordination fabric.",
    version="0.1.0",
)

broker = StreamBroker()
locker = DistributedAgentLocker()
interceptor = DeadlockLoopInterceptor()


class BroadcastRequest(BaseModel):
    task: str = Field(min_length=1)
    history: list[dict[str, Any]] = Field(default_factory=list)
    topic: str = Field(default="agent-events", min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class BroadcastResponse(BaseModel):
    accepted: bool
    topic: str
    stream_id: str


class ClaimRequest(BaseModel):
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)


class ClaimResponse(BaseModel):
    task_id: str
    agent_id: str
    lease_acquired: bool
    lock_ttl_ms: int


@app.post("/v1/bus/broadcast", response_model=BroadcastResponse)
async def broadcast_event(request: BroadcastRequest) -> BroadcastResponse:
    """Validate and broadcast an agent event vector into the Redis Streams fabric."""

    try:
        trajectory_is_safe = interceptor.validate_trajectory(request.history)
    except (TypeError, ValueError) as exc:
        logger.warning("Rejected malformed broadcast trajectory", extra={"error": str(exc)})
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"accepted": False, "reason": str(exc)},
        ) from exc

    if not trajectory_is_safe:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "accepted": False,
                "reason": "agent handoff trajectory contains a non-progressing communication cycle",
            },
        )

    event_payload = {
        "task": request.task,
        "history": request.history,
        "payload": request.payload,
        "environment": settings.AGENT_BUS_ENV,
    }

    try:
        stream_id = await broker.publish_event(request.topic, event_payload)
    except (RedisError, ValueError) as exc:
        logger.exception("Broadcast publish failed", extra={"topic": request.topic})
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"accepted": False, "reason": "broker publish operation failed"},
        ) from exc

    return BroadcastResponse(accepted=True, topic=request.topic, stream_id=stream_id)


@app.post("/v1/bus/claim", response_model=ClaimResponse)
async def claim_task(request: ClaimRequest) -> ClaimResponse:
    """Attempt to acquire an atomic lease for a task."""

    try:
        lease_acquired = await locker.acquire_task_lock(request.task_id, request.agent_id)
    except (RedisError, ValueError) as exc:
        logger.exception(
            "Task claim failed",
            extra={"task_id": request.task_id, "agent_id": request.agent_id},
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"lease_acquired": False, "reason": "task lock operation failed"},
        ) from exc

    return ClaimResponse(
        task_id=request.task_id,
        agent_id=request.agent_id,
        lease_acquired=lease_acquired,
        lock_ttl_ms=settings.LOCK_TTL_MS,
    )
