"""Regression tests for the Oct 2026 audit fixes (detector, broker DLQ, API-key auth)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

from app import main as main_module
from app.broker import StreamBroker
from app.config import settings
from app.detector import DeadlockLoopInterceptor


def _ring(agents: list[str], laps: int = 1) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    revision = 0
    for _ in range(laps):
        for i, agent in enumerate(agents):
            revision += 1
            history.append(
                {"from": agent, "to": agents[(i + 1) % len(agents)], "revision": revision}
            )
    return history


# --- detector -------------------------------------------------------------


def test_long_acyclic_history_does_not_recurse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "MAX_HISTORY_LENGTH", 10_000)
    history = [{"from": f"a{i}", "to": f"a{i + 1}"} for i in range(5_000)]
    assert DeadlockLoopInterceptor().validate_trajectory(history) is True


def test_history_longer_than_cap_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "MAX_HISTORY_LENGTH", 3)
    history = [{"from": f"a{i}", "to": f"a{i + 1}"} for i in range(4)]
    with pytest.raises(ValueError, match="MAX_HISTORY_LENGTH"):
        DeadlockLoopInterceptor().validate_trajectory(history)
    # exactly at the cap is fine
    assert DeadlockLoopInterceptor().validate_trajectory(history[:3]) is True


def test_progressing_cycle_of_exactly_max_depth_agents_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "MAX_LOOP_DEPTH", 4)
    assert DeadlockLoopInterceptor().validate_trajectory(_ring(["A", "B", "C", "D"])) is True


def test_progressing_cycle_longer_than_max_depth_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "MAX_LOOP_DEPTH", 4)
    assert DeadlockLoopInterceptor().validate_trajectory(_ring(["A", "B", "C", "D", "E"])) is False


def test_two_agent_progressing_cycle_allowed_at_depth_two(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "MAX_LOOP_DEPTH", 2)
    assert DeadlockLoopInterceptor().validate_trajectory(_ring(["A", "B"], laps=2)) is True
    monkeypatch.setattr(settings, "MAX_LOOP_DEPTH", 1)
    assert DeadlockLoopInterceptor().validate_trajectory(_ring(["A", "B"], laps=2)) is False


def test_non_progressing_cycle_rejected_even_when_short() -> None:
    history = [{"from": "A", "to": "B", "revision": 1}, {"from": "B", "to": "A", "revision": 1}]
    assert DeadlockLoopInterceptor().validate_trajectory(history) is False


def test_second_cycle_is_also_evaluated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "MAX_LOOP_DEPTH", 4)
    # A<->B progresses; X<->Y carries no progression state at all.
    history = _ring(["A", "B"]) + [{"from": "X", "to": "Y"}, {"from": "Y", "to": "X"}]
    assert DeadlockLoopInterceptor().validate_trajectory(history) is False
    assert DeadlockLoopInterceptor().validate_trajectory(_ring(["A", "B"])) is True


def test_find_cycles_returns_closed_paths() -> None:
    cycles = DeadlockLoopInterceptor._find_cycles({"A": {"B"}, "B": {"C"}, "C": {"A"}})
    assert cycles == [["A", "B", "C", "A"]]
    assert DeadlockLoopInterceptor._find_cycles({"A": {"B"}, "B": set()}) == []
    assert DeadlockLoopInterceptor._find_cycles({"A": {"A"}}) == [["A", "A"]]


def test_find_cycles_diamond_is_acyclic() -> None:
    adjacency = {"A": {"B", "C"}, "B": {"D"}, "C": {"D"}, "D": set()}
    assert DeadlockLoopInterceptor._find_cycles(adjacency) == []


@pytest.mark.asyncio
async def test_oversized_history_returns_422_from_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "MAX_HISTORY_LENGTH", 2)
    history: list[dict[str, Any]] = [{"from": f"a{i}", "to": f"a{i + 1}"} for i in range(3)]
    request = main_module.BroadcastRequest(task="t", history=history)
    publish = AsyncMock(return_value="1-0")
    with patch.object(main_module.broker, "publish_event", publish):
        with pytest.raises(HTTPException) as exc_info:
            await main_module.broadcast_event(request)
    assert exc_info.value.status_code == 422
    publish.assert_not_awaited()


# --- broker poison messages ------------------------------------------------


def _broker_with_entries(entries: list[tuple[Any, Any]]) -> tuple[StreamBroker, AsyncMock]:
    redis_mock = AsyncMock()
    redis_mock.xgroup_create.return_value = True
    redis_mock.xreadgroup.side_effect = [[(b"swarmbus:stream:t", entries)]]
    return StreamBroker(redis_client=redis_mock), redis_mock


@pytest.mark.asyncio
async def test_malformed_entries_are_dead_lettered_and_acked() -> None:
    good = (b"3-0", {b"payload": json.dumps({"ok": True}).encode()})
    broker, redis_mock = _broker_with_entries(
        [
            (b"1-0", {b"payload": b"{not json"}),
            (b"2-0", {b"other": b"x"}),
            (b"2-5", None),
            (b"2-6", {b"payload": b"[1, 2]"}),
            good,
        ]
    )
    gen = broker.subscribe_topic("t", "g", "c")
    event = await gen.__anext__()
    assert event["ok"] is True and event["_stream_id"] == "3-0"
    await gen.aclose()

    dlq_calls = [c for c in redis_mock.xadd.await_args_list if c.args[0] == "swarmbus:dlq:t"]
    assert [c.args[1]["source_stream_id"] for c in dlq_calls] == ["1-0", "2-0", "2-5", "2-6"]
    assert all(c.args[1]["consumer_group"] == "g" and c.args[1]["reason"] for c in dlq_calls)
    assert json.loads(dlq_calls[0].args[1]["raw_fields"]) == {"payload": "{not json"}
    acked = [c.args[2] for c in redis_mock.xack.await_args_list]
    assert acked == [b"1-0", b"2-0", b"2-5", b"2-6"]
    for call in redis_mock.xack.await_args_list:
        assert call.args[:2] == ("swarmbus:stream:t", "g")


@pytest.mark.asyncio
async def test_good_entry_acked_only_after_consumer_resumes() -> None:
    broker, redis_mock = _broker_with_entries([(b"9-0", {b"payload": b'{"n": 1}'})])
    gen = broker.subscribe_topic("t", "g", "c")
    event = await gen.__anext__()
    assert event["n"] == 1
    redis_mock.xack.assert_not_awaited()
    redis_mock.xreadgroup.side_effect = [RuntimeError("stop")]
    with pytest.raises(RuntimeError, match="stop"):
        await gen.__anext__()
    redis_mock.xack.assert_awaited_once_with("swarmbus:stream:t", "g", b"9-0")
    redis_mock.xadd.assert_not_awaited()


def test_dead_letter_key() -> None:
    assert StreamBroker.dead_letter_key(" t ") == "swarmbus:dlq:t"


# --- API key auth ----------------------------------------------------------


async def _auth(x_api_key: str | None = None, authorization: str | None = None) -> int:
    try:
        await main_module.require_api_key(x_api_key=x_api_key, authorization=authorization)
    except HTTPException as exc:
        return exc.status_code
    return 200


@pytest.mark.asyncio
async def test_auth_disabled_when_no_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SWARMBUS_API_KEY", None)
    assert await _auth() == 200
    monkeypatch.setattr(settings, "SWARMBUS_API_KEY", "")
    assert await _auth() == 200


@pytest.mark.asyncio
async def test_auth_required_when_key_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "SWARMBUS_API_KEY", "s3cret")
    assert await _auth() == 401
    assert await _auth(x_api_key="wrong") == 401
    assert await _auth(x_api_key="") == 401
    assert await _auth(authorization="Bearer wrong") == 401
    assert await _auth(authorization="s3cret") == 401
    assert await _auth(authorization="Basic s3cret") == 401
    assert await _auth(x_api_key="s3cret") == 200
    assert await _auth(authorization="Bearer s3cret") == 200
    assert await _auth(authorization="bearer  s3cret ") == 200
    # an explicit wrong X-API-Key is not rescued by a correct bearer token
    assert await _auth(x_api_key="wrong", authorization="Bearer s3cret") == 401


@pytest.mark.parametrize("path", ["/v1/bus/broadcast", "/v1/bus/claim"])
def test_bus_routes_depend_on_api_key(path: str) -> None:
    route = next(r for r in main_module.app.routes if isinstance(r, APIRoute) and r.path == path)
    calls = [dep.call for dep in route.dependant.dependencies]
    assert main_module.require_api_key in calls


def test_health_route_is_public() -> None:
    route = next(
        r for r in main_module.app.routes if isinstance(r, APIRoute) and r.path == "/healthz"
    )
    calls = [dep.call for dep in route.dependant.dependencies]
    assert main_module.require_api_key not in calls


def test_find_cycles_excludes_finished_branches_from_cycle_path() -> None:
    # B is fully explored (dead end) before C closes the loop back to A.
    adjacency: dict[str, set[str]] = {"A": {"B", "C"}, "B": set(), "C": {"A"}}
    assert DeadlockLoopInterceptor._find_cycles(adjacency) == [["A", "C", "A"]]
