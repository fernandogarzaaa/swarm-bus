# SwarmBus

SwarmBus is an event-driven multi-agent message broker and coordination fabric built with Python 3.11+, FastAPI, and Redis Streams. It is designed to coordinate autonomous agent task distribution while reducing two common failure modes: concurrent duplicate task execution and runaway cyclic agent handoff loops.

## Features

- Redis Streams publish/subscribe core with consumer group support.
- Atomic Redis task leases using `SET NX PX`.
- Lua-guarded lock release to prevent cross-agent lease deletion.
- Directed-graph trajectory validation with DFS cycle detection.
- FastAPI ingress endpoints for event broadcast and task claim operations.
- Async tests that mock Redis state transitions.
- Static typing and lint configuration for `mypy` and `ruff`.

## Project Layout

```text
swarm_bus/
├── pyproject.toml
├── README.md
├── app/
│   ├── __init__.py
│   ├── broker.py
│   ├── config.py
│   ├── detector.py
│   ├── locker.py
│   └── main.py
└── tests/
    ├── __init__.py
    └── test_bus.py
```

## Requirements

- Python `>=3.11,<3.14`
- Redis reachable from the application runtime
- `uv` recommended for dependency management

## Configuration

SwarmBus reads configuration from environment variables through Pydantic Settings.

| Variable | Default | Purpose |
| --- | --- | --- |
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis connection string |
| `MAX_LOOP_DEPTH` | `4` | Maximum tolerated progressing cycle depth |
| `LOCK_TTL_MS` | `10000` | Redis task lease TTL in milliseconds |
| `AGENT_BUS_ENV` | `production` | Environment label included in brokered events |

## Install

```bash
uv sync --dev
```

If you are not using `uv`, install the runtime and development dependencies from `pyproject.toml` with your preferred Python package manager.

## Run

Start Redis, then run the FastAPI application:

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API

### Broadcast an Agent Event

```bash
curl -X POST http://127.0.0.1:8000/v1/bus/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "task": "build-plan",
    "topic": "agent-events",
    "history": [
      {"from": "planner", "to": "coder", "revision": 1}
    ],
    "payload": {"priority": "high"}
  }'
```

If the handoff history contains a non-progressing communication cycle, SwarmBus returns HTTP `422` and does not publish the event.

### Claim a Task Lease

```bash
curl -X POST http://127.0.0.1:8000/v1/bus/claim \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "task-123",
    "agent_id": "agent-alpha"
  }'
```

Only one agent can hold a task lease for a given `task_id` until the Redis TTL expires or the owning agent releases it.

## Verification

```bash
uv run pytest -q
uv run ruff check .
uv run mypy app tests
```

The current test suite covers race-condition defense for task claiming and graph-based loop interception.
