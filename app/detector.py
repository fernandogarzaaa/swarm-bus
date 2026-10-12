"""Graph-based interceptor for cyclic multi-agent handoff trajectories."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)


class DeadlockLoopInterceptor:
    """Validate agent handoff trajectories before they enter the broker fabric."""

    _PROGRESSION_KEYS = (
        "payload_state",
        "state",
        "payload",
        "state_hash",
        "revision",
        "version",
        "checkpoint",
    )

    def validate_trajectory(self, execution_history: list[dict[str, Any]]) -> bool:
        """Return False when the active handoff path contains a non-progressing cycle."""

        if not isinstance(execution_history, list):
            raise TypeError("execution_history must be a list of handoff dictionaries")
        if len(execution_history) > settings.MAX_HISTORY_LENGTH:
            raise ValueError(
                f"execution_history exceeds MAX_HISTORY_LENGTH ({settings.MAX_HISTORY_LENGTH})"
            )
        if len(execution_history) < 2:
            return True

        adjacency: dict[str, set[str]] = defaultdict(set)
        node_fingerprints: dict[str, list[str]] = defaultdict(list)

        for index, handoff in enumerate(execution_history):
            source_agent = self._extract_agent(handoff, "from", index)
            target_agent = self._extract_agent(handoff, "to", index)
            adjacency[source_agent].add(target_agent)
            adjacency.setdefault(target_agent, set())
            fingerprint = self._progression_fingerprint(handoff)
            node_fingerprints[source_agent].append(fingerprint)
            node_fingerprints[target_agent].append(fingerprint)

        cycles = self._find_cycles(adjacency)
        if not cycles:
            return True

        for cycle_path in cycles:
            if not self._cycle_is_tolerated(cycle_path, node_fingerprints):
                logger.critical(
                    "SwarmBus circuit breaker intercepted non-progressing agent communication loop",
                    extra={
                        "cycle_path": cycle_path,
                        "max_loop_depth": settings.MAX_LOOP_DEPTH,
                        "execution_history_length": len(execution_history),
                    },
                )
                return False

        logger.warning(
            "Cyclic handoff observed with payload progression inside configured loop depth",
            extra={"cycles": cycles, "max_loop_depth": settings.MAX_LOOP_DEPTH},
        )
        return True

    @classmethod
    def _cycle_is_tolerated(
        cls,
        cycle_path: list[str],
        node_fingerprints: dict[str, list[str]],
    ) -> bool:
        """A cycle is tolerated only if it progresses and spans <= MAX_LOOP_DEPTH agents.

        ``cycle_path`` is closed (its first node is repeated at the end), so the
        number of distinct agents in the cycle is ``len(cycle_path) - 1``.
        """

        cycle_agents = len(cycle_path) - 1
        has_progression = cls._cycle_has_progression(cycle_path, node_fingerprints)
        return has_progression and cycle_agents <= settings.MAX_LOOP_DEPTH

    @staticmethod
    def _extract_agent(handoff: dict[str, Any], key: str, index: int) -> str:
        if not isinstance(handoff, dict):
            raise TypeError(f"execution_history[{index}] must be a dictionary")
        value = handoff.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"execution_history[{index}].{key} must be a non-empty string")
        return value.strip()

    @classmethod
    def _progression_fingerprint(cls, handoff: dict[str, Any]) -> str:
        progression_payload: dict[str, Any] = {}
        for key in cls._PROGRESSION_KEYS:
            if key in handoff:
                progression_payload[key] = handoff[key]

        if not progression_payload:
            progression_payload = {
                key: value for key, value in handoff.items() if key not in {"from", "to"}
            }

        if not progression_payload:
            return "no-progression-state"

        serialized = json.dumps(
            progression_payload,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _cycle_has_progression(
        cycle_path: Iterable[str],
        node_fingerprints: dict[str, list[str]],
    ) -> bool:
        observed_fingerprints: set[str] = set()
        for node in cycle_path:
            observed_fingerprints.update(node_fingerprints.get(node, []))
        return (
            len(observed_fingerprints) > 1 and "no-progression-state" not in observed_fingerprints
        )

    @staticmethod
    def _find_cycles(adjacency: dict[str, set[str]]) -> list[list[str]]:
        """Return one closed cycle path per DFS back edge, e.g. ``["A", "B", "A"]``.

        Iterative DFS, so arbitrarily long acyclic histories cannot raise
        ``RecursionError``. Neighbours are visited in sorted order for determinism.
        """

        visiting: set[str] = set()
        visited: set[str] = set()
        cycles: list[list[str]] = []

        for root in sorted(adjacency):
            if root in visited:
                continue
            path: list[str] = [root]
            visiting.add(root)
            frames: list[tuple[str, list[str]]] = [(root, sorted(adjacency.get(root, set())))]
            while frames:
                node, pending = frames[-1]
                if not pending:
                    frames.pop()
                    path.pop()
                    visiting.discard(node)
                    visited.add(node)
                    continue
                neighbor = pending.pop(0)
                if neighbor in visiting:
                    cycle_start = path.index(neighbor)
                    cycles.append([*path[cycle_start:], neighbor])
                elif neighbor not in visited:
                    visiting.add(neighbor)
                    path.append(neighbor)
                    frames.append((neighbor, sorted(adjacency.get(neighbor, set()))))
        return cycles
