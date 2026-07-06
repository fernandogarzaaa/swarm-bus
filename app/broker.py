"""Redis Streams backed asynchronous event broker."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator, Mapping
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from app.config import settings

logger = logging.getLogger(__name__)

RedisScalar = bytes | str | int | float


class StreamBroker:
    """Publish and subscribe to Redis Stream topics with consumer groups."""

    def __init__(self, redis_client: Redis | None = None) -> None:
        self.redis: Redis = redis_client or Redis.from_url(
            settings.REDIS_URL,
            decode_responses=False,
            socket_timeout=5,
            socket_connect_timeout=5,
            health_check_interval=30,
        )

    @staticmethod
    def _stream_key(topic: str) -> str:
        clean_topic = topic.strip()
        if not clean_topic:
            raise ValueError("topic must be a non-empty string")
        return f"swarmbus:stream:{clean_topic}"

    @staticmethod
    def _decode(value: RedisScalar) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @classmethod
    def _decode_event(
        cls,
        stream_id: RedisScalar,
        fields: Mapping[RedisScalar, RedisScalar],
        topic: str,
    ) -> dict[str, Any]:
        raw_payload = fields.get(b"payload")
        if raw_payload is None:
            raw_payload = fields.get("payload")
        if raw_payload is None:
            logger.error(
                "Redis stream entry is missing payload field",
                extra={"stream_id": cls._decode(stream_id), "topic": topic},
            )
            raise ValueError("stream entry missing payload field")

        payload_text = cls._decode(raw_payload)
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            logger.error(
                "Redis stream entry contains malformed JSON payload",
                extra={"stream_id": cls._decode(stream_id), "topic": topic},
            )
            raise ValueError("stream entry payload is not valid JSON") from exc

        if not isinstance(payload, dict):
            logger.error(
                "Redis stream entry payload is not a JSON object",
                extra={"stream_id": cls._decode(stream_id), "topic": topic},
            )
            raise ValueError("stream entry payload must be a JSON object")

        payload["_stream_id"] = cls._decode(stream_id)
        payload["_topic"] = topic
        return payload

    async def publish_event(self, topic: str, message: dict[str, Any]) -> str:
        """Append a JSON-serialized event payload to a Redis Stream."""

        stream_key = self._stream_key(topic)
        try:
            serialized_message = json.dumps(message, separators=(",", ":"), sort_keys=True)
            stream_id = await self.redis.xadd(stream_key, {"payload": serialized_message})
            decoded_stream_id = self._decode(stream_id)
            logger.info(
                "Published event to Redis stream",
                extra={"topic": topic, "stream_key": stream_key, "stream_id": decoded_stream_id},
            )
            return decoded_stream_id
        except (TypeError, ValueError) as exc:
            logger.exception("Failed to serialize event payload", extra={"topic": topic})
            raise ValueError("message must be JSON serializable") from exc
        except RedisError:
            logger.exception("Redis publish operation failed", extra={"topic": topic})
            raise

    async def _ensure_group(self, stream_key: str, consumer_group: str) -> None:
        try:
            await self.redis.xgroup_create(stream_key, consumer_group, id="0", mkstream=True)
            logger.info(
                "Created Redis consumer group",
                extra={"stream_key": stream_key, "consumer_group": consumer_group},
            )
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    "Redis consumer group already exists",
                    extra={"stream_key": stream_key, "consumer_group": consumer_group},
                )
                return
            logger.exception(
                "Failed to create Redis consumer group",
                extra={"stream_key": stream_key, "consumer_group": consumer_group},
            )
            raise
        except RedisError:
            logger.exception(
                "Redis consumer group initialization failed",
                extra={"stream_key": stream_key, "consumer_group": consumer_group},
            )
            raise

    async def subscribe_topic(
        self,
        topic: str,
        consumer_group: str,
        consumer_name: str,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield Redis Stream messages from a consumer group and ack after processing."""

        if not consumer_group.strip():
            raise ValueError("consumer_group must be a non-empty string")
        if not consumer_name.strip():
            raise ValueError("consumer_name must be a non-empty string")

        stream_key = self._stream_key(topic)
        await self._ensure_group(stream_key, consumer_group)
        read_pending = True

        while True:
            stream_id_selector = "0" if read_pending else ">"
            try:
                packets = await self.redis.xreadgroup(
                    groupname=consumer_group,
                    consumername=consumer_name,
                    streams={stream_key: stream_id_selector},
                    count=10,
                    block=5_000,
                )
            except RedisError:
                logger.exception(
                    "Redis consumer read failed",
                    extra={
                        "topic": topic,
                        "consumer_group": consumer_group,
                        "consumer_name": consumer_name,
                    },
                )
                raise

            if not packets:
                read_pending = False
                continue

            delivered_any = False
            for _stream_name, entries in packets:
                for stream_id, fields in entries:
                    delivered_any = True
                    decoded_stream_id = self._decode(stream_id)
                    event = self._decode_event(stream_id, fields, topic)
                    try:
                        yield event
                    except GeneratorExit:
                        logger.info(
                            "Consumer generator closed before acknowledgment",
                            extra={"topic": topic, "stream_id": decoded_stream_id},
                        )
                        raise
                    except Exception:
                        logger.exception(
                            "Consumer processing failed before acknowledgment",
                            extra={"topic": topic, "stream_id": decoded_stream_id},
                        )
                        raise
                    else:
                        try:
                            await self.redis.xack(stream_key, consumer_group, stream_id)
                            logger.debug(
                                "Acknowledged Redis stream event",
                                extra={"topic": topic, "stream_id": decoded_stream_id},
                            )
                        except RedisError:
                            logger.exception(
                                "Redis acknowledgment failed",
                                extra={"topic": topic, "stream_id": decoded_stream_id},
                            )
                            raise

            if not delivered_any:
                read_pending = False
