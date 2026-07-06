"""Runtime configuration for SwarmBus."""

from __future__ import annotations

import logging

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Environment-backed settings for the SwarmBus runtime."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    REDIS_URL: str = Field(default="redis://127.0.0.1:6379/0")
    MAX_LOOP_DEPTH: int = Field(default=4, ge=1)
    LOCK_TTL_MS: int = Field(default=10_000, ge=100)
    AGENT_BUS_ENV: str = Field(default="production")


try:
    settings = Settings()
except Exception:
    logger.exception("Failed to load SwarmBus settings from environment")
    raise
