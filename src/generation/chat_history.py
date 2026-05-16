from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class ChatHistoryStore:
    def __init__(self, redis_url: str, ttl_seconds: int = 604800) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._ttl_seconds = ttl_seconds

    @staticmethod
    def from_env() -> "ChatHistoryStore":
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        ttl_seconds = int(os.getenv("CHAT_HISTORY_TTL_SECONDS", "604800"))
        return ChatHistoryStore(redis_url=redis_url, ttl_seconds=ttl_seconds)

    @staticmethod
    def _key(session_id: str) -> str:
        return f"rag:chat:history:{session_id}"

    def get_history(self, session_id: str) -> list[dict[str, Any]]:
        try:
            rows = self._redis.lrange(self._key(session_id), 0, -1)
        except RedisError:
            logger.exception("chat_history_read_failed session_id=%s", session_id)
            return []

        messages: list[dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row)
                if isinstance(parsed, dict):
                    messages.append(parsed)
            except json.JSONDecodeError:
                continue
        return messages

    def append_exchange(
        self,
        *,
        session_id: str,
        query: str,
        answer: str,
        sources: list[dict[str, str]],
        model: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        user_message = {
            "id": f"{int(datetime.now(timezone.utc).timestamp() * 1000)}-u",
            "type": "user",
            "content": query,
            "timestamp": now,
        }
        assistant_message = {
            "id": f"{int(datetime.now(timezone.utc).timestamp() * 1000)}-a",
            "type": "assistant",
            "content": answer,
            "timestamp": now,
            "sources": sources,
            "model": model,
        }
        key = self._key(session_id)
        try:
            self._redis.rpush(key, json.dumps(user_message), json.dumps(assistant_message))
            self._redis.expire(key, self._ttl_seconds)
        except RedisError:
            logger.exception("chat_history_write_failed session_id=%s", session_id)
