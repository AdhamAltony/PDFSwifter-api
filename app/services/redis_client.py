from __future__ import annotations

from functools import lru_cache
from typing import Optional, TYPE_CHECKING

from app.config import REDIS_URL

if TYPE_CHECKING:
    import redis


@lru_cache(maxsize=1)
def get_redis() -> Optional["redis.Redis"]:
    """Return a configured Redis client, or None if Redis is not configured/available.

    This is intentionally best-effort so the API can still run without Redis.
    """

    if not REDIS_URL:
        return None

    try:
        import redis  # type: ignore
    except Exception:
        return None

    client = redis.Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
        health_check_interval=30,
        retry_on_timeout=True,
    )

    try:
        client.ping()
    except Exception:
        return None

    return client
