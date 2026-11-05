"""Redis cache-aside and a distributed lock, both degrading gracefully.

The rule this module exists to enforce: a cache outage and a cache miss take
the same code path. If Redis is unreachable, every call here logs and returns
the "not cached" answer, so the caller falls through to the database. A cache
that can take the service down has made reliability worse, not better.
"""

from __future__ import annotations

import json
import logging
import secrets
from typing import Any

logger = logging.getLogger(__name__)


class NullCache:
    """Used when Redis is disabled. Always misses, always grants the lock.

    Granting the lock is correct here: with no shared store there is nothing to
    coordinate, so this is single-instance behaviour.
    """

    enabled = False

    def get(self, key: str) -> Any | None:
        return None

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        return None

    def delete(self, *keys: str) -> None:
        return None

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        return "local"

    def release_lock(self, name: str, token: str) -> bool:
        return True

    def ping(self) -> bool:
        return False


class RedisCache:
    enabled = True

    # Release is a compare-and-delete: only delete the key if we still hold it.
    # A plain DELETE would let a caller whose lock had already expired delete
    # the lock a *different* caller now holds.
    _RELEASE_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """

    def __init__(self, client) -> None:
        self._client = client
        self._release = client.register_script(self._RELEASE_SCRIPT)

    def get(self, key: str) -> Any | None:
        try:
            raw = self._client.get(key)
        except Exception as exc:
            logger.warning("cache get failed for %s, falling through: %s", key, exc)
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            # A corrupt or stale-format entry is a miss, not an error.
            logger.warning("cache entry for %s is not valid JSON, treating as miss", key)
            return None

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        try:
            self._client.set(key, json.dumps(value, default=str), ex=ttl_seconds)
        except Exception as exc:
            logger.warning("cache set failed for %s: %s", key, exc)

    def delete(self, *keys: str) -> None:
        if not keys:
            return
        try:
            self._client.delete(*keys)
        except Exception as exc:
            logger.warning("cache delete failed for %s: %s", keys, exc)

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        """SET NX EX: set only if absent, and always with an expiry.

        The expiry is what stops a crashed holder blocking the job forever.
        The token identifies us, so release cannot delete someone else's lock.
        """
        token = secrets.token_hex(16)
        try:
            acquired = self._client.set(name, token, nx=True, ex=ttl_seconds)
        except Exception as exc:
            # Fail OPEN on a lock error. The job is idempotent, so a duplicate
            # run is wasteful but harmless, whereas refusing to run means data
            # silently stops arriving - the worse of the two failures.
            logger.warning("lock acquire failed for %s, proceeding without: %s", name, exc)
            return "degraded"
        return token if acquired else None

    def release_lock(self, name: str, token: str) -> bool:
        if token in ("local", "degraded"):
            return True
        try:
            return bool(self._release(keys=[name], args=[token]))
        except Exception as exc:
            logger.warning("lock release failed for %s: %s", name, exc)
            return False

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:
            return False


def build_cache(redis_enabled: bool, redis_url: str):
    if not redis_enabled:
        logger.info("redis disabled, using null cache")
        return NullCache()
    try:
        import redis as redis_lib

        client = redis_lib.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        logger.info("redis connected")
        return RedisCache(client)
    except Exception as exc:
        # Starting without the cache beats not starting at all.
        logger.warning("redis unavailable (%s), falling back to null cache", exc)
        return NullCache()
