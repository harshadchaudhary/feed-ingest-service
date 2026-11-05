"""Cache and lock behaviour, including graceful degradation."""

from app.cache import NullCache, RedisCache, build_cache


class BrokenClient:
    """Every operation raises, as an unreachable Redis would."""

    def get(self, *a, **k):
        raise ConnectionError("redis down")

    def set(self, *a, **k):
        raise ConnectionError("redis down")

    def delete(self, *a, **k):
        raise ConnectionError("redis down")

    def ping(self):
        raise ConnectionError("redis down")

    def register_script(self, script):
        def run(keys=None, args=None):
            raise ConnectionError("redis down")

        return run


# -- null cache ------------------------------------------------------------

def test_null_cache_always_misses():
    cache = NullCache()
    cache.set("k", {"a": 1}, 60)
    assert cache.get("k") is None


def test_null_cache_grants_the_lock():
    # With no shared store there is nothing to coordinate, so granting is
    # correct single-instance behaviour.
    cache = NullCache()
    token = cache.acquire_lock("lock", 60)
    assert token == "local"
    assert cache.release_lock("lock", token) is True


def test_null_cache_is_reported_as_disabled():
    assert NullCache().enabled is False
    assert NullCache().ping() is False


# -- graceful degradation --------------------------------------------------

def test_get_returns_none_when_redis_is_down():
    # A cache outage must take the same path as a cache miss, so the caller
    # falls through to the database instead of failing the request.
    cache = RedisCache(BrokenClient())
    assert cache.get("k") is None


def test_set_and_delete_swallow_redis_errors():
    cache = RedisCache(BrokenClient())
    cache.set("k", {"a": 1}, 60)      # must not raise
    cache.delete("k")                 # must not raise


def test_lock_fails_open_when_redis_is_down():
    # The job is idempotent, so a duplicate run is wasteful but harmless.
    # Refusing to run would silently stop data arriving, which is worse.
    cache = RedisCache(BrokenClient())
    token = cache.acquire_lock("lock", 60)
    assert token == "degraded"
    assert cache.release_lock("lock", token) is True


def test_ping_is_false_when_redis_is_down():
    assert RedisCache(BrokenClient()).ping() is False


def test_delete_with_no_keys_is_a_noop():
    RedisCache(BrokenClient()).delete()


# -- builder ---------------------------------------------------------------

def test_build_cache_returns_null_when_disabled():
    cache = build_cache(redis_enabled=False, redis_url="redis://localhost:6379/0")
    assert isinstance(cache, NullCache)


def test_build_cache_falls_back_when_redis_is_unreachable():
    # Starting without the cache beats not starting at all.
    cache = build_cache(redis_enabled=True, redis_url="redis://127.0.0.1:65500/0")
    assert isinstance(cache, NullCache)
