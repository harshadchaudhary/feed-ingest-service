"""The job itself: locking, retries, partial failure, watermark, idempotency."""

import random
from datetime import datetime

import pytest

from app.cache import NullCache
from app.fetcher import FeedFetcher, FetchError, backoff_delay
from app.ingest import JOB_NAME, LOCK_KEY, IngestJob


# -- fakes -----------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class FakeClient:
    """Returns a queued response per call, and records what was requested."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url):
        self.calls.append(url)
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RefusingCache(NullCache):
    """Simulates another instance already holding the lock."""

    def acquire_lock(self, name, ttl_seconds):
        return None


class RecordingCache(NullCache):
    def __init__(self):
        self.deleted = []
        self.released = []

    def delete(self, *keys):
        self.deleted.extend(keys)

    def release_lock(self, name, token):
        self.released.append(name)
        return True


def make_job(store, responses, cache=None, feeds=("https://feed.test/rss",)):
    fetcher = FeedFetcher(
        max_attempts=3,
        backoff_base_seconds=0,
        client=FakeClient(responses),
        sleep=lambda _: None,
    )
    return IngestJob(
        store=store,
        fetcher=fetcher,
        cache=cache or NullCache(),
        feed_urls=list(feeds),
    )


# -- happy path ------------------------------------------------------------

def test_run_ingests_items(store, rss_sample):
    report = make_job(store, [FakeResponse(200, rss_sample)]).run()

    assert report.succeeded
    assert report.items_seen == 2
    assert report.items_inserted == 2
    assert report.items_skipped == 0
    assert store.count_items() == 2


def test_second_run_inserts_nothing(store, rss_sample):
    make_job(store, [FakeResponse(200, rss_sample)]).run()

    # Fresh job, same feed content. The watermark filters the older item and
    # the content hash catches the rest, so nothing new is written.
    second = make_job(store, [FakeResponse(200, rss_sample)]).run()

    assert second.succeeded
    assert second.items_inserted == 0
    assert store.count_items() == 2


def test_run_advances_the_watermark(store, rss_sample):
    make_job(store, [FakeResponse(200, rss_sample)]).run()
    assert store.get_watermark(JOB_NAME) == datetime(2026, 11, 5, 11, 0)


def test_run_invalidates_the_cached_page_on_insert(store, rss_sample):
    cache = RecordingCache()
    make_job(store, [FakeResponse(200, rss_sample)], cache=cache).run()
    assert "items:recent" in cache.deleted


def test_run_does_not_invalidate_when_nothing_inserted(store, rss_sample):
    make_job(store, [FakeResponse(200, rss_sample)]).run()

    cache = RecordingCache()
    make_job(store, [FakeResponse(200, rss_sample)], cache=cache).run()
    assert cache.deleted == []


# -- locking ---------------------------------------------------------------

def test_run_is_skipped_when_the_lock_is_held(store, rss_sample):
    job = make_job(store, [FakeResponse(200, rss_sample)], cache=RefusingCache())
    report = job.run()

    assert report.skipped_run is True
    assert report.succeeded is False
    assert report.items_inserted == 0
    # Nothing was fetched or written, which is the point of the lock.
    assert store.count_items() == 0


def test_lock_is_released_after_a_normal_run(store, rss_sample):
    cache = RecordingCache()
    make_job(store, [FakeResponse(200, rss_sample)], cache=cache).run()
    assert cache.released == [LOCK_KEY]


def test_lock_is_released_even_when_the_run_raises(store):
    cache = RecordingCache()

    class Exploding(FeedFetcher):
        def fetch(self, url):
            raise KeyboardInterrupt("simulated hard failure")

    job = IngestJob(
        store=store,
        fetcher=Exploding(client=FakeClient([])),
        cache=cache,
        feed_urls=["https://feed.test/rss"],
    )

    # KeyboardInterrupt is not caught by the per-feed handler, so it escapes.
    # The lock must still have been released on the way out, or the job stays
    # blocked until the TTL expires.
    with pytest.raises(KeyboardInterrupt):
        job.run()
    assert cache.released == [LOCK_KEY]


# -- failure handling ------------------------------------------------------

def test_one_bad_feed_does_not_stop_the_others(store, rss_sample):
    job = make_job(
        store,
        [
            FakeResponse(500),
            FakeResponse(500),
            FakeResponse(500),
            FakeResponse(200, rss_sample),
        ],
        feeds=("https://bad.test/rss", "https://good.test/rss"),
    )
    report = job.run()

    assert report.feeds_attempted == 2
    assert report.feeds_failed == 1
    assert report.items_inserted == 2        # the good feed still landed
    assert report.succeeded is False         # but the run is reported failed
    assert any("bad.test" in e for e in report.errors)


def test_unparseable_feed_is_recorded_not_raised(store):
    report = make_job(store, [FakeResponse(200, "not xml")]).run()

    assert report.feeds_failed == 1
    assert report.items_inserted == 0
    assert "not valid XML" in report.errors[0]


def test_failed_run_does_not_advance_the_watermark(store):
    make_job(store, [FakeResponse(200, "not xml")]).run()
    assert store.get_watermark(JOB_NAME) is None


def test_no_feeds_configured_is_a_successful_noop(store):
    report = make_job(store, [], feeds=()).run()
    assert report.succeeded
    assert report.feeds_attempted == 0
    assert report.items_inserted == 0


# -- fetcher ---------------------------------------------------------------

def test_fetcher_retries_transient_failures_then_succeeds(rss_sample):
    client = FakeClient([FakeResponse(503), FakeResponse(200, rss_sample)])
    fetcher = FeedFetcher(
        max_attempts=3, backoff_base_seconds=0, client=client, sleep=lambda _: None
    )

    result = fetcher.fetch("https://feed.test/rss")
    assert result.status_code == 200
    assert result.attempts == 2


def test_fetcher_gives_up_after_max_attempts():
    client = FakeClient([FakeResponse(503), FakeResponse(503), FakeResponse(503)])
    fetcher = FeedFetcher(
        max_attempts=3, backoff_base_seconds=0, client=client, sleep=lambda _: None
    )

    with pytest.raises(FetchError) as exc:
        fetcher.fetch("https://feed.test/rss")
    assert exc.value.attempts == 3
    assert len(client.calls) == 3


def test_fetcher_does_not_retry_a_permanent_failure():
    client = FakeClient([FakeResponse(404)])
    fetcher = FeedFetcher(
        max_attempts=3, backoff_base_seconds=0, client=client, sleep=lambda _: None
    )

    with pytest.raises(FetchError):
        fetcher.fetch("https://feed.test/rss")
    # Retrying a 404 would burn the budget on a request that cannot succeed.
    assert len(client.calls) == 1


def test_fetcher_retries_transport_errors(rss_sample):
    client = FakeClient([ConnectionError("refused"), FakeResponse(200, rss_sample)])
    fetcher = FeedFetcher(
        max_attempts=3, backoff_base_seconds=0, client=client, sleep=lambda _: None
    )

    assert fetcher.fetch("https://feed.test/rss").attempts == 2


def test_backoff_grows_and_stays_within_the_jitter_window():
    rng = random.Random(1234)
    # Full jitter: uniform(0, base * 2**n). Bounded above, and the ceiling
    # doubles each attempt.
    for attempt in range(5):
        ceiling = 0.5 * (2 ** attempt)
        assert 0 <= backoff_delay(attempt, 0.5, jitter=rng) <= ceiling


def test_backoff_is_randomised_not_fixed():
    rng = random.Random(7)
    delays = {backoff_delay(3, 0.5, jitter=rng) for _ in range(20)}
    # Without jitter, every instance that failed together retries together.
    assert len(delays) > 1
