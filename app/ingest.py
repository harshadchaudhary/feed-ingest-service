"""The scheduled job: fetch, normalise, deduplicate, store, advance watermark.

The job is written so that running it twice in a row produces no new rows the
second time. Three things combine to give that:

  1. the content hash is the natural key, with a unique constraint behind it
  2. the high-water mark means only newer items are even considered
  3. the distributed lock stops two instances running it concurrently

One feed failing must not abandon the others, so failures are collected per
feed and reported rather than raised.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime

from .fetcher import FeedFetcher, FetchError
from .normalize import (
    FeedParseError,
    deduplicate,
    max_published,
    newer_than,
    parse_feed,
)
from .store import Store

logger = logging.getLogger(__name__)

JOB_NAME = "feed_ingest"
LOCK_KEY = "lock:feed_ingest"


@dataclass
class IngestReport:
    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    feeds_attempted: int = 0
    feeds_failed: int = 0
    items_seen: int = 0
    items_inserted: int = 0
    items_skipped: int = 0
    skipped_run: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return not self.skipped_run and self.feeds_failed == 0

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "feeds_attempted": self.feeds_attempted,
            "feeds_failed": self.feeds_failed,
            "items_seen": self.items_seen,
            "items_inserted": self.items_inserted,
            "items_skipped": self.items_skipped,
            "skipped_run": self.skipped_run,
            "succeeded": self.succeeded,
            "errors": self.errors,
        }


class IngestJob:
    def __init__(
        self,
        store: Store,
        fetcher: FeedFetcher,
        cache,
        feed_urls: list[str],
        lock_ttl_seconds: int = 300,
        clock=None,
    ) -> None:
        self._store = store
        self._fetcher = fetcher
        self._cache = cache
        self._feed_urls = feed_urls
        self._lock_ttl = lock_ttl_seconds
        self._clock = clock or _utcnow
        self._pending_watermark: datetime | None = None

    def run(self) -> IngestReport:
        run_id = uuid.uuid4().hex[:12]
        report = IngestReport(run_id=run_id, started_at=self._clock())
        # Reset per run; a previous run's value must not leak into this one.
        self._pending_watermark = None

        # Overlap prevention. If a previous run is still going, do not start a
        # second one: they would compete for the same feeds and the same rows.
        token = self._cache.acquire_lock(LOCK_KEY, self._lock_ttl)
        if token is None:
            logger.info("run=%s skipped: another run holds the lock", run_id)
            report.skipped_run = True
            report.finished_at = self._clock()
            return report

        try:
            self._run_locked(report)
        finally:
            # Always release, even on an exception, or the TTL becomes the only
            # thing that frees the job.
            self._cache.release_lock(LOCK_KEY, token)

        report.finished_at = self._clock()

        self._store.record_run(
            JOB_NAME,
            success=report.succeeded,
            watermark=self._pending_watermark,
            error="; ".join(report.errors) if report.errors else None,
        )

        logger.info(
            "run=%s finished feeds=%d failed=%d seen=%d inserted=%d skipped=%d",
            run_id,
            report.feeds_attempted,
            report.feeds_failed,
            report.items_seen,
            report.items_inserted,
            report.items_skipped,
        )
        return report

    def _run_locked(self, report: IngestReport) -> None:
        if not self._feed_urls:
            logger.info("run=%s no feeds configured, nothing to do", report.run_id)
            return

        watermark = self._store.get_watermark(JOB_NAME)

        for url in self._feed_urls:
            report.feeds_attempted += 1
            try:
                result = self._fetcher.fetch(url)
                items = parse_feed(result.body, source=url)
            except (FetchError, FeedParseError) as exc:
                # Expected failure of one feed. Record it and carry on with
                # the rest - an upstream outage on one source must not stop
                # collection from every other source.
                report.feeds_failed += 1
                report.errors.append(f"{url}: {exc}")
                logger.warning("run=%s feed failed %s: %s", report.run_id, url, exc)
                continue
            except Exception as exc:
                report.feeds_failed += 1
                report.errors.append(f"{url}: unexpected {type(exc).__name__}: {exc}")
                logger.exception("run=%s unexpected failure for %s", report.run_id, url)
                continue

            report.items_seen += len(items)

            fresh = newer_than(deduplicate(items), watermark)
            inserted, skipped = self._store.insert_new_items(fresh)
            report.items_inserted += inserted
            report.items_skipped += skipped

            # Track the newest publish date across all feeds. Advanced only
            # after the whole run, and only on success, so a partial failure
            # does not skip past items it never stored.
            candidate = max_published(fresh)
            if candidate is not None and (
                self._pending_watermark is None or candidate > self._pending_watermark
            ):
                self._pending_watermark = candidate

            # The items list changed, so any cached page of it is now stale.
            # Delete rather than update: two concurrent writers updating a
            # cached value can interleave and leave the older one in place.
            if inserted:
                self._cache.delete("items:recent")


def _utcnow() -> datetime:
    from datetime import timezone

    return datetime.now(timezone.utc)
