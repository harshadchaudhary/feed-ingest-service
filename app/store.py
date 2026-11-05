"""Database access. The only module that writes rows.

The upsert is written so that running the job twice inserts nothing the second
time. That is enforced by a unique constraint, not by a prior SELECT: two
concurrent runs can both pass a check-then-insert, so the database has to be
the arbiter.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from .models import FeedItem, JobState, RawPayload, utcnow
from .normalize import NormalisedItem

logger = logging.getLogger(__name__)


class Store:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    # -- feed items --------------------------------------------------------

    def insert_new_items(self, items: list[NormalisedItem]) -> tuple[int, int]:
        """Insert items, skipping ones already present.

        Returns (inserted, skipped).

        Each item is committed in its own transaction. That is deliberate: a
        single duplicate must not roll back the whole batch, and
        IntegrityError poisons the session until the transaction is unwound.
        """
        inserted = 0
        skipped = 0

        for item in items:
            with self._session_factory() as session:
                row = FeedItem(
                    content_hash=item.content_hash,
                    source=item.source,
                    title=item.title[:1000],
                    url=item.url[:2000],
                    summary=item.summary,
                    author=item.author,
                    published_at=_naive_utc(item.published_at),
                    ingested_at=utcnow().replace(tzinfo=None),
                )
                session.add(row)
                try:
                    session.commit()
                    inserted += 1
                except IntegrityError:
                    # Already ingested. This is the normal, expected path on a
                    # re-run and is not an error.
                    session.rollback()
                    skipped += 1

        return inserted, skipped

    def count_items(self) -> int:
        with self._session_factory() as session:
            return session.scalar(select(func.count()).select_from(FeedItem)) or 0

    def list_items(
        self, source: str | None = None, limit: int = 20, offset: int = 0
    ) -> list[FeedItem]:
        with self._session_factory() as session:
            stmt = select(FeedItem)
            if source:
                stmt = stmt.where(FeedItem.source == source)
            # Newest first. published_at can be NULL, so id is the tiebreak
            # and guarantees a stable order for pagination.
            stmt = stmt.order_by(
                FeedItem.published_at.desc().nullslast(), FeedItem.id.desc()
            )
            stmt = stmt.limit(limit).offset(offset)
            return list(session.scalars(stmt))

    def get_item(self, item_id: int) -> FeedItem | None:
        with self._session_factory() as session:
            return session.get(FeedItem, item_id)

    # -- raw payloads ------------------------------------------------------

    def store_raw(self, source: str, event_id: str | None, body: str) -> int | None:
        """Store a webhook body before parsing it.

        Returns the row id, or None when this event_id was already received -
        which is how a retried delivery is deduplicated.
        """
        with self._session_factory() as session:
            row = RawPayload(source=source, event_id=event_id, body=body)
            session.add(row)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                logger.info(
                    "duplicate webhook delivery ignored source=%s event_id=%s",
                    source,
                    event_id,
                )
                return None
            return row.id

    def mark_raw_processed(self, raw_id: int, error: str | None = None) -> None:
        with self._session_factory() as session:
            row = session.get(RawPayload, raw_id)
            if row is None:
                return
            row.processed_at = utcnow().replace(tzinfo=None)
            row.error = error
            session.commit()

    def count_unprocessed_raw(self) -> int:
        with self._session_factory() as session:
            stmt = (
                select(func.count())
                .select_from(RawPayload)
                .where(RawPayload.processed_at.is_(None))
            )
            return session.scalar(stmt) or 0

    # -- job state ---------------------------------------------------------

    def get_state(self, job_name: str) -> JobState | None:
        with self._session_factory() as session:
            return session.get(JobState, job_name)

    def get_watermark(self, job_name: str) -> datetime | None:
        state = self.get_state(job_name)
        return state.high_water_mark if state else None

    def record_run(
        self,
        job_name: str,
        *,
        success: bool,
        watermark: datetime | None = None,
        error: str | None = None,
    ) -> None:
        """Record the outcome of a run, and advance the watermark on success.

        The watermark only ever moves forward. A batch that happens to contain
        an older item must not rewind it, or every later run re-fetches work
        it has already done.
        """
        with self._session_factory() as session:
            state = session.get(JobState, job_name)
            if state is None:
                state = JobState(job_name=job_name, runs=0)
                session.add(state)

            now = utcnow().replace(tzinfo=None)
            state.runs = (state.runs or 0) + 1
            state.last_run_at = now

            if success:
                state.last_success_at = now
                state.last_error = None
                new_mark = _naive_utc(watermark)
                if new_mark is not None and (
                    state.high_water_mark is None or new_mark > state.high_water_mark
                ):
                    state.high_water_mark = new_mark
            else:
                state.last_error = (error or "unknown error")[:2000]

            session.commit()


def _naive_utc(value: datetime | None) -> datetime | None:
    """Drop the tzinfo after converting to UTC.

    SQLite cannot store an offset, so a mix of aware and naive values compares
    incorrectly. Everything is normalised to naive UTC at this boundary and
    nowhere else.
    """
    if value is None:
        return None
    if value.tzinfo is not None:
        from datetime import timezone

        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value
