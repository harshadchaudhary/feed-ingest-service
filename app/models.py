"""Database schema and session management.

Three tables:
  feed_items     - the normalised, deduplicated result
  raw_payloads   - webhook bodies, stored before parsing
  job_state      - the high-water mark, so a run resumes instead of restarting
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class FeedItem(Base):
    __tablename__ = "feed_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # The natural key. A content hash over the fields that identify an item,
    # so re-ingesting the same item is a no-op rather than a duplicate row.
    # This is what makes the job idempotent.
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    source: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(1000), nullable=False)
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    author: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # When the item itself says it was published (event time), as opposed to
    # when we happened to store it (processing time). Both are kept, because
    # incremental fetching keys off published_at and debugging keys off
    # ingested_at.
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)

    __table_args__ = (
        UniqueConstraint("content_hash", name="uq_feed_items_content_hash"),
        Index("ix_feed_items_source_published", "source", "published_at"),
    )


class RawPayload(Base):
    """Webhook bodies, stored before any parsing.

    Ingest-then-process: if the parser breaks because a provider changed its
    schema, the data is still here and can be reprocessed. Without this, a
    parse failure is permanent data loss.
    """

    __tablename__ = "raw_payloads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(255), nullable=False)

    # The provider's own event id, when it sends one. Unique so a retried
    # delivery of the same event is rejected at the database level rather
    # than relying on application logic alone.
    event_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    body: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("source", "event_id", name="uq_raw_payloads_source_event"),
    )


class JobState(Base):
    """One row per job, holding its high-water mark.

    The job fetches only items newer than `high_water_mark`, so a crash
    resumes from the last success instead of reprocessing everything.
    """

    __tablename__ = "job_state"

    job_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    high_water_mark: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


def build_engine(database_url: str):
    # check_same_thread=False is required because APScheduler runs the job on a
    # worker thread while FastAPI serves requests on others.
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, connect_args=connect_args, future=True)


def build_session_factory(engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def create_schema(engine) -> None:
    Base.metadata.create_all(engine)
