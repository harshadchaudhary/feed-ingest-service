"""Entrypoint. Runs the API, and optionally the in-process scheduler.

Two deployment shapes are supported deliberately, because they are the two
real options and they have different trade-offs:

  1. API + in-process APScheduler (this file, FEED_SCHEDULE_MINUTES set).
     Simple, one container. But every replica runs the timer, so the Redis
     lock is what stops N replicas doing the same work N times.

  2. API only, with the job triggered externally as a Kubernetes CronJob
     calling `python -m app.main run-once`. The cluster guarantees one pod
     per schedule, which is the stronger guarantee - no lock race at all.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

from fastapi.concurrency import run_in_threadpool

from .api import create_app
from .config import get_settings
from .logging_setup import configure_logging

logger = logging.getLogger(__name__)


def build() -> "object":
    """Application factory, for `uvicorn app.main:build --factory`."""
    configure_logging()
    settings = get_settings()

    lifespan = _scheduler_lifespan(settings) if settings.schedule_minutes > 0 else None
    return create_app(settings, lifespan=lifespan)


def _scheduler_lifespan(settings):
    """Build an ASGI lifespan handler that owns the scheduler.

    Lifespan rather than the deprecated on_event hooks, and a context manager
    so shutdown runs even if startup partly failed.
    """

    @asynccontextmanager
    async def lifespan(app):
        scheduler = _build_scheduler(app, settings)

        if scheduler is not None:
            scheduler.start()
            logger.info(
                "scheduler started, every %d minute(s)", settings.schedule_minutes
            )
            if settings.run_on_startup:
                await run_in_threadpool(app.state.job.run)

        try:
            yield
        finally:
            if scheduler is not None:
                # wait=True lets an in-flight run finish and release its lock.
                # Killing it mid-run leaves the lock held until its TTL expires.
                scheduler.shutdown(wait=True)
                logger.info("scheduler stopped")

    return lifespan


def _build_scheduler(app, settings):
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        logger.warning("apscheduler not installed, scheduling disabled")
        return None

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        app.state.job.run,
        trigger="interval",
        minutes=settings.schedule_minutes,
        id="feed_ingest",
        # If a run is still going when the next fire time arrives, do not queue
        # a second one. The lock would refuse it anyway; this avoids the
        # attempt.
        max_instances=1,
        # Collapse missed fire times into one rather than replaying each one
        # after a pause.
        coalesce=True,
        # Tolerate a late fire (GC pause, host hiccup) instead of dropping it.
        misfire_grace_time=60,
    )
    return scheduler


def run_once() -> int:
    """One ingestion run, then exit. For a Kubernetes CronJob or a manual run."""
    configure_logging()
    settings = get_settings()
    app = create_app(settings)
    report = app.state.job.run()

    import json

    print(json.dumps(report.as_dict(), indent=2))
    # Non-zero exit on failure so a CronJob is marked failed and alerts.
    return 0 if report.succeeded else 1


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "run-once":
        return run_once()

    configure_logging()
    settings = get_settings()

    try:
        import uvicorn
    except ImportError:
        print("uvicorn is not installed. pip install -r requirements.txt", file=sys.stderr)
        return 1

    uvicorn.run("app.main:build", factory=True, host="0.0.0.0", port=8000, log_config=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
