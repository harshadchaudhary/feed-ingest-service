"""FastAPI application: read API, webhook intake, health and manual trigger.

Endpoints are declared `def`, not `async def`, on purpose. SQLAlchemy's driver
here is synchronous, so an `async def` endpoint would block the event loop for
the whole query and stall every other request. A sync endpoint is run by
FastAPI in a threadpool instead, which is the correct pairing for a blocking
driver.
"""

from __future__ import annotations

import json
import logging

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from .cache import build_cache
from .config import Settings, get_settings
from .fetcher import FeedFetcher
from .ingest import JOB_NAME, IngestJob
from .models import build_engine, build_session_factory, create_schema
from .normalize import FeedParseError, deduplicate, parse_feed
from .security import verify_signature
from .store import Store

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# response models
# --------------------------------------------------------------------------

class ItemOut(BaseModel):
    id: int
    source: str
    title: str
    url: str
    summary: str | None = None
    author: str | None = None
    published_at: str | None = None
    ingested_at: str


class HealthOut(BaseModel):
    status: str
    cache: str
    last_run_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None
    runs: int = 0
    unprocessed_payloads: int = 0


class RunOut(BaseModel):
    run_id: str
    succeeded: bool
    skipped_run: bool
    feeds_attempted: int
    feeds_failed: int
    items_seen: int
    items_inserted: int
    items_skipped: int
    errors: list[str] = []


def _item_out(row) -> ItemOut:
    return ItemOut(
        id=row.id,
        source=row.source,
        title=row.title,
        url=row.url,
        summary=row.summary,
        author=row.author,
        published_at=row.published_at.isoformat() if row.published_at else None,
        ingested_at=row.ingested_at.isoformat(),
    )


# --------------------------------------------------------------------------
# application factory
# --------------------------------------------------------------------------

def create_app(settings: Settings | None = None, lifespan=None) -> FastAPI:
    settings = settings or get_settings()

    engine = build_engine(settings.database_url)
    session_factory = build_session_factory(engine)
    create_schema(engine)

    store = Store(session_factory)
    cache = build_cache(settings.redis_enabled, settings.redis_url)
    fetcher = FeedFetcher(
        timeout_seconds=settings.fetch_timeout_seconds,
        max_attempts=settings.fetch_max_attempts,
        backoff_base_seconds=settings.fetch_backoff_base_seconds,
    )
    job = IngestJob(
        store=store,
        fetcher=fetcher,
        cache=cache,
        feed_urls=settings.feed_url_list,
        lock_ttl_seconds=settings.lock_ttl_seconds,
    )

    app = FastAPI(
        title="Feed Ingest Service",
        version="1.0.0",
        description=(
            "Scheduled feed ingestion with idempotent upserts, a distributed "
            "lock, high-water-mark incremental fetching and HMAC-verified "
            "webhook intake."
        ),
        # Injected by main.py so the scheduler's lifetime is tied to the
        # app's. Tests pass nothing and get no scheduler.
        lifespan=lifespan,
    )

    # Hung on the app so tests and the scheduler can reach the same instances.
    app.state.settings = settings
    app.state.store = store
    app.state.cache = cache
    app.state.job = job

    def get_store() -> Store:
        return app.state.store

    # ---------------- health ----------------------------------------------

    @app.get("/healthz", response_model=HealthOut, tags=["ops"])
    def healthz(store: Store = Depends(get_store)) -> HealthOut:
        """Liveness and job freshness.

        Deliberately reports job state rather than failing on it. A liveness
        probe that fails because a dependency blipped restarts every pod at
        once and turns a small problem into an outage.
        """
        state = store.get_state(JOB_NAME)
        return HealthOut(
            status="ok",
            cache="redis" if app.state.cache.enabled and app.state.cache.ping() else "none",
            last_run_at=state.last_run_at.isoformat() if state and state.last_run_at else None,
            last_success_at=(
                state.last_success_at.isoformat() if state and state.last_success_at else None
            ),
            last_error=state.last_error if state else None,
            runs=state.runs if state else 0,
            unprocessed_payloads=store.count_unprocessed_raw(),
        )

    # ---------------- read API --------------------------------------------

    @app.get("/items", response_model=list[ItemOut], tags=["items"])
    def list_items(
        source: str | None = None,
        limit: int = 20,
        offset: int = 0,
        store: Store = Depends(get_store),
    ) -> list[ItemOut]:
        if limit < 1 or limit > 100:
            raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
        if offset < 0:
            raise HTTPException(status_code=400, detail="offset must not be negative")

        # Cache-aside, and only for the default first page: that is the hot
        # key. Caching every filter combination would fill the cache with
        # entries read once.
        cache_key = "items:recent"
        cacheable = source is None and offset == 0 and limit == 20

        if cacheable:
            cached = app.state.cache.get(cache_key)
            if cached is not None:
                return [ItemOut(**row) for row in cached]

        rows = [_item_out(r) for r in store.list_items(source, limit, offset)]

        if cacheable:
            app.state.cache.set(
                cache_key,
                [r.model_dump() for r in rows],
                app.state.settings.cache_ttl_seconds,
            )

        return rows

    @app.get("/items/{item_id}", response_model=ItemOut, tags=["items"])
    def get_item(item_id: int, store: Store = Depends(get_store)) -> ItemOut:
        row = store.get_item(item_id)
        if row is None:
            raise HTTPException(status_code=404, detail="item not found")
        return _item_out(row)

    # ---------------- manual run ------------------------------------------

    @app.post("/jobs/ingest/run", response_model=RunOut, tags=["ops"])
    def run_job() -> RunOut:
        report = app.state.job.run()
        return RunOut(**{k: v for k, v in report.as_dict().items() if k in RunOut.model_fields})

    # ---------------- webhook ---------------------------------------------

    @app.post("/webhooks/feed", tags=["webhook"])
    async def receive_webhook(
        request: Request,
        response: Response,
        x_signature: str | None = Header(default=None, alias="X-Signature"),
        x_event_id: str | None = Header(default=None, alias="X-Event-Id"),
        store: Store = Depends(get_store),
    ) -> dict:
        """Accept a pushed feed document.

        Order matters here:
          1. verify the signature over the RAW body
          2. store the raw body (deduplicated on the provider's event id)
          3. return 202 immediately
          4. parse and store items

        Steps 3 and 4 are in the same request only because this service is
        small. The raw row is what makes that safe: if parsing fails, the body
        is already durable and can be reprocessed, so a parser bug costs a
        retry rather than the data.

        This handler is `async` because the raw body has to be awaited - the
        signature must be checked against the exact bytes received, not
        against a re-serialised copy. The blocking database work is therefore
        pushed to a threadpool rather than run on the event loop.
        """
        raw_body = await request.body()

        if not verify_signature(app.state.settings.webhook_secret, raw_body, x_signature):
            # 401, not 400: the request is well formed, it is just not
            # authenticated. Do not say which part of the signature was wrong.
            raise HTTPException(status_code=401, detail="invalid signature")

        body_text = raw_body.decode("utf-8", errors="replace")

        raw_id = await run_in_threadpool(
            store.store_raw, source="webhook", event_id=x_event_id, body=body_text
        )
        if raw_id is None:
            # Already seen this event id. Providers retry, so this is the
            # normal path, not an error - and it must stay a 200 or the
            # provider keeps retrying.
            response.status_code = 200
            return {"status": "duplicate", "inserted": 0}

        try:
            items = deduplicate(parse_feed(body_text, source="webhook"))
        except FeedParseError as exc:
            await run_in_threadpool(store.mark_raw_processed, raw_id, error=str(exc))
            # 202: we have accepted and durably stored the payload, we just
            # could not parse it. Returning 400 would make the provider retry
            # a body that will never parse.
            response.status_code = 202
            return {"status": "stored_unparsed", "inserted": 0, "error": str(exc)}

        inserted, skipped = await run_in_threadpool(store.insert_new_items, items)
        await run_in_threadpool(store.mark_raw_processed, raw_id)

        if inserted:
            app.state.cache.delete("items:recent")

        response.status_code = 202
        return {
            "status": "accepted",
            "items_seen": len(items),
            "inserted": inserted,
            "skipped": skipped,
        }

    return app
