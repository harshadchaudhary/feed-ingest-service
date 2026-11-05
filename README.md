# Feed Ingest Service

A scheduled ingestion service in Python: it polls RSS/Atom feeds, normalises and
deduplicates the items, stores them, and serves them over a small read API. It
also accepts pushed feeds through an HMAC-verified webhook.

The point of the project is the **correctness properties**, not the feed parsing.
Everything here is a scheduled-job problem that shows up in real systems:

| Property | How it is achieved | Proven by |
|---|---|---|
| **Idempotent ingestion** | SHA-256 content hash as the natural key, behind a unique constraint | `test_insert_is_idempotent`, and a live second run inserting 0 of 20 |
| **Resumable** | A persisted high-water mark; only newer items are considered | `test_run_advances_the_watermark`, `test_watermark_never_moves_backwards` |
| **No overlapping runs** | Redis `SET NX EX` lock with a TTL and compare-and-delete release | `test_run_is_skipped_when_the_lock_is_held`, `test_lock_is_released_even_when_the_run_raises` |
| **Survives upstream failure** | Timeout on every call, bounded retries, exponential backoff with full jitter, no retry on permanent errors | `test_fetcher_*`, `test_one_bad_feed_does_not_stop_the_others` |
| **No data loss on a parser bug** | The raw webhook body is stored *before* parsing | `test_webhook_stores_an_unparseable_body_rather_than_losing_it` |
| **Webhook is safe and idempotent** | HMAC-SHA256 over the raw body, constant-time compare, dedup on the provider's event id | `test_security.py`, `test_webhook_redelivery_of_the_same_event_is_a_noop` |
| **Cache cannot cause an outage** | A cache error takes the same path as a cache miss | `test_get_returns_none_when_redis_is_down`, `test_lock_fails_open_when_redis_is_down` |
| **Silent stoppage is detectable** | `/healthz` reports last run, last success and last error | `test_healthz_reports_run_state_after_a_run` |

## Running it

Nothing needs to be installed but Python. SQLite is the default database and
Redis is off by default.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt    # Windows
# source .venv/bin/activate && pip install -r requirements-dev.txt   # Linux/macOS

python -m pytest                     # 95 tests
python -m app.main                   # API on http://127.0.0.1:8000
```

Interactive API docs at <http://127.0.0.1:8000/docs>.

One ingestion run then exit — this is what a Kubernetes CronJob would call:

```bash
FEED_FEED_URLS="https://hnrss.org/frontpage" python -m app.main run-once
```

Full stack with Postgres and Redis:

```bash
docker compose up --build
```

## Configuration

Every setting is an environment variable prefixed `FEED_`, or a line in `.env`.
See `.env.example`; every value there is already the default, so an empty `.env`
is valid. `FEED_WEBHOOK_SECRET` must be changed for any real deployment.

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Status, cache backend, last run / last success / last error, unprocessed payload count |
| `GET` | `/items` | Newest first. `?source=`, `?limit=` (1–100), `?offset=` |
| `GET` | `/items/{id}` | One item, or `404` |
| `POST` | `/jobs/ingest/run` | Trigger a run now and return its report |
| `POST` | `/webhooks/feed` | Accept a pushed RSS/Atom document |

### Calling the webhook

The signature is HMAC-SHA256 of the **raw** body, hex encoded, prefixed
`sha256=`. It must be computed over the exact bytes sent: parsing and
re-serialising changes key order and whitespace, and the signature then fails.

```bash
BODY=$(cat feed.xml)
SIG="sha256=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$FEED_WEBHOOK_SECRET" -hex | awk '{print $2}')"

curl -X POST http://127.0.0.1:8000/webhooks/feed \
  -H "X-Signature: $SIG" \
  -H "X-Event-Id: evt-1" \
  --data-binary "$BODY"
```

Response codes, and why each one:

| Code | Meaning | Reasoning |
|---|---|---|
| `202` | Accepted and stored | The work may not be finished, so not `200` |
| `202` + `stored_unparsed` | Durably stored, could not be parsed | `400` would make the provider retry a body that will never parse |
| `200` + `duplicate` | This event id was already received | Must be a 2xx, or the provider retries forever |
| `401` | Signature missing or wrong | The request is well formed, just not authenticated |

## Design notes

**Why the pure layer is separate.** `app/normalize.py` has no I/O, no database
and no clock — everything is a function of its arguments. That is why
`tests/test_normalize.py` needs no fixtures and no infrastructure, and it is
where all the logic worth getting wrong lives.

**Why endpoints are `def`, not `async def`.** The SQLAlchemy driver here is
synchronous. An `async def` endpoint would block the event loop for the whole
query and stall every other request; a `def` endpoint is run by FastAPI in a
threadpool instead. The webhook handler is the exception: it is `async` because
the raw body must be awaited, so its blocking database work is pushed to a
threadpool explicitly with `run_in_threadpool`.

**Why inserts are one transaction each.** A single duplicate must not roll back
the whole batch, and an `IntegrityError` poisons the session until the
transaction is unwound. So the uniqueness check is left to the database rather
than a prior `SELECT` — two concurrent runs can both pass a check-then-insert.

**Why the lock fails open.** If Redis is unreachable, `acquire_lock` returns a
`degraded` token and the run proceeds. The job is idempotent, so a duplicate run
is wasteful but harmless, whereas refusing to run means data silently stops
arriving — the worse of the two failures.

**Why items without a published date are always kept.** We cannot prove they
are old, and the content hash stops a duplicate anyway. Dropping them would
silently lose data from feeds that omit dates.

**Two deployment shapes.** The in-process APScheduler is simple but every
replica runs the timer, so the Redis lock is what prevents N replicas doing the
same work N times. `run-once` as a Kubernetes CronJob is the stronger guarantee:
the cluster creates exactly one pod per schedule, so there is no lock race at
all. Both are supported because both are real choices.

## What is verified, and what is not

Verified by running it:

- **95/95 tests pass** (`python -m pytest`).
- **The server boots and every endpoint behaves**: `/healthz` 200, `/items` 200,
  `/items/{id}` 200 and 404, `/items?limit=0` 400, `/jobs/ingest/run` 200,
  `/openapi.json` 200.
- **Webhook auth works live**: no signature → 401, wrong signature → 401, valid
  signature → 202 with 2 items inserted.
- **Webhook idempotency works live**: the same `X-Event-Id` returned
  `200 duplicate, inserted 0`; a *new* event id with the *same* content returned
  `202, inserted 0, skipped 2` — so both layers of deduplication were exercised
  independently.
- **Real network ingestion works**: against `hnrss.org/frontpage`, run 1 saw 20
  items and inserted 20; run 2 immediately after saw 20 and inserted 0, with the
  high-water mark advanced. 20 rows in the database.

**Not verified:**

- **The Redis code path has never run against a real Redis server.** No Redis
  was available on the machine this was built on. The cache and lock are covered
  by unit tests, including the failure paths, but those use a fake client. The
  `SET NX EX` call, the Lua compare-and-delete release, and genuine contention
  between two instances are **untested against a live broker**.
- **`docker compose up` has not been run**, so the Postgres path and the
  health-gated startup ordering are unproven. SQLite is the only database this
  has actually used.

## Layout

```
app/
  config.py         typed settings from the environment
  models.py         SQLAlchemy schema, engine and session factory
  normalize.py      PURE: parsing, cleaning, hashing, dedup, watermark filter
  store.py          the only module that writes rows
  cache.py          Redis cache-aside and lock, both degrading gracefully
  fetcher.py        HTTP with timeout, bounded retries, jittered backoff
  security.py       HMAC signature computation and constant-time verification
  ingest.py         the job: lock, fetch, normalise, store, advance watermark
  api.py            FastAPI app and endpoints
  logging_setup.py  one JSON object per log line
  main.py           entrypoint: API + scheduler, or `run-once`
tests/              95 tests
```
