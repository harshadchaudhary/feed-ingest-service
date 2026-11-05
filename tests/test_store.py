"""Storage behaviour: idempotent inserts, webhook dedup, watermark movement."""

from datetime import datetime, timezone

from app.ingest import JOB_NAME
from app.normalize import parse_feed


def test_insert_is_idempotent(store, rss_sample):
    items = parse_feed(rss_sample, source="example")

    inserted, skipped = store.insert_new_items(items)
    assert (inserted, skipped) == (2, 0)

    # Running the job again must insert nothing. This is the headline property.
    inserted, skipped = store.insert_new_items(items)
    assert (inserted, skipped) == (0, 2)

    assert store.count_items() == 2


def test_insert_continues_past_a_duplicate_in_the_same_batch(store, rss_sample):
    items = parse_feed(rss_sample, source="example")
    # A duplicate must not roll back the rest of the batch.
    batch = [items[0], items[0], items[1]]
    inserted, skipped = store.insert_new_items(batch)
    assert inserted == 2
    assert skipped == 1
    assert store.count_items() == 2


def test_list_items_newest_first(store, rss_sample):
    store.insert_new_items(parse_feed(rss_sample, source="example"))
    rows = store.list_items()
    assert [r.title for r in rows] == ["Second article", "First article"]


def test_list_items_filters_by_source(store, rss_sample):
    store.insert_new_items(parse_feed(rss_sample, source="example"))
    store.insert_new_items(parse_feed(rss_sample, source="other"))
    assert store.count_items() == 4
    assert len(store.list_items(source="other")) == 2


def test_list_items_paginates(store, rss_sample):
    store.insert_new_items(parse_feed(rss_sample, source="example"))
    page1 = store.list_items(limit=1, offset=0)
    page2 = store.list_items(limit=1, offset=1)
    assert page1[0].id != page2[0].id


def test_get_item_returns_none_when_missing(store):
    assert store.get_item(9999) is None


# -- raw payloads ----------------------------------------------------------

def test_store_raw_deduplicates_on_event_id(store):
    first = store.store_raw("webhook", "evt-1", '{"a":1}')
    assert first is not None

    # A retried delivery of the same event is rejected by the unique
    # constraint, which is what makes the webhook idempotent.
    second = store.store_raw("webhook", "evt-1", '{"a":1}')
    assert second is None


def test_store_raw_allows_many_without_event_id(store):
    # A NULL event_id cannot collide, so an unidentified delivery is stored
    # rather than silently dropped.
    assert store.store_raw("webhook", None, "a") is not None
    assert store.store_raw("webhook", None, "b") is not None


def test_mark_raw_processed_clears_the_backlog(store):
    raw_id = store.store_raw("webhook", "evt-2", "body")
    assert store.count_unprocessed_raw() == 1
    store.mark_raw_processed(raw_id)
    assert store.count_unprocessed_raw() == 0


def test_mark_raw_processed_records_the_error(store, session_factory):
    from app.models import RawPayload

    raw_id = store.store_raw("webhook", "evt-3", "bad")
    store.mark_raw_processed(raw_id, error="not valid XML")

    with session_factory() as session:
        row = session.get(RawPayload, raw_id)
        assert row.error == "not valid XML"
        assert row.processed_at is not None


# -- job state -------------------------------------------------------------

def test_record_run_creates_state_and_advances_watermark(store):
    mark = datetime(2026, 11, 5, 11, 0, tzinfo=timezone.utc)
    store.record_run(JOB_NAME, success=True, watermark=mark)

    state = store.get_state(JOB_NAME)
    assert state.runs == 1
    assert state.last_success_at is not None
    assert state.last_error is None
    assert store.get_watermark(JOB_NAME) == mark.replace(tzinfo=None)


def test_watermark_never_moves_backwards(store):
    later = datetime(2026, 11, 5, 11, 0, tzinfo=timezone.utc)
    earlier = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)

    store.record_run(JOB_NAME, success=True, watermark=later)
    store.record_run(JOB_NAME, success=True, watermark=earlier)

    # An older batch must not rewind progress, or every later run re-fetches.
    assert store.get_watermark(JOB_NAME) == later.replace(tzinfo=None)


def test_failed_run_records_error_and_leaves_watermark(store):
    mark = datetime(2026, 11, 5, 11, 0, tzinfo=timezone.utc)
    store.record_run(JOB_NAME, success=True, watermark=mark)
    store.record_run(JOB_NAME, success=False, error="upstream 503")

    state = store.get_state(JOB_NAME)
    assert state.runs == 2
    assert state.last_error == "upstream 503"
    # The watermark must not advance on a failure, or the items that were
    # never stored are skipped forever.
    assert store.get_watermark(JOB_NAME) == mark.replace(tzinfo=None)


def test_get_watermark_is_none_before_any_run(store):
    assert store.get_watermark(JOB_NAME) is None
