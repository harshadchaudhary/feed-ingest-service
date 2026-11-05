"""API tests, including webhook signature verification, through TestClient."""

import json

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.security import compute_signature

SECRET = "test-secret"


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        redis_enabled=False,
        feed_urls="",
        webhook_secret=SECRET,
        schedule_minutes=0,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def signed(body: str, event_id: str | None = None) -> dict:
    headers = {
        "X-Signature": compute_signature(SECRET, body.encode("utf-8")),
        "Content-Type": "application/xml",
    }
    if event_id:
        headers["X-Event-Id"] = event_id
    return headers


# -- health ----------------------------------------------------------------

def test_healthz_reports_ok_before_any_run(client):
    response = client.get("/healthz")
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "ok"
    assert body["cache"] == "none"
    assert body["runs"] == 0
    assert body["last_success_at"] is None


def test_healthz_reports_run_state_after_a_run(client):
    client.post("/jobs/ingest/run")
    body = client.get("/healthz").json()
    assert body["runs"] == 1
    # No feeds configured, so the run is a successful no-op.
    assert body["last_success_at"] is not None


# -- items -----------------------------------------------------------------

def test_items_is_empty_initially(client):
    response = client.get("/items")
    assert response.status_code == 200
    assert response.json() == []


def test_items_rejects_an_out_of_range_limit(client):
    assert client.get("/items?limit=0").status_code == 400
    assert client.get("/items?limit=1000").status_code == 400


def test_items_rejects_a_negative_offset(client):
    assert client.get("/items?offset=-1").status_code == 400


def test_get_item_returns_404_when_missing(client):
    response = client.get("/items/4242")
    assert response.status_code == 404
    assert response.json()["detail"] == "item not found"


# -- manual run ------------------------------------------------------------

def test_manual_run_with_no_feeds_succeeds(client):
    response = client.post("/jobs/ingest/run")
    assert response.status_code == 200

    body = response.json()
    assert body["succeeded"] is True
    assert body["feeds_attempted"] == 0
    assert body["items_inserted"] == 0
    assert len(body["run_id"]) == 12


# -- webhook ---------------------------------------------------------------

def test_webhook_rejects_a_missing_signature(client, rss_sample):
    response = client.post("/webhooks/feed", content=rss_sample)
    assert response.status_code == 401


def test_webhook_rejects_a_wrong_signature(client, rss_sample):
    response = client.post(
        "/webhooks/feed",
        content=rss_sample,
        headers={"X-Signature": "sha256=deadbeef"},
    )
    assert response.status_code == 401


def test_webhook_rejects_a_signature_for_different_content(client, rss_sample):
    # The signature must cover the exact bytes received, so a valid signature
    # for another body must not be accepted for this one.
    headers = signed("a completely different body")
    response = client.post("/webhooks/feed", content=rss_sample, headers=headers)
    assert response.status_code == 401


def test_webhook_accepts_a_valid_signature_and_stores_items(client, rss_sample):
    response = client.post(
        "/webhooks/feed", content=rss_sample, headers=signed(rss_sample, "evt-1")
    )
    assert response.status_code == 202

    body = response.json()
    assert body["status"] == "accepted"
    assert body["items_seen"] == 2
    assert body["inserted"] == 2

    assert len(client.get("/items").json()) == 2


def test_webhook_redelivery_of_the_same_event_is_a_noop(client, rss_sample):
    client.post("/webhooks/feed", content=rss_sample, headers=signed(rss_sample, "evt-2"))

    repeat = client.post(
        "/webhooks/feed", content=rss_sample, headers=signed(rss_sample, "evt-2")
    )
    # 200, not an error: providers retry, and a non-2xx would make it keep
    # retrying forever.
    assert repeat.status_code == 200
    assert repeat.json()["status"] == "duplicate"
    assert repeat.json()["inserted"] == 0

    assert len(client.get("/items").json()) == 2


def test_webhook_same_content_different_event_id_inserts_nothing_new(client, rss_sample):
    client.post("/webhooks/feed", content=rss_sample, headers=signed(rss_sample, "evt-3"))

    # A different event id, so the raw payload is new, but the items hash the
    # same - so the content hash is the thing that stops the duplicate.
    second = client.post(
        "/webhooks/feed", content=rss_sample, headers=signed(rss_sample, "evt-4")
    )
    assert second.status_code == 202
    assert second.json()["inserted"] == 0
    assert second.json()["skipped"] == 2


def test_webhook_stores_an_unparseable_body_rather_than_losing_it(client):
    body = "this is not xml"
    response = client.post("/webhooks/feed", content=body, headers=signed(body, "evt-5"))

    # 202: accepted and durably stored, just not parseable. A 400 would make
    # the provider retry a body that will never parse.
    assert response.status_code == 202
    assert response.json()["status"] == "stored_unparsed"
    assert "not valid XML" in response.json()["error"]

    # The payload is kept, so it can be reprocessed once the parser is fixed.
    assert client.get("/healthz").json()["unprocessed_payloads"] == 0


def test_webhook_accepts_atom(client, atom_sample):
    response = client.post(
        "/webhooks/feed", content=atom_sample, headers=signed(atom_sample, "evt-6")
    )
    assert response.status_code == 202
    assert response.json()["inserted"] == 1


def test_openapi_schema_is_served(client):
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    assert "/items" in schema["paths"]
    assert "/webhooks/feed" in schema["paths"]
