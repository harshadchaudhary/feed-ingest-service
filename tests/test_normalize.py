"""Tests for the pure layer. No database, no network, no fixtures beyond text."""

from datetime import datetime, timedelta, timezone

import pytest

from app.normalize import (
    FeedParseError,
    clean_text,
    content_hash,
    deduplicate,
    max_published,
    newer_than,
    normalise_url,
    parse_date,
    parse_feed,
)


# -- text ------------------------------------------------------------------

def test_clean_text_strips_markup_and_collapses_whitespace():
    assert clean_text("<p>Hello   there</p>\n\n") == "Hello there"


def test_clean_text_handles_none_and_empty():
    assert clean_text(None) == ""
    assert clean_text("   ") == ""


def test_clean_text_normalises_unicode_so_equal_strings_hash_equally():
    precomposed = "café"            # e-acute as one code point
    decomposed = "café"            # e + combining acute
    assert precomposed != decomposed
    assert clean_text(precomposed) == clean_text(decomposed)


# -- urls ------------------------------------------------------------------

def test_normalise_url_lowercases_scheme_and_host_only():
    assert normalise_url("HTTPS://Example.COM/Path/To") == "https://example.com/Path/To"


def test_normalise_url_drops_trailing_slash():
    assert normalise_url("https://example.com/a/") == "https://example.com/a"


def test_normalise_url_preserves_path_case():
    # Many sites serve different content for different path casing.
    assert normalise_url("https://example.com/AbC") != normalise_url("https://example.com/abc")


# -- dates -----------------------------------------------------------------

def test_parse_date_rfc822():
    parsed = parse_date("Tue, 04 Nov 2026 09:30:00 GMT")
    assert parsed == datetime(2026, 11, 4, 9, 30, tzinfo=timezone.utc)


def test_parse_date_iso8601_with_z():
    assert parse_date("2026-11-04T09:30:00Z") == datetime(2026, 11, 4, 9, 30, tzinfo=timezone.utc)


def test_parse_date_converts_offset_to_utc():
    assert parse_date("2026-11-04T15:00:00+05:30") == datetime(
        2026, 11, 4, 9, 30, tzinfo=timezone.utc
    )


def test_parse_date_returns_none_for_junk_rather_than_raising():
    # One unparseable date must not discard an otherwise usable item.
    assert parse_date("not a date") is None
    assert parse_date(None) is None
    assert parse_date("") is None


# -- hashing ---------------------------------------------------------------

def test_content_hash_is_stable_across_calls():
    a = content_hash("src", "id-1", "A Title")
    b = content_hash("src", "id-1", "A Title")
    assert a == b


def test_content_hash_ignores_title_case_and_whitespace():
    assert content_hash("src", "id-1", "A  Title") == content_hash("src", "id-1", "a title")


def test_content_hash_differs_per_source():
    assert content_hash("src-a", "id-1", "T") != content_hash("src-b", "id-1", "T")


def test_content_hash_separator_cannot_be_forged():
    # Joining with a null byte stops one field's value impersonating two.
    assert content_hash("a", "b", "c") != content_hash("a\x00b", "", "c")


# -- rss parsing -----------------------------------------------------------

def test_parse_rss_extracts_all_fields(rss_sample):
    items = parse_feed(rss_sample, source="example")
    assert len(items) == 2

    first = items[0]
    assert first.title == "First article"            # whitespace collapsed
    assert first.url == "https://example.com/articles/one"   # normalised
    assert first.summary == "Some summary"           # markup stripped
    assert first.author == "A Writer"                # dc:creator fallback
    assert first.published_at == datetime(2026, 11, 4, 9, 30, tzinfo=timezone.utc)
    assert len(first.content_hash) == 64


def test_parse_atom_prefers_alternate_link(atom_sample):
    items = parse_feed(atom_sample, source="atom")
    assert len(items) == 1
    item = items[0]
    # rel="edit" appears first in the document and must not win.
    assert item.url == "https://example.org/entry/1"
    assert item.author == "Atom Author"
    assert item.summary == "Atom summary"


def test_parse_feed_rejects_non_xml():
    with pytest.raises(FeedParseError):
        parse_feed("this is not xml at all", source="x")


def test_parse_feed_rejects_unknown_root():
    with pytest.raises(FeedParseError):
        parse_feed("<something><else/></something>", source="x")


def test_parse_feed_skips_items_with_no_title_and_no_url():
    body = """<rss version="2.0"><channel>
      <item><description>orphan</description></item>
      <item><title>Real</title><link>https://e.com/x</link></item>
    </channel></rss>"""
    items = parse_feed(body, source="x")
    assert len(items) == 1
    assert items[0].title == "Real"


def test_same_feed_parsed_twice_yields_identical_hashes(rss_sample):
    # This is the property that makes ingestion idempotent.
    first = parse_feed(rss_sample, source="example")
    second = parse_feed(rss_sample, source="example")
    assert [i.content_hash for i in first] == [i.content_hash for i in second]


# -- dedup and watermark ---------------------------------------------------

def test_deduplicate_keeps_first_occurrence_order(rss_sample):
    items = parse_feed(rss_sample, source="example")
    duplicated = [items[0], items[1], items[0]]
    unique = deduplicate(duplicated)
    assert len(unique) == 2
    assert unique[0].content_hash == items[0].content_hash


def test_newer_than_filters_older_items(rss_sample):
    items = parse_feed(rss_sample, source="example")
    watermark = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
    fresh = newer_than(items, watermark)
    assert [i.title for i in fresh] == ["Second article"]


def test_newer_than_with_no_watermark_keeps_everything(rss_sample):
    items = parse_feed(rss_sample, source="example")
    assert len(newer_than(items, None)) == len(items)


def test_newer_than_keeps_items_without_a_published_date():
    body = """<rss version="2.0"><channel>
      <item><title>No date</title><link>https://e.com/n</link></item>
    </channel></rss>"""
    items = parse_feed(body, source="x")
    # We cannot prove it is old, and the content hash stops a duplicate, so
    # dropping it would silently lose data from feeds that omit dates.
    assert len(newer_than(items, datetime.now(timezone.utc) + timedelta(days=1))) == 1


def test_max_published_picks_the_latest(rss_sample):
    items = parse_feed(rss_sample, source="example")
    assert max_published(items) == datetime(2026, 11, 5, 11, 0, tzinfo=timezone.utc)


def test_max_published_is_none_when_no_dates():
    body = """<rss version="2.0"><channel>
      <item><title>No date</title><link>https://e.com/n</link></item>
    </channel></rss>"""
    assert max_published(parse_feed(body, source="x")) is None
