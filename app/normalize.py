"""Pure parsing, normalisation and hashing. No I/O, no database, no clock.

Everything in this module is a function of its arguments, which is why it has
real unit tests and no fixtures. All the logic worth getting wrong lives here;
the modules that touch the network and the database stay thin on purpose.

RSS 2.0 and Atom are both handled, because a real feed list contains both.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Atom and Dublin Core namespaces, as they appear in the parsed tags.
ATOM_NS = "{http://www.w3.org/2005/Atom}"
DC_NS = "{http://purl.org/dc/elements/1.1/}"

_WHITESPACE = re.compile(r"\s+")
_TAGS = re.compile(r"<[^>]+>")


@dataclass(frozen=True, slots=True)
class NormalisedItem:
    content_hash: str
    source: str
    title: str
    url: str
    summary: str | None
    author: str | None
    published_at: datetime | None


class FeedParseError(ValueError):
    """The body was not parseable as RSS or Atom."""


# --------------------------------------------------------------------------
# text helpers
# --------------------------------------------------------------------------

def clean_text(value: str | None) -> str:
    """Strip markup, collapse whitespace, normalise Unicode form.

    Unicode normalisation matters for hashing: the same visible string can have
    two different byte sequences (a precomposed e-acute vs e + combining
    accent). Without NFC, those hash differently and the same item is stored
    twice.
    """
    if not value:
        return ""
    text = _TAGS.sub(" ", value)
    text = unicodedata.normalize("NFC", text)
    return _WHITESPACE.sub(" ", text).strip()


def normalise_url(url: str | None) -> str:
    """Trim, drop a trailing slash, and lowercase the scheme and host only.

    The path stays case-sensitive because many sites serve different content
    for different path casing. Only scheme and host are case-insensitive per
    the URL spec.
    """
    if not url:
        return ""
    url = url.strip()
    match = re.match(r"^([A-Za-z][A-Za-z0-9+.\-]*://)([^/?#]*)(.*)$", url)
    if match:
        scheme, host, rest = match.groups()
        url = scheme.lower() + host.lower() + rest
    if url.endswith("/") and len(url) > 1:
        url = url[:-1]
    return url


# --------------------------------------------------------------------------
# dates
# --------------------------------------------------------------------------

def parse_date(value: str | None) -> datetime | None:
    """Parse an RSS (RFC 822) or Atom (ISO 8601) date into aware UTC.

    Returns None rather than raising: one unparseable date must not discard an
    otherwise usable item. A missing published_at is handled downstream.
    """
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None

    # Atom / ISO 8601 first; 'Z' is not accepted by fromisoformat before 3.11.
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return _as_utc(parsed)
    except ValueError:
        pass

    # RSS / RFC 822, e.g. "Tue, 04 Nov 2026 09:30:00 GMT"
    try:
        return _as_utc(parsedate_to_datetime(raw))
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    # A naive timestamp is assumed to be UTC. Storing a mix of naive and aware
    # values is what produces off-by-hours bugs later, so everything is
    # converted at the boundary exactly once.
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# --------------------------------------------------------------------------
# hashing
# --------------------------------------------------------------------------

def content_hash(source: str, identity: str, title: str) -> str:
    """The natural key for an item.

    Built from the source plus the most stable identifier the feed gave us
    (its guid/id if present, otherwise its URL) plus the cleaned title. The
    same item re-fetched hashes identically, which is what makes ingestion
    idempotent.

    The parts are joined with a null byte so that a value containing the
    separator cannot forge a different item's hash.
    """
    parts = "\x00".join(
        [
            source.strip().lower(),
            identity.strip(),
            clean_text(title).lower(),
        ]
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# feed parsing
# --------------------------------------------------------------------------

def parse_feed(body: str, source: str) -> list[NormalisedItem]:
    """Parse an RSS or Atom document into normalised items.

    Items with neither a title nor a URL are skipped: there is nothing to
    identify them by and nothing useful to show.
    """
    try:
        root = ET.fromstring(body.strip())
    except ET.ParseError as exc:
        raise FeedParseError(f"not valid XML: {exc}") from exc

    if root.tag == f"{ATOM_NS}feed":
        raw_items = [_atom_entry(e) for e in root.findall(f"{ATOM_NS}entry")]
    else:
        channel = root.find("channel")
        container = channel if channel is not None else root
        raw_items = [_rss_item(i) for i in container.findall("item")]
        if not raw_items and channel is None and root.tag != "rss":
            raise FeedParseError(f"unrecognised feed root element: {root.tag!r}")

    items: list[NormalisedItem] = []
    for raw in raw_items:
        item = _build(raw, source)
        if item is not None:
            items.append(item)
    return items


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    return element.text


def _rss_item(node: ET.Element) -> dict[str, str | None]:
    author = _text(node.find("author")) or _text(node.find(f"{DC_NS}creator"))
    return {
        "guid": _text(node.find("guid")),
        "title": _text(node.find("title")),
        "url": _text(node.find("link")),
        "summary": _text(node.find("description")),
        "author": author,
        "published": _text(node.find("pubDate")),
    }


def _atom_entry(node: ET.Element) -> dict[str, str | None]:
    # Atom puts the URL in a <link> attribute, preferring rel="alternate".
    url = None
    for link in node.findall(f"{ATOM_NS}link"):
        rel = link.get("rel", "alternate")
        if rel == "alternate":
            url = link.get("href")
            break
        if url is None:
            url = link.get("href")

    author_node = node.find(f"{ATOM_NS}author")
    author = _text(author_node.find(f"{ATOM_NS}name")) if author_node is not None else None

    return {
        "guid": _text(node.find(f"{ATOM_NS}id")),
        "title": _text(node.find(f"{ATOM_NS}title")),
        "url": url,
        "summary": _text(node.find(f"{ATOM_NS}summary"))
        or _text(node.find(f"{ATOM_NS}content")),
        "author": author,
        "published": _text(node.find(f"{ATOM_NS}published"))
        or _text(node.find(f"{ATOM_NS}updated")),
    }


def _build(raw: dict[str, str | None], source: str) -> NormalisedItem | None:
    title = clean_text(raw.get("title"))
    url = normalise_url(raw.get("url"))

    if not title and not url:
        return None

    # Prefer the feed's own identifier; it survives a URL change or a title
    # edit. Fall back to the URL, then to the title.
    identity = (raw.get("guid") or "").strip() or url or title

    summary = clean_text(raw.get("summary")) or None
    author = clean_text(raw.get("author")) or None

    return NormalisedItem(
        content_hash=content_hash(source, identity, title),
        source=source,
        title=title or url,
        url=url,
        summary=summary,
        author=author,
        published_at=parse_date(raw.get("published")),
    )


def deduplicate(items: list[NormalisedItem]) -> list[NormalisedItem]:
    """Drop repeats within a single batch, keeping first occurrence order.

    A feed can legitimately list the same item twice, and two feeds can carry
    the same article. The database unique constraint is the real guard; this
    keeps a batch from fighting itself before it gets there.
    """
    seen: set[str] = set()
    unique: list[NormalisedItem] = []
    for item in items:
        if item.content_hash in seen:
            continue
        seen.add(item.content_hash)
        unique.append(item)
    return unique


def newer_than(
    items: list[NormalisedItem], watermark: datetime | None
) -> list[NormalisedItem]:
    """Keep items published strictly after the watermark.

    Items with no published date are always kept: we cannot prove they are old,
    and the content hash will stop a duplicate anyway. Dropping them would
    silently lose data from feeds that omit dates.
    """
    if watermark is None:
        return list(items)
    cutoff = _as_utc(watermark)
    return [
        item
        for item in items
        if item.published_at is None or item.published_at > cutoff
    ]


def max_published(items: list[NormalisedItem]) -> datetime | None:
    dates = [i.published_at for i in items if i.published_at is not None]
    return max(dates) if dates else None
