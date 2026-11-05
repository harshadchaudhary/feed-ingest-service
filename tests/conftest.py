"""Shared fixtures.

Every test gets its own in-memory SQLite database, so tests cannot see each
other's rows and can run in any order.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.cache import NullCache
from app.models import Base
from app.store import Store


@pytest.fixture
def session_factory():
    # StaticPool keeps one connection alive; without it each connection gets a
    # separate empty in-memory database and nothing persists between calls.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    engine.dispose()


@pytest.fixture
def store(session_factory):
    return Store(session_factory)


@pytest.fixture
def cache():
    return NullCache()


RSS_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Example Feed</title>
    <item>
      <guid>item-001</guid>
      <title>First   article</title>
      <link>https://Example.COM/articles/one/</link>
      <description>&lt;p&gt;Some  summary&lt;/p&gt;</description>
      <dc:creator>A Writer</dc:creator>
      <pubDate>Tue, 04 Nov 2026 09:30:00 GMT</pubDate>
    </item>
    <item>
      <guid>item-002</guid>
      <title>Second article</title>
      <link>https://example.com/articles/two</link>
      <description>Another summary</description>
      <pubDate>Wed, 05 Nov 2026 11:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

ATOM_SAMPLE = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Example</title>
  <entry>
    <id>urn:uuid:aaaa</id>
    <title>Atom entry</title>
    <link rel="edit" href="https://example.org/edit/1"/>
    <link rel="alternate" href="https://example.org/entry/1"/>
    <summary>Atom summary</summary>
    <author><name>Atom Author</name></author>
    <published>2026-11-04T09:30:00Z</published>
  </entry>
</feed>
"""


@pytest.fixture
def rss_sample():
    return RSS_SAMPLE


@pytest.fixture
def atom_sample():
    return ATOM_SAMPLE
