"""HTTP fetching with a timeout, bounded retries, and backoff with jitter.

Every outbound call has a timeout. A call without one can hang indefinitely and
hold a worker thread for the life of the process, which is how one slow
upstream takes a whole service down.

Retries are capped and use exponential backoff *with jitter*. Without jitter,
every instance that failed at the same moment retries at the same moment and
builds a thundering herd that keeps the upstream down.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 408 Request Timeout, 429 Too Many Requests, and the 5xx family are transient:
# the same request may well succeed later. A 400 or 404 will not, so retrying
# it just wastes time and hides the real problem.
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    def __init__(self, url: str, attempts: int, reason: str) -> None:
        super().__init__(f"fetch failed for {url} after {attempts} attempt(s): {reason}")
        self.url = url
        self.attempts = attempts
        self.reason = reason


@dataclass(frozen=True, slots=True)
class FetchResult:
    url: str
    status_code: int
    body: str
    attempts: int


def backoff_delay(attempt: int, base_seconds: float, jitter: random.Random | None = None) -> float:
    """Exponential backoff with full jitter: random between 0 and base * 2^n.

    Full jitter spreads retries across the whole window rather than clustering
    them at the end of it, which is what actually prevents a retry storm.
    """
    rng = jitter or random
    ceiling = base_seconds * (2 ** attempt)
    return rng.uniform(0, ceiling)


class FeedFetcher:
    def __init__(
        self,
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        backoff_base_seconds: float = 0.5,
        client=None,
        sleep=time.sleep,
    ) -> None:
        self._timeout = timeout_seconds
        self._max_attempts = max(1, max_attempts)
        self._backoff_base = backoff_base_seconds
        self._client = client
        self._sleep = sleep

    def fetch(self, url: str) -> FetchResult:
        client = self._client or self._build_client()
        last_reason = "unknown"

        for attempt in range(self._max_attempts):
            try:
                response = client.get(url)
            except Exception as exc:
                # A transport error (DNS, connection refused, timeout) is
                # retryable; the request may not have reached the server at all.
                last_reason = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "fetch attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    self._max_attempts,
                    url,
                    last_reason,
                )
            else:
                status = response.status_code
                if status < 400:
                    return FetchResult(
                        url=url,
                        status_code=status,
                        body=response.text,
                        attempts=attempt + 1,
                    )

                last_reason = f"HTTP {status}"
                if status not in RETRYABLE_STATUS:
                    # Permanent. Stop immediately rather than burning the
                    # retry budget on a request that cannot succeed.
                    raise FetchError(url, attempt + 1, last_reason)

                logger.warning(
                    "fetch attempt %d/%d got %s for %s",
                    attempt + 1,
                    self._max_attempts,
                    last_reason,
                    url,
                )

            if attempt < self._max_attempts - 1:
                self._sleep(backoff_delay(attempt, self._backoff_base))

        raise FetchError(url, self._max_attempts, last_reason)

    def _build_client(self):
        import httpx

        return httpx.Client(
            timeout=self._timeout,
            follow_redirects=True,
            headers={"User-Agent": "feed-ingest-service/1.0"},
        )
