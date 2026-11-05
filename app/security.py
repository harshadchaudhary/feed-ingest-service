"""Webhook signature verification.

A webhook endpoint is public, so anything that arrives is untrusted until the
signature checks out. The signature is computed over the *raw* body: parsing
and re-serialising JSON changes the bytes (key order, whitespace, unicode
escaping) and the HMAC no longer matches.

Comparison is constant-time. A normal `==` on bytes short-circuits at the first
differing byte, and the timing difference is enough to recover a valid
signature one byte at a time.
"""

from __future__ import annotations

import hashlib
import hmac


def compute_signature(secret: str, raw_body: bytes) -> str:
    """HMAC-SHA256 of the raw body, hex encoded, prefixed with the algorithm.

    The `sha256=` prefix follows the convention used by GitHub and others, and
    means the scheme can be changed later without ambiguity.
    """
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(secret: str, raw_body: bytes, provided: str | None) -> bool:
    if not provided:
        return False
    expected = compute_signature(secret, raw_body)
    return hmac.compare_digest(expected, provided.strip())
