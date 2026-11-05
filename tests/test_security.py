"""Signature verification."""

from app.security import compute_signature, verify_signature

SECRET = "shhh"


def test_signature_is_prefixed_and_hex():
    sig = compute_signature(SECRET, b"payload")
    assert sig.startswith("sha256=")
    assert len(sig) == len("sha256=") + 64


def test_signature_is_deterministic():
    assert compute_signature(SECRET, b"x") == compute_signature(SECRET, b"x")


def test_signature_changes_with_the_body():
    assert compute_signature(SECRET, b"a") != compute_signature(SECRET, b"b")


def test_signature_changes_with_the_secret():
    assert compute_signature("one", b"a") != compute_signature("two", b"a")


def test_verify_accepts_a_matching_signature():
    body = b'{"event":"created"}'
    assert verify_signature(SECRET, body, compute_signature(SECRET, body)) is True


def test_verify_tolerates_surrounding_whitespace():
    body = b"payload"
    assert verify_signature(SECRET, body, "  " + compute_signature(SECRET, body) + " ")


def test_verify_rejects_a_missing_signature():
    assert verify_signature(SECRET, b"payload", None) is False
    assert verify_signature(SECRET, b"payload", "") is False


def test_verify_rejects_a_tampered_body():
    sig = compute_signature(SECRET, b"original")
    assert verify_signature(SECRET, b"tampered", sig) is False


def test_verify_rejects_the_wrong_secret():
    sig = compute_signature("attacker", b"payload")
    assert verify_signature(SECRET, b"payload", sig) is False


def test_verify_rejects_an_unprefixed_digest():
    import hashlib
    import hmac

    bare = hmac.new(SECRET.encode(), b"payload", hashlib.sha256).hexdigest()
    assert verify_signature(SECRET, b"payload", bare) is False


def test_byte_for_byte_body_matters():
    # Re-serialising JSON changes the bytes, so the signature no longer
    # matches. This is why the raw body is used, not a parsed copy.
    compact = b'{"a":1}'
    spaced = b'{"a": 1}'
    assert compute_signature(SECRET, compact) != compute_signature(SECRET, spaced)
