"""The parts that need no Azure at all: the message contract, the retry maths, the claim decision, the settings."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from qpipe import InvalidMessage, Settings, backoff, decide, fee_cents, parse, process
from qpipe.messages import MAX_AMOUNT, MAX_BYTES

from .conftest import event

GOOD = json.loads(event())


def test_a_good_message_parses():
    e = parse(event(amount=1999, currency="EUR"))
    assert (e.event_id, e.merchant, e.amount_cents, e.currency) == ("evt-1", "acme", 1999, "EUR")
    assert json.loads(e.to_json()) == {**GOOD, "amount_cents": 1999, "currency": "EUR"}


def bad(**changes):
    doc = {**GOOD, **changes}
    return json.dumps({k: v for k, v in doc.items() if v is not ...})


@pytest.mark.parametrize("raw,fragment", [
    ("not json", "not valid JSON"), ("", "not valid JSON"), ("[1]", "not an object"), ("null", "not an object"), ('"x"', "not an object"),
    (bad(event_id=...), "missing field"), (bad(merchant=...), "missing field"), (bad(amount_cents=...), "missing field"), (bad(currency=...), "missing field"),
    (bad(extra="x"), "unexpected field"),
    (bad(event_id=""), "event_id"), (bad(event_id="a b"), "event_id"), (bad(event_id="a/b"), "event_id"), (bad(event_id="x" * 65), "event_id"),
    (bad(event_id=5), "event_id"), (bad(event_id="é"), "event_id"),
    (bad(merchant=""), "merchant"), (bad(merchant="x" * 65), "merchant"), (bad(merchant="a;b"), "merchant"), (bad(merchant=None), "merchant"),
    (bad(amount_cents=0), "amount_cents"), (bad(amount_cents=-5), "amount_cents"), (bad(amount_cents=MAX_AMOUNT + 1), "amount_cents"),
    (bad(amount_cents=10.5), "amount_cents"), (bad(amount_cents=10.0), "amount_cents"), (bad(amount_cents="10"), "amount_cents"),
    (bad(amount_cents=True), "amount_cents"), (bad(amount_cents=None), "amount_cents"),
    (bad(currency="JPY"), "currency"), (bad(currency="usd"), "currency"), (bad(currency=None), "currency"),
    (json.dumps({**GOOD, "merchant": "x" * (MAX_BYTES + 1)}), "larger than"),
])
def test_invalid_messages_are_refused_with_a_reason(raw, fragment):
    with pytest.raises(InvalidMessage, match=fragment):
        parse(raw)


def test_boundaries_of_what_is_valid():
    assert parse(bad(amount_cents=1)).amount_cents == 1
    assert parse(bad(amount_cents=MAX_AMOUNT)).amount_cents == MAX_AMOUNT
    assert parse(bad(event_id="x" * 64)).event_id == "x" * 64
    assert parse(bad(merchant="x" * 64)).merchant == "x" * 64
    assert parse(bad(event_id="A_b-9")).event_id == "A_b-9"


def test_the_size_limit_counts_bytes_not_characters():
    pad = "é" * (MAX_BYTES // 2)   # two bytes each: over the limit in bytes, under it in characters
    with pytest.raises(InvalidMessage, match="larger than"):
        parse(json.dumps({**GOOD, "merchant": pad}))


def test_a_non_text_message_is_refused():
    with pytest.raises(InvalidMessage):
        parse(b"bytes")  # type: ignore[arg-type]


def test_the_business_step_is_a_pure_function_so_a_redo_writes_the_same_result():
    e = parse(event(amount=10_000))
    assert process(e) == process(e) == {"event_id": "evt-1", "merchant": "acme", "amount_cents": 10_000, "currency": "USD", "fee_cents": 320}
    assert [fee_cents(a) for a in (1, 34, 35, 1000)] == [30, 30, 31, 59]


# --- backoff ------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("count,expected", [(1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (6, 60), (20, 60), (0, 2), (-3, 2)])
def test_backoff_doubles_up_to_the_cap(count, expected):
    assert backoff(count, 2, 60) == expected


# --- the claim decision -------------------------------------------------------------------------------------------

NOW = datetime(2026, 10, 17, 12, 0, 0, tzinfo=timezone.utc)


def row(state="claimed", owner="a", age=0):
    return {"state": state, "owner": owner, "claimed_at": (NOW - timedelta(seconds=age)).isoformat()}


@pytest.mark.parametrize("entity,me,expected", [
    (None, "me", "create"),
    (row("done"), "me", "done"),
    (row("done", owner="me"), "me", "done"),
    (row(owner="other", age=0), "me", "busy"),
    (row(owner="other", age=29.9), "me", "busy"),
    (row(owner="other", age=30), "me", "takeover"),   # exactly as old as the claim lasts: expired
    (row(owner="other", age=300), "me", "takeover"),
    (row(owner="me", age=0), "me", "takeover"),       # my own earlier claim (a retry) is mine to take again
])
def test_decide(entity, me, expected):
    assert decide(entity, me, NOW, 30) == expected


# --- settings -------------------------------------------------------------------------------------------------------

def test_settings_defaults_are_valid():
    Settings("x")


@pytest.mark.parametrize("kw", [
    dict(max_deliveries=0), dict(batch=0), dict(batch=33), dict(visibility_timeout=0), dict(backoff_base=0), dict(defer_seconds=0),
    dict(visibility_timeout=30, claim_seconds=29), dict(poison_queue="orders"),
])
def test_bad_settings_are_refused(kw):
    with pytest.raises(ValueError):
        Settings("x", **kw)


def test_a_claim_may_be_as_short_as_the_visibility_timeout_but_not_shorter():
    Settings("x", visibility_timeout=30, claim_seconds=30)
