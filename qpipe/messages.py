"""The message contract. A message that breaks it can never succeed, so it must not be retried."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

MAX_BYTES = 8192  # far below the queue's own 64 KiB limit: an order event has no business being bigger
CURRENCIES = {"USD", "EUR", "GBP"}
MAX_AMOUNT = 10_000_000  # cents
_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")  # also safe as a table row key
_MERCHANT = re.compile(r"^[A-Za-z0-9 ._-]{1,64}$")
_FIELDS = {"event_id", "merchant", "amount_cents", "currency"}


class InvalidMessage(Exception):
    """The message can never be processed, however often it is retried."""


@dataclass(frozen=True)
class Event:
    event_id: str
    merchant: str
    amount_cents: int
    currency: str

    def to_json(self) -> str:
        return json.dumps({"event_id": self.event_id, "merchant": self.merchant, "amount_cents": self.amount_cents,
                           "currency": self.currency}, separators=(",", ":"))


def parse(raw: str) -> Event:
    if not isinstance(raw, str):
        raise InvalidMessage("the message is not text")
    if len(raw.encode("utf-8")) > MAX_BYTES:
        raise InvalidMessage(f"the message is larger than {MAX_BYTES} bytes")
    try:
        doc = json.loads(raw)
    except ValueError:
        raise InvalidMessage("the message is not valid JSON") from None
    if not isinstance(doc, dict):
        raise InvalidMessage("the message is JSON but not an object")
    missing = sorted(_FIELDS - set(doc))
    if missing:
        raise InvalidMessage(f"missing field(s): {', '.join(missing)}")
    extra = sorted(set(doc) - _FIELDS)
    if extra:
        raise InvalidMessage(f"unexpected field(s): {', '.join(extra)}")
    if not isinstance(doc["event_id"], str) or not _ID.match(doc["event_id"]):
        raise InvalidMessage("event_id must be 1-64 letters, digits, '_' or '-'")
    if not isinstance(doc["merchant"], str) or not _MERCHANT.match(doc["merchant"]):
        raise InvalidMessage("merchant must be 1-64 letters, digits, spaces, '.', '_' or '-'")
    amount = doc["amount_cents"]
    if isinstance(amount, bool) or not isinstance(amount, int) or not 1 <= amount <= MAX_AMOUNT:
        raise InvalidMessage(f"amount_cents must be a whole number from 1 to {MAX_AMOUNT}")
    if doc["currency"] not in CURRENCIES:
        raise InvalidMessage(f"currency must be one of {', '.join(sorted(CURRENCIES))}")
    return Event(doc["event_id"], doc["merchant"], amount, doc["currency"])


def fee_cents(amount_cents: int) -> int:
    """2.9% plus 30 cents, rounded down: a made-up fee, deterministic so a redo gives the identical result."""
    return amount_cents * 29 // 1000 + 30


def process(event: Event) -> dict:
    """The business step. It must be a pure function of the event: a retry has to write the same result."""
    return {"event_id": event.event_id, "merchant": event.merchant, "amount_cents": event.amount_cents,
            "currency": event.currency, "fee_cents": fee_cents(event.amount_cents)}
