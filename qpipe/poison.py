"""Looking at and replaying the poison queue. Replaying is for AFTER the cause is fixed."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .store import Store


@dataclass
class ReplaySummary:
    replayed: int = 0
    skipped_invalid: int = 0  # replaying a message that can never be valid only sends it round again
    malformed: int = 0  # not a poison record at all
    reasons: list[str] = field(default_factory=list)


def peek(store: Store, limit: int = 32) -> list[dict]:
    """Read poison records without removing or hiding them (a real peek: it does not change visibility or the delivery count)."""
    out = []
    for m in store.poison_queue.peek_messages(max_messages=limit):
        try:
            out.append(json.loads(m.content))
        except ValueError:
            out.append({"reason": "not a poison record", "original": m.content})
    return out


def replay(store: Store, limit: int = 32, include_invalid: bool = False, hide_seconds: int = 30) -> ReplaySummary:
    """Move poison messages back to the main queue. Invalid ones are left alone unless include_invalid is set.
    What is looked at but not moved stays hidden for hide_seconds, so two replays do not fight over the same messages."""
    summary = ReplaySummary()
    for m in store.poison_queue.receive_messages(max_messages=limit, messages_per_page=limit, visibility_timeout=hide_seconds):
        try:
            body = json.loads(m.content)
            original, reason = body["original"], str(body["reason"])
        except (ValueError, KeyError, TypeError):
            summary.malformed += 1  # leave it: it stays hidden for hide_seconds and is then visible again for a human
            continue
        if reason.startswith("invalid message") and not include_invalid:
            summary.skipped_invalid += 1
            continue
        store.queue.send_message(original)
        store.poison_queue.delete_message(m)  # only after the copy is on the main queue: a crash here duplicates, never loses
        summary.replayed += 1
        summary.reasons.append(reason)
    return summary
