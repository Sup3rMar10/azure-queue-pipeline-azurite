"""The worker: at-least-once delivery turned into effectively-once results.

A storage queue delivers a message at least once. A message that is received but not deleted becomes visible again
after the visibility timeout, so a crashed worker's message comes back, and so does one that was merely slow. The
guarantees here come from three things working together:

  * the result is written to a blob whose name comes from the event id, with canonical content, so a redo is harmless
  * a dedupe row (claimed -> done) in a table, taken with a conditional write, so two workers do not process one event
  * deleting the message only AFTER the result is written and the row is marked done

Every order of "crash" between those steps ends with exactly one correct result and, eventually, no message left.
A message that can never succeed (invalid) goes straight to the poison queue; one that keeps failing goes there after
max_deliveries; a failure that might pass is retried later with a growing delay.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Callable

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

from .messages import Event, InvalidMessage, parse, process
from .store import Store


def receipt_lost(error: Exception) -> bool:
    """True if the queue says this worker no longer holds the message: it was deleted (MessageNotFound), or it became visible
    again and was received or updated by someone else, which changes the pop receipt (PopReceiptMismatch). Any other error is not this."""
    if isinstance(error, ResourceNotFoundError):
        return True
    code = getattr(error, "error_code", None)
    return isinstance(error, HttpResponseError) and getattr(code, "value", code) in {"PopReceiptMismatch", "MessageNotFound"}


class SimulatedCrash(BaseException):
    """Raised by a test hook to stand in for the process being killed. A BaseException, so no `except Exception` hides it."""


@dataclass
class Outcome:
    kind: str  # processed | duplicate | poison | retry | deferred
    event_id: str | None = None
    detail: str = ""


def backoff(dequeue_count: int, base: int, cap: int) -> int:
    """Seconds to hide a failed message: base, 2*base, 4*base ... up to cap. The first delivery has dequeue_count 1."""
    return min(cap, base * 2 ** (max(dequeue_count, 1) - 1))


class Worker:
    def __init__(self, store: Store, name: str | None = None, handler: Callable[[Event], dict] = process,
                 hooks: dict[str, Callable[[], None]] | None = None):
        self.store, self.s = store, store.s
        self.name = name or f"{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.handler, self.hooks = handler, hooks or {}

    def _hook(self, point: str) -> None:
        if point in self.hooks:
            self.hooks[point]()

    def run_once(self) -> list[Outcome]:
        """Receive up to one batch and handle each message. Returns what happened to each."""
        messages = self.store.queue.receive_messages(max_messages=self.s.batch, messages_per_page=self.s.batch,
                                                     visibility_timeout=self.s.visibility_timeout)
        return [self._handle(m) for m in messages]

    # --- one message ------------------------------------------------------------------------------------------

    def _delete(self, msg) -> bool:
        try:
            self.store.queue.delete_message(msg)
            return True
        except (ResourceNotFoundError, HttpResponseError) as e:
            if receipt_lost(e):  # someone else has the message now, or already finished it
                return False
            raise

    def _hide(self, msg, seconds: int) -> bool:
        """Hide the message for a while. False if this worker no longer holds it."""
        try:
            self.store.queue.update_message(msg, visibility_timeout=seconds)
            return True
        except (ResourceNotFoundError, HttpResponseError) as e:
            if receipt_lost(e):
                return False
            raise

    def _to_poison(self, msg, reason: str, event_id: str | None) -> Outcome:
        self.store.poison(msg.content, reason, msg.dequeue_count)
        self._delete(msg)
        return Outcome("poison", event_id, reason)

    def _handle(self, msg) -> Outcome:
        try:
            event = parse(msg.content)
        except InvalidMessage as e:
            return self._to_poison(msg, f"invalid message: {e}", None)  # retrying cannot help

        row = self.store.state(event.event_id)
        if row and row["state"] == "done":  # a duplicate, or a redelivery after a crash that came too late to matter
            self._delete(msg)
            return Outcome("duplicate", event.event_id)
        if msg.dequeue_count > self.s.max_deliveries:
            return self._to_poison(msg, f"delivered {msg.dequeue_count} times (more than {self.s.max_deliveries}) without success", event.event_id)

        verdict = self.store.claim(event.event_id, self.name)
        if verdict == "done":
            self._delete(msg)
            return Outcome("duplicate", event.event_id)
        if verdict == "busy":  # another worker is on it; look again shortly rather than do it twice
            held = self._hide(msg, self.s.defer_seconds)
            return Outcome("deferred", event.event_id, "another worker holds a live claim" + ("" if held else " (and the message is no longer ours)"))

        try:
            self._hook("after_claim")
            result = self.handler(event)
            self.store.write_result(event.event_id, result)
            self._hook("after_result")
            self.store.mark_done(event.event_id)
            self._hook("before_delete")
        except Exception as e:  # noqa: BLE001 - a failure that might pass on another attempt
            self.store.release(event.event_id, self.name)
            delay = backoff(msg.dequeue_count, self.s.backoff_base, self.s.backoff_cap)
            self._hide(msg, delay)
            return Outcome("retry", event.event_id, f"{type(e).__name__}: {e} (next try in {delay}s)")

        gone = not self._delete(msg)
        return Outcome("processed", event.event_id, "the message had already been removed or taken over" if gone else "")
