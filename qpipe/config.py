from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    connection_string: str
    queue: str = "orders"
    poison_queue: str = "orders-poison"
    table: str = "processed"
    container: str = "results"
    max_deliveries: int = 5  # a message delivered more often than this goes to the poison queue
    visibility_timeout: int = 30  # seconds a received message stays hidden from other workers while it is processed
    claim_seconds: int = 30  # how long one worker's claim on an event keeps other workers away from it
    backoff_base: int = 2  # seconds; the retry delay doubles with each delivery...
    backoff_cap: int = 60  # ...up to this
    defer_seconds: int = 5  # how long to hide a message whose event another worker is working on
    batch: int = 16

    def __post_init__(self):
        if self.max_deliveries < 1 or self.batch < 1 or self.batch > 32:
            raise ValueError("max_deliveries must be at least 1 and batch between 1 and 32 (the queue's own limit)")
        if min(self.visibility_timeout, self.claim_seconds, self.backoff_base, self.backoff_cap, self.defer_seconds) < 1:
            raise ValueError("all times must be at least one second")
        if self.claim_seconds < self.visibility_timeout:
            # A claim that expires before its message becomes visible again lets a second worker take over a worker
            # that is merely slow, so the same event would be processed twice at the same moment.
            raise ValueError("claim_seconds must be at least visibility_timeout")
        if self.queue == self.poison_queue:
            raise ValueError("the poison queue must be a different queue")
