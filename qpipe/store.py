"""Everything that touches Azure Storage: the queues, the dedupe table, the result blobs.

The dedupe table holds one row per event. A row is "claimed" while a worker is processing the event and "done" once the
result is written. The claim is taken with a conditional write (create-if-absent, or replace-if-unchanged by ETag), so
two workers racing for the same event cannot both win.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient

from .config import Settings

PARTITION = "event"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def decide(entity: dict | None, owner: str, now: datetime, claim_seconds: int) -> str:
    """What should a worker do about an event, given its row? create | takeover | busy | done. A pure function."""
    if entity is None:
        return "create"
    if entity["state"] == "done":
        return "done"
    age = (now - datetime.fromisoformat(entity["claimed_at"])).total_seconds()
    if entity["owner"] == owner or age >= claim_seconds:
        return "takeover"  # my own claim (a retry), or a claim whose owner has gone quiet for too long
    return "busy"


class Store:
    def __init__(self, settings: Settings, clock: Callable[[], datetime] = utcnow):
        self.s, self.clock = settings, clock
        qs = QueueServiceClient.from_connection_string(settings.connection_string)
        self.queue = qs.get_queue_client(settings.queue)
        self.poison_queue = qs.get_queue_client(settings.poison_queue)
        self.table = TableServiceClient.from_connection_string(settings.connection_string).get_table_client(settings.table)
        self.container = BlobServiceClient.from_connection_string(settings.connection_string).get_container_client(settings.container)

    def ensure(self) -> "Store":
        for create in (self.queue.create_queue, self.poison_queue.create_queue, self.table.create_table, self.container.create_container):
            try:
                create()
            except ResourceExistsError:
                pass
        return self

    # --- the dedupe table -------------------------------------------------------------------------------

    def state(self, event_id: str) -> dict | None:
        try:
            entity = self.table.get_entity(PARTITION, event_id)
        except ResourceNotFoundError:
            return None
        return dict(entity) | {"_etag": entity.metadata["etag"]}

    def claim(self, event_id: str, owner: str) -> str:
        """Try to become the one worker processing this event. Returns claimed | busy | done."""
        for _ in range(3):  # a little contention is normal: look again after losing a race
            entity = self.state(event_id)
            now = self.clock()
            verdict = decide(entity, owner, now, self.s.claim_seconds)
            if verdict in ("done", "busy"):
                return verdict
            row = {"PartitionKey": PARTITION, "RowKey": event_id, "state": "claimed", "owner": owner, "claimed_at": now.isoformat(),
                   "attempts": (entity["attempts"] + 1) if entity else 1}
            try:
                if verdict == "create":
                    self.table.create_entity(row)
                else:
                    self.table.update_entity(row, mode=UpdateMode.REPLACE, etag=entity["_etag"], match_condition=MatchConditions.IfNotModified)
                return "claimed"
            except (ResourceExistsError, ResourceModifiedError):
                continue
        return "busy"

    def mark_done(self, event_id: str) -> None:
        self.table.upsert_entity({"PartitionKey": PARTITION, "RowKey": event_id, "state": "done", "done_at": self.clock().isoformat()},
                                 mode=UpdateMode.MERGE)

    def release(self, event_id: str, owner: str) -> None:
        """Give a claim up after a failure so a retry does not have to wait for it to expire. Only my own, only if unchanged."""
        entity = self.state(event_id)
        if entity and entity["state"] == "claimed" and entity["owner"] == owner:
            try:
                self.table.delete_entity(PARTITION, event_id, etag=entity["_etag"], match_condition=MatchConditions.IfNotModified)
            except (ResourceNotFoundError, ResourceModifiedError):
                pass

    # --- results --------------------------------------------------------------------------------------------

    @staticmethod
    def _blob(event_id: str) -> str:
        return f"{event_id}.json"

    def write_result(self, event_id: str, result: dict) -> None:
        """Idempotent: the blob name comes from the event id and the content is canonical, so a redo overwrites with the same bytes."""
        data = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
        self.container.upload_blob(self._blob(event_id), data, overwrite=True)

    def read_result(self, event_id: str) -> dict | None:
        try:
            return json.loads(self.container.download_blob(self._blob(event_id)).readall())
        except ResourceNotFoundError:
            return None

    def result_etag(self, event_id: str) -> str:
        return self.container.get_blob_client(self._blob(event_id)).get_blob_properties().etag

    # --- the poison queue -------------------------------------------------------------------------------------

    def poison(self, raw: str, reason: str, dequeue_count: int) -> None:
        self.poison_queue.send_message(json.dumps({"reason": reason, "original": raw, "dequeue_count": dequeue_count,
                                                   "at": self.clock().isoformat()}))

    # --- looking ----------------------------------------------------------------------------------------------

    def stats(self) -> dict:
        rows = list(self.table.list_entities())
        return {
            "queue": self.queue.get_queue_properties().approximate_message_count,
            "poison": self.poison_queue.get_queue_properties().approximate_message_count,
            "done": sum(1 for r in rows if r["state"] == "done"),
            "claimed": sum(1 for r in rows if r["state"] == "claimed"),
            "results": sum(1 for _ in self.container.list_blobs()),
        }
