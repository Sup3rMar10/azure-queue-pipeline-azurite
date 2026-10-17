# azure-queue-pipeline-azurite

![ci](https://github.com/Sup3rMar10/azure-queue-pipeline-azurite/actions/workflows/ci.yml/badge.svg)

**A queue-processing pipeline for Azure Storage Queues that turns at-least-once delivery into effectively-once results, tested against a real Azurite emulator** (Microsoft's Azure Storage emulator, which speaks the actual Queue, Table and Blob protocols to the actual Azure SDKs). Poison messages, retries with backoff, duplicate events, workers that crash at every possible point, workers that are merely slow, and two workers racing: each is a test that runs against real queue, table and blob semantics, not a mock.

> **What is verified, and what is not.** 87 tests pass locally on Python 3.13 with Azurite 3.37 (64 need no Azure at all; 23 run against Azurite). I changed the code on purpose ten times (the poison threshold by one, the claim expiry boundary, the backoff formula, bool accepted as an amount, `mark_done` removed, the result blob made non-overwritable, the claim not released after a failure, a lost pop receipt not recognised, and so on) and tests failed each time. **No Azure subscription is involved.** Azurite is an *emulator*: it shows that my code uses the SDKs and the queue/table/blob protocols correctly, **not** how the real service behaves under load, throttling, latency, regional failover or its exact timing, and nothing here ran on Azure.

## The idea

A storage queue delivers a message *at least* once: if a worker receives it and does not delete it, it becomes visible again after the **visibility timeout**, so a crashed worker's message comes back (and so does a slow worker's). The guarantees come from three things used together (`qpipe/worker.py`):

1. **An idempotent result.** The result goes to a blob named after the event id with canonical content, so doing the work twice writes the same bytes to the same place.
2. **A dedupe row** per event in a table: `claimed` while a worker is processing it, `done` afterwards. A claim is a conditional write (create-if-absent, or replace-if-unchanged by ETag), so two workers racing for one event cannot both win; a claim expires, so a dead worker's event is not stuck forever.
3. **Delete the message last**, only after the result is written and the row is `done`.

```
receive ─▶ invalid? ─────────────────────────────▶ poison queue (reason + original)       never retried
        ─▶ row says done? ─▶ delete                                                        duplicate
        ─▶ delivered too many times? ────────────▶ poison queue
        ─▶ claim: another live claim? ─▶ hide briefly                                      deferred
        ─▶ claim ─▶ process ─▶ write blob ─▶ row done ─▶ delete message                    processed
              └ failure ─▶ release claim ─▶ hide for 1s, 2s, 4s ... (capped)               retry
```

## The crash matrix (each is a test against Azurite)

| Crash point | What happens | Final state |
|---|---|---|
| after claiming | message returns; the dead worker's claim blocks others until it expires (a live claim *defers* the message instead of duplicating work) | one result, `attempts = 2` |
| after writing the result, before `done` | message returns, claim expires, a second worker redoes the step | **one** result, byte for byte identical (the business step ran twice, the result exists once) |
| after `done`, before deleting | message returns, the row says `done`, so it is only deleted | result **not** rewritten (its ETag is unchanged) |
| a worker that is slow, not dead (longer than the visibility timeout) | another worker sees a live claim and defers; the slow one finishes, its delete fails with a stale pop receipt and is treated as "someone else has it now" | one result, no duplicate work |

## Other behaviour that is tested

- **Poison:** invalid messages (not JSON, not an object, missing or extra fields, wrong types, negative or fractional or boolean amounts, unknown currency, oversized) go straight to the poison queue with a reason and are never retried; a message that keeps failing goes there after `max_deliveries` (the limit is exclusive: delivery number `max_deliveries` is still processed). A completed event's message is **never** poisoned, however many times it came back.
- **Retry:** a failure releases the claim and hides the message for `base * 2^(delivery - 1)` seconds, capped.
- **Replay:** after a fix, `replay` moves poison messages back; it leaves invalid ones alone (replaying a bad message only sends it round again) unless told otherwise, and copies to the main queue *before* deleting from the poison queue (a crash duplicates, never loses).
- **Concurrency:** two workers draining 24 events process each exactly once; eight racing for one event give exactly one winner; six racing for an *expired* claim give exactly one winner.

## What the emulator taught me

- **A stale pop receipt is not "not found".** When a message comes back and someone else receives it, the first worker's delete fails with `PopReceiptMismatch` (an `HttpResponseError`), not `ResourceNotFoundError`. My first worker only handled the latter and crashed in the slow-worker test; it now treats both as "this message is no longer mine" and re-raises every other error.
- **My first `peek` was not a peek.** It used `receive_messages`, which hides what it reads and counts a delivery, so it hid the very messages the next step was going to replay. It now uses `peek_messages`, and a test pins that peeking changes neither visibility nor the delivery count.
- **Deferring counts as a delivery.** Every time a message reappears its delivery count rises, even if the worker only deferred it. So the row is checked *before* the delivery limit: an event that is already done is deleted, never poisoned for being deferred a lot.
- Launching Azurite through its `.cmd` wrapper on Windows left an orphaned node process; the tests start it through `node azurite.js` or the `azurite` command, and read a throwaway account key generated per run (nothing secret is committed).

## Honest limits

- **Azurite is not Azure.** Visibility timing, ETag behaviour, pop-receipt errors and dequeue counts are what Azurite does; the real service may differ in details I did not see, and none of its scale, throttling or latency is exercised. Run these tests against a real storage account before trusting them.
- **Exactly-once is not claimed.** The business step can run more than once; only the *result* is single. A handler with other side effects (an email, a payment call) must be idempotent on its own, or sit behind the same claim in a transaction this design does not provide.
- **Claim timing is a trade-off.** `claim_seconds` must be at least `visibility_timeout`, but a longer claim delays recovery after a crash by that long; a handler that runs longer than `claim_seconds` can be taken over while still working (the identical-result design makes that harmless, not free). There is no heartbeat that extends a claim or the visibility timeout.
- **One partition, no ordering.** Storage queues do not guarantee order and the dedupe table uses one partition key; throughput of the dedupe table would need partitioning at scale. The fee in `process` is made up.
- A message whose delivery count exceeds the limit while its claim is held by a crashed worker waits for the claim to expire before it can be judged. `queue` in `stats()` is the service's *approximate* count.
- The `Settings` defaults (30 s timeouts, five deliveries) are illustrations, not tuned values.

## Run it

```
npm install --global azurite        # or: set AZURITE_JS to the path of azurite.js
python -m pip install -r requirements.txt pytest
python -m pytest                    # starts its own Azurite on free ports, one throwaway account per run

# a worker against a local Azurite (default development account), with a mix of good, duplicate, invalid and failing events
azurite --silent --location ./azurite-data &
QPIPE_CONNECTION_STRING="UseDevelopmentStorage=true" python -m qpipe demo
```
