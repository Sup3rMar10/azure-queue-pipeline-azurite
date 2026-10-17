"""The pipeline against a real Azurite: crashes at every step, redelivery, retries, poison, replay, concurrency."""

import json
import threading
import time

import pytest

from qpipe import SimulatedCrash, Store, Worker, peek, process, parse, replay

from .conftest import Clock, drain, event, kinds, make_settings, worker


class Counting:
    """A handler that counts calls per event, optionally failing the first N calls of each."""

    def __init__(self, fail_first=0, always_fail=False):
        self.calls, self.fail_first, self.always_fail, self.lock = {}, fail_first, always_fail, threading.Lock()

    def __call__(self, ev):
        with self.lock:
            n = self.calls[ev.event_id] = self.calls.get(ev.event_id, 0) + 1
        if self.always_fail or n <= self.fail_first:
            raise ConnectionError("the downstream system is unavailable")
        return process(ev)


def crash_at(point):
    def boom():
        raise SimulatedCrash(point)
    return {point: boom}


def send(store, *raws):
    for r in raws:
        store.queue.send_message(r)


def nothing_left(store):
    s = store.stats()
    return s["queue"] == 0


# --- the ordinary cases -------------------------------------------------------------------------------------------


def test_a_batch_of_good_events_is_processed_stored_marked_done_and_removed(env):
    settings, store = env
    send(store, *[event(i, amount=1000 * i) for i in range(1, 6)])
    outcomes = worker(store).run_once()
    assert kinds(outcomes) == ["processed"] * 5
    for i in range(1, 6):
        assert store.read_result(f"evt-{i}") == process(parse(event(i, amount=1000 * i)))
    s = store.stats()
    assert (s["done"], s["claimed"], s["results"], s["poison"]) == (5, 0, 5, 0)
    assert nothing_left(store)


def test_the_same_event_sent_twice_is_processed_once(env):
    _, store = env
    handler = Counting()
    send(store, event(1), event(1))
    assert kinds(worker(store, handler=handler).run_once()) == ["processed", "duplicate"]
    assert handler.calls == {"evt-1": 1} and store.stats()["results"] == 1 and nothing_left(store)


def test_a_duplicate_that_arrives_much_later_is_still_recognised(env):
    _, store = env
    handler = Counting()
    w = worker(store, handler=handler)
    send(store, event(1))
    w.run_once()
    send(store, event(1))
    assert kinds(w.run_once()) == ["duplicate"] and handler.calls == {"evt-1": 1}


INVALID = ["not json", "[]", json.dumps({"event_id": "x"}), event(1, amount=-5), event(1, amount=12.5), event(1, currency="JPY"),
           json.dumps({**json.loads(event(1)), "extra": 1}), json.dumps({**json.loads(event(1)), "merchant": "x" * 9000})]


def test_invalid_messages_go_straight_to_the_poison_queue_with_a_reason_and_are_never_retried(env):
    _, store = env
    handler = Counting()
    send(store, *INVALID)
    outcomes = worker(store, handler=handler).run_once()
    assert kinds(outcomes) == ["poison"] * len(INVALID) and handler.calls == {}
    records = peek(store)
    assert len(records) == len(INVALID) and all(r["reason"].startswith("invalid message:") for r in records)
    assert {r["original"] for r in records} == set(INVALID)
    assert store.stats()["results"] == 0 and nothing_left(store)


# --- crashes: every point between claiming and deleting ------------------------------------------------------------


def two_workers(connection_string, claim_seconds=2):
    settings = make_settings(connection_string, claim_seconds=claim_seconds)
    clock = Clock()
    a, b = Store(settings, clock).ensure(), Store(settings, clock)
    return settings, clock, a, b


def test_a_crash_after_claiming_is_recovered_by_another_worker_once_the_claim_expires(connection_string):
    settings, clock, sa, sb = two_workers(connection_string)
    send(sa, event(1))
    with pytest.raises(SimulatedCrash):
        worker(sa, "a", hooks=crash_at("after_claim")).run_once()
    assert sa.state("evt-1")["state"] == "claimed" and sa.read_result("evt-1") is None
    assert worker(sb, "b").run_once() == [], "the message is hidden while it is still within its visibility timeout"
    clock.advance(settings.claim_seconds + 1)
    handler = Counting()
    outcomes = drain([worker(sb, "b", handler=handler)], lambda: sb.state("evt-1")["state"] == "done")
    assert "processed" in kinds(outcomes) and handler.calls == {"evt-1": 1}
    assert sb.read_result("evt-1") == process(parse(event(1))) and sb.state("evt-1")["attempts"] == 2
    drain([worker(sb, "b")], lambda: nothing_left(sb))


def test_another_workers_live_claim_defers_the_message_instead_of_processing_it_twice(connection_string):
    settings, clock, sa, sb = two_workers(connection_string, claim_seconds=60)
    send(sa, event(1))
    with pytest.raises(SimulatedCrash):
        worker(sa, "a", hooks=crash_at("after_claim")).run_once()
    handler = Counting()
    wb = worker(sb, "b", handler=handler)
    deadline, outcomes = time.time() + 20, []
    while not outcomes and time.time() < deadline:
        outcomes = wb.run_once()
        time.sleep(0.25)
    assert kinds(outcomes) == ["deferred"] and handler.calls == {} and sb.read_result("evt-1") is None


def test_a_crash_after_writing_the_result_but_before_marking_done_ends_with_one_identical_result(connection_string):
    settings, clock, sa, sb = two_workers(connection_string)
    send(sa, event(1))
    first = Counting()
    with pytest.raises(SimulatedCrash):
        worker(sa, "a", handler=first, hooks=crash_at("after_result")).run_once()
    before = sa.read_result("evt-1")
    assert before == process(parse(event(1))) and sa.state("evt-1")["state"] == "claimed"
    clock.advance(settings.claim_seconds + 1)
    second = Counting()
    drain([worker(sb, "b", handler=second)], lambda: sb.state("evt-1")["state"] == "done")
    assert first.calls == second.calls == {"evt-1": 1}, "the business step ran twice: at-least-once delivery"
    assert sb.read_result("evt-1") == before and sb.stats()["results"] == 1, "...but there is one result, byte for byte the same"
    drain([worker(sb, "b")], lambda: nothing_left(sb))


def test_a_crash_after_marking_done_but_before_deleting_only_deletes_on_redelivery(connection_string):
    settings, clock, sa, sb = two_workers(connection_string)
    send(sa, event(1))
    with pytest.raises(SimulatedCrash):
        worker(sa, "a", hooks=crash_at("before_delete")).run_once()
    assert sa.state("evt-1")["state"] == "done"
    etag = sa.result_etag("evt-1")
    handler = Counting()
    outcomes = drain([worker(sb, "b", handler=handler)], lambda: nothing_left(sb))
    assert kinds(outcomes) == ["duplicate"] and handler.calls == {}
    assert sb.result_etag("evt-1") == etag, "the result was not rewritten"


def test_a_slow_worker_whose_message_came_back_finishes_cleanly_and_nothing_is_done_twice(connection_string):
    settings = make_settings(connection_string, claim_seconds=60)
    sa = Store(settings).ensure()
    sb = Store(settings)
    send(sa, event(1))
    handler = Counting()

    def slow(ev):
        time.sleep(3.2)   # longer than the 2s visibility timeout
        return handler(ev)

    results = {}
    t = threading.Thread(target=lambda: results.update(a=worker(sa, "slow", handler=slow).run_once()))
    t.start()
    time.sleep(2.6)  # the message has reappeared; another worker looks at it while the first is still busy
    b_outcomes = worker(sb, "fast", handler=handler).run_once()
    t.join()
    assert kinds(b_outcomes) == ["deferred"]
    assert kinds(results["a"]) == ["processed"] and "already been removed or taken over" in results["a"][0].detail, "its pop receipt went stale"
    drain([worker(sb, "fast", handler=handler)], lambda: nothing_left(sb))
    assert handler.calls == {"evt-1": 1} and sb.stats()["results"] == 1


# --- failures that might pass: retry with backoff ---------------------------------------------------------------------


def test_a_failure_is_retried_later_with_a_growing_delay_and_then_succeeds(env):
    _, store = env
    handler = Counting(fail_first=2)
    w = worker(store, handler=handler)
    send(store, event(1))
    first = w.run_once()
    assert kinds(first) == ["retry"] and "next try in 1s" in first[0].detail
    assert store.state("evt-1") is None, "the claim was released so a retry does not wait for it to expire"
    assert w.run_once() == [], "hidden for the backoff period"
    second = drain([w], lambda: handler.calls.get("evt-1", 0) >= 2)
    assert "next try in 2s" in second[-1].detail
    drain([w], lambda: store.state("evt-1") and store.state("evt-1")["state"] == "done")
    assert handler.calls == {"evt-1": 3} and store.read_result("evt-1") == process(parse(event(1)))
    assert store.stats()["poison"] == 0


# --- poison after too many deliveries -----------------------------------------------------------------------------------


def test_a_message_that_keeps_failing_is_moved_to_the_poison_queue_after_max_deliveries(connection_string):
    settings = make_settings(connection_string, max_deliveries=3, backoff_base=1, backoff_cap=1)
    store = Store(settings).ensure()
    handler = Counting(always_fail=True)
    send(store, event(1))
    outcomes = drain([worker(store, handler=handler)], lambda: store.stats()["poison"] >= 1 or len(peek(store)) > 0)
    assert kinds(outcomes) == ["retry", "retry", "retry", "poison"] and handler.calls == {"evt-1": 3}
    record = peek(store)[0]
    assert "delivered 4 times" in record["reason"] and record["original"] == event(1) and record["dequeue_count"] == 4
    assert store.read_result("evt-1") is None and store.state("evt-1") is None


def test_the_delivery_limit_is_exclusive_the_last_allowed_delivery_still_processes(connection_string):
    settings = make_settings(connection_string, max_deliveries=1)
    store = Store(settings).ensure()
    send(store, event(1))
    assert kinds(worker(store).run_once()) == ["processed"], "delivery number max_deliveries must not be poisoned"


def test_a_completed_event_is_never_poisoned_however_often_its_message_comes_back(connection_string):
    settings = make_settings(connection_string, max_deliveries=1, claim_seconds=2)
    store = Store(settings).ensure()
    send(store, event(1))
    list(store.queue.receive_messages(visibility_timeout=1))   # delivery 1, by someone who then vanished
    assert store.claim("evt-1", "ghost") == "claimed"
    store.mark_done("evt-1")
    outcomes = drain([worker(store)], lambda: nothing_left(store))
    assert kinds(outcomes) == ["duplicate"] and store.stats()["poison"] == 0


def test_peeking_does_not_hide_or_count_a_delivery(env):
    _, store = env
    store.poison("x", "because", 3)
    for _ in range(3):
        assert len(peek(store)) == 1
    m = next(iter(store.poison_queue.receive_messages(visibility_timeout=1)))
    assert m.dequeue_count == 1


def test_replaying_the_poison_queue_after_a_fix_processes_the_events(connection_string):
    settings = make_settings(connection_string, max_deliveries=1, backoff_base=1, backoff_cap=1)
    store = Store(settings).ensure()
    send(store, event(1), event(2))
    drain([worker(store, handler=Counting(always_fail=True))], lambda: len(peek(store)) == 2)
    summary = replay(store)
    assert summary.replayed == 2 and all("delivered 2 times" in r for r in summary.reasons)
    drain([worker(store)], lambda: store.stats()["done"] == 2)
    assert store.read_result("evt-1") and store.read_result("evt-2")
    assert peek(store) == []


def test_replay_leaves_invalid_messages_alone_unless_told_otherwise(env):
    _, store = env
    send(store, "not json", event(1, amount=-1))
    worker(store).run_once()
    first = replay(store, hide_seconds=2)
    assert (first.replayed, first.skipped_invalid) == (0, 2)
    assert replay(store, hide_seconds=2).skipped_invalid == 0, "what the first replay looked at is hidden from the next"
    time.sleep(2.5)
    forced = replay(store, include_invalid=True)
    assert forced.replayed == 2
    assert kinds(worker(store).run_once()) == ["poison", "poison"], "still invalid, so they go round again: replaying cannot fix a bad message"


def test_a_poison_queue_entry_that_is_not_a_poison_record_is_reported_not_crashed_on(env):
    _, store = env
    store.poison_queue.send_message("hello")
    assert peek(store) == [{"reason": "not a poison record", "original": "hello"}]
    assert replay(store, hide_seconds=2).malformed == 1


# --- concurrency -------------------------------------------------------------------------------------------------------


def test_two_workers_draining_one_queue_process_every_event_exactly_once(env):
    _, store = env
    handler = Counting()
    n = 24
    send(store, *[event(i) for i in range(n)])
    stop = threading.Event()

    def run(name):
        w = worker(Store(store.s), name, handler=handler)
        while not stop.is_set():
            w.run_once()
            time.sleep(0.05)

    threads = [threading.Thread(target=run, args=(f"w{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.time() + 40
    while time.time() < deadline and store.stats()["done"] < n:
        time.sleep(0.25)
    stop.set()
    for t in threads:
        t.join()
    assert store.stats()["done"] == n and store.stats()["results"] == n
    assert handler.calls == {f"evt-{i}": 1 for i in range(n)}


def test_racing_for_one_event_has_exactly_one_winner(env):
    settings, store = env
    results, barrier = [], threading.Barrier(8)

    def go(i):
        s = Store(settings)
        barrier.wait()
        results.append(s.claim("evt-1", f"w{i}"))

    threads = [threading.Thread(target=go, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results).count("claimed") == 1 and results.count("busy") == 7


def test_a_claim_that_has_expired_can_be_taken_by_exactly_one_of_several_racers(connection_string):
    settings = make_settings(connection_string)
    clock = Clock()
    store = Store(settings, clock).ensure()
    assert store.claim("evt-1", "dead") == "claimed"
    clock.advance(settings.claim_seconds + 1)
    results, barrier = [], threading.Barrier(6)

    def go(i):
        barrier.wait()
        results.append(Store(settings, clock).claim("evt-1", f"w{i}"))

    threads = [threading.Thread(target=go, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count("claimed") == 1 and results.count("busy") == 5
    assert store.state("evt-1")["attempts"] == 2


def test_a_claim_taken_again_by_its_owner_counts_as_another_attempt(env):
    _, store = env
    assert store.claim("evt-1", "me") == "claimed" and store.claim("evt-1", "me") == "claimed"
    assert store.state("evt-1")["attempts"] == 2 and store.claim("evt-1", "other") == "busy"


def test_releasing_only_removes_my_own_claim(env):
    _, store = env
    store.claim("evt-1", "me")
    store.release("evt-1", "someone-else")
    assert store.state("evt-1") is not None
    store.release("evt-1", "me")
    assert store.state("evt-1") is None
    store.release("evt-1", "me")   # releasing nothing is fine
    store.claim("evt-2", "me")
    store.mark_done("evt-2")
    store.release("evt-2", "me")
    assert store.state("evt-2")["state"] == "done", "a finished event's row is never released"


def test_stats_describe_the_system(env):
    _, store = env
    send(store, event(1), event(2), "bad")
    worker(store).run_once()
    s = store.stats()
    assert (s["done"], s["results"], s["poison"], s["queue"], s["claimed"]) == (2, 2, 1, 0, 0)
