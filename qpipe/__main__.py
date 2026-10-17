"""python -m qpipe demo | work | stats | replay    (set QPIPE_CONNECTION_STRING; for a local try-out run Azurite first)

`demo` sends a mix of events (good, a duplicate, invalid ones, one the handler fails on twice), runs a worker until the queue
is empty and prints what happened to each message. It uses short timings so it finishes in about ten seconds.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from . import Settings, Store, Worker, peek, process, replay


def settings_from_env(**overrides) -> Settings:
    conn = os.environ.get("QPIPE_CONNECTION_STRING")
    if not conn:
        print("set QPIPE_CONNECTION_STRING (for Azurite: its documented development connection string)", file=sys.stderr)
        raise SystemExit(2)
    return Settings(conn, **overrides)


def demo() -> int:
    import json
    from .worker import Outcome

    store = Store(settings_from_env(queue="demo", poison_queue="demo-poison", table="demo", container="demo", visibility_timeout=2,
                                    claim_seconds=2, defer_seconds=1, backoff_base=1, backoff_cap=2)).ensure()
    ev = lambda i, **kw: json.dumps({"event_id": f"d-{i}", "merchant": "acme", "amount_cents": 1000 * i, "currency": "USD", **kw})  # noqa: E731
    for raw in (ev(1), ev(2), ev(1), "not json", ev(3, amount_cents=-5), ev(4)):
        store.queue.send_message(raw)
    failures = {"d-4": 2}

    def handler(e):
        if failures.get(e.event_id, 0) > 0:
            failures[e.event_id] -= 1
            raise ConnectionError("the downstream system is unavailable")
        return process(e)

    w = Worker(store, name="demo", handler=handler)
    deadline, seen = time.time() + 40, []
    while time.time() < deadline:
        outs: list[Outcome] = w.run_once()
        for o in outs:
            print(f"  {o.kind:<9} {o.event_id or '-':<5} {o.detail}")
        seen += outs
        s = store.stats()
        if s["queue"] == 0:
            break
        time.sleep(0.5)
    print("stats:", store.stats())
    for r in peek(store):
        print("poison:", r["reason"])
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="qpipe")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo")
    w = sub.add_parser("work")
    w.add_argument("--once", action="store_true")
    sub.add_parser("stats")
    r = sub.add_parser("replay")
    r.add_argument("--include-invalid", action="store_true")
    a = p.parse_args()
    if a.cmd == "demo":
        return demo()
    store = Store(settings_from_env()).ensure()
    if a.cmd == "stats":
        print(store.stats())
    elif a.cmd == "replay":
        print(replay(store, include_invalid=a.include_invalid))
    else:
        worker = Worker(store)
        while True:
            for o in worker.run_once():
                print(f"{o.kind} {o.event_id or '-'} {o.detail}")
            if a.once:
                break
            time.sleep(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
