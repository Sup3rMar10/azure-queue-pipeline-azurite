"""Starts a real Azurite (Microsoft's Azure Storage emulator) once per run; each test gets its own queues, table and container.

Azurite is found on PATH (`npm install -g azurite`) or through AZURITE_JS (the path of its azurite.js, run with node).
If it is missing the tests ERROR, they are not skipped: a suite that silently skips its only integration layer proves nothing.
"""

import base64
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from qpipe import Settings, Store, Worker


def azurite_command() -> list[str]:
    js = os.environ.get("AZURITE_JS")
    if js:
        return ["node", js]
    exe = shutil.which("azurite")
    if exe:
        return [exe]
    raise RuntimeError("Azurite was not found: run `npm install -g azurite` or set AZURITE_JS to the path of azurite.js")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_listening(port: int, proc: subprocess.Popen, seconds: float = 30) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"Azurite exited early with code {proc.returncode}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Azurite did not listen on {port} within {seconds}s")


@pytest.fixture(scope="session")
def connection_string():
    account = "qpipeacct"
    key = base64.b64encode(os.urandom(64)).decode()  # a throwaway key for this run: nothing secret is committed
    ports = {name: free_port() for name in ("blob", "queue", "table")}
    data = tempfile.mkdtemp(prefix="azurite-")
    cmd = [*azurite_command(), "--silent", "--location", data, "--skipApiVersionCheck",
           *[arg for n, p in ports.items() for arg in (f"--{n}Port", str(p), f"--{n}Host", "127.0.0.1")]]
    proc = subprocess.Popen(cmd, env={**os.environ, "AZURITE_ACCOUNTS": f"{account}:{key}"}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for p in ports.values():
            wait_listening(p, proc)
        yield (f"DefaultEndpointsProtocol=http;AccountName={account};AccountKey={key};"
               + ";".join(f"{n.capitalize()}Endpoint=http://127.0.0.1:{p}/{account}" for n, p in ports.items()) + ";")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(data, ignore_errors=True)


class Clock:
    """A clock tests can move forward, to expire a claim without waiting for it."""

    def __init__(self):
        self.now = datetime.now(timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def make_settings(connection_string: str, **overrides) -> Settings:
    tag = uuid.uuid4().hex[:10]
    base = dict(connection_string=connection_string, queue=f"q-{tag}", poison_queue=f"q-{tag}-poison", table=f"t{tag}",
                container=f"c-{tag}", visibility_timeout=2, claim_seconds=2, defer_seconds=1, backoff_base=1, backoff_cap=4)
    base.update(overrides)
    return Settings(**base)


@pytest.fixture()
def env(connection_string):
    """(settings, store) with fresh, empty queues, table and container."""
    settings = make_settings(connection_string)
    return settings, Store(settings).ensure()


@pytest.fixture()
def clock():
    return Clock()


def event(n=1, amount=1000, merchant="acme", currency="USD", event_id=None) -> str:
    import json
    return json.dumps({"event_id": event_id or f"evt-{n}", "merchant": merchant, "amount_cents": amount, "currency": currency})


def worker(store, name="w", **kw) -> Worker:
    return Worker(store, name=name, **kw)


def drain(workers, done, timeout=40.0, pause=0.25):
    """Run the workers round-robin until done() is true. Returns every outcome seen. Fails loudly on timeout."""
    seen, deadline = [], time.time() + timeout
    while time.time() < deadline:
        for w in workers:
            seen.extend(w.run_once())
        if done():
            return seen
        time.sleep(pause)
    raise AssertionError(f"not finished after {timeout}s; outcomes so far: {[(o.kind, o.event_id) for o in seen]}")


def kinds(outcomes):
    return [o.kind for o in outcomes]
