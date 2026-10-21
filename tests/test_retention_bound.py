"""HTTP outbox retention bound (HIVEMIND-TRANSPORT-1 §4).

§4 lets an HTTP binding retain an unpolled frame "until retrieved, until
session close, or up to a documented retention bound". The binding used to
have no bound at all: a class-level ``defaultdict(Queue)`` that grew forever,
per frame and per client key, and was never swept. A publicly reachable node
could be pushed into unbounded memory growth by connecting and never polling.

These tests pin both halves of the bound — the per-client frame cap and the
per-client staleness sweep — and the FIFO delivery §2 still requires.
"""

import sys
import time
from unittest.mock import patch

import pytest

from ovos_utils.log import LOG

from hivemind_http_protocol import (DEFAULT_MAX_UNDELIVERED, RetentionQueue,
                                    RetentionStore)


class TestPerClientFrameCap:
    """TRANSPORT-1 §4: one client that never polls cannot grow without limit."""

    def test_queue_never_exceeds_maxsize(self):
        q = RetentionQueue(maxsize=8, ttl=60)
        for i in range(1000):
            q.put(f"frame-{i}")
        assert len(q) == 8

    def test_the_oldest_frames_are_the_ones_dropped(self):
        q = RetentionQueue(maxsize=3, ttl=60)
        for i in range(5):
            q.put(f"frame-{i}")
        assert q.drain() == ["frame-2", "frame-3", "frame-4"]

    def test_draining_preserves_arrival_order(self):
        q = RetentionQueue(maxsize=10, ttl=60)
        for i in range(4):
            q.put(f"frame-{i}")
        assert q.drain() == ["frame-0", "frame-1", "frame-2", "frame-3"]

    def test_a_drained_queue_is_empty(self):
        q = RetentionQueue(maxsize=10, ttl=60)
        q.put("frame")
        q.drain()
        assert q.drain() == []

    def test_a_drop_is_logged_rather_than_silent(self):
        q = RetentionQueue(maxsize=1, ttl=60)
        q.put("first", key="client-a")
        with patch.object(LOG, "warning") as warn:
            q.put("second", key="client-a")
        assert "client-a" in warn.call_args[0][0]


class TestStaleClientSweep:
    """TRANSPORT-1 §4: the number of retained clients is bounded too."""

    def test_a_client_that_stops_polling_is_evicted(self):
        store = RetentionStore(maxsize=10, ttl=0.05)
        store.queue_for("gone").put("frame")
        time.sleep(0.1)
        store.queue_for("active")
        assert "gone" not in store

    def test_a_client_that_keeps_receiving_but_never_polls_is_evicted(self):
        clock = [0.0]
        store = RetentionStore(maxsize=10, ttl=10)
        with patch("hivemind_http_protocol.time.monotonic", lambda: clock[0]):
            store.queue_for("deaf").put("frame-0")
            for tick in range(1, 10):
                clock[0] = float(tick)
                store.queue_for("deaf").put(f"frame-{tick}")
            clock[0] = 11.0
            store.queue_for("other")
        assert "deaf" not in store

    def test_reading_counts_as_activity(self):
        clock = [0.0]
        with patch("hivemind_http_protocol.time.monotonic", lambda: clock[0]):
            store = RetentionStore(maxsize=10, ttl=10)
            outbox = store.queue_for("poller")
            for _ in range(3):
                outbox.put("frame")
                clock[0] += 6
                assert store.queue_for("poller").drain() == ["frame"]

    def test_an_eviction_is_logged_rather_than_silent(self):
        store = RetentionStore(maxsize=10, ttl=0.05)
        store.queue_for("gone").put("frame")
        time.sleep(0.1)
        with patch.object(LOG, "warning") as warn:
            store.sweep()
        assert "gone" in warn.call_args[0][0]


class TestDefaultsAreBounded:
    """A deployment that configures nothing is still bounded."""

    def test_the_default_store_caps_frames(self):
        store = RetentionStore()
        for i in range(DEFAULT_MAX_UNDELIVERED * 2):
            store.queue_for("client").put(f"frame-{i}")
        assert len(store["client"]) == DEFAULT_MAX_UNDELIVERED


class TestConcurrentDelivery:
    """Frames put from several threads while another thread drains are each
    delivered exactly once."""

    @pytest.fixture(autouse=True)
    def fast_thread_switching(self):
        old = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            yield
        finally:
            sys.setswitchinterval(old)

    def test_every_frame_is_delivered_exactly_once(self):
        import threading

        writers, per_writer = 4, 5000
        q = RetentionQueue(maxsize=writers * per_writer, ttl=60)
        expected = {f"w{w}-{i}" for w in range(writers) for i in range(per_writer)}
        done = threading.Event()
        delivered = []

        def write(w):
            for i in range(per_writer):
                q.put(f"w{w}-{i}")

        def read():
            while not done.is_set():
                delivered.extend(q.drain())

        threads = [threading.Thread(target=write, args=(w,)) for w in range(writers)]
        reader = threading.Thread(target=read)
        reader.start()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        done.set()
        reader.join()
        delivered.extend(q.drain())

        assert len(delivered) == len(expected)
        assert set(delivered) == expected
        for w in range(writers):
            order = [int(f.split("-")[1]) for f in delivered if f.startswith(f"w{w}-")]
            assert order == list(range(per_writer))

    def test_sweep_and_queue_for_survive_concurrent_churn(self):
        import threading

        store = RetentionStore(maxsize=4, ttl=0)
        errors = []

        def churn(n):
            try:
                for i in range(2000):
                    store.queue_for(f"c{n}-{i % 20}").put("x")
                    store.pop(f"c{n}-{(i + 7) % 20}", None)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=churn, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
