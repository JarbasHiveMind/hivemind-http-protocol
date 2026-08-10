"""HTTP outbox retention bound (HIVEMIND-TRANSPORT-1 §4).

§4 lets an HTTP binding retain an unpolled frame "until retrieved, until
session close, or up to a documented retention bound". The binding used to
have no bound at all: a class-level ``defaultdict(Queue)`` that grew forever,
per frame and per client key, and was never swept. A publicly reachable node
could be pushed into unbounded memory growth by connecting and never polling.

These tests pin both halves of the bound — the per-client frame cap and the
per-client staleness sweep — and the FIFO delivery §2 still requires.
"""

import time
from unittest.mock import patch

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

    def test_an_active_client_survives_the_sweep(self):
        store = RetentionStore(maxsize=10, ttl=0.2)
        store.queue_for("active").put("frame")
        for _ in range(3):
            time.sleep(0.1)
            store.queue_for("active").put("frame")
        assert "active" in store

    def test_reading_counts_as_activity(self):
        store = RetentionStore(maxsize=10, ttl=0.2)
        store.queue_for("poller").put("frame")
        for _ in range(3):
            time.sleep(0.1)
            store.queue_for("poller").drain()
        assert "poller" in store

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
