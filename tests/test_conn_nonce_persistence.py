"""Regression tests for cross-replica conn_nonce persistence.

hivemind-core namespaces a non-admin client's OVOS session_id with
``HiveMindClientConnection.conn_nonce`` (minted lazily per connection
object, see ``layer1_session_id``). ``ClientRegistry`` — the connection
cache used by ``HiveMindHttpHandler.get_client`` — is local to each HTTP
replica (see its docstring). With the redis session backend, the same
logical client can cache-miss on different replicas across requests, and
each miss used to mint a brand-new nonce, silently resetting the client's
OVOS session (context/active skill/dialog) every time it hopped replicas.

These tests use a real MasterNode from hivescope (matching the pattern in
test_handlers.py) so HiveMindClientConnection gets a valid identity/key
path, and a fake redis session-state double that behaves like the real
``RedisHttpSessionState`` for the nonce methods under test.
"""
import time
from unittest.mock import MagicMock, patch

import pytest

from hivemind_http_protocol import ClientRegistry, HiveMindHttpHandler, RedisHttpSessionState


class _MiniRedis:
    """Just enough of the redis.Redis surface for RedisHttpSessionState."""

    def __init__(self):
        self.data = {}

    def setex(self, key, ttl, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def delete(self, *keys):
        for k in keys:
            self.data.pop(k, None)

    def exists(self, key):
        return int(key in self.data)

    def expire(self, key, ttl):
        pass

    def rpush(self, key, value):
        self.data.setdefault(key, []).append(value)

    def lpop(self, key):
        items = self.data.get(key) or []
        if not items:
            return None
        return items.pop(0)


def _redis_state():
    state = RedisHttpSessionState.__new__(RedisHttpSessionState)
    state.client = _MiniRedis()
    state.prefix = "hivemind-http"
    state.session_ttl_s = 3600
    state.queue_ttl_s = 300
    return state


def test_redis_session_state_nonce_round_trip():
    state = _redis_state()
    assert state.get_nonce("k1") is None
    state.set_nonce("k1", "nonce-abc")
    assert state.get_nonce("k1") == "nonce-abc"


def test_redis_session_state_disconnect_clears_nonce():
    state = _redis_state()
    state.set_nonce("k1", "nonce-abc")
    state.disconnect("k1")
    assert state.get_nonce("k1") is None


@pytest.fixture(scope="module")
def master():
    pytest.importorskip("hivescope")
    from hivescope.node import MasterNode
    return MasterNode.create("HH-NONCE", require_crypto=False, handshake_enabled=True)


def _make_user(client_id=7, name="nonceclient", is_admin=False):
    u = MagicMock()
    u.client_id = client_id
    u.name = name
    u.crypto_key = "cryptokey"
    u.allowed_types = []
    u.can_propagate = True
    u.can_escalate = True
    u.can_broadcast = True
    u.is_admin = is_admin
    u.password = None
    return u


class _FakeRedisNonceState:
    """Behaves like RedisHttpSessionState's nonce methods, backed by a dict
    shared across "replicas" the way redis itself would be."""

    def __init__(self, shared_store=None):
        self.store = shared_store if shared_store is not None else {}

    def get_nonce(self, key):
        return self.store.get(key)

    def set_nonce(self, key, nonce):
        self.store[key] = nonce

    def disconnect(self, key):
        self.store.pop(key, None)

    # Unused by get_client, but present so the double matches the real
    # RedisHttpSessionState surface if a test exercises those paths too.
    def connect(self, key, replica_id):
        pass

    def is_connected(self, key):
        return False


def _handler_for_replica(proto, redis_state):
    """A HiveMindHttpHandler instance simulating a fresh replica: its own
    empty ClientRegistry cache, sharing only the redis-backed store."""
    h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
    h.hm_protocol = proto
    h.registry = ClientRegistry()
    h.redis_state = redis_state
    h.session_backend = "redis"
    h.db_sync = HiveMindHttpHandler.db_sync
    h.db_sync.reset()
    return h


def test_conn_nonce_stable_across_replicas(master):
    """The load-bearing regression check: two independent replica caches,
    sharing one redis-backed nonce store, must resolve the same client key
    to the SAME conn_nonce (hence the same layer1_session_id) even though
    each replica constructs its own HiveMindClientConnection object."""
    proto = master.hm_protocol
    user = _make_user()
    shared_store = {}
    replica_a = _handler_for_replica(proto, _FakeRedisNonceState(shared_store))
    replica_b = _handler_for_replica(proto, _FakeRedisNonceState(shared_store))

    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        client_a = replica_a.get_client("agent", "sharedclientkey")
        client_b = replica_b.get_client("agent", "sharedclientkey")

    assert client_a is not client_b  # genuinely distinct connection objects
    assert client_a.conn_nonce == client_b.conn_nonce
    assert client_a.layer1_session_id == client_b.layer1_session_id


def test_distinct_keys_get_distinct_nonces(master):
    proto = master.hm_protocol
    user = _make_user()
    shared_store = {}
    replica_a = _handler_for_replica(proto, _FakeRedisNonceState(shared_store))
    replica_b = _handler_for_replica(proto, _FakeRedisNonceState(shared_store))

    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        client_a = replica_a.get_client("agent", "keyone")
        client_b = replica_b.get_client("agent", "keytwo")

    assert client_a.conn_nonce != client_b.conn_nonce


def test_memory_backend_unaffected(master):
    """No redis_state at all: get_client caches the connection object
    in-process, so the same key returns the identical cached object and
    therefore trivially the same nonce."""
    proto = master.hm_protocol
    user = _make_user()
    h = _handler_for_replica(proto, redis_state=None)
    h.session_backend = "memory"

    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        first = h.get_client("agent", "memkey")
        second = h.get_client("agent", "memkey")

    assert first is second
    assert first.conn_nonce == second.conn_nonce


def test_nonce_cleared_on_disconnect(master):
    proto = master.hm_protocol
    user = _make_user()
    shared_store = {}
    state = _FakeRedisNonceState(shared_store)
    replica_a = _handler_for_replica(proto, state)

    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        client = replica_a.get_client("agent", "disconnkey")

    assert state.get_nonce("disconnkey") == client.conn_nonce
    state.disconnect("disconnkey")
    assert state.get_nonce("disconnkey") is None
