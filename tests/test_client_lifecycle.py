"""Coverage for get_client() database access and per-key cleanup.

Targets:
- db.sync() runs only after an api-key miss, debounced across requests.
- do_disconnect() drops both message queues, not just the text one.
"""
from types import SimpleNamespace

import pytest

from hivemind_http_protocol import (
    ClientDatabaseSync,
    HiveMindHttpHandler,
    _redis_config_from_server,
    _redis_session_prefix_from_config,
    _redis_url_from_config,
)


class _DB:
    """Stands in for ClientDatabase: counts syncs, knows a fixed key set."""

    def __init__(self, known=()):
        self.known = set(known)
        self.sync_count = 0
        self.lookup_count = 0

    def sync(self):
        self.sync_count += 1

    def get_client_by_api_key(self, key):
        self.lookup_count += 1
        if key not in self.known:
            return None
        return SimpleNamespace(
            client_id=1, name="node", crypto_key=None, password=None,
            skill_blacklist=[], intent_blacklist=[], allowed_types=["speak"],
            can_propagate=True, can_escalate=True, is_admin=False,
            can_broadcast=True,
        )


class _Protocol:
    identity = SimpleNamespace(private_key=None, site_id="http-site")
    # HiveMindClientConnection builds its per-connection HandShake from the
    # protocol's cached RSA identity key rather than re-importing the PEM on
    # every connection. None is the right stand-in here: these tests never
    # run a handshake, and a real key would cost a 2048-bit import per test
    # for nothing.
    identity_rsa_key = None

    def __init__(self, db):
        self.db = db
        self.invalid_keys = []

    def handle_invalid_key_connected(self, client):
        self.invalid_keys.append(client.key)


def _handler(db):
    handler = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
    handler.hm_protocol = _Protocol(db)
    return handler


@pytest.fixture(autouse=True)
def _reset_handler_state():
    HiveMindHttpHandler.registry.clear()
    HiveMindHttpHandler.session_backend = "memory"
    HiveMindHttpHandler.redis_state = None
    HiveMindHttpHandler.db_sync.reset()


def test_known_key_is_resolved_without_syncing():
    db = _DB(known={"good"})
    client = _handler(db).get_client("agent", "good")

    assert client is not None
    assert db.sync_count == 0
    assert db.lookup_count == 1


def test_unknown_key_syncs_once_then_retries_lookup():
    db = _DB()
    assert _handler(db).get_client("agent", "bad") is None

    # miss -> sync -> look again, rather than sync -> look
    assert db.sync_count == 1
    assert db.lookup_count == 2


def test_key_added_after_sync_is_accepted():
    db = _DB()

    def _sync():
        db.sync_count += 1
        db.known.add("late")

    db.sync = _sync
    assert _handler(db).get_client("agent", "late") is not None
    assert db.sync_count == 1


def test_repeated_unknown_keys_do_not_sync_per_request():
    """An unknown key must not drive one db.sync() per HTTP request."""
    db = _DB()
    handler = _handler(db)
    # widen the window so the assertion is about the debounce, not about how
    # long the loop happens to take
    HiveMindHttpHandler.db_sync.debounce_s = 60.0
    try:
        for _ in range(5):
            assert handler.get_client("agent", "bad", cache=False) is None
    finally:
        HiveMindHttpHandler.db_sync.debounce_s = 1.0

    assert db.sync_count == 1


def test_disconnect_drops_both_message_queues():
    db = _DB(known={"good"})
    client = _handler(db).get_client("agent", "good")

    client.send_msg("hello", False)
    client.send_msg(b"\x00binary", True)
    assert "good" in HiveMindHttpHandler.registry.undelivered
    assert "good" in HiveMindHttpHandler.registry.undelivered_bin

    client.disconnect()

    assert "good" not in HiveMindHttpHandler.registry
    assert "good" not in HiveMindHttpHandler.registry.undelivered
    # undelivered_bin used to leak the queue and its payloads for the life
    # of the process, since only undelivered was popped
    assert "good" not in HiveMindHttpHandler.registry.undelivered_bin


def test_disconnect_is_idempotent():
    db = _DB(known={"good"})
    client = _handler(db).get_client("agent", "good")
    client.disconnect()
    client.disconnect()


def test_disconnect_accepts_a_close_code_and_reason():
    """hivemind-core calls client.disconnect(1008, reason) on handshake
    rejection; the callback wired in here must not TypeError on that call."""
    db = _DB(known={"good"})
    client = _handler(db).get_client("agent", "good")
    client.disconnect(1008, "invalid credentials")

    assert "good" not in HiveMindHttpHandler.registry


class TestClientDatabaseSync:
    def test_debounces_within_window(self):
        db = _DB()
        sync = ClientDatabaseSync(debounce_s=60.0)
        sync.sync(db)
        sync.sync(db)
        assert db.sync_count == 1

    def test_failure_is_replayed_inside_the_window(self):
        boom = RuntimeError("database unreachable")

        class _Failing:
            calls = 0

            def sync(self):
                _Failing.calls += 1
                raise boom

        db = _Failing()
        sync = ClientDatabaseSync(debounce_s=60.0)
        with pytest.raises(RuntimeError):
            sync.sync(db)
        with pytest.raises(RuntimeError):
            sync.sync(db)
        # the second caller got the cached error, not a second attempt
        assert _Failing.calls == 1

    def test_window_reset_allows_retry(self):
        db = _DB()
        sync = ClientDatabaseSync(debounce_s=60.0)
        sync.sync(db)
        sync.reset()
        sync.sync(db)
        assert db.sync_count == 2


def test_redis_url_from_database_config_quotes_credentials():
    url = _redis_url_from_config({
        "host": "redis",
        "port": 6379,
        "db": 2,
        "username": "user:name",
        "password": "p@ss/word",
    })
    assert url == "redis://user%3Aname:p%40ss%2Fword@redis:6379/2"


def test_redis_config_from_server_returns_only_redis_backend(monkeypatch):
    monkeypatch.setattr(
        "hivemind_http_protocol.get_server_config",
        lambda: {
            "database": {
                "module": "hivemind-redis-db-plugin",
                "hivemind-redis-db-plugin": {"host": "redis", "port": 6379},
            },
        },
    )
    assert _redis_config_from_server() == {"host": "redis", "port": 6379}


def test_redis_config_from_server_ignores_non_redis_backend(monkeypatch):
    monkeypatch.setattr(
        "hivemind_http_protocol.get_server_config",
        lambda: {
            "database": {
                "module": "hivemind-sqlite-db-plugin",
                "hivemind-sqlite-db-plugin": {"name": "clients"},
            },
        },
    )
    assert _redis_config_from_server() == {}


def test_redis_session_prefix_stays_inside_database_keyspace():
    assert (
        _redis_session_prefix_from_config({"index_prefix": "hub-prefix"})
        == "hub-prefix:hivemind-http"
    )
    assert (
        _redis_session_prefix_from_config({"index_prefix": "hub-prefix:"})
        == "hub-prefix:hivemind-http"
    )
    assert _redis_session_prefix_from_config({}) == "hivemind-http"
