"""Unit tests for HiveMindHttpHandler and all derived handler classes.

Covers lines 147-347 (get_client, ConnectHandler, DisconnectHandler,
SendMessageHandler, GetMessagesHandler, GetBinMessagesHandler).

Uses a real MasterNode from hivescope (so HiveMindClientConnection gets a
valid identity/private-key path) but patches the DB and protocol callbacks
to keep tests deterministic and side-effect-free.
"""
import asyncio
from collections import defaultdict
from queue import Queue
from unittest.mock import MagicMock, patch

import pybase64
import pytest

from hivemind_http_protocol import (
    ClientRegistry,
    HiveMindHttpHandler,
    ConnectHandler,
    DisconnectHandler,
    SendMessageHandler,
    GetMessagesHandler,
    GetBinMessagesHandler,
)


# ---------------------------------------------------------------------------
# Module-level fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def master():
    """One MasterNode reused across all tests in this module."""
    pytest.importorskip("hivescope")
    from hivescope.node import MasterNode
    return MasterNode.create("HH", require_crypto=False, handshake_enabled=True)


def _encode(s: str) -> str:
    return pybase64.b64encode(s.encode("utf-8")).decode("ascii")


def _make_user(
    *,
    client_id=42,
    name="testclient",
    crypto_key="cryptokey",
    allowed_types=None,
    can_propagate=True,
    can_escalate=True,
    is_admin=False,
    password=None,
):
    u = MagicMock()
    u.client_id = client_id
    u.name = name
    u.crypto_key = crypto_key
    u.allowed_types = allowed_types or []
    u.can_propagate = can_propagate
    u.can_escalate = can_escalate
    u.is_admin = is_admin
    u.password = password
    return u


def _clean_class_state():
    """Reset class-level dicts to prevent test bleed."""
    HiveMindHttpHandler.registry = ClientRegistry()
    HiveMindHttpHandler.session_backend = "memory"
    HiveMindHttpHandler.redis_state = None
    HiveMindHttpHandler.db_sync.reset()


class _FakeRedisState:
    def __init__(self):
        self.connected = set()
        self.messages = defaultdict(list)
        self.bin_messages = defaultdict(list)

    def connect(self, key, replica_id):
        self.connected.add(key)

    def is_connected(self, key):
        return key in self.connected

    def disconnect(self, key):
        self.connected.discard(key)
        self.messages.pop(key, None)
        self.bin_messages.pop(key, None)

    def enqueue(self, key, payload, is_bin):
        target = self.bin_messages if is_bin else self.messages
        target[key].append(payload)

    def drain(self, key, is_bin):
        target = self.bin_messages if is_bin else self.messages
        return target.pop(key, [])


def _make_handler(cls, auth_value, proto, *, extra_get_arg=None):
    """Return a handler instance bypassing Tornado's __init__."""
    _clean_class_state()
    cls.hm_protocol = proto

    h = cls.__new__(cls)
    h.set_status = MagicMock()
    h.set_cookie = MagicMock()
    h.set_header = MagicMock()
    h.write = MagicMock()

    def _get_arg(name, default=""):
        if name == "authorization":
            return auth_value
        if extra_get_arg and name in extra_get_arg:
            return extra_get_arg[name]
        return default

    h.get_argument = MagicMock(side_effect=_get_arg)
    return h


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# get_client
# ---------------------------------------------------------------------------

class TestGetClient:
    def test_returns_cached_client(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        existing = MagicMock()
        HiveMindHttpHandler.registry.clients["mykey"] = existing

        h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
        result = h.get_client("agent", "mykey", cache=True)
        assert result is existing

    def test_invalid_key_returns_none(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        with patch.object(proto.db, "get_client_by_api_key", return_value=None):
            with patch.object(proto, "handle_invalid_key_connected") as mock_invalid:
                h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
                result = h.get_client("agent", "badkey", cache=False)
                assert result is None
                mock_invalid.assert_called_once()

    def test_valid_key_builds_client(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            result = h.get_client("agent", "validkey", cache=False)
            assert result is not None
            assert result.crypto_key == user.crypto_key
            assert result.is_admin == user.is_admin
            assert result.can_propagate == user.can_propagate

    def test_valid_key_caches_client(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            result = h.get_client("agent", "cachekey", cache=True)
            assert HiveMindHttpHandler.registry.get("cachekey") is result

    def test_no_cache_does_not_store(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            h.get_client("agent", "nocachekey", cache=False)
            assert "nocachekey" not in HiveMindHttpHandler.registry

    def test_user_with_password_sets_handshake(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        # password must satisfy the runtime strength backstop (poorman-handshake 2.x)
        user = _make_user(password="correct-horse-battery-staple-9$")
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            result = h.get_client("agent", "pwdkey", cache=False)
            assert result is not None
            assert result.pswd_handshake is not None

    def test_user_without_password_no_handshake(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user(password=None)
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            result = h.get_client("agent", "nopwdkey", cache=False)
            assert result is not None
            assert not getattr(result, "pswd_handshake", None)

    def test_do_send_text_goes_to_undelivered(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            client = h.get_client("agent", "sendkey", cache=False)
            client.send_msg("hello", is_bin=False)
            assert not HiveMindHttpHandler.registry.undelivered["sendkey"].empty()
            assert HiveMindHttpHandler.registry.undelivered["sendkey"].get() == "hello"

    def test_do_send_binary_goes_to_undelivered_bin(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            client = h.get_client("agent", "binkey", cache=False)
            client.send_msg(b"\x00\x01", is_bin=True)
            assert not HiveMindHttpHandler.registry.undelivered_bin["binkey"].empty()

    def test_do_disconnect_removes_client_and_queue(self, master):
        proto = master.hm_protocol
        _clean_class_state()
        HiveMindHttpHandler.hm_protocol = proto

        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
            client = h.get_client("agent", "disckey", cache=True)
            HiveMindHttpHandler.registry.undelivered["disckey"].put("msg")
            client.disconnect()
            assert "disckey" not in HiveMindHttpHandler.registry
            assert "disckey" not in HiveMindHttpHandler.registry.undelivered


# ---------------------------------------------------------------------------
# ConnectHandler
# ---------------------------------------------------------------------------

class TestConnectHandler:
    def test_missing_auth_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(ConnectHandler, "", proto)
        _run(h.post())
        h.write.assert_called_with({"error": "Missing authorization"})

    def test_invalid_key_does_not_call_handle_new_client(self, master):
        proto = master.hm_protocol
        with patch.object(proto.db, "get_client_by_api_key", return_value=None):
            with patch.object(proto, "handle_invalid_key_connected"):
                with patch.object(proto, "handle_new_client") as mock_new:
                    h = _make_handler(ConnectHandler, _encode("agent:badkey"), proto)
                    _run(h.post())
                    mock_new.assert_not_called()

    def test_valid_connect_calls_handle_new_client(self, master):
        proto = master.hm_protocol
        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_new_client") as mock_new:
                h = _make_handler(ConnectHandler, _encode("agent:validkey"), proto)
                _run(h.post())
                mock_new.assert_called_once()
                h.write.assert_called_with({"status": "Connected"})

    def test_no_crypto_key_handshake_disabled_require_crypto_triggers_invalid_protocol(self, master):
        proto = master.hm_protocol
        user = _make_user(crypto_key=None)
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_invalid_protocol_version") as mock_ipv:
                with patch.object(proto, "handle_new_client") as mock_new:
                    with patch.object(type(proto), "handshake_enabled",
                                      new_callable=lambda: property(lambda s: False)):
                        with patch.object(type(proto), "require_crypto",
                                          new_callable=lambda: property(lambda s: True)):
                            h = _make_handler(ConnectHandler, _encode("agent:key"), proto)
                            _run(h.post())
                            mock_ipv.assert_called_once()
                            mock_new.assert_not_called()

    def test_connect_exception_returns_500(self, master):
        proto = master.hm_protocol
        with patch.object(proto.db, "sync", side_effect=RuntimeError("boom")):
            h = _make_handler(ConnectHandler, _encode("agent:key"), proto)
            _run(h.post())
            h.set_status.assert_called_with(500)
            h.write.assert_called_with({"error": "Connection failed"})


# ---------------------------------------------------------------------------
# DisconnectHandler
# ---------------------------------------------------------------------------

class TestDisconnectHandler:
    def test_missing_auth_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(DisconnectHandler, "", proto)
        _run(h.post())
        h.write.assert_called_with({"error": "Missing authorization"})

    def test_not_connected_returns_already_disconnected(self, master):
        proto = master.hm_protocol
        h = _make_handler(DisconnectHandler, _encode("agent:key"), proto)
        # clients is empty after _clean_class_state
        _run(h.post())
        h.write.assert_called_with({"error": "Already Disconnected"})

    def test_connected_client_is_disconnected(self, master):
        proto = master.hm_protocol
        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_client_disconnected") as mock_disc:
                h = _make_handler(DisconnectHandler, _encode("agent:disckey"), proto)
                mock_client = MagicMock()
                DisconnectHandler.registry.clients["disckey"] = mock_client
                _run(h.post())
                mock_disc.assert_called_once()
                h.write.assert_called_with({"status": "Disconnected"})

    def test_shared_session_disconnects_without_local_client(self, master):
        proto = master.hm_protocol
        h = _make_handler(DisconnectHandler, _encode("agent:sharedkey"), proto)
        h.redis_state = _FakeRedisState()
        h.session_backend = "redis"
        h.redis_state.connect("sharedkey", "other-replica")

        with patch.object(proto, "handle_client_disconnected") as mock_disc:
            _run(h.post())

        mock_disc.assert_not_called()
        assert not h.redis_state.is_connected("sharedkey")
        h.write.assert_called_with({"status": "Disconnected"})

    def test_disconnect_exception_returns_500(self, master):
        proto = master.hm_protocol
        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_client_disconnected",
                               side_effect=RuntimeError("fail")):
                h = _make_handler(DisconnectHandler, _encode("agent:errkey"), proto)
                DisconnectHandler.registry.clients["errkey"] = MagicMock()
                _run(h.post())
                h.set_status.assert_called_with(500)
                h.write.assert_called_with({"error": "Disconnection failed"})


# ---------------------------------------------------------------------------
# SendMessageHandler
# ---------------------------------------------------------------------------

class TestSendMessageHandler:
    def test_missing_auth_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(SendMessageHandler, "", proto)
        _run(h.post())
        h.write.assert_called_with({"error": "Missing authorization"})

    def test_not_connected_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(SendMessageHandler, _encode("agent:key"), proto)
        # clients empty
        _run(h.post())
        h.write.assert_called_with({"error": "Client is not connected"})

    def test_missing_message_returns_400(self, master):
        proto = master.hm_protocol
        user = _make_user()
        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            h = _make_handler(SendMessageHandler, _encode("agent:msgkey"), proto,
                              extra_get_arg={"message": ""})
            SendMessageHandler.registry.clients["msgkey"] = MagicMock()
            _run(h.post())
            h.set_status.assert_called_with(400)
            h.write.assert_called_with({"error": "Missing message"})

    def test_valid_message_dispatched(self, master):
        proto = master.hm_protocol
        user = _make_user()

        from hivemind_bus_client.message import HiveMessage, HiveMessageType
        from ovos_bus_client.message import Message
        bus_msg = Message("test_msg", {})
        hive_msg = HiveMessage(HiveMessageType.BUS, payload=bus_msg)

        mock_client = MagicMock()
        mock_client.decode.return_value = hive_msg

        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_message") as mock_handle:
                h = _make_handler(SendMessageHandler, _encode("agent:sendmsg"), proto,
                                  extra_get_arg={"message": "encoded_payload"})
                # inject mock client to avoid re-building from DB
                SendMessageHandler.registry.clients["sendmsg"] = mock_client
                _run(h.post())
                mock_handle.assert_called_once()
                h.write.assert_called_with({"status": "message sent"})

    def test_shared_session_reconnects_local_client_before_dispatch(self, master):
        proto = master.hm_protocol
        user = _make_user()

        from hivemind_bus_client.message import HiveMessage, HiveMessageType
        from ovos_bus_client.message import Message
        bus_msg = Message("test_msg", {})
        hive_msg = HiveMessage(HiveMessageType.BUS, payload=bus_msg)

        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_new_client") as mock_new:
                with patch.object(proto, "handle_message") as mock_handle:
                    h = _make_handler(SendMessageHandler, _encode("agent:sharedsend"), proto,
                                      extra_get_arg={"message": "encoded_payload"})
                    h.redis_state = _FakeRedisState()
                    h.session_backend = "redis"
                    h.redis_state.connect("sharedsend", "other-replica")
                    with patch.object(h, "get_client") as mock_get_client:
                        mock_client = MagicMock()
                        mock_client.decode.return_value = hive_msg
                        mock_get_client.return_value = mock_client
                        _run(h.post())

        mock_new.assert_called_once_with(mock_client)
        mock_handle.assert_called_once()
        h.write.assert_called_with({"status": "message sent"})

    def test_b64_audio_message_dispatched(self, master):
        proto = master.hm_protocol
        user = _make_user()

        from hivemind_bus_client.message import HiveMessage, HiveMessageType
        from ovos_bus_client.message import Message
        bus_msg = Message("recognizer_loop:b64_audio", {})
        hive_msg = HiveMessage(HiveMessageType.BUS, payload=bus_msg)

        mock_client = MagicMock()
        mock_client.decode.return_value = hive_msg

        with patch.object(proto.db, "get_client_by_api_key", return_value=user):
            with patch.object(proto, "handle_message") as mock_handle:
                h = _make_handler(SendMessageHandler, _encode("agent:audiokey"), proto,
                                  extra_get_arg={"message": "audio_encoded"})
                SendMessageHandler.registry.clients["audiokey"] = mock_client
                _run(h.post())
                mock_handle.assert_called_once()
                h.write.assert_called_with({"status": "message sent"})

    def test_send_exception_returns_500(self, master):
        proto = master.hm_protocol
        with patch.object(proto, "handle_message", side_effect=RuntimeError("boom")):
            h = _make_handler(SendMessageHandler, _encode("agent:errkey"), proto,
                              extra_get_arg={"message": "msg"})
            SendMessageHandler.registry.clients["errkey"] = MagicMock()
            _run(h.post())
            h.set_status.assert_called_with(500)
            h.write.assert_called_with({"error": "Message sending failed"})


# ---------------------------------------------------------------------------
# GetMessagesHandler
# ---------------------------------------------------------------------------

class TestGetMessagesHandler:
    def test_missing_auth_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, "", proto)
        _run(h.get())
        h.write.assert_called_with({"error": "Missing authorization"})

    def test_not_connected_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:key"), proto)
        _run(h.get())
        h.write.assert_called_with({"error": "Client is not connected"})

    def test_empty_queue_returns_empty_messages(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:emptykey"), proto)
        GetMessagesHandler.registry.clients["emptykey"] = MagicMock()
        _run(h.get())
        h.write.assert_called_with({"status": "messages retrieved", "messages": []})

    def test_queued_messages_are_returned(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:qkey"), proto)
        GetMessagesHandler.registry.clients["qkey"] = MagicMock()
        HiveMindHttpHandler.registry.undelivered["qkey"].put("msg1")
        HiveMindHttpHandler.registry.undelivered["qkey"].put("msg2")
        _run(h.get())
        h.write.assert_called_with({"status": "messages retrieved", "messages": ["msg1", "msg2"]})

    def test_shared_session_messages_are_returned_without_local_client(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:sharedq"), proto)
        h.redis_state = _FakeRedisState()
        h.session_backend = "redis"
        h.redis_state.connect("sharedq", "other-replica")
        h.redis_state.enqueue("sharedq", "msg1", False)
        h.redis_state.enqueue("sharedq", "msg2", False)

        _run(h.get())

        h.write.assert_called_with({"status": "messages retrieved", "messages": ["msg1", "msg2"]})

    def test_get_messages_exception_returns_500(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:exckey"), proto)
        GetMessagesHandler.registry.clients["exckey"] = MagicMock()
        with patch.object(HiveMindHttpHandler.registry.undelivered["exckey"], "empty",
                          side_effect=RuntimeError("fail")):
            _run(h.get())
        h.set_status.assert_called_with(500)
        h.write.assert_called_with({"error": "Retrieving messages failed"})

    def test_get_nowait_exception_breaks_loop(self, master):
        """Inner except: get_nowait raises while queue reports non-empty."""
        proto = master.hm_protocol
        h = _make_handler(GetMessagesHandler, _encode("agent:getnowaitkey"), proto)
        GetMessagesHandler.registry.clients["getnowaitkey"] = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get_nowait.side_effect = RuntimeError("get_nowait fail")
        HiveMindHttpHandler.registry.undelivered["getnowaitkey"] = mock_queue

        _run(h.get())
        # The inner except breaks; we still write the (empty) messages list
        h.write.assert_called_with({"status": "messages retrieved", "messages": []})


# ---------------------------------------------------------------------------
# GetBinMessagesHandler
# ---------------------------------------------------------------------------

class TestGetBinMessagesHandler:
    def test_missing_auth_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, "", proto)
        _run(h.get())
        h.write.assert_called_with({"error": "Missing authorization"})

    def test_not_connected_returns_error(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, _encode("agent:key"), proto)
        _run(h.get())
        h.write.assert_called_with({"error": "Client is not connected"})

    def test_empty_queue_returns_empty_messages(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, _encode("agent:binempty"), proto)
        GetBinMessagesHandler.registry.clients["binempty"] = MagicMock()
        _run(h.get())
        h.write.assert_called_with({"status": "messages retrieved", "b64_messages": []})

    def test_queued_bin_messages_are_returned(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, _encode("agent:binq"), proto)
        GetBinMessagesHandler.registry.clients["binq"] = MagicMock()
        HiveMindHttpHandler.registry.undelivered_bin["binq"].put("b64data1")
        HiveMindHttpHandler.registry.undelivered_bin["binq"].put("b64data2")
        _run(h.get())
        h.write.assert_called_with({"status": "messages retrieved", "b64_messages": ["b64data1", "b64data2"]})

    def test_get_bin_messages_exception_returns_500(self, master):
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, _encode("agent:binexc"), proto)
        GetBinMessagesHandler.registry.clients["binexc"] = MagicMock()
        with patch.object(HiveMindHttpHandler.registry.undelivered_bin["binexc"], "empty",
                          side_effect=RuntimeError("fail")):
            _run(h.get())
        h.set_status.assert_called_with(500)
        h.write.assert_called_with({"error": "Retrieving messages failed"})

    def test_get_nowait_exception_breaks_loop(self, master):
        """Inner except: get_nowait raises while queue reports non-empty."""
        proto = master.hm_protocol
        h = _make_handler(GetBinMessagesHandler, _encode("agent:binnowait"), proto)
        GetBinMessagesHandler.registry.clients["binnowait"] = MagicMock()

        mock_queue = MagicMock()
        mock_queue.empty.return_value = False
        mock_queue.get_nowait.side_effect = RuntimeError("get_nowait fail")
        HiveMindHttpHandler.registry.undelivered_bin["binnowait"] = mock_queue

        _run(h.get())
        h.write.assert_called_with({"status": "messages retrieved", "b64_messages": []})
