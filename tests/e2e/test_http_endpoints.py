"""End-to-end test through the real HTTP endpoints.

The listener runs on a local port with its own route table. A real
HiveMindHTTPClient (Noise handshake, encrypted frames in both directions) and
plain HTTP requests talk to it. Nothing calls a handler directly.
"""
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import MagicMock, patch

import pybase64
import pytest
from hivemind_bus_client.http_client import HiveMindHTTPClient
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from json_database import JsonStorage
from ovos_spec_tools.message import Message
from tornado import ioloop

from hivemind_http_protocol import (
    ClientRegistry,
    HiveMindHttpHandler,
    HiveMindHttpProtocol,
)

ACCESS_KEY = "e2e-access-key"
PASSWORD = "correct-horse-battery-staple-9$"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _auth(key: str = ACCESS_KEY) -> str:
    return pybase64.b64encode(f"e2e-agent:{key}".encode()).decode()


def _request(base: str, method: str, route: str, key: str = ACCESS_KEY):
    query = urllib.parse.urlencode({"authorization": _auth(key)})
    request = urllib.request.Request(f"{base}{route}?{query}", method=method,
                                     data=b"" if method == "POST" else None)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


@pytest.fixture(scope="module")
def listener():
    from hivescope.node import MasterNode
    master = MasterNode.create("HTTP", require_crypto=False, handshake_enabled=True)
    proto = master.hm_protocol
    user = MagicMock()
    user.client_id = 7
    user.name = "e2e"
    user.allowed_types = []
    user.can_broadcast = True
    user.can_propagate = True
    user.can_escalate = True
    user.is_admin = False
    user.password = PASSWORD

    def lookup(key):
        return user if key == ACCESS_KEY else None

    loops = []
    real_current = ioloop.IOLoop.current

    def recording_current(*args, **kwargs):
        loop = real_current(*args, **kwargs)
        if threading.current_thread() is not threading.main_thread() and loop not in loops:
            loops.append(loop)
        return loop

    port = _free_port()
    saved = (HiveMindHttpHandler.hm_protocol, HiveMindHttpHandler.registry,
             HiveMindHttpHandler.redis_state, HiveMindHttpHandler.session_backend)
    with patch.object(proto.db, "get_client_by_api_key", side_effect=lookup), \
            patch.object(ioloop.IOLoop, "current", recording_current):
        server = HiveMindHttpProtocol(
            config={"host": "127.0.0.1", "port": port}, hm_protocol=proto)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 10
        while not loops and time.monotonic() < deadline:
            time.sleep(0.05)
        assert loops, "listener did not start"
        time.sleep(0.2)
        try:
            yield f"http://127.0.0.1:{port}", proto
        finally:
            loops[0].add_callback(loops[0].stop)
            thread.join(timeout=10)
            assert not thread.is_alive()
            loops[0].close(all_fds=True)
            (HiveMindHttpHandler.hm_protocol, HiveMindHttpHandler.registry,
             HiveMindHttpHandler.redis_state,
             HiveMindHttpHandler.session_backend) = saved


@pytest.fixture(autouse=True)
def fresh_registry(listener):
    HiveMindHttpHandler.registry = ClientRegistry()
    yield
    HiveMindHttpHandler.registry = ClientRegistry()


def _wait_for(predicate, timeout=15.0):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.05)
    return predicate()


def _bus_messages(hive_messages, msg_type):
    return [m.payload for m in hive_messages
            if m.msg_type == HiveMessageType.BUS
            and isinstance(m.payload, Message)
            and m.payload.msg_type == msg_type]


@pytest.fixture
def client(listener, tmp_path, monkeypatch):
    base, _ = listener
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    port = int(base.rsplit(":", 1)[1])
    hm_client = HiveMindHTTPClient(key=ACCESS_KEY, host="http://127.0.0.1",
                                   port=port, useragent="e2e-agent",
                                   self_signed=False, http_timeout=10,
                                   password=PASSWORD,
                                   identity=NodeIdentity(JsonStorage(str(tmp_path / "identity.json"))))
    try:
        yield hm_client
    finally:
        if hm_client.connected.is_set():
            hm_client.disconnect()
        hm_client.shutdown()


def test_noise_session_carries_frames_both_ways_over_http(listener, client):
    _, proto = listener
    received_by_hub = []
    real_handle_message = proto.handle_message

    def record(message, conn):
        received_by_hub.append(message)
        return real_handle_message(message, conn)

    received_by_client = []
    client.on_mycroft("e2e.pong", lambda msg: received_by_client.append(msg))

    with patch.object(proto, "handle_message", record):
        client.connect()
        client.wait_for_handshake()
        assert client.noise_transport is not None

        client.emit(Message("e2e.ping", {"n": 1}))
        assert _wait_for(lambda: _bus_messages(received_by_hub, "e2e.ping"))
    ping = _bus_messages(received_by_hub, "e2e.ping")[0]
    assert ping.data == {"n": 1}

    hub_side = HiveMindHttpHandler.registry.get(ACCESS_KEY)
    hub_side.send(HiveMessage(HiveMessageType.BUS, Message("e2e.pong", {"n": 2})))
    assert _wait_for(lambda: received_by_client)
    assert received_by_client[0].msg_type == "e2e.pong"
    assert received_by_client[0].data == {"n": 2}

    client.disconnect()
    assert ACCESS_KEY not in HiveMindHttpHandler.registry


def test_polling_a_session_that_was_never_opened_reports_it(listener):
    base, _ = listener
    assert _request(base, "GET", "/get_messages") == (
        200, {"error": "Client is not connected"})
    assert _request(base, "POST", "/disconnect") == (
        200, {"error": "Already Disconnected"})


def test_unknown_key_is_refused_with_403(listener):
    base, _ = listener
    assert _request(base, "POST", "/connect", key="not-a-key") == (
        403, {"error": "Invalid authorization"})


def test_sending_before_connecting_is_refused_with_409(listener):
    base, _ = listener
    assert _request(base, "POST", "/send_message") == (
        409, {"error": "Client is not connected"})
