"""Noise transport frames over HTTP, end to end.

A protocol v3 session carries every post-handshake message as an encrypted
binary frame. These run real Noise transports through the real handler and
a real HiveMindClientConnection, so the contract with the client side (base64
with the standard alphabet, ``binary=1``, one frame per POST, chunking above
one frame) is exercised rather than mocked.
"""
import asyncio
import logging
from unittest.mock import MagicMock, patch

import pybase64
import pytest
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.noise import (
    NOISE_PATTERN_XX,
    NOISE_SUITE_CHACHA,
    NoiseTransport,
    build_prologue,
    start_noise_handshake,
)
from ovos_bus_client.message import Message

from hivemind_http_protocol import (
    DisconnectHandler,
    HiveMindHttpHandler,
    SendMessageHandler,
)
from tests.test_handlers import _encode, _make_handler, _make_user


@pytest.fixture(scope="module")
def master():
    pytest.importorskip("hivescope")
    from hivescope.node import MasterNode
    return MasterNode.create("HF", require_crypto=False, handshake_enabled=True)


def _run(coro):
    return asyncio.run(coro)


def _transport_pair():
    node_id = "hub-node"
    prologue = build_prologue({"node_id": node_id, "handshake": True},
                              NOISE_PATTERN_XX, NOISE_SUITE_CHACHA)
    password = "harness-" + "deadbeef"
    initiator = start_noise_handshake(
        initiator=True, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
        password=password, node_id=node_id, prologue=prologue)
    responder = start_noise_handshake(
        initiator=False, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
        password=password, node_id=node_id, prologue=prologue)
    responder.read_message(initiator.write_message())
    initiator.read_message(responder.write_message())
    responder.read_message(initiator.write_message())
    return NoiseTransport(initiator), NoiseTransport(responder)


def _real_connection(proto, key, transport):
    from hivemind_core.protocol import HiveMindClientConnection
    client = HiveMindClientConnection(
        key=key, disconnect=MagicMock(), send_msg=MagicMock(),
        name="agent", hm_protocol=proto)
    client.noise_transport = transport
    return client


def _post_frame(proto, key, frame, flag="1"):
    h = _make_handler(SendMessageHandler, _encode(f"agent:{key}"), proto,
                      extra_get_arg={"message": pybase64.b64encode(frame).decode("ascii"),
                                     "binary": flag})
    return h


@pytest.mark.parametrize("size", [40, 200_000])
def test_frames_from_a_real_transport_arrive_as_one_message(master, size):
    proto = master.hm_protocol
    node_side, hub_side = _transport_pair()
    client = _real_connection(proto, "noisekey", hub_side)
    payload = HiveMessage(HiveMessageType.BUS, payload=Message("test_msg", {"text": "x" * size}))
    frames = []
    node_side.send_message(payload.serialize(), frames.append)
    assert len(frames) >= (2 if size > 65_000 else 1)

    received = []
    with patch.object(proto, "handle_message", side_effect=lambda m, c: received.append(m)):
        for frame in frames:
            h = _post_frame(proto, "noisekey", frame)
            SendMessageHandler.registry.clients["noisekey"] = client
            _run(h.post())
            h.set_status.assert_not_called()
        statuses = [h.write.call_args[0][0]["status"]]

    assert statuses == ["message sent"]
    assert len(received) == 1
    assert received[0].msg_type == HiveMessageType.BUS
    assert received[0].payload.data == {"text": "x" * size}


def test_a_chunk_is_acknowledged_as_buffered(master):
    proto = master.hm_protocol
    node_side, hub_side = _transport_pair()
    client = _real_connection(proto, "noisekey", hub_side)
    frames = []
    node_side.send_message("y" * 100_000, frames.append)

    with patch.object(proto, "handle_message") as handle:
        h = _post_frame(proto, "noisekey", frames[0])
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())

    h.write.assert_called_with({"status": "buffered"})
    handle.assert_not_called()


def test_a_frame_with_no_noise_session_is_refused_with_409(master):
    """The listener restarted (memory backend) or dropped the session: the
    frame cannot be decrypted, and the client must be told to handshake
    again rather than get a 500 from ciphertext parsed as a bitstring."""
    proto = master.hm_protocol
    client = _real_connection(proto, "noisekey", None)
    with patch.object(proto, "handle_message") as handle:
        h = _post_frame(proto, "noisekey", b"\x00ciphertext")
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())

    h.set_status.assert_called_with(409)
    assert "Noise session" in h.write.call_args[0][0]["error"]
    handle.assert_not_called()


def test_a_client_that_is_not_connected_gets_409(master):
    proto = master.hm_protocol
    h = _make_handler(SendMessageHandler, _encode("agent:ghost"), proto,
                      extra_get_arg={"message": "x", "binary": "1"})
    _run(h.post())
    h.set_status.assert_called_with(409)
    h.write.assert_called_with({"error": "Client is not connected"})


@pytest.mark.parametrize("flag", ["1", "true", "YES", " True "])
def test_the_binary_flag_accepts_the_usual_spellings(master, flag):
    proto = master.hm_protocol
    client = MagicMock()
    client.decode.return_value = HiveMessage(HiveMessageType.BUS, payload=Message("test_msg", {}))
    with patch.object(proto, "handle_message"):
        h = _post_frame(proto, "noisekey", b"frame", flag=flag)
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())
    client.decode.assert_called_once_with(b"frame")


def test_without_the_flag_the_payload_stays_text(master):
    proto = master.hm_protocol
    client = MagicMock()
    client.decode.return_value = HiveMessage(HiveMessageType.BUS, payload=Message("test_msg", {}))
    with patch.object(proto, "handle_message"):
        h = _make_handler(SendMessageHandler, _encode("agent:noisekey"), proto,
                          extra_get_arg={"message": "plain-text", "binary": "0"})
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())
    client.decode.assert_called_once_with("plain-text")


def test_malformed_base64_is_a_400_not_a_500(master):
    proto = master.hm_protocol
    client = MagicMock()
    with patch.object(proto, "handle_message") as handle:
        h = _make_handler(SendMessageHandler, _encode("agent:noisekey"), proto,
                          extra_get_arg={"message": "%%%%", "binary": "1"})
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())
    h.set_status.assert_called_with(400)
    client.decode.assert_not_called()
    handle.assert_not_called()


def test_the_decrypted_message_is_not_logged_at_info(master, caplog):
    """A decrypted BUS message carries the user's words: the type at info,
    the envelope only at debug."""
    proto = master.hm_protocol
    node_side, hub_side = _transport_pair()
    client = _real_connection(proto, "noisekey", hub_side)
    frames = []
    secret = "the user said something private"
    node_side.send_message(
        HiveMessage(HiveMessageType.BUS, payload=Message("test_msg", {"text": secret}))
        .serialize(), frames.append)

    with patch.object(proto, "handle_message"), caplog.at_level(logging.INFO):
        h = _post_frame(proto, "noisekey", frames[0])
        SendMessageHandler.registry.clients["noisekey"] = client
        _run(h.post())

    assert secret not in caplog.text


def test_a_core_initiated_disconnect_reaches_the_protocol_and_drops_the_session(master):
    """Over a websocket the socket close reaches the core; over HTTP nothing
    closes, so the connection's disconnect callback has to do it."""
    proto = master.hm_protocol
    user = _make_user()
    h = _make_handler(HiveMindHttpHandler, _encode("agent:dkey"), proto)
    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        client = h.get_client("agent", "dkey")
    assert "dkey" in HiveMindHttpHandler.registry

    with patch.object(proto, "handle_client_disconnected") as gone:
        client.disconnect(1008, "tampered frame")

    gone.assert_called_once_with(client)
    assert "dkey" not in HiveMindHttpHandler.registry


def test_disconnect_releases_the_cached_connection(master):
    """A later /connect must run handle_new_client again (HELLO and offer)
    rather than find a connection whose Noise session is gone."""
    proto = master.hm_protocol
    user = _make_user()
    h = _make_handler(DisconnectHandler, _encode("agent:dkey"), proto)
    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        client = h.get_client("agent", "dkey")
    assert "dkey" in HiveMindHttpHandler.registry

    with patch.object(proto, "handle_client_disconnected") as gone:
        _run(h.post())

    gone.assert_called_once_with(client)
    h.write.assert_called_with({"status": "Disconnected"})
    assert "dkey" not in HiveMindHttpHandler.registry


def test_disconnect_releases_the_cached_connection_even_if_the_protocol_raises(master):
    """The cached connection goes away whether or not the protocol callback
    succeeds, otherwise a later /connect would silently reuse a client whose
    Noise session is gone."""
    proto = master.hm_protocol
    user = _make_user()
    h = _make_handler(DisconnectHandler, _encode("agent:dkey"), proto)
    with patch.object(proto.db, "get_client_by_api_key", return_value=user):
        h.get_client("agent", "dkey")
    assert "dkey" in HiveMindHttpHandler.registry

    with patch.object(proto, "handle_client_disconnected", side_effect=RuntimeError("boom")):
        _run(h.post())

    h.set_status.assert_called_with(500)
    h.write.assert_called_with({"error": "Disconnection failed"})
    assert "dkey" not in HiveMindHttpHandler.registry
    assert not h.is_connected("dkey")
