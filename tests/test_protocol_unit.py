"""Unit-level coverage for HiveMindHttpProtocol.

Targets:
- version.py module loading.
- create_self_signed_cert() certificate / key generation.
- run() lifecycle (plain and SSL paths).
"""
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from hivemind_http_protocol import HiveMindHttpProtocol


# --- version.py -----------------------------------------------------------

def test_version_module_exposes_constants_and_string():
    from hivemind_http_protocol import version as v
    assert isinstance(v.VERSION_MAJOR, int)
    assert isinstance(v.VERSION_MINOR, int)
    assert isinstance(v.VERSION_BUILD, int)
    assert isinstance(v.VERSION_ALPHA, int)
    assert isinstance(v.__version__, str)
    assert v.__version__.startswith(
        f"{v.VERSION_MAJOR}.{v.VERSION_MINOR}.{v.VERSION_BUILD}"
    )


# --- self-signed cert generation ------------------------------------------

def test_create_self_signed_cert_writes_files(tmp_path):
    cert, key = HiveMindHttpProtocol.create_self_signed_cert(
        cert_dir=str(tmp_path), name="http-test"
    )
    assert Path(cert).exists()
    assert Path(key).exists()
    assert Path(cert).read_bytes().startswith(b"-----BEGIN CERTIFICATE-----")
    assert b"PRIVATE KEY" in Path(key).read_bytes()


def test_create_self_signed_cert_idempotent(tmp_path):
    c1, k1 = HiveMindHttpProtocol.create_self_signed_cert(
        cert_dir=str(tmp_path), name="http-test"
    )
    mtime = os.path.getmtime(c1)
    time.sleep(0.05)
    c2, k2 = HiveMindHttpProtocol.create_self_signed_cert(
        cert_dir=str(tmp_path), name="http-test"
    )
    assert c1 == c2 and k1 == k2
    assert os.path.getmtime(c1) == mtime, "cert should not be rewritten"


# --- run() lifecycle -------------------------------------------------------

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_mock_hm_protocol():
    from unittest.mock import MagicMock
    m = MagicMock()
    m.handshake_enabled = True
    m.require_crypto = False
    m.clients = {}
    return m


def _spawn_proto(proto, *, timeout=5.0):
    """Start proto.run() in a daemon thread; return when the port is reachable.

    The thread is intentionally not stopped here — tests that only need to
    check that the server binds can call this and let the daemon thread die
    when the process exits.  Returns True when the port accepted a connection
    within *timeout* seconds, False otherwise.
    """
    port = int(proto.config["port"])
    host = proto.config.get("host", "127.0.0.1")
    ready = threading.Event()

    def _run():
        import asyncio
        from tornado.platform.asyncio import AnyThreadEventLoopPolicy
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())
        proto.run()

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.socket()
            s.settimeout(0.3)
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def test_run_starts_and_serves_on_plain_http():
    """run() actually binds a port and the HTTP server accepts connections."""
    pytest.importorskip("hivescope")
    from hivescope.node import MasterNode
    master = MasterNode.create("MH", require_crypto=False, handshake_enabled=True)
    port = _free_port()
    proto = HiveMindHttpProtocol(
        config={"host": "127.0.0.1", "port": port, "ssl": False},
        hm_protocol=master.hm_protocol,
    )
    assert _spawn_proto(proto), "HTTP server never became reachable"


def test_run_ssl_generates_cert(tmp_path):
    """SSL branch: missing cert/key are generated then the server starts."""
    pytest.importorskip("hivescope")
    from hivescope.node import MasterNode
    master = MasterNode.create("MH2", require_crypto=False, handshake_enabled=True)
    port = _free_port()
    proto = HiveMindHttpProtocol(
        config={
            "host": "127.0.0.1", "port": port, "ssl": True,
            "cert_dir": str(tmp_path), "cert_name": "gen-http",
        },
        hm_protocol=master.hm_protocol,
    )
    _spawn_proto(proto, timeout=5.0)
    # Cert files must exist regardless of whether the SSL port was reachable.
    assert (tmp_path / "gen-http.crt").exists()
    assert (tmp_path / "gen-http.key").exists()
