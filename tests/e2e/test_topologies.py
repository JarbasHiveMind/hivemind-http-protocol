"""Multi-satellite smoke checks for the http-protocol repo.

The hivescope harness is in-process, so the protocol-specific transport
isn't exercised here yet — these tests only verify that the in-process
fixture + assertion plumbing works for HTTP-protocol consumers. Real
HTTP-transport coverage belongs in a future hivescope plugin.
"""

from hivescope.scenarios import three_satellites
from hivescope.assertions import (
    assert_handshake_complete,
    assert_client_registered,
)


def test_three_satellites_handshake():
    b = three_satellites()
    b.start_all()
    try:
        m = b.get_master("M0")
        for i in range(3):
            s = b.get_satellite(f"S{i}")
            assert_handshake_complete(m, s)
            assert_client_registered(m, s.peer)
    finally:
        b.stop_all()


def test_disconnect_clears_peer():
    b = three_satellites()
    b.start_all()
    try:
        m = b.get_master("M0")
        s1 = b.get_satellite("S1")
        assert s1.peer in m.connected_peers()
        s1.disconnect()
        assert s1.peer not in m.connected_peers()
    finally:
        b.stop_all()
