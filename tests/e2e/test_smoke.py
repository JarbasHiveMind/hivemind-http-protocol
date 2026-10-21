"""Smoke test verifying hivescope wiring against hivemind-http-protocol."""
import pytest
from hivescope.assertions import assert_handshake_complete

hivescope = pytest.importorskip("hivescope")


def test_hivescope_wiring_handshake(hive):
    """A single-satellite topology completes a handshake end-to-end."""
    master, satellite = hive
    assert_handshake_complete(master, satellite)
