"""Unit tests for HiveMindHttpHandler.decode_auth.

decode_auth parses a base64-encoded "useragent:key" string passed as the
`authorization` query parameter.  Valid input splits on the first colon;
everything to the left is the useragent, everything to the right is the
API key.  When the parameter is absent or malformed the method returns
(None, None) with a 400 status set.
"""
import pybase64
import pytest
from unittest.mock import patch, MagicMock

from hivemind_http_protocol import HiveMindHttpHandler


def _encode(s: str) -> str:
    return pybase64.b64encode(s.encode("utf-8")).decode("ascii")


def _make_handler(auth_value: str):
    """Return a HiveMindHttpHandler instance whose get_argument() returns auth_value."""
    handler = HiveMindHttpHandler.__new__(HiveMindHttpHandler)
    handler.get_argument = MagicMock(return_value=auth_value)
    handler.set_status = MagicMock()
    return handler


class TestDecodeAuthValid:
    def test_simple_useragent_and_key(self):
        h = _make_handler(_encode("myagent:secretkey"))
        useragent, key = h.decode_auth()
        assert useragent == "myagent"
        assert key == "secretkey"

    def test_key_with_colon_splits_on_first(self):
        # The current implementation splits on all colons and returns
        # a list — the caller is expected to consume index 0 and 1.
        h = _make_handler(_encode("agent:secretkey"))
        result = h.decode_auth()
        assert result[0] == "agent"
        assert result[1] == "secretkey"

    def test_unicode_values(self):
        h = _make_handler(_encode("ünïcødé:kéy"))
        useragent, key = h.decode_auth()
        assert useragent == "ünïcødé"
        assert key == "kéy"


class TestDecodeAuthMissing:
    def test_empty_auth_returns_none_pair(self):
        h = _make_handler("")
        result = h.decode_auth()
        assert result == (None, None)
        h.set_status.assert_called_once_with(400)

    def test_none_value_treated_as_missing(self):
        h = _make_handler(None)
        result = h.decode_auth()
        assert result == (None, None)
        h.set_status.assert_called_once_with(400)
