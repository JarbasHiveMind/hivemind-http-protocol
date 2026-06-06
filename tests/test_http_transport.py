from types import SimpleNamespace

import hivemind_http_protocol as http_protocol


def test_get_client_accepts_client_without_message_blacklist(monkeypatch):
    """Redis alpha clients no longer expose message_blacklist."""

    class FakeConnection:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    user = SimpleNamespace(
        client_id="client-1",
        name="Preview Bridge",
        crypto_key="secret",
        skill_blacklist=[],
        intent_blacklist=[],
        allowed_types=[],
        can_propagate=False,
        can_escalate=False,
        is_admin=False,
        password=None,
    )
    db = SimpleNamespace(
        sync=lambda: None,
        get_client_by_api_key=lambda key: user,
    )
    hm_protocol = SimpleNamespace(
        db=db,
        handle_invalid_key_connected=lambda client: None,
    )
    handler = http_protocol.HiveMindHttpHandler.__new__(http_protocol.HiveMindHttpHandler)
    handler.hm_protocol = hm_protocol
    handler.clients = {}
    handler.undelivered = {}
    handler.undelivered_bin = {}

    monkeypatch.setattr(http_protocol, "HiveMindClientConnection", FakeConnection)

    client = handler.get_client("preview", "api-key")

    assert client.msg_blacklist == []
    assert client.skill_blacklist == []
    assert client.intent_blacklist == []

