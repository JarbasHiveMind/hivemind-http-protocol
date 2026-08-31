"""Both transports must reach the same admission decision for one DB row.

A node accepts clients over WebSocket and over HTTP, and each builds its
connection object from the same database record. Any ACL field one transport
copies and the other does not is a weaker door into the same node: the
`hivemind-core` command that sets it appears to work and is enforced on one
transport only.
"""
import inspect
import re

import hivemind_http_protocol

#: Every ACL field a transport must carry from the database row onto the
#: connection. Listed explicitly rather than derived from the other transport
#: at runtime, so the check does not depend on which version happens to be
#: installed. When a new ACL field is added, add it here.
REQUIRED_ACL_FIELDS = {
    "crypto_key",
    "allowed_types",
    "can_broadcast",
    "can_propagate",
    "can_escalate",
    "is_admin",
}


def _copied_fields() -> set:
    src = inspect.getsource(hivemind_http_protocol)
    return {m.group(1)
            for m in re.finditer(r"client\.(\w+)\s*=\s*user\.(\w+)", src)}


def test_every_acl_field_is_carried_from_the_database():
    missing = REQUIRED_ACL_FIELDS - _copied_fields()

    assert not missing, (
        f"HTTP clients silently keep the permissive dataclass default for: "
        f"{sorted(missing)}")


def test_broadcast_permission_is_among_them():
    """`hivemind-core blacklist-broadcast` exists to set this False; without
    the copy it was a no-op over HTTP while being enforced over WebSocket."""
    assert "can_broadcast" in _copied_fields()
