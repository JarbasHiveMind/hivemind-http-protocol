# Architecture

## Class hierarchy

```
hivemind_plugin_manager.protocols.NetworkProtocol  (abstract)
        │
        └─ hivemind_http_protocol.HiveMindHttpProtocol
                │
                └─ Tornado HTTP application with 5 route handlers
```

`HiveMindHttpProtocol.run()` is the blocking server entry point called by
`hivemind-core`. It builds a Tornado `Application` with five REST routes and
starts the `IOLoop`.

## Polling model

Unlike the WebSocket transport, HTTP connections are not persistent. The server
maintains an in-memory per-client message queue. Outbound messages (server to client)
are held in the queue until the client polls `/get_messages` or
`/get_binary_messages`. The client must `/connect` before sending or polling.

This model adds latency proportional to the polling interval, but works in
environments where long-lived connections are blocked.

## Session state

The default session backend is `memory`, which stores connected clients and
pending replies inside the current process. That is suitable for one listener
process, or for deployments where the reverse proxy pins every HTTP session to
the same replica. The server emits `X-HiveMind-HTTP-Replica`,
`X-HiveMind-HTTP-Session-Backend`, and a `hivemind_http_replica` cookie so a
proxy can route stickily when desired.

For horizontally scaled listeners, configure `session_backend: redis`. Redis
stores the connected flag and pending text/binary reply queues, so `/send_message`
and `/get_messages` may land on different replicas without losing replies.

### Retention bound

A polled transport has no backpressure: nothing tells the server that a client
stopped reading. HIVEMIND-TRANSPORT-1 §4 therefore permits a documented
retention bound, and this transport applies two.

A client holds at most `max_undelivered` frames (default 256). Past that the
oldest frame is dropped, because a client that resumes polling wants the
current state of the conversation.

A client that does not poll for `undelivered_ttl` seconds (default 300) has its
whole queue discarded. This bounds the number of queues, not just their size —
without it, one queue is left behind per access key that ever connects.

Both drops are logged at WARNING.

The bound lives in `RetentionQueue` (one client's outbox) and `RetentionStore` (the
outboxes, with stale clients swept on each access). `ClientRegistry` owns the connection
cache and both stores, text and binary, so dropping a client drops all three together.

## Route handlers

| Route | Handler | Purpose |
|---|---|---|
| `/connect` | `ConnectHandler` | Opens a session and populates `HiveMindClientConnection` from the database row — every ACL field, not a subset (see [ACL field parity](#acl-field-parity) below). |
| `/disconnect` | `DisconnectHandler` | Tears down the session. |
| `/send_message` | `SendMessageHandler` | Accepts an encoded HiveMessage and dispatches it. |
| `/get_messages` | `GetMessagesHandler` | Returns and drains the text-message queue. |
| `/get_binary_messages` | `GetBinMessagesHandler` | Returns and drains the binary-message queue (Base64). |

## ACL field parity

A node accepts clients over WebSocket and over HTTP, and both transports
build `HiveMindClientConnection` from the same database row. `ConnectHandler`
copies every ACL field the WebSocket transport copies: `allowed_types`, `can_broadcast`, `can_propagate`, `can_escalate`, `is_admin`,
`intent_blacklist`, `skill_blacklist`. A field either transport leaves out
keeps the connection dataclass's permissive default instead of the
database's actual value for that client.

`can_broadcast`, `intent_blacklist`, and `skill_blacklist` were missing from
this list until the field carried over: `hivemind-core blacklist-broadcast`
was enforced over WebSocket and a silent no-op over HTTP against the same
node, and per-client skill/intent blacklists did not apply to an HTTP client
at all. Any new ACL field added to the client-connection dataclass must be
copied here too, or the same gap reopens for that field.

## TLS

Identical to the WebSocket transport: set `ssl: true` in config. A self-signed
cert is auto-generated if the key file does not exist. See
[hivemind-websocket-protocol: TLS setup](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/operations.md#tls-setup).

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and entry-point registration pattern.

---
[← API](api.md) · [Home](../README.md) · [Operations →](operations.md)
