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

## Route handlers

| Route | Handler | Purpose |
|---|---|---|
| `/connect` | `ConnectHandler` | Opens a session and populates `HiveMindClientConnection`. |
| `/disconnect` | `DisconnectHandler` | Tears down the session. |
| `/send_message` | `SendMessageHandler` | Accepts an encoded HiveMessage and dispatches it. |
| `/get_messages` | `GetMessagesHandler` | Returns and drains the text-message queue. |
| `/get_binary_messages` | `GetBinMessagesHandler` | Returns and drains the binary-message queue (Base64). |

## TLS

Identical to the WebSocket transport: set `ssl: true` in config. A self-signed
cert is auto-generated if the key file does not exist. See
[hivemind-websocket-protocol: TLS setup](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/operations.md#tls-setup).

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and entry-point registration pattern.

---
[← API](api.md) · [Home](../README.md) · [Operations →](operations.md)
