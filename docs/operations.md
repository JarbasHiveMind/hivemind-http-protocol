# Operations

## When to use HTTP vs WebSocket

| Scenario | Recommended transport |
|---|---|
| Normal satellite or client | WebSocket (default) |
| Environment blocks long-lived TCP | HTTP |
| IoT device behind HTTP-only proxy | HTTP |
| Need TTS/STT binary streaming | WebSocket + audio-binary-protocol |

## Polling interval

The client library (`HiveMindHTTPClient`) handles polling internally. For custom
clients, poll `/get_messages` and `/get_binary_messages` every 1–5 seconds.
Polling too frequently wastes resources; polling too slowly increases response
latency.

## Port selection

The default WebSocket transport uses port `5678`. Use `5679` (or any other
available port) for the HTTP transport to avoid conflicts when running both:

```json
{
  "network_protocol": {
    "hivemind-websocket-plugin": {"port": 5678},
    "hivemind-http-plugin": {"port": 5679}
  }
}
```

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and `pyproject.toml` entry-point registration pattern.
The MQTT transport in this same cluster is another concrete example.

---
[← Architecture](architecture.md) · [Home](../README.md)
