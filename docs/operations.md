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

## Multi-replica deployments

HTTP keeps a short-lived server session because replies are buffered until the
client polls for them. With the default `memory` backend, all requests for one
client must reach the same listener process. Use one of these options before
benchmarking or running multiple HTTP listener replicas:

- enable sticky sessions in the reverse proxy using the
  `hivemind_http_replica` cookie or `X-HiveMind-HTTP-Replica` header
- configure Redis-backed HTTP session state

Redis mode shares the connected flag and pending reply queues across replicas:

```json
{
  "network_protocol": {
    "hivemind-http-plugin": {
      "port": 5679,
      "session_backend": "redis",
      "session_redis_url": "redis://redis:6379/0",
      "session_prefix": "hivemind-http",
      "session_ttl_s": 3600,
      "queue_ttl_s": 300
    }
  }
}
```

The same can be configured with environment variables:

| Variable | Purpose |
|---|---|
| `HIVEMIND_HTTP_SESSION_BACKEND=redis` | Enable shared HTTP state. |
| `HIVEMIND_HTTP_REDIS_URL=redis://...` | Redis connection URL. |
| `HIVEMIND_HTTP_REPLICA_ID=pod-name` | Optional value exposed in headers/cookie. |

Install the optional dependency when Redis mode is used:

```bash
pip install 'hivemind-http-protocol[redis]'
```

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
