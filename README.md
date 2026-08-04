# hivemind-http-protocol

REST/HTTP transport plugin for [hivemind-core](https://github.com/JarbasHiveMind/HiveMind-core).

An alternative to the default WebSocket transport. Clients use HTTP polling (POST to send,
GET to receive) instead of a persistent WebSocket connection. Suitable for environments where
long-lived TCP connections are not possible (firewalls, IoT gateways, HTTP-only proxies).

## Where it fits

```
hivemind-core
  └── hivemind-plugin-manager  (NetworkProtocolFactory loads plugins by entry-point)
        └── hivemind-http-protocol  ← this repo
              └── Tornado HTTP server (REST endpoints)
```

The plugin registers under the `hivemind.network.protocol` entry-point group as
`hivemind-http-plugin`. It can run alongside the WebSocket transport if both are listed
in the `network_protocol` config.

## Install

```bash
pip install hivemind-http-protocol
```

## Quickstart

Add to `~/.config/hivemind-core/server.json`:

```json
{
  "network_protocol": {
    "module": "hivemind-http-plugin",
    "hivemind-http-plugin": {
      "host": "0.0.0.0",
      "port": 5679
    }
  }
}
```

Start hivemind-core:

```bash
hivemind-core listen
```

### Running alongside WebSocket

Both transports can run at the same time by configuring them both:

```json
{
  "network_protocol": {
    "hivemind-websocket-plugin": {
      "host": "0.0.0.0",
      "port": 5678
    },
    "hivemind-http-plugin": {
      "host": "0.0.0.0",
      "port": 5679
    }
  }
}
```

### Python client example

```python
from hivemind_bus_client.http_client import HiveMindHTTPClient, BinaryDataCallbacks
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message


class MyBinaryCallbacks(BinaryDataCallbacks):
    def handle_receive_tts(self, bin_data: bytes, utterance: str,
                           lang: str, file_name: str):
        print(f"received {len(bin_data)} bytes of TTS for: {utterance}")


client = HiveMindHTTPClient(
    host="http://localhost",
    port=5679,
    bin_callbacks=MyBinaryCallbacks(),
)
client.emit(HiveMessage(HiveMessageType.BUS,
                        Message("speak:synth", {"utterance": "hello world"})))
```

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Bind address. |
| `port` | `5679` | Listen port. |
| `ssl` | `false` | Enable TLS. |
| `cert_dir` | `$XDG_DATA_HOME/hivemind` | Directory for TLS cert/key files. |
| `cert_name` | `hivemind` | Base filename for cert and key. |
| `retention_seconds` | `300` | How long an undelivered frame is kept for a peer that has not polled. |
| `max_queued_frames` | `512` | How many undelivered frames are kept per peer, oldest dropped first. |

### Message retention

There is no server push on this binding, so the server holds a peer's outbound
frames until the peer polls them. That obligation is bounded, as
**HIVEMIND-TRANSPORT-1 §4** requires: a frame is dropped once it has waited
`retention_seconds`, and a peer's queue never holds more than
`max_queued_frames`. Closing the session drops both queues.

The defaults are five minutes and 512 frames. The reference client polls every
second, so a peer that keeps polling never loses a frame. Raise
`retention_seconds` if your peers poll on a much longer cycle; a peer that
never polls at all is what the bound protects the server from.

## REST API

Authentication uses an HTTP `authorization` parameter (not a header) containing
a Base64-encoded `useragent:access_key` string.

| Endpoint | Method | Description |
|---|---|---|
| `/connect` | POST | Register a client session. Parameters: `authorization`. |
| `/disconnect` | POST | Remove a client session. Parameters: `authorization`. |
| `/send_message` | POST | Send a HiveMessage. Parameters: `authorization`, `message`. |
| `/get_messages` | GET | Poll for pending text messages. Parameters: `authorization`. |
| `/get_binary_messages` | GET | Poll for pending binary messages (Base64-encoded). Parameters: `authorization`. |

See [docs/api.md](docs/api.md) for full endpoint documentation.

## Docs

- [docs/api.md](docs/api.md): REST endpoint reference
- [docs/architecture.md](docs/architecture.md): handler lifecycle, polling model, TLS
- [docs/operations.md](docs/operations.md): authoring a transport plugin
