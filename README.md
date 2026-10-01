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
    key="my-access-key",
    password="my-password",
    host="http://localhost",
    port=5679,
    bin_callbacks=MyBinaryCallbacks(),
)
client.connect()   # calls POST /connect and completes the handshake
client.start()     # background thread that polls for messages

client.emit(HiveMessage(HiveMessageType.BUS,
                        Message("speak:synth", {"utterance": "hello world"})))
```

`emit()` raises `ConnectionAbortedError` if `connect()` was not called first, and the
server answers a poll from an unconnected key with `{"error": "Client is not connected"}`.

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Bind address. |
| `port` | `5679` | Listen port. |
| `ssl` | `false` | Enable TLS. |
| `cert_dir` | `$XDG_DATA_HOME/hivemind` | Directory for TLS cert/key files. |
| `cert_name` | `hivemind` | Base filename for cert and key. |
| `max_undelivered` | `256` | Frames held per client between polls. The oldest is dropped when the cap is reached. |
| `undelivered_ttl` | `300` | Seconds a client can stop polling before its held frames are discarded. |

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
