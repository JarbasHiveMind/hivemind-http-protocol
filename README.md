# HiveMind HTTP Protocol

The HiveMind HTTP Protocol provides an alternative REST-based implementation for message exchange in the HiveMind ecosystem. 


---

## Configuration

This plugin integrates with the `hivemind-core` framework. It is not a standalone project, and its behavior is controlled by the `hivemind-core` configuration.

To enable and configure the HiveMind HTTP Protocol, update the `network_protocol` entry in the `hivemind-core` configuration file. Below is an example configuration:

```json
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
```

---
## Client Library


```python
from hivemind_bus_client.http_client import HiveMindHTTPClient, BinaryDataCallbacks
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message


class BinaryDataHandler(BinaryDataCallbacks):
    def handle_receive_tts(self, bin_data: bytes,
                           utterance: str,
                           lang: str,
                           file_name: str):
        # we can play it or save to file or whatever
        print(f"got {len(bin_data)} bytes of TTS audio")
        print(f"utterance: {utterance}", f"lang: {lang}", f"file_name: {file_name}")
        # got 33836 bytes of TTS audio
        # utterance: hello world lang: en-US file_name: 5eb63bbbe01eeed093cb22bb8f5acdc3.wav
        
# not passing key etc so it uses hivemind identity file for details
client = HiveMindHTTPClient(host="http://localhost", port=5679,
                            bin_callbacks=BinaryDataHandler())

client.emit(HiveMessage(HiveMessageType.BUS,
                        Message("speak:synth", {"utterance": "hello world"})))
```

---

## REST API Documentation

### Authentication

Authentication is handled via an HTTP `authorization` parameter in the request. The value should be a Base64-encoded string in the format `useragent:access_key`.

### Endpoints

#### 1. Connect to the Server

**Endpoint:** `/connect`  
**Method:** `POST`

**Request Parameters:**
- `authorization` (string, mandatory): Base64-encoded `useragent:access_key`.

**Response:**
- `200 OK`: `{ "status": "Connected" }`
- `400 Bad Request`: `{ "error": "Missing authorization" }`
- `500 Internal Server Error`: `{ "error": "Connection failed" }`

---

#### 2. Disconnect from the Server

**Endpoint:** `/disconnect`  
**Method:** `POST`

**Request Parameters:**
- `authorization` (string, mandatory): Base64-encoded `useragent:access_key`.

**Response:**
- `200 OK`: `{ "status": "Disconnected" }`
- `400 Bad Request`: `{ "error": "Missing authorization" }`
- `500 Internal Server Error`: `{ "error": "Disconnection failed" }`

---

#### 3. Send a Message

**Endpoint:** `/send_message`  
**Method:** `POST`

**Request Parameters:**
- `authorization` (string, mandatory): Base64-encoded `useragent:access_key`.
- `message` (string, mandatory): Encoded message payload.

**Response:**
- `200 OK`: `{ "status": "message sent" }`
- `400 Bad Request`: `{ "error": "Missing message" }`
- `500 Internal Server Error`: `{ "error": "Message sending failed" }`

---

#### 4. Retrieve Messages

**Endpoint:** `/get_messages`  
**Method:** `GET`

**Request Parameters:**
- `authorization` (string, mandatory): Base64-encoded `useragent:access_key`.

**Response:**
- `200 OK`: `{ "messages": ["message1", "message2"] }`
- `400 Bad Request`: `{ "error": "Missing authorization" }`
- `500 Internal Server Error`: `{ "error": "Failed to retrieve messages" }`

---

#### 5. Retrieve Binary Messages

**Endpoint:** `/get_binary_messages`  
**Method:** `GET`

**Request Parameters:**
- `authorization` (string, mandatory): Base64-encoded `useragent:access_key`.

**Response:**
- `200 OK`: `{ "messages": ["Base64Message1", "Base64Message2"] }`
- `400 Bad Request`: `{ "error": "Missing authorization" }`
- `500 Internal Server Error`: `{ "error": "Failed to retrieve messages" }`

---

## Notes

- The `connect` and `disconnect` endpoints enable state management in scenarios where persistent connections are not feasible
- Binary messages are Base64-encoded to ensure compatibility with REST APIs, which are text-based protocols.


---

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for bug reports or feature requests.

---

## License

This project is licensed under the Apache 2.0 License. See the `LICENSE` file for more details.
