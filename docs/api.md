# REST API Reference

All endpoints use HTTP `authorization` as a request parameter (not a header).
The value is Base64-encoded `useragent:access_key`.

## Authentication

```
authorization = base64("my-satellite:abc123apikey")
```

The `useragent` is the client's display name. The `access_key` is the API key
provisioned by `hivemind-core add-client`.

## Endpoints

### POST /connect

Register a client session on the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"status": "Connected"}`
- `400 Bad Request`: `{"error": "Missing authorization"}`
- `500 Internal Server Error`: `{"error": "Connection failed"}`

---

### POST /disconnect

Remove a client session from the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"status": "Disconnected"}`
- `400 Bad Request`: `{"error": "Missing authorization"}`
- `500 Internal Server Error`: `{"error": "Disconnection failed"}`

---

### POST /send_message

Send a HiveMessage to the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.
- `message` (string, required): Encoded HiveMessage payload.

**Responses:**
- `200 OK`: `{"status": "message sent"}`
- `400 Bad Request`: `{"error": "Missing message"}`
- `500 Internal Server Error`: `{"error": "Message sending failed"}`

---

### GET /get_messages

Poll for pending text messages from the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"messages": ["<encoded_message1>", "<encoded_message2>"]}`
- `400 Bad Request`: `{"error": "Missing authorization"}`
- `500 Internal Server Error`: `{"error": "Failed to retrieve messages"}`

The `messages` list may be empty if no messages are pending. Clients should
poll at an appropriate interval (e.g. every 1–5 seconds).

---

### GET /get_binary_messages

Poll for pending binary messages from the server (for example, TTS audio).

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"messages": ["<base64_message1>", "<base64_message2>"]}`
- `400 Bad Request`: `{"error": "Missing authorization"}`
- `500 Internal Server Error`: `{"error": "Failed to retrieve messages"}`

Binary payloads (e.g. TTS WAV data) are Base64-encoded in the response.

## Polling model

The HTTP transport is stateful on the server side: the server buffers outbound
messages per client session until the client polls `/get_messages` or
`/get_binary_messages`. Clients must `/connect` before sending or polling,
and `/disconnect` when done.

For real-time voice assistant use cases the WebSocket transport is preferred.
Use HTTP when persistent TCP connections are not available.

---
[Home](../README.md) · [Architecture →](architecture.md)
