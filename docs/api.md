# REST API Reference

All endpoints use HTTP `authorization` as a request parameter (not a header).
The value is Base64-encoded `useragent:access_key`.

Responses include `X-HiveMind-HTTP-Replica` and
`X-HiveMind-HTTP-Session-Backend` headers. With the default in-memory session
backend, clients or reverse proxies should keep one HTTP session on the same
replica. Redis-backed session state removes that requirement.

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
- `200 OK` with an error body: `{"error": "Missing authorization"}` — the handler
  writes the error without setting a status, so the code stays 200
- `403 Forbidden`: `{"error": "Invalid authorization"}` when the access key is not known.
- `500 Internal Server Error`: `{"error": "Connection failed"}`

---

### POST /disconnect

Remove a client session from the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"status": "Disconnected"}`
- `200 OK`: `{"error": "Already Disconnected"}` when no session exists for that key.
- `200 OK` with an error body: `{"error": "Missing authorization"}` — the handler
  writes the error without setting a status, so the code stays 200
- `500 Internal Server Error`: `{"error": "Disconnection failed"}`

---

### POST /send_message

Send a HiveMessage to the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.
- `message` (string, required): Encoded HiveMessage payload. On a protocol v3 (Noise) session this is one Noise transport frame, base64-encoded with the standard alphabet and padding (`validate=True` decoding: no whitespace, no urlsafe `-_`).
- `binary` (string, optional): `1`, `true` or `yes` marks `message` as a base64-encoded Noise transport frame. Absent otherwise; the legacy text path is unchanged.

**Responses:**
- `200 OK`: `{"status": "message sent"}`
- `200 OK`: `{"status": "buffered"}` — a chunk of a multi-frame Noise message was accepted; the message is dispatched when its last frame arrives.
- `400 Bad Request`: `{"error": "Missing message"}`
- `400 Bad Request`: `{"error": "Malformed binary frame"}` — `binary` was set but `message` is not valid base64.
- `403 Forbidden`: `{"error": "Invalid authorization"}`
- `409 Conflict`: `{"error": "Client is not connected"}` when `/connect` was not called first or the session was dropped; call `/connect` again.
- `409 Conflict`: `{"error": "No Noise session; reconnect and handshake"}` — a binary frame arrived on a connection that has no Noise session (the listener restarted, or the session was dropped); call `/connect` again and redo the handshake.
- `500 Internal Server Error`: `{"error": "Message sending failed"}`

Request bodies are capped at 1 MiB. A Noise transport frame carries at most 65000 bytes of plaintext, so larger messages arrive as several frames and no single request needs more.

---

### GET /get_messages

Poll for pending text messages from the server.

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"status": "messages retrieved", "messages": ["<encoded_message1>", "<encoded_message2>"]}`
- `200 OK`: `{"error": "Client is not connected"}` when `/connect` was not called first.
- `200 OK` with an error body: `{"error": "Missing authorization"}` — the handler
  writes the error without setting a status, so the code stays 200
- `500 Internal Server Error`: `{"error": "Retrieving messages failed"}`

The `messages` list may be empty if no messages are pending. Clients should
poll at an appropriate interval (e.g. every 1–5 seconds).

---

### GET /get_binary_messages

Poll for pending binary messages from the server (for example, TTS audio).

**Parameters:**
- `authorization` (string, required): Base64-encoded `useragent:access_key`.

**Responses:**
- `200 OK`: `{"status": "messages retrieved", "b64_messages": ["<base64_message1>", "<base64_message2>"]}`
- `200 OK`: `{"error": "Client is not connected"}` when `/connect` was not called first.
- `200 OK` with an error body: `{"error": "Missing authorization"}` — the handler
  writes the error without setting a status, so the code stays 200
- `500 Internal Server Error`: `{"error": "Retrieving messages failed"}`

Binary payloads (e.g. TTS WAV data) are Base64-encoded in the response.

## Polling model

The HTTP transport is stateful on the server side: the server buffers outbound
messages per client session until the client polls `/get_messages` or
`/get_binary_messages`. Clients must `/connect` before sending or polling,
and `/disconnect` when done. A call that skips `/connect` still answers `200`, with
`{"error": "Client is not connected"}` as the body, so a client must read the body and
not only the status code.

For real-time voice assistant use cases the WebSocket transport is preferred.
Use HTTP when persistent TCP connections are not available.

---
[Home](../README.md) · [Architecture →](architecture.md)
