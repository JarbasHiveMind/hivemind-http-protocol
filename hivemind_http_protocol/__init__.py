import asyncio
import dataclasses
import os
import os.path
import random
import time
from collections import deque
from os import makedirs
from os.path import exists, join
from socket import gethostname
from typing import Dict, Any, Optional, Tuple, Union

import pybase64
from OpenSSL import crypto
from ovos_bus_client.session import Session
from ovos_utils.log import LOG
from ovos_utils.xdg_utils import xdg_data_home
from tornado import ioloop
from tornado import web
from tornado.platform.asyncio import AnyThreadEventLoopPolicy

from hivemind_bus_client.message import HiveMessageType
try:
    from hivemind_core.config import runtime_password_min_bits
except ImportError:  # released hivemind-core without the helper
    import os

    def runtime_password_min_bits():
        return 0.0 if os.environ.get("HIVEMIND_DISABLE_PASSWORD_STRENGTH_CHECK", "").strip().lower() in ("1", "true", "yes", "on") else 40.0

from hivemind_core.protocol import (
    HiveMindListenerProtocol,
    HiveMindClientConnection,
    HiveMindNodeType
)
from hivemind_plugin_manager.protocols import ClientCallbacks
from hivemind_plugin_manager.protocols import NetworkProtocol
from poorman_handshake import PasswordHandShake


# HIVEMIND-TRANSPORT-1 §4 retention bound. The reference client polls every
# second, so five minutes is well over the "order of magnitude above the poll
# interval" the spec asks for, and 512 frames is more than a conversational
# peer accumulates between polls. Both are documented in the README and
# overridable through the ``retention_seconds`` / ``max_queued_frames``
# config keys.
DEFAULT_RETENTION_SECONDS = 300.0
DEFAULT_MAX_QUEUED_FRAMES = 512


class OutboundQueue:
    """A peer's undelivered frames, held until it polls (TRANSPORT-1 §4).

    This binding has no server push, so the node owes a polling peer its
    frames. The obligation has to end somewhere: a peer that opens a session
    and never polls would otherwise grow this queue until the node runs out
    of memory, and a publicly reachable node does not choose who connects.

    Two bounds end it — a frame older than ``retention_seconds`` is dropped,
    and the queue never holds more than ``max_frames``, oldest first.
    """

    def __init__(self, retention_seconds: float, max_frames: int) -> None:
        self.retention_seconds = retention_seconds
        self.max_frames = max_frames
        self._frames: "deque[Tuple[float, Union[bytes, str]]]" = deque()

    def __len__(self) -> int:
        return len(self._frames)

    def put(self, payload: Union[bytes, str]) -> None:
        self._expire()
        dropped = 0
        while len(self._frames) >= self.max_frames:
            self._frames.popleft()
            dropped += 1
        if dropped:
            LOG.warning(f"outbound queue full ({self.max_frames} frames), "
                        f"dropped the {dropped} oldest undelivered frame(s)")
        self._frames.append((time.monotonic(), payload))

    def drain(self) -> list:
        """Hand over everything still within the retention bound."""
        self._expire()
        frames = [payload for _, payload in self._frames]
        self._frames.clear()
        return frames

    def _expire(self) -> None:
        deadline = time.monotonic() - self.retention_seconds
        dropped = 0
        while self._frames and self._frames[0][0] < deadline:
            self._frames.popleft()
            dropped += 1
        if dropped:
            LOG.warning(f"dropped {dropped} undelivered frame(s) unpolled for "
                        f"more than {self.retention_seconds}s")


@dataclasses.dataclass
class HiveMindHttpProtocol(NetworkProtocol):
    """
    HTTP handler for managing HiveMind client connections.

    Attributes:
        hm_protocol (Optional[HiveMindListenerProtocol]): The protocol instance for handling HiveMind messages.
    """
    config: Dict[str, Any] = dataclasses.field(default_factory=dict)
    hm_protocol: Optional[HiveMindListenerProtocol] = None
    callbacks: ClientCallbacks = dataclasses.field(default_factory=ClientCallbacks)

    def run(self):
        LOG.debug(f"HTTP server config: {self.config}")
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())
        HiveMindHttpHandler.hm_protocol = self.hm_protocol
        HiveMindHttpHandler.retention_seconds = float(
            self.config.get("retention_seconds", DEFAULT_RETENTION_SECONDS))
        HiveMindHttpHandler.max_queued_frames = int(
            self.config.get("max_queued_frames", DEFAULT_MAX_QUEUED_FRAMES))

        ssl = self.config.get("ssl", False)
        cert_dir: str = self.config.get("cert_dir") or f"{xdg_data_home()}/hivemind"
        cert_name: str = self.config.get("cert_name") or "hivemind"
        host = self.config.get("host", "0.0.0.0")
        port = int(self.config.get("port", 5679))

        routes = [
            (r"/connect", ConnectHandler),
            (r"/disconnect", DisconnectHandler),
            (r"/send_message", SendMessageHandler),
            (r"/get_messages", GetMessagesHandler),
            (r"/get_binary_messages", GetBinMessagesHandler),
        ]
        application = web.Application(routes)
        if ssl:
            cert_file = f"{cert_dir}/{cert_name}.crt"
            key_file = f"{cert_dir}/{cert_name}.key"
            if not os.path.isfile(key_file):
                LOG.info(f"Generating self-signed SSL certificate")
                cert_file, key_file = self.create_self_signed_cert(cert_dir, cert_name)
            LOG.debug("Using SSL key at " + key_file)
            LOG.debug("Using SSL certificate at " + cert_file)
            ssl_options = {"certfile": cert_file, "keyfile": key_file}
            LOG.info(f"HTTPS listener started at port: {port}")
            application.listen(port, host, ssl_options=ssl_options)
        else:
            LOG.info(f"HTTP listener started at port: {port}")
            application.listen(port, host)

        ioloop.IOLoop.current().start()

    @staticmethod
    def create_self_signed_cert(
            cert_dir: str = f"{xdg_data_home()}/hivemind",
            name: str = "hivemind"
    ) -> Tuple[str, str]:
        """
        Create a self-signed certificate and key pair if they do not already exist.

        Args:
            cert_dir (str): The directory where the certificate and key will be stored.
            name (str): The base name for the certificate and key files.

        Returns:
            Tuple[str, str]: The paths to the created certificate and key files.
        """
        cert_file = name + ".crt"
        key_file = name + ".key"
        cert_path = join(cert_dir, cert_file)
        key_path = join(cert_dir, key_file)
        makedirs(cert_dir, exist_ok=True)

        if not exists(join(cert_dir, cert_file)) or not exists(join(cert_dir, key_file)):
            # Create a key pair
            k = crypto.PKey()
            k.generate_key(crypto.TYPE_RSA, 2048)

            # Create a self-signed certificate
            cert = crypto.X509()
            cert.get_subject().C = "PT"
            cert.get_subject().ST = "Europe"
            cert.get_subject().L = "Mountains"
            cert.get_subject().O = "Jarbas AI"
            cert.get_subject().OU = "Powered by HiveMind"
            cert.get_subject().CN = gethostname()
            cert.set_serial_number(random.randint(0, 2000))
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)
            cert.set_issuer(cert.get_subject())
            cert.set_pubkey(k)
            cert.sign(k, "sha256")

            open(cert_path, "wb").write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
            open(key_path, "wb").write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))

        return cert_path, key_path


class HiveMindHttpHandler(web.RequestHandler):
    """Base handler for HTTP requests."""
    hm_protocol = None

    # Class-level properties for managing client state and message queues
    clients: Dict[str, HiveMindClientConnection] = {}
    undelivered: Dict[str, OutboundQueue] = {}  # Non-binary messages
    undelivered_bin: Dict[str, OutboundQueue] = {}  # Binary messages
    retention_seconds: float = DEFAULT_RETENTION_SECONDS
    max_queued_frames: int = DEFAULT_MAX_QUEUED_FRAMES

    @classmethod
    def queue_for(cls, store: Dict[str, OutboundQueue], key: str) -> OutboundQueue:
        if key not in store:
            store[key] = OutboundQueue(cls.retention_seconds, cls.max_queued_frames)
        return store[key]

    def decode_auth(self):
        auth = self.get_argument("authorization", "")
        if not auth:
            self.set_status(400)
            return None, None
        userpass_encoded = bytes(auth, encoding="utf-8")
        userpass_decoded = pybase64.b64decode(userpass_encoded).decode("utf-8")
        return userpass_decoded.split(":")

    def get_client(self, useragent, key, cache=True) -> Optional[HiveMindClientConnection]:
        if cache and key in self.clients:
            return self.clients[key]

        def do_send(payload: Union[bytes, str], is_bin: bool):
            if is_bin:
                payload = pybase64.b64encode(payload).decode("utf-8")
                self.queue_for(self.undelivered_bin, key).put(payload)
            else:
                self.queue_for(self.undelivered, key).put(payload)

        def do_disconnect():
            # TRANSPORT-1 §4: the session closing ends the retention
            # obligation for both queues
            self.undelivered.pop(key, None)
            self.undelivered_bin.pop(key, None)
            self.clients.pop(key, None)

        client = HiveMindClientConnection(
            key=key,
            disconnect=do_disconnect,
            send_msg=do_send,
            sess=Session(session_id="default"),  # will be re-assigned once client sends handshake
            name=useragent,
            hm_protocol=self.hm_protocol
        )
        self.hm_protocol.db.sync()
        user = self.hm_protocol.db.get_client_by_api_key(key)
        if not user:
            LOG.error("Client provided an invalid Access key")
            self.hm_protocol.handle_invalid_key_connected(client)
            return None

        client.name = f"{useragent}::{user.client_id}::{user.name}"
        client.crypto_key = user.crypto_key
        client.skill_blacklist = user.skill_blacklist or []
        client.intent_blacklist = user.intent_blacklist or []
        client.allowed_types = user.allowed_types
        client.can_propagate = user.can_propagate
        client.can_escalate = user.can_escalate
        client.is_admin = user.is_admin
        if user.password:
            # pre-shared password to derive aes_key
            client.pswd_handshake = PasswordHandShake(user.password, min_bits=runtime_password_min_bits())

        client.node_type = HiveMindNodeType.NODE  # TODO . placeholder
        if cache:
            self.clients[key] = client
        return client


class ConnectHandler(HiveMindHttpHandler):
    async def post(self):
        try:
            useragent, key = self.decode_auth()
            if not key:
                self.write({"error": "Missing authorization"})
                return

            client = self.get_client(useragent, key)

            if (
                    not client.crypto_key
                    and not self.hm_protocol.handshake_enabled
                    and self.hm_protocol.require_crypto
            ):
                LOG.error(
                    "No pre-shared crypto key for client and handshake disabled, "
                    "but configured to require crypto!"
                )
                # clients requiring handshake support might fail here
                self.hm_protocol.handle_invalid_protocol_version(client)
                return

            self.hm_protocol.handle_new_client(client)
            self.write({"status": "Connected"})
        except Exception as e:
            LOG.error(f"Connection failed: {e}")
            self.set_status(500)
            self.write({"error": "Connection failed"})


class DisconnectHandler(HiveMindHttpHandler):
    async def post(self):

        try:
            useragent, key = self.decode_auth()
            if not key:
                self.write({"error": "Missing authorization"})
                return
            if key in HiveMindHttpHandler.clients:
                client = self.get_client(useragent, key)
                LOG.info(f"disconnecting client: {client.peer}")
                self.hm_protocol.handle_client_disconnected(client)
                self.write({"status": "Disconnected"})
            else:
                self.write({"error": "Already Disconnected"})
        except Exception as e:
            LOG.error(f"Disconnection failed: {e}")
            self.set_status(500)
            self.write({"error": "Disconnection failed"})


class SendMessageHandler(HiveMindHttpHandler):
    async def post(self):
        try:
            useragent, key = self.decode_auth()
            if not key:
                self.write({"error": "Missing authorization"})
                return
            # refuse if connect wasnt called first
            if key not in HiveMindHttpHandler.clients:
                self.write({"error": "Client is not connected"})
                return

            client = self.get_client(useragent, key)

            message = self.get_argument("message", "")
            if not message:
                self.set_status(400)
                self.write({"error": "Missing message"})
                return

            message = client.decode(message)
            if (
                    message.msg_type == HiveMessageType.BUS
                    and message.payload.msg_type == "recognizer_loop:b64_audio"
            ):
                LOG.info(f"Received {client.peer} sent base64 audio for STT")
            else:
                LOG.info(f"Received {client.peer} message: {message}")
            self.hm_protocol.handle_message(message, client)

            self.write({"status": "message sent"})
        except Exception as e:
            LOG.error(f"Message sending failed: {e}")
            self.set_status(500)
            self.write({"error": "Message sending failed"})


class GetMessagesHandler(HiveMindHttpHandler):

    async def get(self):
        try:
            useragent, key = self.decode_auth()
            if not key:
                self.write({"error": "Missing authorization"})
                return

            # refuse if connect wasnt called first
            if key not in HiveMindHttpHandler.clients:
                self.write({"error": "Client is not connected"})
                return

            messages = self.queue_for(HiveMindHttpHandler.undelivered, key).drain()
            self.write({"status": "messages retrieved", "messages": messages})
        except Exception as e:
            LOG.error(f"Retrieving messages failed: {e}")
            self.set_status(500)
            self.write({"error": "Retrieving messages failed"})


class GetBinMessagesHandler(HiveMindHttpHandler):

    async def get(self):
        try:
            useragent, key = self.decode_auth()
            if not key:
                self.write({"error": "Missing authorization"})
                return

            # refuse if connect wasnt called first
            if key not in HiveMindHttpHandler.clients:
                self.write({"error": "Client is not connected"})
                return

            messages = self.queue_for(HiveMindHttpHandler.undelivered_bin, key).drain()
            self.write({"status": "messages retrieved", "b64_messages": messages})
        except Exception as e:
            LOG.error(f"Retrieving messages failed: {e}")
            self.set_status(500)
            self.write({"error": "Retrieving messages failed"})
