import asyncio
import dataclasses
import json
import os
import os.path
import random
import threading
import time
from collections import defaultdict
from queue import Queue
from os import makedirs
from os.path import exists, join
from socket import gethostname
from typing import Dict, Any, Optional, Tuple, Union
from urllib.parse import quote

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
    from hivemind_core.config import get_server_config, runtime_password_min_bits
except ImportError:  # released hivemind-core without the helper
    import os

    def get_server_config():
        return {}

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


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _redis_url_from_config(config: Dict[str, Any]) -> str:
    host = config.get("host")
    port = config.get("port", 6379)
    db = config.get("db", 0)
    if not host:
        return ""
    username = config.get("username")
    password = config.get("password")
    if username and password:
        auth = f"{quote(str(username), safe='')}:{quote(str(password), safe='')}@"
    elif password:
        auth = f":{quote(str(password), safe='')}@"
    else:
        auth = ""
    return f"redis://{auth}{host}:{port}/{db}"


def _redis_config_from_server() -> Dict[str, Any]:
    try:
        database = get_server_config().get("database", {})
    except Exception as exc:
        LOG.warning("Could not read HiveMind server database config: %s", exc)
        return {}
    module = database.get("module")
    config = database.get(module, {}) if module else {}
    if module != "hivemind-redis-db-plugin":
        return {}
    return dict(config)


def _redis_session_prefix_from_config(config: Dict[str, Any]) -> str:
    db_prefix = config.get("index_prefix") or config.get("prefix")
    if not db_prefix:
        return "hivemind-http"
    return f"{str(db_prefix).strip(':')}:hivemind-http"


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
        HiveMindHttpHandler.configure_session_state(self.config)

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
                LOG.info("Generating self-signed SSL certificate")
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


class ClientDatabaseSync:
    """Collapses concurrent ``db.sync()`` calls into one per ``debounce_s``.

    HTTP is request-oriented, so an unknown api-key would otherwise sync the
    database once per request rather than once per connection. One instance
    is shared by every handler, so the state is deliberately process-wide.

    A failing sync is remembered for the rest of the window and re-raised at
    the callers that arrive during it, rather than each of them retrying a
    database that has just proven unreachable.
    """

    def __init__(self, debounce_s: float = 1.0):
        self.debounce_s = debounce_s
        self._lock = threading.Lock()
        self._last_ts: Optional[float] = None
        self._last_error: Optional[Exception] = None

    def reset(self) -> None:
        with self._lock:
            self._last_ts = None
            self._last_error = None

    def sync(self, db: Any) -> None:
        sync = getattr(db, "sync", None)
        if not callable(sync):
            return
        with self._lock:
            now = time.monotonic()
            if self._last_ts is not None and now - self._last_ts < self.debounce_s:
                if self._last_error is not None:
                    raise self._last_error
                return
            self._last_ts = now
            try:
                sync()
            except Exception as exc:
                self._last_error = exc
                raise
            else:
                self._last_error = None


class RedisHttpSessionState:
    """Shared HTTP session state for multi-replica deployments.

    The HTTP protocol buffers outbound messages until the client polls for
    them. With multiple listener replicas, process memory is not enough: the
    request that sends a message and the request that polls for replies may
    land on different pods. This store keeps only the small pieces that need
    to cross replicas: the connected flag and the pending text/binary queues.
    """

    def __init__(
            self,
            redis_url: str,
            prefix: str = "hivemind-http",
            session_ttl_s: int = 3600,
            queue_ttl_s: int = 300
    ):
        if not redis_url:
            raise ValueError("redis_url is required for Redis HTTP session state")
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError(
                "Install hivemind-http-protocol[redis] or redis to use "
                "Redis HTTP session state"
            ) from exc
        self.client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
        )
        self.prefix = prefix.strip(":") or "hivemind-http"
        self.session_ttl_s = session_ttl_s
        self.queue_ttl_s = queue_ttl_s

    def _key(self, key: str, suffix: str) -> str:
        return f"{self.prefix}:{suffix}:{key}"

    def connect(self, key: str, replica_id: str) -> None:
        value = json.dumps({"replica_id": replica_id, "ts": time.time()})
        self.client.setex(self._key(key, "session"), self.session_ttl_s, value)

    def touch(self, key: str) -> None:
        self.client.expire(self._key(key, "session"), self.session_ttl_s)

    def is_connected(self, key: str) -> bool:
        return bool(self.client.exists(self._key(key, "session")))

    def disconnect(self, key: str) -> None:
        self.client.delete(
            self._key(key, "session"),
            self._key(key, "messages"),
            self._key(key, "bin_messages")
        )

    def enqueue(self, key: str, payload: str, is_bin: bool) -> None:
        queue_key = self._key(key, "bin_messages" if is_bin else "messages")
        self.client.rpush(queue_key, payload)
        self.client.expire(queue_key, self.queue_ttl_s)
        self.touch(key)

    def drain(self, key: str, is_bin: bool) -> list:
        queue_key = self._key(key, "bin_messages" if is_bin else "messages")
        messages = []
        while True:
            payload = self.client.lpop(queue_key)
            if payload is None:
                break
            messages.append(payload)
        self.touch(key)
        return messages


class HiveMindHttpHandler(web.RequestHandler):
    """Base handler for HTTP requests."""
    hm_protocol = None
    replica_id = gethostname()
    session_backend = "memory"
    redis_state: Optional[RedisHttpSessionState] = None

    # Class-level properties for managing client state and message queues
    clients: Dict[str, HiveMindClientConnection] = {}
    undelivered: Dict[str, Queue] = defaultdict(Queue)  # Non-binary messages
    undelivered_bin: Dict[str, Queue] = defaultdict(Queue)  # Binary messages
    db_sync = ClientDatabaseSync()

    @classmethod
    def configure_session_state(cls, config: Dict[str, Any]) -> None:
        backend = (
            config.get("session_backend")
            or os.environ.get("HIVEMIND_HTTP_SESSION_BACKEND")
            or "memory"
        ).strip().lower()
        cls.replica_id = str(
            config.get("replica_id")
            or os.environ.get("HIVEMIND_HTTP_REPLICA_ID")
            or gethostname()
        )
        if backend == "redis":
            redis_config = _redis_config_from_server()
            redis_url = (
                config.get("session_redis_url")
                or config.get("redis_url")
                or os.environ.get("HIVEMIND_HTTP_REDIS_URL")
                or os.environ.get("REDIS_URL")
                or _redis_url_from_config(redis_config)
                or ""
            )
            prefix = config.get("session_prefix")
            if not prefix:
                prefix = _redis_session_prefix_from_config(redis_config)
            cls.redis_state = RedisHttpSessionState(
                redis_url=redis_url,
                prefix=str(prefix),
                session_ttl_s=_as_int(config.get("session_ttl_s"), 3600),
                queue_ttl_s=_as_int(config.get("queue_ttl_s"), 300),
            )
            cls.session_backend = "redis"
            LOG.info("HTTP session state backend: redis")
        elif backend == "memory":
            cls.redis_state = None
            cls.session_backend = "memory"
            LOG.info("HTTP session state backend: memory")
        else:
            raise ValueError(f"Unsupported HTTP session backend: {backend}")

    def set_default_headers(self):
        self.set_header("X-HiveMind-HTTP-Replica", self.replica_id)
        self.set_header("X-HiveMind-HTTP-Session-Backend", self.session_backend)

    def mark_affinity(self):
        # Reverse proxies can use this cookie for sticky-session routing when
        # the memory backend is used. Redis-backed deployments do not need it,
        # but the header still makes routing visible during troubleshooting.
        self.set_cookie(
            "hivemind_http_replica",
            self.replica_id,
            httponly=True,
            samesite="Lax"
        )

    def mark_connected(self, key: str) -> None:
        if self.redis_state is not None:
            self.redis_state.connect(key, self.replica_id)

    def clear_connected(self, key: str) -> None:
        if self.redis_state is not None:
            self.redis_state.disconnect(key)
        self.undelivered.pop(key, None)
        self.undelivered_bin.pop(key, None)
        self.clients.pop(key, None)

    def is_connected(self, key: str) -> bool:
        if key in self.clients:
            return True
        if self.redis_state is not None:
            return self.redis_state.is_connected(key)
        return False

    def enqueue_message(self, key: str, payload: Union[bytes, str], is_bin: bool) -> None:
        if is_bin:
            payload = pybase64.b64encode(payload).decode("utf-8")
        elif isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        if self.redis_state is not None:
            self.redis_state.enqueue(key, payload, is_bin)
        elif is_bin:
            self.undelivered_bin[key].put(payload)
        else:
            self.undelivered[key].put(payload)

    def drain_messages(self, key: str, is_bin: bool) -> list:
        if self.redis_state is not None:
            return self.redis_state.drain(key, is_bin)

        messages = []
        queue = self.undelivered_bin[key] if is_bin else self.undelivered[key]
        while not queue.empty():
            try:
                messages.append(queue.get_nowait())
            except Exception:
                # Handle unexpected errors (unlikely with get_nowait)
                break
        return messages

    def reconnect_local_client(self, useragent: str, key: str) -> Optional[HiveMindClientConnection]:
        was_local = key in self.clients
        client = self.get_client(useragent, key)
        if client is not None and not was_local:
            self.hm_protocol.handle_new_client(client)
        return client

    def decode_auth(self):
        auth = self.get_argument("authorization", "")
        if not auth:
            self.set_status(400)
            return None, None
        userpass_encoded = bytes(auth, encoding="utf-8")
        userpass_decoded = pybase64.b64decode(userpass_encoded).decode("utf-8")
        return userpass_decoded.split(":", 1)

    def get_client(self, useragent, key, cache=True) -> Optional[HiveMindClientConnection]:
        if cache and key in self.clients:
            return self.clients[key]

        def do_disconnect():
            self.clear_connected(key)

        client = HiveMindClientConnection(
            key=key,
            disconnect=do_disconnect,
            send_msg=lambda payload, is_bin: self.enqueue_message(key, payload, is_bin),
            sess=Session(session_id="default"),  # will be re-assigned once client sends handshake
            name=useragent,
            hm_protocol=self.hm_protocol
        )
        user = self.hm_protocol.db.get_client_by_api_key(key)
        if not user:
            # the key may have been added since the last sync; refresh once
            # per debounce window and look again before rejecting. Syncing
            # first would let any unknown key drive a sync per request.
            self.db_sync.sync(self.hm_protocol.db)
            user = self.hm_protocol.db.get_client_by_api_key(key)
        if not user:
            LOG.error("Client provided an invalid Access key")
            self.hm_protocol.handle_invalid_key_connected(client)
            return None

        client.name = f"{useragent}::{user.client_id}::{user.name}"
        client.crypto_key = user.crypto_key
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

            was_local = key in self.clients
            client = self.get_client(useragent, key)
            if client is None:
                self.set_status(403)
                self.write({"error": "Invalid authorization"})
                return

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

            if not was_local:
                self.hm_protocol.handle_new_client(client)
            self.mark_connected(key)
            self.mark_affinity()
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
            elif self.is_connected(key):
                # The client was connected on another replica. Clear the
                # shared session so subsequent polls stop finding it.
                self.clear_connected(key)
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
            if not self.is_connected(key):
                self.write({"error": "Client is not connected"})
                return

            client = self.reconnect_local_client(useragent, key)
            if client is None:
                self.set_status(403)
                self.write({"error": "Invalid authorization"})
                return

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
            if not self.is_connected(key):
                self.write({"error": "Client is not connected"})
                return

            messages = self.drain_messages(key, is_bin=False)
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
            if not self.is_connected(key):
                self.write({"error": "Client is not connected"})
                return

            messages = self.drain_messages(key, is_bin=True)
            self.write({"status": "messages retrieved", "b64_messages": messages})
        except Exception as e:
            LOG.error(f"Retrieving messages failed: {e}")
            self.set_status(500)
            self.write({"error": "Retrieving messages failed"})
