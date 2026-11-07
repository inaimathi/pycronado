# src/pycronado/websocket.py

import asyncio
import time
import json
import re
from collections import defaultdict, deque
import threading
import weakref
from typing import Any, Optional, Sequence

import tornado.ioloop
import tornado.web
import tornado.websocket

from . import token
from .auth import UserMixin
from .mixins import JSONSerializationMixin, LoggerMixin, date_serializer
from .util import normalize_origin


class WebSocketHub(LoggerMixin):
    """Process-local pub/sub with bounded ingress and per-client output.

    publish()/broadcast() are thread-safe and return False on ingress overflow.
    Other operations must run on the owning IOLoop. Slow clients are closed;
    reliable messages are never silently dropped from their output queue.
    """

    def __init__(self, ioloop=None, *, max_subscriptions=128, max_channel_length=256,
                 max_pending_messages=256, max_pending_bytes=1024 * 1024,
                 max_publications=1024, max_clients=10000, write_timeout=30):
        limits = (max_subscriptions, max_channel_length, max_pending_messages,
                  max_pending_bytes, max_publications, max_clients)
        if any(type(value) is not int or value <= 0 for value in limits) or write_timeout <= 0:
            raise ValueError("Hub limits and write_timeout must be positive")
        self.ioloop = ioloop or tornado.ioloop.IOLoop.current()
        self.max_subscriptions = max_subscriptions
        self.max_channel_length = max_channel_length
        self.max_pending_messages = max_pending_messages
        self.max_pending_bytes = max_pending_bytes
        self.max_publications = max_publications
        self.max_clients = max_clients
        self.write_timeout = write_timeout
        self.channels = defaultdict(set)
        self._memberships = {}
        self._clients = set()
        self._detached = weakref.WeakSet()
        self._outgoing = {}
        self._ingress = deque()
        self._lock = threading.Lock()
        self._scheduled = False
        self._closed = False

    def register(self, client):
        if self._closed or client in self._detached or (client not in self._clients and len(self._clients) >= self.max_clients):
            return False
        self._clients.add(client)
        return True

    def validate_channel(self, channel):
        return isinstance(channel, str) and 0 < len(channel) <= self.max_channel_length

    def subscribe(self, channel: str, client) -> bool:
        if not self.validate_channel(channel):
            return False
        memberships = self._memberships.get(client, set())
        if channel not in memberships and len(memberships) >= self.max_subscriptions:
            return False
        if not self.register(client):
            return False
        self.channels[channel].add(client)
        self._memberships.setdefault(client, set()).add(channel)
        return True

    def unsubscribe(self, channel: str, client) -> None:
        subs = self.channels.get(channel)
        if subs is not None:
            subs.discard(client)
            if not subs:
                self.channels.pop(channel, None)
        memberships = self._memberships.get(client)
        if memberships is not None:
            memberships.discard(channel)
            if not memberships:
                self._memberships.pop(client, None)

    def unsubscribe_all(self, client) -> None:
        for channel in tuple(self._memberships.get(client, ())):
            self.unsubscribe(channel, client)
        # Kept for callers inspecting the legacy handler attribute.
        channels = getattr(client, "_ws_channels", None)
        if channels is not None:
            channels.clear()

    def detach(self, client):
        self._detached.add(client)
        self.unsubscribe_all(client)
        self._clients.discard(client)
        state = self._outgoing.pop(client, None)
        if state is not None:
            state["queue"].clear()
            state["bytes"] = 0
            if state["timer"] is not None:
                state["timer"].cancel()

    def _disconnect(self, client, code=1013, reason="slow consumer"):
        self.detach(client)
        close = getattr(client, "close", None)
        if close is not None:
            close(code=code, reason=reason)

    def close(self):
        with self._lock:
            self._closed = True
            self._ingress.clear()
        for client in tuple(self._clients):
            self._disconnect(client, code=1001, reason="server shutdown")

    def _submit(self, channel, message):
        with self._lock:
            if self._closed or len(self._ingress) >= self.max_publications:
                return False
            self._ingress.append((channel, message))
            if not self._scheduled:
                self._scheduled = True
                self.ioloop.add_callback(self._drain_ingress)
        return True

    def publish(self, channel: str, message: Any) -> bool:
        """Enqueue without blocking; False means closed, invalid, or overloaded.

        Do not mutate a message after publishing it. The ingress count bounds
        retained objects; applications must also bound their payload sizes.
        """
        return self.validate_channel(channel) and self._submit(channel, message)

    def broadcast(self, message: Any) -> bool:
        return self._submit(None, message)

    def _drain_ingress(self):
        # Yield between batches so producers cannot monopolize the IOLoop.
        for _ in range(64):
            with self._lock:
                if not self._ingress:
                    self._scheduled = False
                    return
                channel, message = self._ingress.popleft()
            if channel is None:
                self._broadcast_now(message)
            else:
                self._publish_now(channel, message)
        self.ioloop.add_callback(self._drain_ingress)

    def _serialize_for(self, client, message: Any) -> str:
        if isinstance(message, str):
            return message
        dumps = getattr(client, "dumps", None)
        if callable(dumps):
            return dumps(message)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"), default=date_serializer)

    def _cacheable(self, client):
        # Share only the known stateless default codec. Custom serializers may
        # depend on identity or mutable per-client state and are never reused.
        dumps = getattr(client, "dumps", None)
        return dumps is None or (
            getattr(dumps, "__func__", None) is JSONSerializationMixin.dumps
            and client.get_json_serializer() is date_serializer)

    def send(self, client, message):
        """Send control messages through the same bounded queue as publications."""
        try:
            encoded = self._serialize_for(client, message)
        except Exception:
            self.logger.exception("WebSocket serialization failed")
            return False
        return self._write(client, encoded)

    def _write(self, client, encoded):
        if self._closed or client in self._detached:
            return False
        if hasattr(client, "JWT") and client.JWT.get("exp", 0) <= time.time():
            self.detach(client)
            client.on_auth_failure()
            return False
        if not self.register(client):
            self._disconnect(client, reason="connection limit")
            return False
        size = len(encoded.encode("utf-8"))
        state = self._outgoing.setdefault(client, {"queue": deque(), "bytes": 0, "active": False, "timer": None})
        if len(state["queue"]) >= self.max_pending_messages or state["bytes"] + size > self.max_pending_bytes:
            self._disconnect(client)
            return False
        state["queue"].append((encoded, size))
        state["bytes"] += size
        if not state["active"]:
            self._drain_client(client, state)
        return True

    def _drain_client(self, client, state):
        while self._outgoing.get(client) is state and state["queue"]:
            if hasattr(client, "JWT") and client.JWT.get("exp", 0) <= time.time():
                self.detach(client)
                client.on_auth_failure()
                return
            encoded, size = state["queue"][0]
            try:
                pending = client.write_message(encoded)
            except tornado.websocket.WebSocketClosedError:
                self.detach(client)
                return
            except Exception:
                self.logger.exception("WebSocket write failed")
                self._disconnect(client, code=1011, reason="write failed")
                return
            if pending is None:
                state["queue"].popleft()
                state["bytes"] -= size
                continue
            state["active"] = True
            state["timer"] = self.ioloop.call_later(self.write_timeout, self._disconnect, client)
            def completed(future):
                try:
                    future.result()
                except (Exception, asyncio.CancelledError):
                    if self._outgoing.get(client) is state:
                        self._disconnect(client, code=1011, reason="write failed")
                    return
                if self._outgoing.get(client) is not state:
                    return
                state["timer"].cancel()
                state["timer"] = None
                _, written = state["queue"].popleft()
                state["bytes"] -= written
                state["active"] = False
                self._drain_client(client, state)
            pending.add_done_callback(completed)
            return
        if self._outgoing.get(client) is state:
            self._outgoing.pop(client, None)

    def _deliver(self, clients, message):
        encoded = None
        for client in clients:
            try:
                if self._cacheable(client):
                    if encoded is None:
                        encoded = self._serialize_for(client, message)
                    payload = encoded
                else:
                    payload = self._serialize_for(client, message)
                self._write(client, payload)
            except Exception:
                # A bad publication must not silently unsubscribe healthy clients.
                self.logger.exception("WebSocket publication failed")

    def _publish_now(self, channel: str, message: Any) -> None:
        self._deliver(tuple(self.channels.get(channel, ())), message)

    def _broadcast_now(self, message: Any) -> None:
        self._deliver(set().union(*self.channels.values()) if self.channels else (), message)


# ---------------------------------------------------------------------------
# Process-global hub helpers
# ---------------------------------------------------------------------------

_GLOBAL_WS_HUB: Optional[WebSocketHub] = None


def set_ws_hub(hub: Optional[WebSocketHub]) -> None:
    global _GLOBAL_WS_HUB
    _GLOBAL_WS_HUB = hub


def get_ws_hub() -> Optional[WebSocketHub]:
    return _GLOBAL_WS_HUB


def publish_ws(channel: str, message: Any) -> bool:
    hub = get_ws_hub()
    if hub is not None:
        return hub.publish(channel, message)
    return False


# ---------------------------------------------------------------------------
# Socket handlers
# ---------------------------------------------------------------------------


class BaseChannelSocketHandler(
    LoggerMixin, JSONSerializationMixin, tornado.websocket.WebSocketHandler
):
    """
    Base channel-based websocket handler with pub/sub support.

    Route examples:
      (r"/ws/([^/]+)", PublicChannelSocketHandler)   # channel from path
      (r"/ws", PublicChannelSocketHandler)           # channel via ?channel=foo

    Supports optional subscribe/unsubscribe messages from client:
      {"action":"subscribe","channel":"jobs"}
      {"action":"unsubscribe","channel":"jobs"}
      {"action":"ping"}
    """

    origin_whitelist: Optional[Sequence[str]] = None

    def get_origin_whitelist(self) -> Optional[Sequence[str]]:
        return getattr(self, "origin_whitelist", None)

    def _hub(self) -> WebSocketHub:
        hub = getattr(self.application, "ws_hub", None)
        if hub is None:
            raise RuntimeError("Application has no ws_hub (start(..., websocket=True))")
        return hub

    def write_json(self, data: Any) -> bool:
        return self._hub().send(self, data)

    def parse_json(self, raw: str) -> Optional[dict]:
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except (ValueError, UnicodeError):
            return None

    # ---- security ----
    # Note: WebSockets use origin checks, not CORS headers.

    def check_origin(self, origin: str) -> bool:
        wl = self.get_origin_whitelist()
        if wl is None:
            return super().check_origin(origin)

        req_origin = normalize_origin(origin)
        allowed = {normalize_origin(o) for o in wl}
        allowed.discard(None)
        return req_origin in allowed

    # ---- auth/subscription hooks (override in subclasses) ----

    def can_connect(self) -> bool:
        return True

    def can_subscribe(self, channel: str) -> bool:
        return True

    def on_auth_failure(self) -> None:
        self._hub().detach(self)
        self.close(code=4001, reason="forbidden")

    # ---- lifecycle ----

    def open(self, channel: Optional[str] = None):
        self._ws_channels = set()
        self._expiry_handle = None
        self._message_tokens = float(self.message_burst)
        self._message_clock = time.monotonic()

        if not self.can_connect():
            return self.on_auth_failure()

        if not self._hub().register(self):
            self.close(code=1013, reason="connection limit")
            return

        if hasattr(self, "JWT"):
            delay = max(0, self.JWT["exp"] - time.time())
            self._expiry_handle = tornado.ioloop.IOLoop.current().call_later(delay, self.on_auth_failure)
        ch = channel or self.get_argument("channel", "default")
        self._subscribe(ch, notify=False)
        self.write_json({"type": "ready", "channels": sorted(self._ws_channels)})

    message_rate = 100  # sustained messages/second; override for your workload
    message_burst = 200

    def on_message(self, message):
        now = time.monotonic()
        self._message_tokens = min(self.message_burst, self._message_tokens + (now - self._message_clock) * self.message_rate)
        self._message_clock = now
        if self._message_tokens < 1:
            self._hub()._disconnect(self, code=1008, reason="message rate limit")
            return
        self._message_tokens -= 1
        payload = self.parse_json(message)
        if payload is None:
            return

        action = payload.get("action")
        channel = payload.get("channel")

        if action == "ping":
            self.write_json({"type": "pong"})
            return

        if action == "subscribe" and isinstance(channel, str):
            self._subscribe(channel, notify=True)
            return

        if action == "unsubscribe" and isinstance(channel, str):
            self._unsubscribe(channel, notify=True)
            return

    def on_close(self):
        handle = getattr(self, "_expiry_handle", None)
        if handle is not None:
            handle.cancel()
        try:
            self._hub().detach(self)
        except Exception:
            self.logger.exception("WebSocket cleanup failed")

    # ---- subscription ops ----

    def _subscribe(self, channel: str, notify: bool = True) -> bool:
        error = None
        if not self._hub().validate_channel(channel):
            error = "invalid channel"
        elif not self.can_subscribe(channel):
            error = "forbidden"
        elif not self._hub().subscribe(channel, self):
            error = "subscription limit"
        if error:
            self.write_json({"type": "error", "message": error})
            return False
        self._ws_channels.add(channel)
        if notify:
            self.write_json({"type": "subscribed", "channel": channel})
        return True

    def _unsubscribe(self, channel: str, notify: bool = True) -> None:
        if not channel:
            return

        self._hub().unsubscribe(channel, self)
        self._ws_channels.discard(channel)

        if notify:
            self.write_json({"type": "unsubscribed", "channel": channel})


class PublicChannelSocketHandler(BaseChannelSocketHandler):
    """Public websocket handler (no auth required)."""

    pass


class ChannelSocketHandler(BaseChannelSocketHandler, UserMixin):
    """
    Authenticated websocket handler (WS equivalent of JSONHandler).

    Token sources (in order):
      1) Authorization: Bearer ...
      2) ?jwt=...                      (browser-friendly but can leak in logs)
      3) Cookie (if ws_jwt_cookie_name is set)
    """

    ws_jwt_query_param = "jwt"
    ws_jwt_cookie_name = None  # set in subclass if you want cookie-based auth

    def jwt(self):
        auth_header = self.request.headers.get("Authorization")
        if auth_header:
            return re.sub("^Bearer +", "", auth_header)

        qname = getattr(self, "ws_jwt_query_param", "jwt")
        if qname:
            qjwt = self.get_argument(qname, None)
            if qjwt:
                return qjwt

        cname = getattr(self, "ws_jwt_cookie_name", None)
        if cname:
            cval = self.get_cookie(cname)
            if cval:
                return cval

        return None

    def decoded_jwt(self):
        if hasattr(self, "JWT"):
            return self.JWT
        return token.decode(self.jwt())

    def can_connect(self) -> bool:
        raw = self.jwt()
        if raw is None:
            return False
        try:
            self.JWT = token.decode(raw)
            return True
        except Exception:
            return False
