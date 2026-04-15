# src/pycronado/websocket.py

import json
import re
from collections import defaultdict
from typing import Any, Optional, Sequence

import tornado.ioloop
import tornado.web
import tornado.websocket

from . import token
from .auth import UserMixin
from .mixins import JSONSerializationMixin, LoggerMixin, date_serializer
from .util import normalize_origin


class WebSocketHub(LoggerMixin):
    """
    Process-local websocket pub/sub hub.

    - Tracks subscribers by channel
    - Safe to publish from any thread
    - Actual socket writes happen on the Tornado IOLoop thread
    """

    def __init__(self, ioloop=None):
        self.ioloop = ioloop or tornado.ioloop.IOLoop.current()
        self.channels = defaultdict(set)  # channel -> set[BaseChannelSocketHandler]

    def subscribe(self, channel: str, client) -> None:
        if channel:
            self.channels[channel].add(client)

    def unsubscribe(self, channel: str, client) -> None:
        subs = self.channels.get(channel)
        if not subs:
            return
        subs.discard(client)
        if not subs:
            self.channels.pop(channel, None)

    def unsubscribe_all(self, client) -> None:
        for ch in list(getattr(client, "_ws_channels", set())):
            self.unsubscribe(ch, client)
        try:
            client._ws_channels.clear()
        except Exception:
            pass

    def publish(self, channel: str, message: Any) -> None:
        """Thread-safe entrypoint. Can be called from handlers, workers, etc."""
        if not channel:
            return
        self.ioloop.add_callback(self._publish_now, channel, message)

    def broadcast(self, message: Any) -> None:
        """Thread-safe broadcast to every connected client in every channel."""
        self.ioloop.add_callback(self._broadcast_now, message)

    def _serialize_for(self, client, message: Any) -> str:
        if isinstance(message, str):
            return message
        dumps = getattr(client, "dumps", None)
        if callable(dumps):
            return dumps(message)
        return json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":"),
            default=date_serializer,
        )

    def _publish_now(self, channel: str, message: Any) -> None:
        for client in list(self.channels.get(channel, ())):
            try:
                client.write_message(self._serialize_for(client, message))
            except tornado.websocket.WebSocketClosedError:
                self.unsubscribe(channel, client)
            except Exception:
                self.unsubscribe(channel, client)

    def _broadcast_now(self, message: Any) -> None:
        all_clients = set()
        for subs in self.channels.values():
            all_clients.update(subs)

        for client in list(all_clients):
            try:
                client.write_message(self._serialize_for(client, message))
            except tornado.websocket.WebSocketClosedError:
                self.unsubscribe_all(client)
            except Exception:
                self.unsubscribe_all(client)


# ---------------------------------------------------------------------------
# Process-global hub helpers
# ---------------------------------------------------------------------------

_GLOBAL_WS_HUB: Optional[WebSocketHub] = None


def set_ws_hub(hub: Optional[WebSocketHub]) -> None:
    global _GLOBAL_WS_HUB
    _GLOBAL_WS_HUB = hub


def get_ws_hub() -> Optional[WebSocketHub]:
    return _GLOBAL_WS_HUB


def publish_ws(channel: str, message: Any) -> None:
    hub = get_ws_hub()
    if hub is not None:
        hub.publish(channel, message)


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

    def write_json(self, data: Any) -> None:
        self.write_message(self.dumps(data))

    def parse_json(self, raw: str) -> Optional[dict]:
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    # ---- security ----
    # Note: WebSockets use origin checks, not CORS headers.

    def check_origin(self, origin: str) -> bool:
        wl = self.get_origin_whitelist()
        if wl is None:
            return True  # legacy allow-all behavior

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
        self.close(code=4001, reason="forbidden")

    # ---- lifecycle ----

    def open(self, channel: Optional[str] = None):
        self._ws_channels = set()

        if not self.can_connect():
            return self.on_auth_failure()

        ch = channel or self.get_argument("channel", "default")
        self._subscribe(ch, notify=False)
        self.write_json({"type": "ready", "channels": [ch]})

    def on_message(self, message):
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
        try:
            self._hub().unsubscribe_all(self)
        except Exception:
            pass

    # ---- subscription ops ----

    def _subscribe(self, channel: str, notify: bool = True) -> None:
        if not channel:
            return
        if not self.can_subscribe(channel):
            if notify:
                self.write_json(
                    {"type": "error", "message": "forbidden", "channel": channel}
                )
            return

        self._hub().subscribe(channel, self)
        self._ws_channels.add(channel)

        if notify:
            self.write_json({"type": "subscribed", "channel": channel})

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
