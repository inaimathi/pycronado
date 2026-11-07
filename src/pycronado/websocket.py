"""WebSocket adapter. Historical hub/helper/class imports remain supported."""
import re
from typing import Optional, Sequence

import tornado.websocket

from . import token
from .auth import UserMixin
from .channel import ChannelHub, ChannelSessionMixin, get_channel_hub, set_channel_hub, publish
from .mixins import JSONSerializationMixin, LoggerMixin
from .util import normalize_origin

WebSocketHub = ChannelHub
get_ws_hub = get_channel_hub
set_ws_hub = set_channel_hub
publish_ws = publish


class BaseChannelSocketHandler(ChannelSessionMixin, LoggerMixin, JSONSerializationMixin,
                               tornado.websocket.WebSocketHandler):
    origin_whitelist: Optional[Sequence[str]] = None

    def get_origin_whitelist(self):
        return self.origin_whitelist

    def _hub(self):
        hub = getattr(self.application, "channel_hub", None) or getattr(self.application, "ws_hub", None)
        if hub is None:
            raise RuntimeError("Application has no channel hub (start(..., websocket=True))")
        return hub

    def check_origin(self, origin):
        whitelist = self.get_origin_whitelist()
        if whitelist is None:
            return super().check_origin(origin)
        allowed = {normalize_origin(value) for value in whitelist} - {None}
        return normalize_origin(origin) in allowed

    def open(self, channel=None):
        self._init_session()
        self._start_session([channel or self.get_argument("channel", "default")])

    def on_message(self, message):
        self.receive_message(message)

    def write_channel_message(self, message):
        try:
            return self.write_message(message)
        except tornado.websocket.WebSocketClosedError as exc:
            raise ConnectionError("WebSocket closed") from exc

    def close(self, code=None, reason=None):
        if hasattr(self, "_session_closed") and not self._session_closed:
            self._cleanup_session()
        super().close(code=code, reason=reason)

    def on_close(self):
        self._cleanup_session()


class PublicChannelSocketHandler(BaseChannelSocketHandler):
    """Anonymous WebSocket session; can_subscribe remains overrideable."""


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
