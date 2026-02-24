# pycronado/core.py

import asyncio
import json
import mimetypes
import os
import re
from asyncio import run  # included for callers as part of the external API
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, AsyncIterator, Callable, Iterator, Optional, Sequence

import tornado
import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.websocket

from . import token
from .util import getLogger, normalize_origin


def date_serializer(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class JSONSerializationMixin:
    """
    Shared JSON serialization behavior for HTTP and WebSocket handlers.
    Mirrors PublicJSONHandler's serializer override pattern.
    """

    json_serializer = None

    def get_json_serializer(self):
        return getattr(self, "json_serializer", None) or date_serializer

    def dumps(self, data):
        return json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
            default=self.get_json_serializer(),
        )


def requires(*param_names, permissions=None):
    # normalize permissions into a list so we can iterate uniformly
    if permissions is None:
        required_perms = None
    elif isinstance(permissions, (list, tuple, set)):
        required_perms = list(permissions)
    else:
        required_perms = [permissions]

    def decorator(method):
        def wrapper(self, *args, **kwargs):
            # --- 1. Extract/validate required params ---
            for param_name in param_names:
                value = self.param(param_name)
                if value is None:
                    return self.jsonerr(f"`{param_name}` parameter is required", 400)
                kwargs[param_name] = value

            # --- 2. Enforce permission(s), if requested ---
            if required_perms is not None:
                has_any = any(
                    getattr(self, "has_permission", lambda *_: False)(perm)
                    for perm in required_perms
                )

                if not has_any:
                    return self.jsonerr("forbidden", 403)

            # --- 3. Call the actual handler ---
            return method(self, *args, **kwargs)

        return wrapper

    return decorator


class NDJSONMixin:
    def expects_ndjson(self) -> bool:
        accept = (self.request.headers.get("Accept") or "").lower()
        return "application/x-ndjson" in accept

    def _ensure_ndjson_headers(self, status: int | None = None) -> None:
        if status is not None:
            self.set_status(status)
        self.set_header("Content-Type", "application/x-ndjson")
        self.set_header("Cache-Control", "no-cache, no-transform")
        self.set_header("Connection", "keep-alive")
        # Prevent proxy buffering (e.g., nginx) of streaming responses
        self.set_header("X-Accel-Buffering", "no")
        self.set_header("Vary", "Accept")
        try:
            self.clear_header("Content-Length")
        except Exception:
            pass

    def _encode_line(self, data: Any) -> str:
        return (data if isinstance(data, str) else self.dumps(data)) + "\n"

    def ndjson_start(self, status: int | None = None) -> None:
        if getattr(self, "_ndjson_started", False):
            return
        self._ensure_ndjson_headers(status)
        try:
            self.flush()
        except Exception:
            # best-effort; continue either way
            pass
        self._ndjson_started = True

    def ndjson(self, data: Any, status: int | None = None) -> None:
        if not getattr(self, "_ndjson_started", False):
            self.ndjson_start(status=status)
        line = self._encode_line(data)
        self.write(line)
        try:
            self.flush()
        except Exception:
            # swallow transient client/transport errors during streaming
            pass

    def ndjson_end(self) -> None:
        try:
            if not getattr(self, "_finished", False):
                self.finish()
        except Exception:
            pass

    def ndjson_pump(
        self,
        it: Iterator[Any],
        status: int = 200,
        on_error: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        """Stream a synchronous iterator into the response."""
        self.ndjson_start(status)
        try:
            for item in it:
                self.ndjson(item)

        except tornado.iostream.StreamClosedError:
            return

        except GeneratorExit:
            try:
                self.ndjson({"type": "error", "message": "stream cancelled"})
            finally:
                self.ndjson_end()

        except Exception as e:
            try:
                payload = (
                    on_error(e)
                    if on_error
                    else {"type": "error", "message": "internal server error"}
                )
                self.ndjson(payload)
            finally:
                self.ndjson_end()

        finally:
            # Idempotent; safe even if already finished above.
            self.ndjson_end()

    # ------------------------------- Async API -------------------------------

    async def andjson_start(self, status: int | None = None) -> None:
        if getattr(self, "_ndjson_started", False):
            return
        self._ensure_ndjson_headers(status)
        try:
            await self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception:
            pass
        self._ndjson_started = True

    async def andjson(self, data: Any, status: int | None = None) -> None:
        if not getattr(self, "_ndjson_started", False):
            await self.andjson_start(status=status)
        line = self._encode_line(data)
        self.write(line)
        try:
            # backpressure-aware; yields IOLoop so other requests keep flowing
            await self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception:
            pass
        # brief cooperative yield for very chatty streams
        await asyncio.sleep(0)

    async def andjson_end(self) -> None:
        try:
            if not getattr(self, "_finished", False):
                self.finish()
        except Exception:
            pass

    async def andjson_pump(
        self,
        ait: AsyncIterator[Any],
        status: int = 200,
        on_error: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        """Stream an asynchronous iterator into the response."""
        await self.andjson_start(status)
        try:
            async for item in ait:
                await self.andjson(item)

        except asyncio.CancelledError:
            try:
                await self.andjson({"type": "error", "message": "stream cancelled"})
            finally:
                await self.andjson_end()

        except Exception as e:
            payload = (
                on_error(e)
                if on_error
                else {"type": "error", "message": "internal server error"}
            )
            try:
                await self.andjson(payload)
            finally:
                await self.andjson_end()

        finally:
            await self.andjson_end()


class WebSocketHub:
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
        """
        Thread-safe entrypoint. Can be called from handlers, workers, etc.
        """
        if not channel:
            return
        self.ioloop.add_callback(self._publish_now, channel, message)

    def broadcast(self, message: Any) -> None:
        """
        Thread-safe broadcast to every connected client in every channel.
        """
        self.ioloop.add_callback(self._broadcast_now, message)

    def _serialize_for(self, client, message: Any) -> str:
        # Keep a string passthrough for flexibility, though most usage is JSON objects.
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


_GLOBAL_WS_HUB = None


def set_ws_hub(hub: Optional[WebSocketHub]) -> None:
    global _GLOBAL_WS_HUB
    _GLOBAL_WS_HUB = hub


def get_ws_hub() -> Optional[WebSocketHub]:
    return _GLOBAL_WS_HUB


def publish_ws(channel: str, message: Any) -> None:
    hub = get_ws_hub()
    if hub is not None:
        hub.publish(channel, message)


class PublicJSONHandler(JSONSerializationMixin, tornado.web.RequestHandler):
    origin_whitelist: Optional[Sequence[str]] = None

    def prepare(self):
        self._logger = None
        self._data = None
        self._apply_cors()

    def get_origin_whitelist(self) -> Optional[Sequence[str]]:
        """
        Override-able hook (mirrors get_json_serializer style).

        - Default: returns self.origin_whitelist
        - Handlers can override to compute from DB, env, etc.
        """
        return getattr(self, "origin_whitelist", None)

    def _apply_cors(self) -> None:
        wl = self.get_origin_whitelist()

        # Default behavior: allow all (legacy / current behavior)
        if wl is None:
            self.set_header("Access-Control-Allow-Origin", "*")
            self.set_header("Access-Control-Allow-Headers", "*")
            self.set_header("Access-Control-Allow-Methods", "*")
            return

        # Whitelist behavior: reflect allowed Origin exactly.
        req_origin = normalize_origin(self.request.headers.get("Origin"))
        allowed = {normalize_origin(o) for o in wl}
        allowed.discard(None)

        if req_origin and req_origin in allowed:
            self.set_header("Access-Control-Allow-Origin", req_origin)
            # Credentialed CORS (cookies) requires:
            # - no '*'
            # - allow-credentials true
            self.set_header("Access-Control-Allow-Credentials", "true")

            # Better compatibility: reflect requested headers/method for preflight.
            req_hdrs = self.request.headers.get("Access-Control-Request-Headers")
            req_meth = self.request.headers.get("Access-Control-Request-Method")
            if req_hdrs:
                self.set_header("Access-Control-Allow-Headers", req_hdrs)
            else:
                self.set_header("Access-Control-Allow-Headers", "*")
            if req_meth:
                self.set_header("Access-Control-Allow-Methods", req_meth)
            else:
                self.set_header("Access-Control-Allow-Methods", "*")

            # Cache correctness when reflecting origins
            try:
                self.add_header("Vary", "Origin")
            except Exception:
                self.set_header("Vary", "Origin")
        else:
            # Disallowed origin: send no CORS allow headers at all.
            # Browser will block.
            try:
                self.clear_header("Access-Control-Allow-Origin")
                self.clear_header("Access-Control-Allow-Headers")
                self.clear_header("Access-Control-Allow-Methods")
                self.clear_header("Access-Control-Allow-Credentials")
            except Exception:
                pass

    def logger(self):
        if self._logger is None:
            self._logger = getLogger(f"handler:{self.request.uri}")
        return self._logger

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")

    def jwt(self):
        auth_header = self.request.headers.get("Authorization")
        if auth_header is not None:
            return re.sub("^Bearer +", "", auth_header)
        return None

    def param(self, param_name, default=None):
        if self._data is None:
            content_type = self.request.headers.get("Content-Type", "")
            if self.request.body and "application/json" in content_type:
                try:
                    parsed = json.loads(self.request.body)
                    self._data = parsed if isinstance(parsed, dict) else {}
                except json.JSONDecodeError:
                    self._data = {}
            else:
                self._data = {}
                json_data = self.get_body_argument("json", None)
                if json_data:
                    try:
                        self._data.update(json.loads(json_data))
                    except json.JSONDecodeError:
                        pass
        return self._data.get(param_name, self.get_argument(param_name, default))

    def file_param(self, param_name, default=None, max_filesize=20 * 1024 * 1024):
        files = self.request.files.get(param_name)
        if not files:
            return default

        file_info = files[0]

        if len(file_info["body"]) > max_filesize:
            return self.jsonerr(
                f"File too large. Maximum size is {max_filesize} bytes.", 413
            )

        return file_info["body"]

    def file(self, fpath, mimetype=None):
        assert os.path.exists(fpath), f"Path {fpath} does not exist"
        assert os.path.isfile(fpath), f"Path {fpath} is not a file"

        if mimetype is None:
            content_type, _encoding = mimetypes.guess_type(fpath)
            mimetype = content_type

        assert mimetype, f"Could not infer mimetype of {fpath} and no default provided"

        self.set_header("Content-Type", mimetype)
        self.set_header("Content-Length", os.path.getsize(fpath))

        with open(fpath, "rb") as f:
            while True:
                chunk = f.read(65 * 1024)
                if not chunk:
                    break
                self.write(chunk)
                self.flush()

        self.finish()

    def filebytes(self, data, mimetype=None, extension=None):
        assert isinstance(data, bytes), f"Data must be bytes, got {type(data)}"
        assert mimetype or extension, "Mimetype or extension is required for filebytes"

        if mimetype is None:
            mimetype = mimetypes.guess_type(f"foo.{extension}")

        self.set_header("Content-Type", mimetype)
        self.set_header("Content-Length", len(data))

        chunk_size = 65 * 1024
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            self.write(chunk)
            self.flush()

        self.finish()

    def jsonerr(self, message, status=500):
        self.json({"status": "error", "message": message}, status)
        self.finish()

    def json(self, data, status=None):
        if status is not None:
            self.set_status(status)
        return self.write(self.dumps(data))

    def options(self, *_args, **_kwargs):
        self.set_status(204)
        self.finish()

    def ok(self, **kwargs):
        return self.json({"status": "ok", **kwargs}, 200)

    def TODO(self, **kwargs):
        return self.json({"status": "TODO", **kwargs}, 501)


class BaseChannelSocketHandler(
    JSONSerializationMixin, tornado.websocket.WebSocketHandler
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

    # ---- helpers ----

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
    """
    Public websocket handler (no auth required).
    """

    pass


class UserMixin:
    def token(self):
        if not hasattr(self, "JWT"):
            self.JWT = self.decoded_jwt()
        return self.JWT

    def issuer(self):
        return self.decoded_jwt()["iss"]

    def user(self):
        jwt = self.decoded_jwt()
        return jwt["user"]

    def username(self):
        return self.user()["username"]

    def user_id(self):
        return f"{self.issuer()}::{self.username()}"

    def permissions(self):
        return self.user().get("permissions", [])

    def has_permission(self, ability, group=None):
        for perm in self.permissions():
            p_group = perm.get("user_group")
            p_ability = perm.get("group_ability")

            if not p_group or not p_ability:
                continue  # malformed row, ignore

            ability_ok = (p_ability == ability) or (p_ability == "*")

            group_ok = group is None or p_group == group or p_group == "*"

            if ability_ok and group_ok:
                return True

        return False


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


class JSONHandler(PublicJSONHandler, UserMixin):
    def prepare(self):
        super().prepare()

        if self.request.method == "OPTIONS":
            return

        if self.jwt() is None:
            self.json({"status": "error", "message": "forbidden"}, 403)
            self.finish()
            return
        try:
            self.JWT = token.decode(self.jwt())
        except Exception:
            self.json({"status": "error", "message": "forbidden"}, 403)
            self.finish()
            return

    def decoded_jwt(self):
        if not hasattr(self, "JWT"):
            self.JWT = token.decode(self.jwt())
        return self.JWT


class Default404Handler(PublicJSONHandler):
    def prepare(self):
        self.json({"status": "error", "message": "not found"}, status=404)
        self.finish()
        return self.request.connection.close()


class HealthHandler(PublicJSONHandler):
    def get(self):
        val = self.param("value", None)
        res = {"status": "ok"}
        if val is not None:
            res["value"] = str(val)[0:256]
        return self.json(res)


async def start(
    name,
    port,
    routes,
    static_path=None,
    static_url_prefix=None,
    default_handler_class=None,
    debug=False,
    websocket=False,
    max_body_size=None,
):
    if default_handler_class is None:
        default_handler_class = Default404Handler

    app = tornado.web.Application(
        routes,
        default_handler_class=default_handler_class,
        debug=debug,
        static_path=static_path,
        static_url_prefix=static_url_prefix,
    )
    app.logger = getLogger(name)

    # Optional websocket infrastructure
    app.ws_hub = None
    if websocket:
        app.ws_hub = WebSocketHub(ioloop=tornado.ioloop.IOLoop.current())
        set_ws_hub(app.ws_hub)
    else:
        set_ws_hub(None)

    app.logger.info(f"  listening on {port}...")
    listen_kwargs = {}
    if max_body_size is not None:
        listen_kwargs["max_body_size"] = int(max_body_size)
    app.listen(int(port), **listen_kwargs)
    await asyncio.Event().wait()
