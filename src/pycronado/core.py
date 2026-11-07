# src/pycronado/core.py

import asyncio
import json
import mimetypes
import os
import re
from asyncio import run  # noqa: F401 – re-exported as part of the public API
from typing import Optional, Sequence
from urllib.parse import quote

import tornado.ioloop
import tornado.iostream
import tornado.web

from . import token
from .auth import UserMixin, requires  # noqa: F401
from .mixins import LoggerMixin  # noqa: F401
from .mixins import (JSONSerializationMixin, NDJSONMixin, _CallableLogger,
                     date_serializer)
from .util import getLogger, normalize_origin
from .websocket import BaseChannelSocketHandler  # noqa: F401
from .websocket import (ChannelSocketHandler, PublicChannelSocketHandler,
                        WebSocketHub, get_ws_hub, publish_ws, set_ws_hub)


def _content_disposition(filename, disposition="attachment"):
    ascii_name = re.sub(r'[^\x20-\x7e]|["\\]', "_", filename)
    return (
        f"{disposition}; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename)}"
    )


class PublicJSONHandler(
    LoggerMixin, JSONSerializationMixin, tornado.web.RequestHandler
):
    origin_whitelist: Optional[Sequence[str]] = None

    def clear(self):
        super().clear()
        # Tornado clears response headers before write_error(), including in
        # applications with their own error formatter. Initialization calls
        # clear() before prepare(); only reapply after request state exists.
        if hasattr(self, "_data"):
            self._apply_cors()

    def prepare(self):
        self._reset_logger()
        self._data = None
        self._apply_cors()

    def on_connection_close(self):
        super().on_connection_close()
        for name in ("_ndjson_task", "_file_task"):
            task = getattr(self, name, None)
            if task is not None and not task.done() and not task.cancelling():
                task.cancel()

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
            # Authorization is a CORS non-wildcard request header. Reflect the
            # preflight's requested header names instead of relying on '*'.
            headers = self.request.headers.get("Access-Control-Request-Headers")
            method = self.request.headers.get("Access-Control-Request-Method")
            self.set_header("Access-Control-Allow-Headers", headers or "*, Authorization")
            self.set_header("Access-Control-Allow-Methods", method or "*")
            self.add_header("Vary", "Access-Control-Request-Headers, Access-Control-Request-Method")
            return

        # Include Vary even for denied/missing origins so caches cannot reuse
        # that response for an allowed origin.
        self.add_header("Vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
        # Whitelist behavior: reflect allowed Origin exactly.
        req_origin = normalize_origin(self.request.headers.get("Origin"))
        allowed = {normalize_origin(o) for o in wl}
        allowed.discard(None)

        if req_origin and req_origin in allowed:
            self.set_header("Access-Control-Allow-Origin", req_origin)
            # Credentialed CORS (cookies) requires no '*' and allow-credentials.
            self.set_header("Access-Control-Allow-Credentials", "true")

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

        else:
            # Disallowed origin: send no CORS allow headers at all.
            try:
                self.clear_header("Access-Control-Allow-Origin")
                self.clear_header("Access-Control-Allow-Headers")
                self.clear_header("Access-Control-Allow-Methods")
                self.clear_header("Access-Control-Allow-Credentials")
            except Exception:
                pass

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")

    def jwt(self):
        auth_header = self.request.headers.get("Authorization")
        if auth_header is not None:
            return re.sub("^Bearer +", "", auth_header)
        return None

    def param(self, param_name, default=None):
        if self._data is None:
            content_type = self.request.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            raw = (self.request.body if content_type == "application/json"
                   else self.get_body_argument("json", None))
            self._data = {}
            if raw:
                try:
                    parsed = json.loads(raw)
                except (ValueError, UnicodeError):
                    raise tornado.web.HTTPError(400, reason="Invalid JSON") from None
                if not isinstance(parsed, dict):
                    raise tornado.web.HTTPError(400, reason="JSON must be an object")
                self._data = parsed
        if param_name in self._data:
            return self._data[param_name]
        return self.get_argument(param_name, default)

    def file_param(self, param_name, default=None, max_filesize=20 * 1024 * 1024):
        files = self.request.files.get(param_name)
        if not files:
            return default

        file_info = files[0]

        if len(file_info["body"]) > max_filesize:
            raise tornado.web.HTTPError(413, reason="File too large")

        return file_info["body"]

    def _schedule_file(self, coroutine):
        # Legacy synchronous handlers may call file()/filebytes() without await.
        # Keep their response open while the asynchronous writer drains it.
        self._auto_finish = False
        async def send():
            try:
                await coroutine
            except asyncio.CancelledError:
                self.request.connection.close()
                raise
            except Exception:
                self.logger.exception("File response failed")
                if self._headers_written:
                    self.request.connection.close()
                else:
                    self.send_error(500)
        self._file_task = asyncio.create_task(send())
        return self._file_task

    def file(self, fpath, mimetype=None):
        """Schedule a streamed response; the returned task can also be awaited."""
        return self._schedule_file(self.afile(fpath, mimetype=mimetype))

    async def afile(self, fpath, mimetype=None, filename=None, chunk_size=256 * 1024):
        """
        Streaming counterpart to file(). Awaits each flush, so memory stays at
        ~one chunk and other requests keep being served during a large send.
        Stops quietly if the client disconnects.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if not await asyncio.to_thread(os.path.isfile, fpath):
            raise FileNotFoundError(fpath)
        if mimetype is None:
            mimetype = mimetypes.guess_type(fpath)[0]
        mimetype = mimetype or "application/octet-stream"

        self.set_header("Content-Type", mimetype)
        self.set_header("Content-Length", await asyncio.to_thread(os.path.getsize, fpath))
        if filename:
            self.set_header("Content-Disposition", _content_disposition(filename))
        try:
            opening = asyncio.create_task(asyncio.to_thread(open, fpath, "rb"))
            try:
                f = await asyncio.shield(opening)
            except asyncio.CancelledError:
                f = await opening
                f.close()
                raise
            with f:
                while True:
                    # Wait for an in-flight read before closing its file on cancellation.
                    read = asyncio.create_task(asyncio.to_thread(f.read, chunk_size))
                    try:
                        chunk = await asyncio.shield(read)
                    except asyncio.CancelledError:
                        await read
                        raise
                    if not chunk:
                        break
                    self.write(chunk)
                    await self.flush()
            await self.finish()
        except tornado.iostream.StreamClosedError:
            return

    def filebytes(self, data, mimetype=None, extension=None):
        """Schedule a bounded streaming write of bytes already in memory."""
        return self._schedule_file(self.afilebytes(data, mimetype, extension))

    async def afilebytes(self, data, mimetype=None, extension=None, chunk_size=256 * 1024):
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if mimetype is None and extension:
            mimetype = mimetypes.guess_type(f"file.{extension}")[0]
        self.set_header("Content-Type", mimetype or "application/octet-stream")
        self.set_header("Content-Length", len(data))
        try:
            for offset in range(0, len(data), chunk_size):
                self.write(data[offset:offset + chunk_size])
                await self.flush()
            await self.finish()
        except tornado.iostream.StreamClosedError:
            return

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


class JSONHandler(PublicJSONHandler, UserMixin):
    jwt_purposes = (None,)

    def prepare(self):
        super().prepare()

        if self.request.method == "OPTIONS":
            return

        raw_jwt = self.jwt()

        if raw_jwt is None:
            # Usually not worth a warning: unauthenticated requests are common.
            self.logger.debug(
                "Authentication rejected: missing JWT method=%s uri=%s",
                self.request.method,
                self.request.path,
            )
            self.json(
                {
                    "status": "error",
                    "message": "forbidden",
                    "code": "invalid_token",
                },
                403,
            )
            self.finish()
            return

        try:
            self.JWT = token.decode(raw_jwt, allowed_purposes=self.jwt_purposes)
        except Exception as exc:
            if isinstance(exc, token.InvalidTokenError):
                self.logger.debug("Authentication rejected: %s", type(exc).__name__)
            else:
                self.logger.exception("JWT verification failed")
            self.json(
                {
                    "status": "error",
                    "message": "forbidden",
                    "code": "invalid_token",
                },
                403,
            )
            self.finish()
            return

    def decoded_jwt(self):
        if not hasattr(self, "JWT"):
            self.JWT = token.decode(self.jwt(), allowed_purposes=self.jwt_purposes)
        return self.JWT


class Default404Handler(PublicJSONHandler):
    def prepare(self):
        super().prepare()
        self.json({"status": "error", "message": "not found"}, status=404)
        self.finish()


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
    address=None,
    log_function=None,
    websocket_hub_options=None,
    websocket_max_message_size=1024 * 1024,
    websocket_ping_interval=30,
    websocket_ping_timeout=30,
):
    if default_handler_class is None:
        default_handler_class = Default404Handler

    app = tornado.web.Application(
        routes,
        default_handler_class=default_handler_class,
        debug=debug,
        static_path=static_path,
        static_url_prefix=static_url_prefix,
        websocket_max_message_size=websocket_max_message_size,
        websocket_ping_interval=websocket_ping_interval,
        websocket_ping_timeout=websocket_ping_timeout,
        **({"log_function": log_function} if log_function is not None else {}),
    )
    app.logger = getLogger(name)

    app.ws_hub = None
    if websocket:
        app.ws_hub = WebSocketHub(ioloop=tornado.ioloop.IOLoop.current(), **(websocket_hub_options or {}))
        set_ws_hub(app.ws_hub)

    app.logger.info(f"  listening on {port}...")
    listen_kwargs = {}
    if max_body_size is not None:
        listen_kwargs["max_body_size"] = int(max_body_size)
    if address is not None:
        listen_kwargs["address"] = address
    server = None
    try:
        server = app.listen(int(port), **listen_kwargs)
        await asyncio.Event().wait()
    finally:
        if app.ws_hub is not None:
            app.ws_hub.close()
            if get_ws_hub() is app.ws_hub:
                set_ws_hub(None)
        if server is not None:
            server.stop()
            await server.close_all_connections()
