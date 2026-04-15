# src/pycronado/core.py

import asyncio
import json
import mimetypes
import os
import re
from asyncio import run  # noqa: F401 – re-exported as part of the public API
from typing import Optional, Sequence

import tornado.ioloop
import tornado.web

from . import token
from .auth import UserMixin, requires  # noqa: F401
from .mixins import (JSONSerializationMixin, LoggerMixin,  # noqa: F401
                     NDJSONMixin, _CallableLogger, date_serializer)
from .util import getLogger, normalize_origin
from .websocket import (BaseChannelSocketHandler,  # noqa: F401
                        ChannelSocketHandler, PublicChannelSocketHandler,
                        WebSocketHub, get_ws_hub, publish_ws, set_ws_hub)


class PublicJSONHandler(
    LoggerMixin, JSONSerializationMixin, tornado.web.RequestHandler
):
    origin_whitelist: Optional[Sequence[str]] = None

    def prepare(self):
        self._reset_logger()
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

            try:
                self.add_header("Vary", "Origin")
            except Exception:
                self.set_header("Vary", "Origin")
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
