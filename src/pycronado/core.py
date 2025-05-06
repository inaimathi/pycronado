import asyncio
import json
import mimetypes
import os
import re
from asyncio import run

import tornado

from . import token
from .util import getLogger


class PublicJSONHandler(tornado.web.RequestHandler):
    def prepare(self):
        self._logger = None
        self._data = None
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "*")
        self.set_header("Access-Control-Allow-Methods", "*")

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
                    self._data = json.loads(self.request.body)
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
            content_type, encoding = mimetypes.guess_type(fpath)
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
        return self.write(json.dumps(data))

    def options(self):
        self.set_status(204)
        self.finish()

    def ok(self, **kwargs):
        return self.json({"status": "ok", **kwargs}, 200)

    def TODO(self, **kwargs):
        return self.json({"status": "TODO", **kwargs}, 501)


class JSONHandler(PublicJSONHandler):
    def prepare(self):
        super().prepare()

        if self.request.method == "OPTIONS":
            return

        if self.jwt() is None:
            self.json({"status": "error", "message": "forbidden"}, 403)
            self.finish()
            return
        try:
            token.decode(self.jwt())
        except Exception:
            self.json({"status": "error", "message": "forbidden"}, 403)
            self.finish()
            return

    def decodedJwt(self):
        return token.decode(self.jwt())


class Default404Handler(PublicJSONHandler):
    def prepare(self):
        self.json({"status": "error", "message": "not found"}, status=404)
        self.finish()
        return self.request.connection.close()


async def start(
    name,
    port,
    routes,
    static_path=None,
    static_url_prefix=None,
    default_handler_class=None,
    debug=False,
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
    app.logger.info(f"  listening on {port}...")
    app.listen(int(port))
    await asyncio.Event().wait()
