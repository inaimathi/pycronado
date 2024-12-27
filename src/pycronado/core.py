import asyncio
import json

import tornado

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

    def param(self, param_name, default=None):
        if self._data is None:
            try:
                self._data = json.loads(self.request.body)
            except json.JSONDecodeError:
                self._data = {}
        return self._data.get(param_name, self.get_argument(param_name, default))

    def json(self, data, status=None):
        if status is not None:
            self.set_status(status)
        return self.write(json.dumps(data))


class Default404Handler(PublicJSONHandler):
    def prepare(self):
        self.json({"status": "error", "message": "not found"}, status=404)
        self.finish()
        return self.request.connection.close()


def getApp(
    name, routes, static_path=None, static_url_prefix=None, default_handler_class=None
):
    if default_handler_class is None:
        default_handler_class = Default404Handler
    app = tornado.web.Application(
        routes,
        default_handler_class=default_handler_class,
        debug=True,
        static_path=static_path,
        static_url_prefix=static_url_prefix,
    )
    app.logger = getLogger(name)
    return app


async def start(app, port):
    app.logger.info(f"Listening on {port}")
    app.listen(int(port))
    await asyncio.Event().wait()


def run(app, port):
    asyncio.run(start(app, port))
