# src/pycronado/mixins.py

import asyncio
import json
from datetime import date, datetime, time
from typing import Any, AsyncIterator, Callable, Iterator, Optional

import tornado.iostream


def date_serializer(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class _CallableLogger:
    """
    Thin proxy around a stdlib logger so that both call styles work:

        self.logger.info(...)   # property-style (preferred)
        self.logger().info(...) # legacy call-style
    """

    __slots__ = ("_log",)

    def __init__(self, log):
        self._log = log

    def __call__(self):
        return self._log

    def __getattr__(self, name):
        return getattr(self._log, name)

    def __repr__(self):
        return repr(self._log)


class LoggerMixin:
    """
    Provides ``self.logger`` (and the legacy ``self.logger()``) backed by a
    lazily-created, per-instance stdlib logger.

    The name is derived from ``self.request.uri`` when available (HTTP/WS
    handlers) and falls back to the class name otherwise (e.g. WebSocketHub).

    Call ``self._reset_logger()`` in ``prepare()`` so the name is refreshed
    for each new request.
    """

    def _reset_logger(self) -> None:
        self.__dict__.pop("_logger_proxy", None)

    @property
    def logger(self) -> _CallableLogger:
        proxy = self.__dict__.get("_logger_proxy")
        if proxy is None:
            path = getattr(getattr(self, "request", None), "path", None)
            name = f"handler:{path}" if path else type(self).__name__
            from .util import getLogger

            proxy = _CallableLogger(getLogger(name))
            self.__dict__["_logger_proxy"] = proxy
        return proxy


class JSONSerializationMixin:
    """
    Shared JSON serialization behavior for HTTP and WebSocket handlers.
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
            await self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception:
            pass
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
