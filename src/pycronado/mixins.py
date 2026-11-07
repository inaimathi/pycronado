# src/pycronado/mixins.py

import asyncio
import json
import logging
from datetime import date, datetime, time
from typing import Any, AsyncIterator, Callable, Iterator, Optional

import tornado.iostream
import tornado.web

from .util import getLogger


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


class _RequestLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[{self.extra['method']} {self.extra['path']}] {msg}", kwargs


class LoggerMixin:
    """Stable class logger with method/path context, excluding query strings.

    Both self.logger.info(...) and the legacy self.logger().info(...) work.
    Explicitly logged application data and custom access loggers remain the
    application's responsibility; never log credentials or entire requests.
    """

    def _reset_logger(self) -> None:
        self.__dict__.pop("_logger_proxy", None)

    @property
    def logger(self) -> _CallableLogger:
        proxy = self.__dict__.get("_logger_proxy")
        if proxy is None:
            log = getLogger(f"{type(self).__module__}.{type(self).__qualname__}")
            request = getattr(self, "request", None)
            if request is not None:
                log = _RequestLogger(log, {"method": request.method, "path": request.path})
            proxy = _CallableLogger(log)
            self.__dict__["_logger_proxy"] = proxy
        return proxy

    def _request_summary(self):
        # Tornado's default access logger calls this hook.
        return f"{self.request.method} {self.request.path} ({self.request.remote_ip})"

    def log_exception(self, typ, value, tb):
        # Tornado's default exception logger also prints repr(request), which
        # contains the URI and headers. Deliberately omit that representation.
        if isinstance(value, tornado.web.HTTPError):
            self.logger.warning("HTTP error status=%s", value.status_code)
        else:
            self.logger.error("Uncaught handler exception", exc_info=(typ, value, tb))


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
    """
    Helpers for streaming newline-delimited JSON responses.

    Existing public interfaces are preserved:

        expects_ndjson()
        ndjson_start(status=None)
        ndjson(data, status=None)
        ndjson_end()
        ndjson_pump(it, status=200, on_error=None)
        andjson_start(status=None)
        andjson(data, status=None)
        andjson_end()
        andjson_pump(ait, status=200, on_error=None)

    Unexpected iterator exceptions are logged with their full traceback before
    the existing generic error payload is sent to the client.
    """

    def _ndjson_logger(self):
        """
        Prefer LoggerMixin's per-request logger when available.

        NDJSONMixin can still be used independently; in that case, fall back
        to Pycronado's configured getLogger().
        """
        try:
            candidate = getattr(self, "logger", None)
        except Exception:
            candidate = None

        if candidate is not None:
            try:
                resolved = candidate() if callable(candidate) else candidate
                if resolved is not None:
                    return resolved
            except Exception:
                # Logging must never interfere with response handling.
                pass

        request = getattr(self, "request", None)
        path = getattr(request, "path", None)
        name = f"{type(self).__module__}.{type(self).__qualname__}"
        return getLogger(name)

    def _log_ndjson_exception(
        self,
        message: str,
        exc: BaseException,
    ) -> None:
        """
        Log an exception with request context and its original traceback.

        This method is deliberately best-effort: logging failure must not mask
        the original stream failure.
        """
        request = getattr(self, "request", None)

        try:
            self._ndjson_logger().error(
                "%s: method=%s uri=%s remote_ip=%s exception=%s",
                message,
                getattr(request, "method", None),
                getattr(request, "path", None),
                getattr(request, "remote_ip", None),
                type(exc).__name__,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
        except Exception:
            pass

    def _ndjson_error_payload(
        self,
        exc: Exception,
        on_error: Optional[Callable[[Exception], Any]],
    ) -> Any:
        """
        Apply the caller's existing on_error callback when supplied.

        If the callback itself fails, log that secondary failure and fall back
        to the same generic payload previously returned by the mixin.
        """
        if on_error is None:
            return {
                "type": "error",
                "message": "internal server error",
            }

        try:
            return on_error(exc)
        except Exception as callback_exc:
            self._log_ndjson_exception(
                "NDJSON on_error callback failed",
                callback_exc,
            )
            return {
                "type": "error",
                "message": "internal server error",
            }

    def expects_ndjson(self) -> bool:
        accept = (self.request.headers.get("Accept") or "").lower()
        return "application/x-ndjson" in accept

    def _ensure_ndjson_headers(self, status: int | None = None) -> None:
        if status is not None:
            self.set_status(status)

        self.set_header("Content-Type", "application/x-ndjson")
        self.set_header("Cache-Control", "no-cache, no-transform")
        self.set_header("Connection", "keep-alive")

        # Prevent proxy buffering, particularly under nginx.
        self.set_header("X-Accel-Buffering", "no")
        self.add_header("Vary", "Accept")

        try:
            self.clear_header("Content-Length")
        except Exception:
            pass

    def _encode_line(self, data: Any) -> str:
        return (data if isinstance(data, str) else self.dumps(data)) + "\n"

    # Synchronous writes are retained for small legacy streams. They cannot
    # yield to network backpressure, so bound their outstanding output.
    ndjson_max_pending_bytes = 1024 * 1024

    def _sync_flush(self, size=0):
        pending_bytes = getattr(self, "_ndjson_pending_bytes", 0) + size
        if pending_bytes > self.ndjson_max_pending_bytes:
            raise BufferError("NDJSON output limit exceeded; use andjson_pump")
        self._ndjson_pending_bytes = pending_bytes
        future = self.flush()
        if future is not None:
            def done(result):
                self._ndjson_pending_bytes -= size
                try:
                    result.result()
                except (Exception, asyncio.CancelledError):
                    self._ndjson_disconnected = True
            future.add_done_callback(done)
        else:
            self._ndjson_pending_bytes -= size

    def ndjson_start(self, status=None):
        if getattr(self, "_ndjson_started", False):
            return
        self._ensure_ndjson_headers(status)
        self._sync_flush()
        self._ndjson_started = True

    def ndjson(self, data, status=None):
        if getattr(self, "_ndjson_disconnected", False):
            raise tornado.iostream.StreamClosedError()
        if not getattr(self, "_ndjson_started", False):
            self.ndjson_start(status)
        line = self._encode_line(data)
        size = len(line.encode("utf-8"))
        if getattr(self, "_ndjson_pending_bytes", 0) + size > self.ndjson_max_pending_bytes:
            raise BufferError("NDJSON output limit exceeded; use andjson_pump")
        self.write(line)
        self._sync_flush(size)

    def ndjson_end(self):
        if not getattr(self, "_finished", False) and not getattr(self, "_ndjson_disconnected", False):
            self.finish()

    def ndjson_pump(self, it: Iterator[Any], status=200, on_error=None, *, close_iterator=True):
        """Consume a bounded legacy stream; close the owned iterator on exit."""
        iterator = iter(it)
        try:
            self.ndjson_start(status)
            for item in iterator:
                self.ndjson(item)
        except tornado.iostream.StreamClosedError:
            self._ndjson_disconnected = True
        except Exception as exc:
            self._log_ndjson_exception("Synchronous NDJSON stream failed", exc)
            try:
                self.ndjson(self._ndjson_error_payload(exc, on_error))
            except (tornado.iostream.StreamClosedError, BufferError):
                pass
        finally:
            try:
                if close_iterator and hasattr(iterator, "close"):
                    iterator.close()
            finally:
                self.ndjson_end()

    async def andjson_start(self, status=None):
        if getattr(self, "_ndjson_started", False):
            return
        self._ensure_ndjson_headers(status)
        await self.flush()
        self._ndjson_started = True

    async def andjson(self, data, status=None):
        if not getattr(self, "_ndjson_started", False):
            await self.andjson_start(status)
        self.write(self._encode_line(data))
        await self.flush()
        await asyncio.sleep(0)

    async def andjson_end(self):
        if not getattr(self, "_finished", False) and not getattr(self, "_ndjson_disconnected", False):
            result = self.finish()
            if result is not None:
                await result

    async def andjson_pump(self, ait: AsyncIterator[Any], status=200, on_error=None, *, close_iterator=True):
        """Consume a stream with backpressure and propagate task cancellation.

        By default the pump owns/closes its iterator. Set close_iterator=False
        only when the caller owns its lifecycle. Background jobs should publish
        through a separate subscription iterator, not run inside this pump.
        """
        iterator = aiter(ait)
        self._ndjson_task = asyncio.current_task()
        try:
            await self.andjson_start(status)
            async for item in iterator:
                await self.andjson(item)
        except tornado.iostream.StreamClosedError:
            self._ndjson_disconnected = True
        except asyncio.CancelledError:
            # No more writes on cancellation; finish cleanup, then propagate.
            self._ndjson_disconnected = True
            connection = getattr(getattr(self, "request", None), "connection", None)
            if connection is not None:
                connection.close()
                # Complete Tornado's response bookkeeping on the closed socket
                # so its task-cancellation handler does not attempt an HTTP 500.
                if not getattr(self, "_finished", False):
                    try:
                        result = self.finish()
                        if result is not None:
                            await result
                    except tornado.iostream.StreamClosedError:
                        pass
            raise
        except Exception as exc:
            self._log_ndjson_exception("Asynchronous NDJSON stream failed", exc)
            try:
                await self.andjson(self._ndjson_error_payload(exc, on_error))
            except tornado.iostream.StreamClosedError:
                self._ndjson_disconnected = True
        finally:
            try:
                if close_iterator and hasattr(iterator, "aclose"):
                    await iterator.aclose()
            finally:
                try:
                    await self.andjson_end()
                except tornado.iostream.StreamClosedError:
                    self._ndjson_disconnected = True
                finally:
                    self._ndjson_task = None
