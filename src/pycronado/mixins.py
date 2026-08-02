# src/pycronado/mixins.py

import asyncio
import json
from datetime import date, datetime, time
from typing import Any, AsyncIterator, Callable, Iterator, Optional

import tornado.iostream

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
        name = f"handler:{path}" if path else type(self).__name__
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
                getattr(request, "uri", None),
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
        self.set_header("Vary", "Accept")

        try:
            self.clear_header("Content-Length")
        except Exception:
            pass

    def _encode_line(self, data: Any) -> str:
        return (data if isinstance(data, str) else self.dumps(data)) + "\n"

    # ---------------------------- Synchronous API ----------------------------

    def ndjson_start(self, status: int | None = None) -> None:
        if getattr(self, "_ndjson_started", False):
            return

        self._ensure_ndjson_headers(status)

        try:
            self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception as exc:
            # Preserve the old behavior of not failing startup because flush
            # failed, but make the problem visible in server logs.
            self._log_ndjson_exception(
                "Failed flushing synchronous NDJSON headers",
                exc,
            )

        self._ndjson_started = True

    def ndjson(self, data: Any, status: int | None = None) -> None:
        if not getattr(self, "_ndjson_started", False):
            self.ndjson_start(status=status)

        line = self._encode_line(data)
        self.write(line)

        try:
            self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception as exc:
            # Preserve the old best-effort flush behavior.
            self._log_ndjson_exception(
                "Failed flushing synchronous NDJSON event",
                exc,
            )

    def ndjson_end(self) -> None:
        try:
            if not getattr(self, "_finished", False):
                self.finish()
        except Exception as exc:
            # Preserve the old best-effort finish behavior.
            self._log_ndjson_exception(
                "Failed finishing synchronous NDJSON response",
                exc,
            )

    def ndjson_pump(
        self,
        it: Iterator[Any],
        status: int = 200,
        on_error: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        """
        Stream a synchronous iterator into the response.

        The method signature and client-facing default error payload are
        unchanged. Unexpected iterator errors are now logged with a traceback.
        """
        self.ndjson_start(status)

        try:
            for item in it:
                self.ndjson(item)

        except tornado.iostream.StreamClosedError:
            # A client disconnect is not an application error.
            return

        except GeneratorExit:
            # Preserve the previous cancellation response behavior.
            try:
                self.ndjson(
                    {
                        "type": "error",
                        "message": "stream cancelled",
                    }
                )
            finally:
                self.ndjson_end()

        except Exception as exc:
            self._log_ndjson_exception(
                "Synchronous NDJSON stream failed",
                exc,
            )

            payload = self._ndjson_error_payload(exc, on_error)

            try:
                self.ndjson(payload)
            except tornado.iostream.StreamClosedError:
                pass
            except Exception as write_exc:
                self._log_ndjson_exception(
                    "Failed writing synchronous NDJSON error event",
                    write_exc,
                )
            finally:
                self.ndjson_end()

        finally:
            self.ndjson_end()

    # ------------------------------ Async API -------------------------------

    async def andjson_start(self, status: int | None = None) -> None:
        if getattr(self, "_ndjson_started", False):
            return

        self._ensure_ndjson_headers(status)

        try:
            await self.flush()
        except tornado.iostream.StreamClosedError:
            return
        except Exception as exc:
            # Preserve the old behavior of allowing streaming to continue.
            self._log_ndjson_exception(
                "Failed flushing asynchronous NDJSON headers",
                exc,
            )

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
        except Exception as exc:
            # Preserve the old best-effort flush behavior.
            self._log_ndjson_exception(
                "Failed flushing asynchronous NDJSON event",
                exc,
            )

        # Preserve the existing cooperative scheduling point.
        await asyncio.sleep(0)

    async def andjson_end(self) -> None:
        try:
            if not getattr(self, "_finished", False):
                self.finish()
        except Exception as exc:
            # Preserve the old best-effort finish behavior.
            self._log_ndjson_exception(
                "Failed finishing asynchronous NDJSON response",
                exc,
            )

    async def andjson_pump(
        self,
        ait: AsyncIterator[Any],
        status: int = 200,
        on_error: Optional[Callable[[Exception], Any]] = None,
    ) -> None:
        """
        Stream an asynchronous iterator into the response.

        The method signature and existing cancellation behavior are preserved.
        Unexpected iterator errors are now logged with their full traceback.
        """
        await self.andjson_start(status)

        try:
            async for item in ait:
                await self.andjson(item)

        except tornado.iostream.StreamClosedError:
            # A client disconnect is not an application error.
            return

        except asyncio.CancelledError:
            # Preserve the old behavior: emit a cancellation event and consume
            # the cancellation rather than re-raising it.
            try:
                await self.andjson(
                    {
                        "type": "error",
                        "message": "stream cancelled",
                    }
                )
            finally:
                await self.andjson_end()

        except Exception as exc:
            self._log_ndjson_exception(
                "Asynchronous NDJSON stream failed",
                exc,
            )

            payload = self._ndjson_error_payload(exc, on_error)

            try:
                await self.andjson(payload)
            except tornado.iostream.StreamClosedError:
                pass
            except Exception as write_exc:
                self._log_ndjson_exception(
                    "Failed writing asynchronous NDJSON error event",
                    write_exc,
                )
            finally:
                await self.andjson_end()

        finally:
            await self.andjson_end()
