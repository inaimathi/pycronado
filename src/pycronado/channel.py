"""Transport-independent pub/sub, session policy and bounded message dispatch."""
import asyncio
import inspect
import json
import threading
import time
import weakref
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

import tornado.ioloop
import tornado.iostream

from .mixins import JSONSerializationMixin, LoggerMixin, date_serializer


@dataclass(frozen=True)
class ChannelEvent:
    """Optional SSE metadata; WebSocket/WebRTC recipients receive data unchanged.

    IDs are application supplied. The hub is live-only, with no replay storage.
    """
    data: Any
    event: str | None = None
    id: str | None = None
    retry: int | None = None

    def __post_init__(self):
        for value in (self.event, self.id):
            if value is not None and (not isinstance(value, str) or len(value) > 1024
                                      or any(c in value for c in "\r\n\0")):
                raise ValueError("Event names/IDs must be single-line strings up to 1024 characters")
        if self.retry is not None and (type(self.retry) is not int or not 0 <= self.retry <= 2147483647):
            raise ValueError("retry must be nonnegative integer milliseconds")


class ChannelHub(LoggerMixin):
    """Process-local pub/sub with bounded ingress and per-client output.

    publish()/broadcast() are thread-safe and return False on ingress overflow.
    Other operations must run on the owning IOLoop. Slow clients are closed;
    reliable messages are never silently dropped from their output queue.
    """

    def __init__(self, ioloop=None, *, max_subscriptions=128, max_channel_length=256,
                 max_pending_messages=256, max_pending_bytes=1024 * 1024,
                 max_publications=1024, max_clients=10000, write_timeout=30):
        limits = (max_subscriptions, max_channel_length, max_pending_messages,
                  max_pending_bytes, max_publications, max_clients)
        if any(type(value) is not int or value <= 0 for value in limits) or write_timeout <= 0:
            raise ValueError("Hub limits and write_timeout must be positive")
        self.ioloop = ioloop or tornado.ioloop.IOLoop.current()
        self.max_subscriptions = max_subscriptions
        self.max_channel_length = max_channel_length
        self.max_pending_messages = max_pending_messages
        self.max_pending_bytes = max_pending_bytes
        self.max_publications = max_publications
        self.max_clients = max_clients
        self.write_timeout = write_timeout
        self.channels = defaultdict(set)
        self._memberships = {}
        self._clients = set()
        self._detached = weakref.WeakSet()
        self._outgoing = {}
        self._ingress = deque()
        self._lock = threading.Lock()
        self._scheduled = False
        self._closed = False

    def register(self, client):
        if self._closed or client in self._detached or (client not in self._clients and len(self._clients) >= self.max_clients):
            return False
        self._clients.add(client)
        return True

    def validate_channel(self, channel):
        return isinstance(channel, str) and 0 < len(channel) <= self.max_channel_length

    def subscribe(self, channel: str, client) -> bool:
        if not self.validate_channel(channel):
            return False
        memberships = self._memberships.get(client, set())
        if channel not in memberships and len(memberships) >= self.max_subscriptions:
            return False
        if not self.register(client):
            return False
        self.channels[channel].add(client)
        self._memberships.setdefault(client, set()).add(channel)
        return True

    def unsubscribe(self, channel: str, client) -> None:
        subs = self.channels.get(channel)
        if subs is not None:
            subs.discard(client)
            if not subs:
                self.channels.pop(channel, None)
        memberships = self._memberships.get(client)
        if memberships is not None:
            memberships.discard(channel)
            if not memberships:
                self._memberships.pop(client, None)

    def unsubscribe_all(self, client) -> None:
        for channel in tuple(self._memberships.get(client, ())):
            self.unsubscribe(channel, client)
        # Kept for callers inspecting the legacy handler attribute.
        channels = getattr(client, "_ws_channels", None)
        if channels is not None:
            channels.clear()

    def detach(self, client):
        self._detached.add(client)
        self.unsubscribe_all(client)
        self._clients.discard(client)
        state = self._outgoing.pop(client, None)
        if state is not None:
            state["queue"].clear()
            state["bytes"] = 0
            if state["timer"] is not None:
                state["timer"].cancel()

    def _disconnect(self, client, code=1013, reason="slow consumer"):
        self.detach(client)
        close = getattr(client, "close", None)
        if close is not None:
            close(code=code, reason=reason)

    def close(self):
        with self._lock:
            self._closed = True
            self._ingress.clear()
        for client in tuple(self._clients):
            self._disconnect(client, code=1001, reason="server shutdown")

    def _submit(self, channel, message):
        with self._lock:
            if self._closed or len(self._ingress) >= self.max_publications:
                return False
            self._ingress.append((channel, message))
            if not self._scheduled:
                self._scheduled = True
                self.ioloop.add_callback(self._drain_ingress)
        return True

    def publish(self, channel: str, message: Any) -> bool:
        """Enqueue without blocking; False means closed, invalid, or overloaded.

        Do not mutate a message after publishing it. The ingress count bounds
        retained objects; applications must also bound their payload sizes.
        """
        return self.validate_channel(channel) and self._submit(channel, message)

    def broadcast(self, message: Any) -> bool:
        return self._submit(None, message)

    def _drain_ingress(self):
        # Yield between batches so producers cannot monopolize the IOLoop.
        for _ in range(64):
            with self._lock:
                if not self._ingress:
                    self._scheduled = False
                    return
                channel, message = self._ingress.popleft()
            if channel is None:
                self._broadcast_now(message)
            else:
                self._publish_now(channel, message)
        self.ioloop.add_callback(self._drain_ingress)

    def _serialize_for(self, client, message: Any) -> str:
        if isinstance(message, ChannelEvent):
            message = message.data
        if isinstance(message, str):
            return message
        dumps = getattr(client, "dumps", None)
        if callable(dumps):
            return dumps(message)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"), default=date_serializer)

    def _cacheable(self, client):
        # Share only the known stateless default codec. Custom serializers may
        # depend on identity or mutable per-client state and are never reused.
        dumps = getattr(client, "dumps", None)
        return dumps is None or (
            getattr(dumps, "__func__", None) is JSONSerializationMixin.dumps
            and client.get_json_serializer() is date_serializer)

    def send(self, client, message):
        """Send control messages through the same bounded queue as publications."""
        try:
            encoded = self._serialize_for(client, message)
        except Exception:
            self.logger.exception("Channel serialization failed")
            return False
        return self._write(client, encoded, message if isinstance(message, ChannelEvent) else None)

    def _write(self, client, encoded, event=None):
        if self._closed or client in self._detached:
            return False
        if hasattr(client, "JWT") and float(client.JWT.get("exp", 0)) <= time.time():
            self.detach(client)
            client.on_auth_failure()
            return False
        if not self.register(client):
            self._disconnect(client, reason="connection limit")
            return False
        formatter = getattr(client, "format_channel_message", None)
        if formatter is not None:
            encoded = formatter(encoded, event)
        size = len(encoded.encode("utf-8"))
        state = self._outgoing.setdefault(client, {"queue": deque(), "bytes": 0, "active": False, "timer": None})
        if len(state["queue"]) >= self.max_pending_messages or state["bytes"] + size > self.max_pending_bytes:
            self._disconnect(client)
            return False
        state["queue"].append((encoded, size))
        state["bytes"] += size
        if not state["active"]:
            self._drain_client(client, state)
        return True

    def _drain_client(self, client, state):
        while self._outgoing.get(client) is state and state["queue"]:
            if hasattr(client, "JWT") and float(client.JWT.get("exp", 0)) <= time.time():
                self.detach(client)
                client.on_auth_failure()
                return
            encoded, size = state["queue"][0]
            try:
                writer = getattr(client, "write_channel_message", client.write_message)
                pending = writer(encoded)
            except (ConnectionError, tornado.iostream.StreamClosedError):
                self._disconnect(client, code=1011, reason="transport closed")
                return
            except Exception:
                self.logger.exception("Channel write failed")
                self._disconnect(client, code=1011, reason="write failed")
                return
            if pending is None:
                state["queue"].popleft()
                state["bytes"] -= size
                continue
            state["active"] = True
            state["timer"] = self.ioloop.call_later(self.write_timeout, self._disconnect, client)
            def completed(future):
                try:
                    future.result()
                except (Exception, asyncio.CancelledError):
                    if self._outgoing.get(client) is state:
                        self._disconnect(client, code=1011, reason="write failed")
                    return
                if self._outgoing.get(client) is not state:
                    return
                state["timer"].cancel()
                state["timer"] = None
                _, written = state["queue"].popleft()
                state["bytes"] -= written
                state["active"] = False
                self._drain_client(client, state)
            pending.add_done_callback(completed)
            return
        if self._outgoing.get(client) is state:
            self._outgoing.pop(client, None)

    def _deliver(self, clients, message):
        encoded = None
        for client in clients:
            try:
                if self._cacheable(client):
                    if encoded is None:
                        encoded = self._serialize_for(client, message)
                    payload = encoded
                else:
                    payload = self._serialize_for(client, message)
                self._write(client, payload, message if isinstance(message, ChannelEvent) else None)
            except Exception:
                # A bad publication must not silently unsubscribe healthy clients.
                self.logger.exception("Channel publication failed")

    def _publish_now(self, channel: str, message: Any) -> None:
        self._deliver(tuple(self.channels.get(channel, ())), message)

    def _broadcast_now(self, message: Any) -> None:
        self._deliver(set().union(*self.channels.values()) if self.channels else (), message)


_GLOBAL_CHANNEL_HUB = None


def set_channel_hub(hub):
    global _GLOBAL_CHANNEL_HUB
    _GLOBAL_CHANNEL_HUB = hub


def get_channel_hub():
    return _GLOBAL_CHANNEL_HUB


def publish(channel, message):
    hub = get_channel_hub()
    return hub.publish(channel, message) if hub is not None else False


class ChannelSessionMixin:
    """Shared subscription policy, expiry and bounded application commands.

    Implement _hub(), write_message() and close(). can_publish defaults to False:
    application messages require explicit opt-in and are never auto-republished.
    """
    message_rate = 100
    message_burst = 200
    max_message_size = 1024 * 1024
    max_incoming_messages = 128
    max_incoming_bytes = 1024 * 1024

    def _init_session(self):
        self._channel_names = set()
        self._ws_channels = self._channel_names  # legacy compatibility
        self._expiry_handle = None
        self._message_tokens = float(self.message_burst)
        self._message_clock = time.monotonic()
        self._incoming = deque()
        self._incoming_bytes = 0
        self._incoming_task = None
        self._session_closed = False
        self._session_started = False

    def can_connect(self):
        return True

    def can_subscribe(self, channel):
        return True

    def can_publish(self, channel):
        return False

    def on_channel_message(self, channel, data):
        """Override (sync or async) to handle authorized application commands."""

    def on_state_message(self, channel, data):
        """WebRTC unordered state input; sequence/reconcile in application code."""

    def on_auth_failure(self):
        self._cleanup_session()
        self.close(code=4001, reason="forbidden")

    def _arm_expiry(self):
        if hasattr(self, "JWT") and self._expiry_handle is None:
            delay = max(0, float(self.JWT["exp"]) - time.time())
            self._expiry_handle = self._hub().ioloop.call_later(delay, self.on_auth_failure)

    def _start_session(self, channels):
        if self._session_closed or self._session_started:
            return False
        if not self.can_connect():
            self.on_auth_failure()
            return False
        if not self._hub().register(self):
            self.close(code=1013, reason="connection limit")
            return False
        self._session_started = True
        self._arm_expiry()
        for channel in channels:
            self._subscribe(channel, notify=False)
        self.write_json({"type": "ready", "channels": sorted(self._channel_names)})
        return True

    def _cleanup_session(self):
        self._session_closed = True
        handle = getattr(self, "_expiry_handle", None)
        if handle is not None:
            handle.cancel()
        self._hub().detach(self)
        task = getattr(self, "_incoming_task", None)
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
        if hasattr(self, "_incoming"):
            self._incoming.clear()
            self._incoming_bytes = 0

    def write_json(self, data):
        if getattr(self, "_session_closed", False):
            return False
        return self._hub().send(self, data)

    def parse_json(self, raw):
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except (ValueError, UnicodeError, RecursionError):
            return None

    def _subscribe(self, channel, notify=True):
        error = None
        if not self._hub().validate_channel(channel):
            error = "invalid channel"
        elif not self.can_subscribe(channel):
            error = "forbidden"
        elif not self._hub().subscribe(channel, self):
            error = "subscription limit"
        if error:
            self.write_json({"type": "error", "message": error})
            return False
        self._channel_names.add(channel)
        if notify:
            self.write_json({"type": "subscribed", "channel": channel})
        return True

    def _unsubscribe(self, channel, notify=True):
        if not channel:
            return
        self._hub().unsubscribe(channel, self)
        self._channel_names.discard(channel)
        if notify:
            self.write_json({"type": "unsubscribed", "channel": channel})

    def receive_message(self, message, *, state=False):
        """Admit on the owning event loop; one worker per connection, no task flood."""
        if self._session_closed or not self._session_started:
            return
        if hasattr(self, "JWT") and float(self.JWT["exp"]) <= time.time():
            return self.on_auth_failure()
        now = time.monotonic()
        self._message_tokens = min(self.message_burst, self._message_tokens +
                                   (now - self._message_clock) * self.message_rate)
        self._message_clock = now
        if self._message_tokens < 1:
            self._hub()._disconnect(self, code=1008, reason="message rate limit")
            return
        self._message_tokens -= 1
        if not isinstance(message, (str, bytes)):
            return
        size = len(message.encode("utf-8") if isinstance(message, str) else message)
        if size > self.max_message_size:
            self._hub()._disconnect(self, code=1009, reason="message too large")
            return
        if len(self._incoming) >= self.max_incoming_messages or self._incoming_bytes + size > self.max_incoming_bytes:
            self._hub()._disconnect(self, code=1013, reason="incoming queue full")
            return
        self._incoming.append((message, size, state))
        self._incoming_bytes += size
        if self._incoming_task is None or self._incoming_task.done():
            self._incoming_task = asyncio.create_task(self._drain_incoming())

    async def _drain_incoming(self):
        try:
            while self._incoming and not self._session_closed:
                message, size, state = self._incoming[0]
                payload = self.parse_json(message)
                if payload is not None:
                    await self._dispatch_message(payload, state=state)
                if self._session_closed:
                    break
                self._incoming.popleft()
                self._incoming_bytes -= size
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Channel command failed")
            self._hub()._disconnect(self, code=1011, reason="command failed")

    async def _dispatch_message(self, payload, *, state=False):
        # Recheck expiry and permissions after any earlier queued command awaits.
        if hasattr(self, "JWT") and float(self.JWT["exp"]) <= time.time():
            return self.on_auth_failure()
        action, channel = payload.get("action"), payload.get("channel")
        if not state:
            if action == "ping":
                self.write_json({"type": "pong"})
                return
            if action == "subscribe" and isinstance(channel, str):
                self._subscribe(channel)
                return
            if action == "unsubscribe" and isinstance(channel, str):
                self._unsubscribe(channel)
                return
        if action == "message":
            if not isinstance(channel, str) or channel not in self._channel_names or not self.can_publish(channel):
                self.write_json({"type": "error", "message": "forbidden"})
                return
            callback = self.on_state_message if state else self.on_channel_message
            result = callback(channel, payload.get("data"))
            if inspect.isawaitable(result):
                await result
