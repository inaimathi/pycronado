"""Live Server-Sent Events subscriptions using the shared channel hub."""
import asyncio
import re

from . import token
from .auth import UserMixin
from .channel import ChannelEvent, ChannelSessionMixin
from .core import PublicJSONHandler


def encode_event(data, event=None):
    """Frame UTF-8 text without allowing payload lines to inject SSE fields."""
    fields = []
    if event is not None:
        if event.event is not None:
            fields.append(f"event: {event.event}")
        if event.id is not None:
            fields.append(f"id: {event.id}")
        if event.retry is not None:
            fields.append(f"retry: {event.retry}")
    fields.extend(f"data: {line}" for line in data.replace('\r\n', '\n').replace('\r', '\n').split('\n'))
    return '\n'.join(fields) + '\n\n'


class BaseChannelSSEHandler(ChannelSessionMixin, PublicJSONHandler):
    """One-way live feed. Reconnect with channel parameters to change subscriptions.

    Event IDs and Last-Event-ID are exposed, but no replay history is retained.
    Clients needing durable delivery must reconcile against application storage.
    """
    heartbeat_interval = 15

    def _hub(self):
        hub = getattr(self.application, 'channel_hub', None) or getattr(self.application, 'ws_hub', None)
        if hub is None:
            raise RuntimeError('Application has no channel hub (start(..., sse=True))')
        return hub

    async def get(self, channel=None):
        self._init_session()
        self._done = asyncio.get_running_loop().create_future()
        self._heartbeat = None
        if not self.can_connect():
            return self.jsonerr('forbidden', 403)
        channels = [channel] if channel is not None else self.get_arguments('channel') or ['default']
        channels = list(dict.fromkeys(channels))
        if len(channels) > self._hub().max_subscriptions or any(not self._hub().validate_channel(c) for c in channels):
            return self.jsonerr('invalid subscriptions', 400)
        if any(not self.can_subscribe(c) for c in channels):
            return self.jsonerr('forbidden', 403)
        self.last_event_id = self.request.headers.get('Last-Event-ID')
        if self.last_event_id is not None and len(self.last_event_id) > 1024:
            return self.jsonerr('invalid event ID', 400)
        if not self._hub().register(self):
            return self.jsonerr('connection limit', 503)
        self.set_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.set_header('Cache-Control', 'no-cache, no-transform')
        self.set_header('X-Accel-Buffering', 'no')
        self.add_header('Vary', 'Accept, Last-Event-ID')
        self._start_session(channels)
        self._heartbeat = asyncio.create_task(self._send_heartbeats())
        try:
            await self._done
        finally:
            self._cleanup_session()
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)

    async def _send_heartbeats(self):
        while not self._session_closed:
            await asyncio.sleep(self.heartbeat_interval)
            self.write_json(ChannelEvent({'type': 'heartbeat'}, event='heartbeat'))

    def format_channel_message(self, encoded, event):
        return encode_event(encoded, event)

    def write_message(self, message):
        if self._session_closed or self._finished:
            raise ConnectionError('SSE connection closed')
        self.write(message)
        return self.flush()

    def close(self, code=None, reason=None):
        if not getattr(self, '_session_closed', True):
            self._cleanup_session()
        done = getattr(self, '_done', None)
        if done is not None and not done.done():
            done.set_result(None)
        # Ending the HTTP connection releases a blocked flush immediately.
        if self.request.connection is not None:
            self.request.connection.close()

    def on_connection_close(self):
        super().on_connection_close()
        if hasattr(self, '_done'):
            self._cleanup_session()
            if not self._done.done():
                self._done.set_result(None)


class PublicChannelSSEHandler(BaseChannelSSEHandler):
    """Anonymous SSE subscriptions."""


class ChannelSSEHandler(BaseChannelSSEHandler, UserMixin):
    """JWT SSE subscriptions. Query and cookie auth are explicitly opt-in.

    Native EventSource cannot set Authorization; use fetch streaming, or an
    explicitly configured cookie/short-lived query token and origin policy.
    """
    sse_jwt_query_param = None
    sse_jwt_cookie_name = None
    jwt_purposes = (None,)

    def jwt(self):
        header = self.request.headers.get('Authorization')
        if header:
            return re.sub('^Bearer +', '', header)
        if self.sse_jwt_query_param:
            value = self.get_argument(self.sse_jwt_query_param, None)
            if value:
                return value
        if self.sse_jwt_cookie_name:
            return self.get_cookie(self.sse_jwt_cookie_name)
        return None

    def decoded_jwt(self):
        return self.JWT

    def can_connect(self):
        try:
            self.JWT = token.decode(self.jwt(), allowed_purposes=self.jwt_purposes)
            return True
        except token.InvalidTokenError:
            return False
