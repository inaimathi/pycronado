"""Optional aiortc data-channel transport with bounded HTTP offer/answer signaling.

Install pycronado[webrtc]. Only browser-to-server, data-only, complete (non-trickle)
SDP offers are supported. ICE server configuration belongs to the server operator.
"""
import asyncio
import copy
import time

import tornado.web

from . import token
from .auth import UserMixin
from .buckets import BucketUnavailable, MemoryBucketStore
from .channel import ChannelSessionMixin
from .core import PublicJSONHandler
from .mixins import JSONSerializationMixin, LoggerMixin


def _aiortc():
    try:
        import aiortc
        return aiortc
    except ImportError as exc:
        raise RuntimeError('WebRTC requires pip install "pycronado[webrtc]"') from exc


class BaseChannelPeer(ChannelSessionMixin, JSONSerializationMixin, LoggerMixin):
    """Persistent peer independent of the transient HTTP signaling handler."""
    requires_auth = False
    max_message_size = 16 * 1024
    max_transport_buffer_bytes = 256 * 1024
    protocol = 'pycronado.v1'

    def __init__(self, manager, pc, *, channel='default', claims=None, context=None):
        self.manager, self.pc = manager, pc
        self.initial_channel = channel
        self.context = dict(context or {})
        if claims is not None:
            self.JWT = copy.deepcopy(claims)
        self._data_channels = {}
        self._write_pending = None
        self._close_task = None
        self._connection_timer = None
        self._init_session()
        pc.on('datachannel', self._accept_data_channel)
        pc.on('connectionstatechange', self._connection_changed)

    def _hub(self):
        return self.manager.hub

    def can_connect(self):
        return not self.requires_auth or hasattr(self, 'JWT')

    def _connection_changed(self):
        if self.pc.connectionState in ('failed', 'closed'):
            self.close(reason='peer disconnected')

    def _accept_data_channel(self, channel):
        valid = (channel.label in ('control', 'state') and channel.label not in self._data_channels
                 and channel.protocol == self.protocol and channel.maxPacketLifeTime is None)
        if channel.label == 'control':
            valid = valid and channel.ordered and channel.maxRetransmits is None
        elif channel.label == 'state':
            valid = valid and not channel.ordered and channel.maxRetransmits == 0
        if not valid or self._session_closed:
            channel.close()
            self.close(reason='invalid data channel')
            return
        self._data_channels[channel.label] = channel
        channel.bufferedAmountLowThreshold = 0
        channel.on('close', lambda: self.close(reason='data channel closed'))
        channel.on('message', lambda message: self.receive_message(message, state=channel.label == 'state'))
        if channel.label == 'control':
            channel.on('bufferedamountlow', self._buffer_drained)
            channel.on('open', self._control_open)
            if channel.readyState == 'open':
                self._control_open()

    def _control_open(self):
        if self._session_started or self._session_closed:
            return
        if self._start_session([self.initial_channel]) and self._connection_timer is not None:
            self._connection_timer.cancel()

    def _buffer_drained(self):
        channel = self._data_channels.get('control')
        if (channel is not None and channel.bufferedAmount == 0
                and self._write_pending is not None and not self._write_pending.done()):
            self._write_pending.set_result(None)

    def write_message(self, encoded):
        channel = self._data_channels.get('control')
        if self._session_closed or channel is None or channel.readyState != 'open':
            raise ConnectionError('WebRTC control channel closed')
        size = len(encoded.encode('utf-8'))
        if size > self.max_message_size or channel.bufferedAmount + size > self.max_transport_buffer_bytes:
            raise ConnectionError('WebRTC transport buffer limit')
        # The hub serializes reliable writes; completion means drained from the
        # local transport buffer, never an acknowledgement by the application.
        pending = asyncio.get_running_loop().create_future()
        self._write_pending = pending
        channel.send(encoded)
        self._buffer_drained()
        return pending

    def send_state(self, channel, data):
        """Best-effort datagram; False means unavailable, too large or congested.

        Call on the owning event loop. Include sequence/tick numbers in data and
        discard obsolete snapshots on receipt. No implicit reliable fallback.
        """
        if self._session_closed or channel not in self._channel_names:
            return False
        if hasattr(self, 'JWT') and float(self.JWT['exp']) <= time.time():
            self.on_auth_failure()
            return False
        transport = self._data_channels.get('state')
        if transport is None or transport.readyState != 'open':
            return False
        encoded = self.dumps({'channel': channel, 'data': data})
        size = len(encoded.encode('utf-8'))
        if size > self.max_message_size or transport.bufferedAmount + size > self.max_transport_buffer_bytes:
            return False
        transport.send(encoded)
        return True

    def close(self, code=None, reason=None):
        if self._close_task is not None:
            return
        self._cleanup_session()
        if self._connection_timer is not None:
            self._connection_timer.cancel()
        if self._write_pending is not None and not self._write_pending.done():
            self._write_pending.cancel()
        self._close_task = asyncio.create_task(self.pc.close())
        self._close_task.add_done_callback(self.manager._peer_closed(self))

    async def aclose(self):
        self.close()
        await asyncio.shield(self._close_task)


class PublicChannelPeer(BaseChannelPeer):
    """Anonymous WebRTC session."""


class ChannelPeer(BaseChannelPeer, UserMixin):
    """JWT identity is verified by signaling and bound to this peer."""
    requires_auth = True

    def decoded_jwt(self):
        return self.JWT


class WebRTCManager(LoggerMixin):
    """Owns peers and negotiation budgets for one application/hub."""
    def __init__(self, hub, *, ice_servers=None, max_peers=256, max_pending_offers=32,
                 offer_timeout=15, connection_timeout=30, max_sdp_bytes=65536,
                 offers_per_minute=20, total_offers_per_minute=300):
        self.rtc = _aiortc()
        if any(type(x) is not int or x <= 0 for x in
               (max_peers, max_pending_offers, max_sdp_bytes, offers_per_minute, total_offers_per_minute)):
            raise ValueError('WebRTC limits must be positive integers')
        if offer_timeout <= 0 or connection_timeout <= 0:
            raise ValueError('WebRTC timeouts must be positive')
        self.hub = hub
        self.configuration = self.rtc.RTCConfiguration(iceServers=[
            self.rtc.RTCIceServer(**server) if isinstance(server, dict) else server
            for server in (ice_servers or [])])
        self.max_peers, self.max_pending_offers = max_peers, max_pending_offers
        self.offer_timeout, self.connection_timeout = offer_timeout, connection_timeout
        self.max_sdp_bytes = max_sdp_bytes
        self.offers_per_minute, self.total_offers_per_minute = offers_per_minute, total_offers_per_minute
        self._budgets = MemoryBucketStore(max_entries=10000)
        self._peers = set()
        self._offers = set()
        self._closed = False

    def _peer_closed(self, peer):
        def done(task):
            self._peers.discard(peer)
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                self.logger.exception('WebRTC peer cleanup failed')
        return done

    def _check_offer(self, offer):
        if not isinstance(offer, dict) or offer.get('type') != 'offer' or not isinstance(offer.get('sdp'), str):
            raise tornado.web.HTTPError(400, reason='Expected an SDP offer')
        sdp = offer['sdp']
        if len(sdp.encode('utf-8')) > self.max_sdp_bytes:
            raise tornado.web.HTTPError(413, reason='SDP too large')
        try:
            from aiortc.sdp import SessionDescription
            parsed = SessionDescription.parse(sdp)
            if (len(parsed.media) != 1 or parsed.media[0].kind != 'application'
                    or not parsed.media[0].ice_candidates
                    or len(parsed.media[0].ice_candidates) > 64):
                raise ValueError('Expected one complete data-only offer')
        except Exception:
            raise tornado.web.HTTPError(400, reason='Invalid data-channel SDP') from None
        return self.rtc.RTCSessionDescription(sdp=sdp, type='offer')

    async def accept(self, offer, *, peer_class=ChannelPeer, channel='default', claims=None,
                     context=None, budget_key='anonymous'):
        if peer_class.requires_auth and claims is None:
            raise tornado.web.HTTPError(403)
        if not self.hub.validate_channel(channel):
            raise tornado.web.HTTPError(400, reason='Invalid channel')
        if self._closed or self.hub._closed or len(self._peers) >= self.max_peers or len(self._offers) >= self.max_pending_offers:
            raise tornado.web.HTTPError(503, reason='WebRTC capacity exhausted')
        try:
            admitted = (self._budgets.consume('global', 60, self.total_offers_per_minute)
                        and self._budgets.consume('peer:' + budget_key, 60, self.offers_per_minute))
        except BucketUnavailable:
            raise tornado.web.HTTPError(503, reason='Signaling limiter unavailable') from None
        if not admitted:
            raise tornado.web.HTTPError(429, reason='Too many offers')
        description = self._check_offer(offer)
        # No await before reservations: admission is atomic on the owning loop.
        pc = self.rtc.RTCPeerConnection(self.configuration)
        peer = None
        task = asyncio.current_task()
        self._offers.add(task)
        try:
            peer = peer_class(self, pc, channel=channel, claims=claims, context=context)
            self._peers.add(peer)
            if not peer.can_connect() or not peer.can_subscribe(channel):
                raise tornado.web.HTTPError(403)
            if not self.hub.register(peer):
                raise tornado.web.HTTPError(503, reason='Connection limit')
            peer._arm_expiry()
            peer._connection_timer = self.hub.ioloop.call_later(self.connection_timeout, peer.close)
            async with asyncio.timeout(self.offer_timeout):
                await pc.setRemoteDescription(description)
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
            if peer._session_closed or (hasattr(peer, 'JWT') and float(peer.JWT['exp']) <= time.time()):
                raise tornado.web.HTTPError(403)
            return peer, {'type': pc.localDescription.type, 'sdp': pc.localDescription.sdp}
        except BaseException as exc:
            if peer is not None:
                await peer.aclose()
            else:
                await pc.close()
            if isinstance(exc, TimeoutError):
                raise tornado.web.HTTPError(504, reason='WebRTC negotiation timed out') from None
            if isinstance(exc, (ValueError, AssertionError)):
                raise tornado.web.HTTPError(400, reason='Invalid WebRTC offer') from None
            raise
        finally:
            self._offers.discard(task)

    def publish_state(self, channel, data):
        """Return number of WebRTC peers accepting this best-effort snapshot.

        Event-loop only. WebSocket and SSE clients receive nothing on this path.
        """
        count = 0
        for peer in tuple(self.hub.channels.get(channel, ())):
            if isinstance(peer, BaseChannelPeer) and peer.manager is self:
                count += bool(peer.send_state(channel, data))
        return count

    async def aclose(self):
        self._closed = True
        offers = [task for task in self._offers if task is not asyncio.current_task()]
        for task in offers:
            task.cancel()
        await asyncio.gather(*offers, return_exceptions=True)
        await asyncio.gather(*(peer.aclose() for peer in tuple(self._peers)), return_exceptions=True)


class BaseChannelOfferHandler(PublicJSONHandler):
    """POST a complete offer; server answers once. No trickle/renegotiation API."""
    peer_class = BaseChannelPeer
    jwt_purposes = (None,)

    def get_peer_context(self):
        return {'remote_ip': self.request.remote_ip}

    async def post(self, channel=None):
        manager = getattr(self.application, 'webrtc_manager', None)
        if manager is None:
            raise tornado.web.HTTPError(503, reason='WebRTC is not enabled')
        # Authentication precedes SDP parsing and peer/certificate allocation.
        claims = None
        if self.peer_class.requires_auth:
            try:
                claims = token.decode(self.jwt(), allowed_purposes=self.jwt_purposes)
            except token.InvalidTokenError:
                return self.jsonerr('forbidden', 403)
        if len(self.request.body) > manager.max_sdp_bytes * 2:
            return self.jsonerr('offer too large', 413)
        offer = {'type': self.param('type'), 'sdp': self.param('sdp')}
        channel = channel or self.param('channel', 'default')
        # IP quotas constrain anonymous and authenticated setup alike. Configure
        # trusted proxy handling in the HTTP server if deployed behind a proxy.
        self._offer_task = asyncio.create_task(manager.accept(
            offer, peer_class=self.peer_class, channel=channel, claims=claims,
            context=self.get_peer_context(), budget_key=self.request.remote_ip))
        peer = None
        try:
            peer, answer = await self._offer_task
            self.set_header('Cache-Control', 'no-store')
            self.json(answer)
            await self.finish()
        except asyncio.CancelledError:
            if peer is not None:
                await peer.aclose()
            if getattr(self, '_signaling_disconnected', False):
                return
            if manager._closed and not asyncio.current_task().cancelling():
                raise tornado.web.HTTPError(503, reason='Server shutting down') from None
            raise
        except BaseException:
            if peer is not None:
                await peer.aclose()
            raise
        finally:
            self._offer_task = None

    def on_connection_close(self):
        self._signaling_disconnected = True
        super().on_connection_close()
        task = getattr(self, '_offer_task', None)
        if task is not None and not task.done():
            task.cancel()


class PublicChannelOfferHandler(BaseChannelOfferHandler):
    peer_class = PublicChannelPeer


class ChannelOfferHandler(BaseChannelOfferHandler):
    peer_class = ChannelPeer
