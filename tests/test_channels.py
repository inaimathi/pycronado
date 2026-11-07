import asyncio
import importlib.util
import json
import subprocess
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import tornado.web
from tornado.httpclient import HTTPRequest
from tornado.tcpclient import TCPClient
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.websocket import websocket_connect

from src.pycronado import token
from src.pycronado.channel import ChannelEvent, ChannelHub, ChannelSessionMixin
from src.pycronado.mixins import JSONSerializationMixin, LoggerMixin
from src.pycronado.sse import PublicChannelSSEHandler, ChannelSSEHandler, encode_event
from src.pycronado.websocket import PublicChannelSocketHandler, WebSocketHub

HAS_WEBRTC = importlib.util.find_spec('aiortc') is not None


class EventTests(unittest.TestCase):
    def test_sse_multiline_and_metadata_cannot_inject_fields(self):
        self.assertEqual(encode_event('a\r\nid: evil\rb\n'),
                         'data: a\ndata: id: evil\ndata: b\ndata: \n\n')
        self.assertEqual(encode_event('value', ChannelEvent(None, event='update', id='7', retry=3000)),
                         'event: update\nid: 7\nretry: 3000\ndata: value\n\n')
        for kwargs in ({'id': 'x\ny'}, {'event': 'a\rdata: x'}, {'id': '\0'}, {'retry': -1}, {'retry': True}):
            with self.assertRaises(ValueError): ChannelEvent('data', **kwargs)

    def test_core_and_sse_import_without_optional_webrtc_dependency(self):
        code = """
import sys
sys.modules['aiortc'] = None
from src.pycronado import core, sse, webrtc
from src.pycronado.channel import ChannelHub
try:
    webrtc.WebRTCManager(ChannelHub())
except RuntimeError as exc:
    assert 'pycronado[webrtc]' in str(exc)
else:
    raise AssertionError('Missing dependency not reported')
"""
        subprocess.run([sys.executable, '-c', code], check=True, capture_output=True)

    def test_legacy_hub_alias(self):
        self.assertIs(WebSocketHub, ChannelHub)


class SSETests(AsyncHTTPTestCase):
    def get_app(self):
        class Restricted(PublicChannelSSEHandler):
            heartbeat_interval = 0.03
            def can_subscribe(self, channel): return channel != 'private'
        self.hub = ChannelHub(max_subscriptions=2)
        self.addCleanup(self.hub.close)
        app = tornado.web.Application([('/events', Restricted), ('/private', ChannelSSEHandler),
                                       ('/ws', PublicChannelSocketHandler)])
        app.channel_hub = self.hub
        return app

    async def open_stream(self, path='/events?channel=room', headers=''):
        tcp = TCPClient()
        stream = await tcp.connect('127.0.0.1', self.get_http_port())
        self.addCleanup(tcp.close)
        self.addCleanup(stream.close)
        # HTTP/1.0 avoids chunk framing in this transport test.
        await stream.write(f'GET {path} HTTP/1.0\r\nHost: localhost\r\n{headers}\r\n'.encode())
        response_headers = await stream.read_until(b'\r\n\r\n')
        self.assertIn(b'200 OK', response_headers)
        self.assertIn(b'text/event-stream', response_headers)
        return stream

    async def event(self, stream):
        return (await asyncio.wait_for(stream.read_until(b'\n\n'), 2)).decode()

    @gen_test
    async def test_websocket_and_sse_share_publications(self):
        stream = await self.open_stream()
        self.assertIn('"type":"ready"', await self.event(stream))
        ws = await websocket_connect(self.get_url('/ws?channel=room').replace('http:', 'ws:'))
        try:
            await ws.read_message()
            self.hub.publish('room', ChannelEvent({'value': 9}, event='update', id='42'))
            frame = await self.event(stream)
            self.assertIn('event: update\nid: 42\ndata: {"value":9}', frame)
            self.assertEqual(json.loads(await ws.read_message()), {'value': 9})
        finally:
            ws.close()
            stream.close()
        await asyncio.sleep(0.02)
        self.assertFalse(self.hub.channels)

    @gen_test
    async def test_heartbeat_and_last_event_id_do_not_imply_replay(self):
        stream = await self.open_stream(headers='Last-Event-ID: old-id\r\n')
        self.assertIn('"type":"ready"', await self.event(stream))
        self.assertIn('event: heartbeat', await self.event(stream))
        subscriber = next(iter(self.hub.channels['room']))
        self.assertEqual(subscriber.last_event_id, 'old-id')
        stream.close()

    def test_denied_and_invalid_subscriptions_fail_before_streaming(self):
        self.assertEqual(self.fetch('/events?channel=private').code, 403)
        self.assertEqual(self.fetch('/events?channel=a&channel=b&channel=c').code, 400)
        self.assertFalse(self.hub.channels)

    def test_sse_auth_does_not_accept_query_tokens_by_default(self):
        with patch.object(token, 'decode', side_effect=token.InvalidTokenError('missing')) as decode:
            self.assertEqual(self.fetch('/private?jwt=SECRET').code, 403)
            decode.assert_called_once_with(None, allowed_purposes=(None,))

    @gen_test
    async def test_authenticated_stream_expires_and_removes_memberships(self):
        claims = {'exp': time.time() + 0.1, 'user': {'username': 'u'}}
        with patch.object(token, 'decode', return_value=claims):
            stream = await self.open_stream('/private?channel=room', 'Authorization: Bearer test\r\n')
            await self.event(stream)
            await asyncio.wait_for(stream.read_until_close(), 1)
        self.assertFalse(self.hub.channels)
        self.assertFalse(self.hub._clients)

    @gen_test
    async def test_shutdown_ends_sse_without_leaving_connections(self):
        stream = await self.open_stream()
        await self.event(stream)
        self.hub.close()
        await asyncio.wait_for(stream.read_until_close(), 1)
        self.assertFalse(self.hub._clients)


class Session(ChannelSessionMixin, JSONSerializationMixin, LoggerMixin):
    def __init__(self, hub):
        self.hub = hub
        self.messages, self.commands, self.closed = [], [], []
        self._init_session()
        self._start_session(['room'])
    def _hub(self): return self.hub
    def write_message(self, message): self.messages.append(json.loads(message))
    def close(self, **kwargs):
        self.closed.append(kwargs)
        self._cleanup_session()


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_application_commands_require_explicit_authorization(self):
        hub = ChannelHub()
        session = Session(hub)
        session.on_channel_message = lambda channel, data: session.commands.append(data)
        session.receive_message('{"action":"message","channel":"room","data":1}')
        await asyncio.sleep(0)
        self.assertFalse(session.commands)
        self.assertEqual(session.messages[-1]['message'], 'forbidden')
        session.can_publish = lambda channel: True
        session.receive_message('{"action":"message","channel":"room","data":2}')
        await asyncio.sleep(0)
        self.assertEqual(session.commands, [2])
        session.receive_message('{"action":"message","channel":"other","data":3}')
        await asyncio.sleep(0)
        self.assertEqual(session.commands, [2])
        hub.close()

    async def test_one_worker_bounded_input_and_cleanup_of_async_command(self):
        hub = ChannelHub()
        session = Session(hub)
        session.max_incoming_messages = 1
        session.can_publish = lambda channel: True
        entered, cleaned = asyncio.Event(), asyncio.Event()
        async def command(channel, data):
            try:
                entered.set()
                await asyncio.Event().wait()
            finally: cleaned.set()
        session.on_channel_message = command
        session.receive_message('{"action":"message","channel":"room","data":1}')
        await entered.wait()
        worker = session._incoming_task
        session.receive_message('{"action":"ping"}')
        await asyncio.wait_for(cleaned.wait(), 1)
        self.assertIs(worker, session._incoming_task)
        self.assertTrue(session.closed)
        self.assertFalse(hub.channels)

    async def test_state_lane_cannot_change_subscriptions(self):
        hub = ChannelHub()
        session = Session(hub)
        session.receive_message('{"action":"subscribe","channel":"other"}', state=True)
        await asyncio.sleep(0)
        self.assertEqual(session._channel_names, {'room'})
        hub.close()


@unittest.skipUnless(HAS_WEBRTC, 'install pycronado[webrtc] for real ICE/DTLS/SCTP tests')
class WebRTCTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import aiortc
        from src.pycronado.webrtc import WebRTCManager
        # Deterministic loopback ICE avoids host interface discovery and external
        # STUN dependencies while exercising actual UDP/DTLS/SCTP traffic.
        discovery = patch("aioice.ice.get_host_addresses", return_value=["127.0.0.1"])
        discovery.start()
        self.addCleanup(discovery.stop)
        self.rtc = aiortc
        self.hub = ChannelHub()
        self.manager = WebRTCManager(self.hub, offer_timeout=5, connection_timeout=5)
        self.remote = aiortc.RTCPeerConnection(aiortc.RTCConfiguration(iceServers=[]))
        self.control = self.remote.createDataChannel('control', protocol='pycronado.v1')
        self.state = self.remote.createDataChannel('state', protocol='pycronado.v1', ordered=False, maxRetransmits=0)
        self.control_messages, self.state_messages = asyncio.Queue(), asyncio.Queue()
        self.control.on('message', self.control_messages.put_nowait)
        self.state.on('message', self.state_messages.put_nowait)

    async def asyncTearDown(self):
        await self.remote.close()
        self.hub.close()
        await self.manager.aclose()
        self.assertFalse(self.manager._peers)
        self.assertFalse(self.hub._clients)

    async def offer(self):
        await self.remote.setLocalDescription(await self.remote.createOffer())
        return {'type': 'offer', 'sdp': self.remote.localDescription.sdp}

    async def connect(self, peer_class=None, claims=None):
        from src.pycronado.webrtc import PublicChannelPeer
        peer, answer = await self.manager.accept(await self.offer(),
            peer_class=peer_class or PublicChannelPeer, claims=claims, channel='room')
        await self.remote.setRemoteDescription(self.rtc.RTCSessionDescription(**answer))
        ready = json.loads(await asyncio.wait_for(self.control_messages.get(), 5))
        self.assertEqual(ready, {'type': 'ready', 'channels': ['room']})
        return peer

    async def test_real_reliable_unreliable_and_application_messages(self):
        from src.pycronado.webrtc import PublicChannelPeer
        incoming, states = asyncio.Queue(), asyncio.Queue()
        class Peer(PublicChannelPeer):
            def can_publish(self, channel): return True
            async def on_channel_message(self, channel, data): await incoming.put((channel, data))
            async def on_state_message(self, channel, data): await states.put((channel, data))
        peer = await self.connect(Peer)
        self.hub.publish('room', {'event': 'spawn'})
        self.assertEqual(json.loads(await asyncio.wait_for(self.control_messages.get(), 2)), {'event': 'spawn'})
        self.control.send('{"action":"ping"}')
        self.assertEqual(json.loads(await asyncio.wait_for(self.control_messages.get(), 2)), {'type': 'pong'})
        self.control.send('{"action":"message","channel":"room","data":{"fire":1}}')
        self.assertEqual(await asyncio.wait_for(incoming.get(), 2), ('room', {'fire': 1}))
        # Both data channels have been negotiated; give the state OPEN handshake a turn.
        for _ in range(100):
            if self.state.readyState == 'open' and 'state' in peer._data_channels: break
            await asyncio.sleep(0.01)
        self.assertEqual(self.manager.publish_state('room', {'tick': 1}), 1)
        self.assertEqual(json.loads(await asyncio.wait_for(self.state_messages.get(), 2)), {'channel': 'room', 'data': {'tick': 1}})
        self.state.send('{"action":"message","channel":"room","data":{"tick":2}}')
        self.assertEqual(await asyncio.wait_for(states.get(), 2), ('room', {'tick': 2}))
        self.assertFalse(peer.send_state('room', 'x' * peer.max_message_size))

    async def test_oversized_reliable_publication_closes_transport(self):
        peer = await self.connect()
        self.hub.publish('room', 'x' * (peer.max_message_size + 1))
        await asyncio.sleep(0)
        self.assertTrue(peer._session_closed)
        await peer.aclose()

    async def test_identity_bound_and_expiry_closes_peer(self):
        from src.pycronado.webrtc import ChannelPeer
        claims = {'exp': time.time() + 2, 'iss': 'test', 'user': {'username': 'alice'}}
        peer = await self.connect(ChannelPeer, claims)
        claims['user']['username'] = 'attacker'
        self.assertEqual(peer.username(), 'alice')
        # Exercise expiry without relying on a narrow handshake timing margin.
        peer.JWT['exp'] = 0
        self.assertFalse(peer.send_state('room', {'tick': 1}))
        await peer.aclose()
        self.assertFalse(self.hub.channels)

    async def test_auth_and_subscription_denial_leave_no_resources(self):
        from src.pycronado.webrtc import ChannelPeer, PublicChannelPeer
        with self.assertRaises(tornado.web.HTTPError) as error:
            await self.manager.accept({}, peer_class=ChannelPeer)
        self.assertEqual(error.exception.status_code, 403)
        class Denied(PublicChannelPeer):
            def can_subscribe(self, channel): return False
        with self.assertRaises(tornado.web.HTTPError) as error:
            await self.manager.accept(await self.offer(), peer_class=Denied)
        self.assertEqual(error.exception.status_code, 403)
        await asyncio.sleep(0)
        self.assertFalse(self.manager._peers)
        self.assertFalse(self.hub._clients)

    async def test_incomplete_and_media_offers_rejected(self):
        from src.pycronado.webrtc import PublicChannelPeer
        for offer in ({}, {'type': 'offer', 'sdp': 'invalid'},
                      {'type': 'offer', 'sdp': 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n'}):
            with self.assertRaises(tornado.web.HTTPError) as error:
                await self.manager.accept(offer, peer_class=PublicChannelPeer)
            self.assertEqual(error.exception.status_code, 400)
        self.assertFalse(self.manager._peers)

    async def test_ignored_answer_times_out_and_releases_peer(self):
        from src.pycronado.webrtc import PublicChannelPeer
        self.manager.connection_timeout = 0.05
        peer, _ = await self.manager.accept(await self.offer(), peer_class=PublicChannelPeer)
        await asyncio.sleep(0.1)
        await peer.aclose()
        self.assertFalse(self.hub._clients)

    async def test_invalid_data_channel_policy_closes_peer(self):
        from src.pycronado.webrtc import PublicChannelPeer
        # An extra channel cannot allocate an unbounded collection of transports.
        self.remote.createDataChannel('unexpected', protocol='pycronado.v1')
        peer, answer = await self.manager.accept(await self.offer(), peer_class=PublicChannelPeer)
        await self.remote.setRemoteDescription(self.rtc.RTCSessionDescription(**answer))
        for _ in range(100):
            if peer._session_closed: break
            await asyncio.sleep(0.01)
        self.assertTrue(peer._session_closed)

    async def test_capacity_and_negotiation_timeout(self):
        from src.pycronado.webrtc import PublicChannelPeer
        offer = await self.offer()
        self.manager.max_peers = 1
        peer, _ = await self.manager.accept(offer, peer_class=PublicChannelPeer)
        with self.assertRaises(tornado.web.HTTPError) as error:
            await self.manager.accept(offer, peer_class=PublicChannelPeer)
        self.assertEqual(error.exception.status_code, 503)
        await peer.aclose()
        await asyncio.sleep(0)
        self.manager.offer_timeout = 0.02
        async def stall(*args): await asyncio.Event().wait()
        with patch.object(self.rtc.RTCPeerConnection, 'setRemoteDescription', stall):
            with self.assertRaises(tornado.web.HTTPError) as error:
                await self.manager.accept(offer, peer_class=PublicChannelPeer)
            self.assertEqual(error.exception.status_code, 504)
        await asyncio.sleep(0)
        self.assertFalse(self.hub._clients)


@unittest.skipUnless(HAS_WEBRTC, 'install pycronado[webrtc]')
class SignalingTests(AsyncHTTPTestCase):
    def get_app(self):
        from src.pycronado.webrtc import WebRTCManager, ChannelOfferHandler, PublicChannelOfferHandler
        self.hub = ChannelHub()
        self.manager = WebRTCManager(self.hub)
        app = tornado.web.Application([('/offer', ChannelOfferHandler), ('/public-offer', PublicChannelOfferHandler)])
        app.channel_hub = self.hub
        app.webrtc_manager = self.manager
        return app

    def tearDown(self):
        self.hub.close()
        self.io_loop.run_sync(self.manager.aclose)
        super().tearDown()

    def test_auth_precedes_negotiation_and_allocation(self):
        with patch.object(token, 'decode', side_effect=token.InvalidTokenError('missing')):
            response = self.fetch('/offer', method='POST', body='not even json',
                                  headers={'Content-Type': 'application/json'})
        self.assertEqual(response.code, 403)
        self.assertFalse(self.manager._peers)

    @gen_test
    async def test_offer_endpoint_end_to_end(self):
        import aiortc
        remote = aiortc.RTCPeerConnection(aiortc.RTCConfiguration(iceServers=[]))
        channel = remote.createDataChannel('control', protocol='pycronado.v1')
        incoming = asyncio.Queue()
        channel.on('message', incoming.put_nowait)
        try:
            with patch('aioice.ice.get_host_addresses', return_value=['127.0.0.1']):
                await remote.setLocalDescription(await remote.createOffer())
                body = json.dumps({'type': 'offer', 'sdp': remote.localDescription.sdp, 'channel': 'room'})
                response = await self.http_client.fetch(HTTPRequest(self.get_url('/public-offer'),
                    method='POST', headers={'Content-Type': 'application/json'}, body=body))
                self.assertEqual(response.headers['Cache-Control'], 'no-store')
                await remote.setRemoteDescription(aiortc.RTCSessionDescription(**json.loads(response.body)))
                self.assertEqual(json.loads(await asyncio.wait_for(incoming.get(), 3))['channels'], ['room'])
                self.hub.publish('room', {'via': 'http signaling'})
                self.assertEqual(json.loads(await asyncio.wait_for(incoming.get(), 2)), {'via': 'http signaling'})
        finally:
            await remote.close()

    @gen_test
    async def test_signaling_disconnect_cancels_inflight_negotiation(self):
        import aiortc
        remote = aiortc.RTCPeerConnection(aiortc.RTCConfiguration(iceServers=[]))
        remote.createDataChannel('control', protocol='pycronado.v1')
        entered, exited = asyncio.Event(), asyncio.Event()
        async def accept(*args, **kwargs):
            try:
                entered.set()
                await asyncio.Event().wait()
            finally: exited.set()
        tcp = TCPClient()
        stream = await tcp.connect('127.0.0.1', self.get_http_port())
        try:
            with patch.object(self.manager, 'accept', accept):
                body = b'{"type":"offer","sdp":"placeholder"}'
                await stream.write(b'POST /public-offer HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
                await entered.wait()
                stream.close()
                await asyncio.wait_for(exited.wait(), 1)
        finally:
            stream.close()
            tcp.close()
            await remote.close()


class ChannelStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_sse_only_start_owns_shared_hub_and_preserves_legacy_alias(self):
        from src.pycronado import core
        from src.pycronado.channel import get_channel_hub
        app = SimpleNamespace(listen=Mock(return_value=SimpleNamespace(
            stop=Mock(), close_all_connections=AsyncMock())))
        with patch.object(core.tornado.web, 'Application', return_value=app):
            task = asyncio.create_task(core.start('test', 1234, [], sse=True,
                channel_hub_options={'max_subscriptions': 3}))
            await asyncio.sleep(0)
            self.assertIs(app.channel_hub, app.ws_hub)
            self.assertIs(get_channel_hub(), app.channel_hub)
            self.assertEqual(app.channel_hub.max_subscriptions, 3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.assertTrue(app.channel_hub._closed)
        self.assertIsNone(get_channel_hub())
