import asyncio
import json
import logging
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlencode

import tornado.iostream
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.tcpclient import TCPClient
from tornado.web import Application
from tornado.websocket import websocket_connect

from src.pycronado import core, token
from src.pycronado.buckets import MemoryBucketStore
from src.pycronado.mixins import JSONSerializationMixin, LoggerMixin, NDJSONMixin
from src.pycronado.util import normalize_origin
from src.pycronado.websocket import PublicChannelSocketHandler, WebSocketHub


class HTTPRegressionTests(AsyncHTTPTestCase):
    def get_app(self):
        class Params(core.PublicJSONHandler):
            def post(self): self.ok(value=self.param('value'))
        class Bytes(core.PublicJSONHandler):
            def get(self): self.filebytes(b'hello' * 100000, extension='txt')
        class Unknown(core.PublicJSONHandler):
            def get(self): self.filebytes(b'unknown', extension='unknown_extension')
        class Disk(core.PublicJSONHandler):
            def get(self): self.file(self.application.test_file)
        class Fail(core.PublicJSONHandler):
            def get(self): raise RuntimeError('deliberate failure')
        class Stream(core.PublicJSONHandler, NDJSONMixin):
            origin_whitelist = ('https://ui.example',)
            async def get(self):
                async def items(): yield {'ok': True}
                await self.andjson_pump(items())
        class WaitingStream(core.PublicJSONHandler, NDJSONMixin):
            async def get(self):
                async def items():
                    try:
                        self.application.producer_started.set()
                        await asyncio.Event().wait()
                        yield 1
                    finally:
                        self.application.producer_closed.set()
                await self.andjson_pump(items())
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        filename = Path(self.tmp.name) / 'test.txt'
        filename.write_bytes(b'file data' * 100000)
        app = Application([('/params', Params), ('/bytes', Bytes), ('/unknown', Unknown),
                           ('/disk', Disk), ('/fail', Fail), ('/stream', Stream), ('/wait', WaitingStream)],
                          default_handler_class=core.Default404Handler)
        app.test_file = str(filename)
        app.producer_started = asyncio.Event()
        app.producer_closed = asyncio.Event()
        return app

    @gen_test
    async def test_disconnect_cancels_a_producer_waiting_for_next_item(self):
        tcp = TCPClient()
        stream = await tcp.connect('127.0.0.1', self.get_http_port())
        try:
            await stream.write(b'GET /wait HTTP/1.1\r\nHost: localhost\r\n\r\n')
            await stream.read_until(b'\r\n\r\n')
            await self._app.producer_started.wait()
            stream.close()
            await asyncio.wait_for(self._app.producer_closed.wait(), 1)
        finally:
            stream.close()
            tcp.close()

    def test_reject_malformed_and_non_object_json(self):
        for content_type, body in [('application/json', '{'), ('application/json', 'null'),
                                   ('application/json', '[]'), ('application/json', b'\xff'),
                                   ('application/x-www-form-urlencoded', 'json=null'),
                                   ('application/x-www-form-urlencoded', 'json=%5B1%5D')]:
            response = self.fetch('/params', method='POST', headers={'Content-Type': content_type}, body=body)
            self.assertEqual(response.code, 400)

    def test_json_takes_precedence_over_query(self):
        response = self.fetch('/params?value=query', method='POST',
                              headers={'Content-Type': 'application/json'}, body='{"value":null}')
        self.assertIsNone(json.loads(response.body)['value'])
        response = self.fetch('/params', method='POST', body=urlencode({'json': '{"value":42}'}))
        self.assertEqual(json.loads(response.body)['value'], 42)

    def test_sync_file_helpers_deliver_complete_responses(self):
        for path, expected in [('/bytes', b'hello' * 100000), ('/disk', b'file data' * 100000)]:
            response = self.fetch(path)
            self.assertEqual(response.code, 200)
            self.assertTrue(response.headers['Content-Type'].startswith('text/plain'))
            self.assertEqual(response.body, expected)
        self.assertEqual(self.fetch('/unknown').headers['Content-Type'], 'application/octet-stream')

    def test_ndjson_preserves_cors_vary(self):
        response = self.fetch('/stream', headers={'Origin': 'https://ui.example'})
        vary = ','.join(response.headers.get_list('Vary')).lower()
        self.assertIn('origin', vary)
        self.assertIn('accept', vary)
        self.assertEqual(json.loads(response.body), {'ok': True})

    def test_invalid_origins_and_denied_cache_variation(self):
        for origin in ['https://[broken', 'https://example.com:nope', 'https://denied.example']:
            response = self.fetch('/stream', headers={'Origin': origin})
            self.assertEqual(response.code, 200)
            self.assertNotIn('Access-Control-Allow-Origin', response.headers)
            self.assertIn('Origin', ','.join(response.headers.get_list('Vary')))

    def test_access_and_exception_logs_omit_query_credentials(self):
        with self.assertLogs(level=logging.INFO) as captured:
            response = self.fetch('/fail?jwt=DO_NOT_LOG&token=ALSO_SECRET',
                                  headers={'Authorization': 'Bearer HEADER_SECRET'})
        self.assertEqual(response.code, 500)
        logs = '\n'.join(captured.output)
        self.assertIn('/fail', logs)
        for secret in ['DO_NOT_LOG', 'ALSO_SECRET', 'HEADER_SECRET']:
            self.assertNotIn(secret, logs)

    def test_default_404_finishes_response_with_cors(self):
        response = self.fetch('/missing')
        self.assertEqual(response.code, 404)
        self.assertEqual(json.loads(response.body)['message'], 'not found')
        self.assertEqual(response.headers['Access-Control-Allow-Origin'], '*')


class WebSocketIntegrationTests(AsyncHTTPTestCase):
    def get_app(self):
        class Denied(PublicChannelSocketHandler):
            def can_subscribe(self, channel): return channel != 'private'
        class Limited(PublicChannelSocketHandler):
            message_rate = 0
            message_burst = 1
        self.hub = WebSocketHub(max_subscriptions=1, max_channel_length=16)
        app = Application([('/ws', Denied), ('/limited', Limited)])
        app.ws_hub = self.hub
        self.addCleanup(self.hub.close)
        return app

    async def connect(self, path):
        return await websocket_connect(self.get_url(path).replace('http:', 'ws:'))

    @gen_test
    async def test_denied_initial_subscription_reports_actual_membership(self):
        client = await self.connect('/ws?channel=private')
        try:
            self.assertEqual(json.loads(await client.read_message())['message'], 'forbidden')
            self.assertEqual(json.loads(await client.read_message()), {'type': 'ready', 'channels': []})
            client.write_message(json.dumps({'action': 'subscribe', 'channel': 'public'}))
            self.assertEqual(json.loads(await client.read_message())['type'], 'subscribed')
            client.write_message(json.dumps({'action': 'subscribe', 'channel': 'other'}))
            self.assertEqual(json.loads(await client.read_message())['message'], 'subscription limit')
            self.assertEqual(set(self.hub.channels), {'public'})
        finally:
            client.close()

    @gen_test
    async def test_messages_are_rate_limited(self):
        client = await self.connect('/limited')
        await client.read_message()
        client.write_message('{"action":"ping"}')
        self.assertEqual(json.loads(await client.read_message())['type'], 'pong')
        client.write_message('{"action":"ping"}')
        self.assertIsNone(await client.read_message())
        self.assertEqual(client.close_code, 1008)
        client.close()
        self.assertFalse(self.hub.channels)

    @gen_test
    async def test_worker_thread_publish_and_disconnect_cleanup(self):
        client = await self.connect('/ws?channel=public')
        await client.read_message()
        accepted = await asyncio.to_thread(self.hub.publish, 'public', {'value': 42})
        self.assertTrue(accepted)
        self.assertEqual(json.loads(await client.read_message()), {'value': 42})
        self.hub.close()
        self.assertIsNone(await client.read_message())
        self.assertFalse(self.hub.channels)
        self.assertFalse(self.hub.publish('public', 'late'))
        client.close()


class Client(JSONSerializationMixin):
    def __init__(self, pending=None):
        self.messages = []
        self.pending = pending
        self.closed = []
    def write_message(self, message):
        self.messages.append(message)
        return self.pending
    def close(self, **kwargs): self.closed.append(kwargs)


class HubTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_consumer_closed_and_fast_consumer_continues(self):
        hub = WebSocketHub(max_pending_messages=2)
        pending = asyncio.get_running_loop().create_future()
        slow, fast = Client(pending), Client()
        hub.subscribe('x', slow); hub.subscribe('x', fast)
        for value in range(3): hub._publish_now('x', str(value))
        self.assertEqual(fast.messages, ['0', '1', '2'])
        self.assertEqual(slow.messages, ['0'])
        self.assertEqual(slow.closed[0]['code'], 1013)
        self.assertNotIn(slow, hub.channels['x'])
        self.assertFalse(hub.send(slow, 'late'))
        pending.set_result(None)
        await asyncio.sleep(0)
        self.assertEqual(slow.messages, ['0'])
        hub.close()

    async def test_queued_message_not_sent_after_token_expires(self):
        hub = WebSocketHub()
        pending = asyncio.get_running_loop().create_future()
        client = Client(pending)
        client.JWT = {'exp': float('inf')}
        client.on_auth_failure = lambda: client.close(code=4001)
        hub.send(client, 'before'); hub.send(client, 'queued')
        client.JWT['exp'] = 0
        pending.set_result(None)
        await asyncio.sleep(0)
        self.assertEqual(client.messages, ['before'])
        self.assertEqual(client.closed[0]['code'], 4001)
        self.assertFalse(hub._outgoing)

    async def test_byte_limit_counts_utf8_and_control_messages(self):
        hub = WebSocketHub(max_pending_bytes=3)
        client = Client()
        self.assertFalse(hub.send(client, 'éé'))
        self.assertTrue(client.closed)
        self.assertEqual(client.messages, [])

    async def test_output_fifo_and_timeout(self):
        hub = WebSocketHub(write_timeout=0.01)
        pending = asyncio.get_running_loop().create_future()
        client = Client(pending)
        hub.send(client, 'a'); hub.send(client, 'b')
        client.pending = None
        pending.set_result(None)
        await asyncio.sleep(0)
        self.assertEqual(client.messages, ['a', 'b'])
        client.pending = asyncio.get_running_loop().create_future()
        hub.send(client, 'c')
        await asyncio.sleep(0.03)
        self.assertTrue(client.closed)
        client.pending.cancel()
        await asyncio.sleep(0)

    async def test_ingress_bound_and_reverse_membership(self):
        hub = WebSocketHub(max_publications=2, max_subscriptions=1)
        client = Client()
        self.assertTrue(hub.subscribe('one', client))
        self.assertFalse(hub.subscribe('two', client))
        self.assertTrue(hub.publish('one', 'a'))
        self.assertTrue(hub.publish('one', 'b'))
        self.assertFalse(hub.publish('one', 'c'))
        await asyncio.sleep(0)
        self.assertEqual(client.messages, ['a', 'b'])
        hub.unsubscribe_all(client)  # no _ws_channels attribute needed
        self.assertFalse(hub.channels)
        hub.close()

    async def test_default_codec_reused_custom_codec_isolated(self):
        hub = WebSocketHub()
        default1, default2 = Client(), Client()
        class Custom(Client):
            def dumps(self, message): return 'private-codec'
        custom = Custom()
        for client in [default1, default2, custom]: hub.subscribe('x', client)
        with patch.object(hub, '_serialize_for', wraps=hub._serialize_for) as encode:
            hub._publish_now('x', {'value': 1})
        self.assertEqual(encode.call_count, 2)
        self.assertEqual(default1.messages, default2.messages)
        self.assertEqual(custom.messages, ['private-codec'])
        with self.assertLogs(hub.logger().name, level='ERROR'):
            hub._publish_now('x', object())
        self.assertIn(default1, hub.channels['x'])
        hub.close()


class StreamProbe(NDJSONMixin):
    _ndjson_started = True
    _finished = False
    def __init__(self):
        self.writes = []
        self.closed = False
    def dumps(self, value): return json.dumps(value)
    def write(self, value): self.writes.append(value)
    async def flush(self): raise tornado.iostream.StreamClosedError()
    def finish(self): self._finished = True


class StreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnect_stops_and_closes_producer(self):
        probe = StreamProbe()
        seen, closed = [], []
        async def source():
            try:
                for value in range(5):
                    seen.append(value)
                    yield value
            finally: closed.append(True)
        await probe.andjson_pump(source())
        self.assertEqual(seen, [0])
        self.assertEqual(closed, [True])

    async def test_cancellation_propagates_and_closes_producer(self):
        probe = StreamProbe()
        entered, closed = asyncio.Event(), []
        async def flush():
            entered.set()
            await asyncio.Event().wait()
        probe.flush = flush
        async def source():
            try: yield 1
            finally: closed.append(True)
        task = asyncio.create_task(probe.andjson_pump(source()))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(closed, [True])
        self.assertEqual(len(probe.writes), 1)

    async def test_legacy_stream_has_output_bound(self):
        probe = StreamProbe()
        probe.ndjson_max_pending_bytes = 5
        pending = asyncio.get_running_loop().create_future()
        probe.flush = lambda: pending
        probe.ndjson('1234')
        with self.assertRaises(BufferError): probe.ndjson('more')
        self.assertEqual(probe.writes, ['1234\n'])
        pending.set_result(None)
        await asyncio.sleep(0)
        self.assertEqual(probe._ndjson_pending_bytes, 0)

    async def test_filebytes_awaits_backpressure_before_next_chunk(self):
        probe = SimpleNamespace()
        probe.set_header = lambda *args: None
        probe.writes = []
        probe.write = probe.writes.append
        gate = asyncio.Event()
        async def flush(): await gate.wait()
        async def finish(): pass
        probe.flush, probe.finish = flush, finish
        task = asyncio.create_task(core.PublicJSONHandler.afilebytes(probe, b'abcdef', chunk_size=2))
        await asyncio.sleep(0)
        self.assertEqual(probe.writes, [b'ab'])
        gate.set()
        await task
        self.assertEqual(probe.writes, [b'ab', b'cd', b'ef'])


class MiscTests(unittest.TestCase):
    def test_logger_registry_stable_and_paths_preserved(self):
        class Handler(LoggerMixin): pass
        before = set(logging.Logger.manager.loggerDict)
        with self.assertLogs(level=logging.INFO) as captured:
            for i in range(200):
                handler = Handler()
                handler.request = SimpleNamespace(method='GET', path=f'/items/{i}', uri=f'/items/{i}?jwt=SECRET')
                handler.logger.info('hello')
                handler.logger().info('legacy')
        added = set(logging.Logger.manager.loggerDict) - before
        self.assertLess(len(added), 10)
        logs = '\n'.join(captured.output)
        self.assertIn('/items/199', logs)
        self.assertNotIn('SECRET', logs)

    def test_origin_validation(self):
        for origin in ['https://x:bad', 'https://[broken', 'https://x:999999']:
            self.assertIsNone(normalize_origin(origin))
        self.assertEqual(normalize_origin('https://[::1]:443'), 'https://[::1]')

    def test_full_bucket_access_does_not_repeat_sweep(self):
        now = [0]
        store = MemoryBucketStore(max_entries=2, clock=lambda: now[0])
        store.consume('a', 10, 1); store.consume('b', 10, 1)
        entries = store._entries
        for _ in range(20): store.consume('a', 10, 1)
        self.assertIs(store._entries, entries)
        now[0] = 61
        self.assertTrue(store.consume('c', 10, 1))

    def test_key_publication_is_complete_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = os.path.join(directory, 'key.pem')
            payloads = [bytes([i]) * 100000 for i in range(1, 9)]
            def attempt(payload):
                try: token.writePrivateKey(payload, filename)
                except FileExistsError: pass
                return Path(filename).read_bytes()
            with ThreadPoolExecutor(8) as workers:
                observed = list(workers.map(attempt, payloads))
            self.assertTrue(all(data == observed[0] for data in observed))
            self.assertIn(observed[0], payloads)
            self.assertEqual(os.stat(filename).st_mode & 0o777, 0o600)
            self.assertEqual(os.listdir(directory), ['key.pem'])


class StartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_websocket_app_preserves_existing_global_hub(self):
        from src.pycronado.websocket import get_ws_hub, set_ws_hub
        hub = WebSocketHub()
        set_ws_hub(hub)
        server = SimpleNamespace(stop=Mock(), close_all_connections=AsyncMock())
        app = SimpleNamespace(listen=Mock(return_value=server))
        try:
            with patch.object(core.tornado.web, 'Application', return_value=app):
                task = asyncio.create_task(core.start('test', 1234, []))
                await asyncio.sleep(0)
                self.assertIs(get_ws_hub(), hub)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
                self.assertIs(get_ws_hub(), hub)
        finally:
            hub.close()
            set_ws_hub(None)

    async def test_bind_failure_cleans_up_hub(self):
        from src.pycronado.websocket import get_ws_hub
        app = SimpleNamespace(listen=Mock(side_effect=OSError('bind failed')))
        with patch.object(core.tornado.web, 'Application', return_value=app):
            with self.assertRaises(OSError):
                await core.start('test', 1234, [], websocket=True)
        self.assertTrue(app.ws_hub._closed)
        self.assertIsNone(get_ws_hub())
