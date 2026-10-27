import asyncio
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import fakeredis
from src.pycronado import token
from src.pycronado.buckets import BucketUnavailable, MemoryBucketStore, RedisBucketStore
from src.pycronado.rate_limiter import RateLimiter
from src.pycronado.core import JSONHandler, PublicJSONHandler
from src.pycronado.websocket import ChannelSocketHandler, WebSocketHub
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application


class BucketTests(unittest.TestCase):
    def test_threads_exactly_one_winner(self):
        store = MemoryBucketStore()
        with ThreadPoolExecutor(16) as pool:
            results = list(pool.map(lambda _: store.consume('x', 3600, 1), range(100)))
        self.assertEqual(sum(results), 1)
        self.assertEqual(store.snapshot()['x']['used'], 100)

    def test_async_exactly_one_winner(self):
        async def run():
            store = MemoryBucketStore()
            async def attempt():
                await asyncio.sleep(0)
                return store.consume('x', 3600, 1)
            return await asyncio.gather(*(attempt() for _ in range(100)))
        self.assertEqual(sum(asyncio.run(run())), 1)

    def test_mixed_threads_and_coroutines(self):
        async def run():
            store = MemoryBucketStore()
            calls = [asyncio.to_thread(store.consume, 'x', 3600, 1) for _ in range(40)]
            async def local():
                await asyncio.sleep(0)
                return store.consume('x', 3600, 1)
            return await asyncio.gather(*calls, *(local() for _ in range(40)))
        self.assertEqual(sum(asyncio.run(run())), 1)

    def test_refill_expiry_and_capacity_fail_closed(self):
        now = [0.0]
        store = MemoryBucketStore(max_entries=1, clock=lambda: now[0])
        self.assertTrue(store.consume('x', 10, 1))
        self.assertFalse(store.consume('x', 10, 1))
        with self.assertRaises(BucketUnavailable): store.consume('y', 10, 1)
        now[0] = 10
        self.assertTrue(store.consume('x', 10, 1))
        now[0] = 71
        self.assertTrue(store.consume('y', 10, 1))
        self.assertNotIn('x', store.snapshot())

    def test_redis_lua_atomic_across_store_instances(self):
        server = fakeredis.FakeServer()
        stores = [RedisBucketStore(fakeredis.FakeStrictRedis(server=server)) for _ in range(8)]
        with ThreadPoolExecutor(16) as pool:
            result = list(pool.map(lambda i: stores[i % 8].consume('rate|u|a', 3600, 1), range(80)))
        self.assertEqual(sum(result), 1)
        self.assertEqual(stores[0].snapshot()['rate|u|a']['used'], 80)

    def test_unavailable_redis_does_not_mint_new_bucket(self):
        class Broken:
            def eval(self, *args): raise ConnectionError('offline')
        limiter = RateLimiter(client=Broken(), action={'tier.free': 1})
        with self.assertRaises(BucketUnavailable): limiter.check_and_increment('u', 'tier.free', 'action')
        class Handler:
            called = False
            def permissions(self): return []
            def user_id(self): return 'u'
            def jsonerr(self, message, status): return status
        h = Handler()
        @limiter.limited('action')
        def work(handler): handler.called = True
        self.assertEqual(work(h), 503)
        self.assertFalse(h.called)
        @limiter.limited('action')
        async def awork(handler): handler.called = True
        self.assertEqual(asyncio.run(awork(h)), 503)
        self.assertFalse(h.called)

    def test_configuration_and_missing_tier_fail_closed(self):
        for value in [0, -1]:
            with self.assertRaises(ValueError): RateLimiter(window=value)
        with self.assertRaises(ValueError): RateLimiter(a={'tier.free': -1})
        limiter = RateLimiter(a={'tier.premium': 5})
        self.assertFalse(limiter.check_and_increment('u', 'tier.free', 'a'))
        self.assertTrue(limiter.check_and_increment('u', 'tier.free', 'unmetered'))


class TokenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.old = token.JWT_PRIVKEY, token.JWT_PUBKEY, token.JWT_ISSUER
        token.ensureFreshKeys(2048)
        token.JWT_ISSUER = 'test-issuer'
    @classmethod
    def tearDownClass(cls): token.JWT_PRIVKEY, token.JWT_PUBKEY, token.JWT_ISSUER = cls.old
    def claims(self):
        import time
        return {'iss': 'test-issuer', 'iat': int(time.time()), 'exp': int(time.time())+60, 'user': {'username': 'u'}}
    def test_expiry_issuer_and_purpose(self):
        for field in ['iat', 'exp']:
            claims = self.claims(); del claims[field]
            with self.assertRaises(Exception): token.decode(token.encode(claims))
        claims = self.claims(); claims['iss'] = 'other'
        with self.assertRaises(Exception): token.decode(token.encode(claims))
        claims = self.claims(); claims['purpose'] = 'password_reset'
        encoded = token.encode(claims)
        with self.assertRaises(Exception): token.decode(encoded)
        self.assertEqual(token.decode(encoded, allowed_purposes=('password_reset',))['purpose'], 'password_reset')
    def test_expired_and_wrong_algorithm_tokens_rejected(self):
        import jwt
        claims = self.claims(); claims['exp'] = claims['iat'] - 1
        with self.assertRaises(Exception): token.decode(token.encode(claims))
        unsigned = jwt.encode(self.claims(), key="", algorithm="none")
        with self.assertRaises(Exception): token.decode(unsigned)

    def test_corrupt_key_is_never_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'key.pem')
            with open(path, 'wb') as f: f.write(b'broken-key')
            with self.assertRaises(Exception): token.ensureKeysFrom(file_path=path)
            with open(path, 'rb') as f: self.assertEqual(f.read(), b'broken-key')
    def test_persistence_failure_is_not_ignored(self):
        with patch.object(token, 'writePrivateKey', side_effect=PermissionError):
            with self.assertRaises(PermissionError): token.ensureKeysFrom(key_size=2048, file_path='/not-present-test-key')


class HandlerTests(AsyncHTTPTestCase):
    def get_app(self):
        class Protected(JSONHandler):
            def post(self): self.application.side_effects += 1; self.ok()
        app = Application([(r'/', Protected)])
        app.side_effects = 0
        return app
    def test_unauthenticated_request_never_runs_handler(self):
        response = self.fetch('/', method='POST', body='{}')
        self.assertEqual(response.code, 403)
        self.assertEqual(self._app.side_effects, 0)


class WebSocketTests(unittest.TestCase):
    def test_expired_socket_is_not_sent_private_data(self):
        async def run():
            hub = WebSocketHub()
            class Client:
                JWT = {"exp": 0}
                _ws_channels = {'private'}
                closed = False
                def write_message(self, message): raise AssertionError("sent expired data")
                def on_auth_failure(self): self.closed = True
            client = Client(); hub.subscribe('private', client)
            hub._publish_now('private', 'secret')
            self.assertTrue(client.closed)
            self.assertNotIn('private', hub.channels)
        asyncio.run(run())

    def test_async_write_failure_is_observed(self):
        async def run():
            hub = WebSocketHub()
            class Client:
                _ws_channels = {'x'}
                def write_message(self, message):
                    f = asyncio.get_running_loop().create_future()
                    f.set_exception(ConnectionError('closed'))
                    return f
            client = Client(); hub.subscribe('x', client)
            hub._publish_now('x', 'data')
            await asyncio.sleep(0)
            self.assertNotIn('x', hub.channels)
        asyncio.run(run())
