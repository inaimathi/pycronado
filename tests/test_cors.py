import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application, HTTPError
from src.pycronado import core


class Public(core.PublicJSONHandler):
    def get(self):
        return self.ok()


class Failure(core.PublicJSONHandler):
    def get(self):
        raise HTTPError(400)
    def write_error(self, status_code, **kwargs):
        # Applications can format errors without reimplementing CORS.
        return self.jsonerr('bad request', status_code)


class RestrictedFailure(Failure):
    origin_whitelist = ('https://ui.example',)


class CORSTests(AsyncHTTPTestCase):
    def get_app(self):
        return Application([('/public', Public), ('/failure', Failure), ('/restricted', RestrictedFailure)])

    def test_default_preflight_explicitly_allows_authorization_and_custom_headers(self):
        requested = 'authorization,content-type,idempotency-key,x-receipt-token,x-download-token'
        r = self.fetch('/public', method='OPTIONS', headers={'Origin': 'https://ui.example',
            'Access-Control-Request-Method': 'POST', 'Access-Control-Request-Headers': requested})
        self.assertEqual(r.code, 204)
        self.assertEqual(r.headers['Access-Control-Allow-Origin'], '*')
        self.assertEqual(r.headers['Access-Control-Allow-Headers'], requested)
        self.assertEqual(r.headers['Access-Control-Allow-Methods'], 'POST')
        self.assertNotIn('Access-Control-Allow-Credentials', r.headers)

    def test_default_actual_response_and_error_keep_framework_cors(self):
        for path, status in [('/public', 200), ('/failure', 400)]:
            r = self.fetch(path, headers={'Origin': 'https://ui.example'})
            self.assertEqual(r.code, status)
            self.assertEqual(r.headers['Access-Control-Allow-Origin'], '*')
            self.assertIn('Authorization', r.headers['Access-Control-Allow-Headers'])

    def test_error_reset_preserves_allowlist_and_does_not_open_denied_origins(self):
        for origin, allowed in [('https://ui.example', True), ('https://other.example', False)]:
            r = self.fetch('/restricted', headers={'Origin': origin})
            self.assertEqual(r.code, 400)
            self.assertEqual(r.headers.get('Access-Control-Allow-Origin'), origin if allowed else None)


class StartTests(unittest.IsolatedAsyncioTestCase):
    async def check_start(self, **options):
        server = SimpleNamespace(stop=Mock(), close_all_connections=AsyncMock())
        app = SimpleNamespace(listen=Mock(return_value=server))
        with patch.object(core.tornado.web, 'Application', return_value=app) as build:
            task = asyncio.create_task(core.start('test', 1234, [], **options))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        server.stop.assert_called_once()
        server.close_all_connections.assert_awaited_once()
        return app, build

    async def test_existing_start_signature_keeps_default_listen_behavior(self):
        app, _ = await self.check_start()
        app.listen.assert_called_once_with(1234)

    async def test_optional_bind_body_limit_and_access_logger_reach_server(self):
        logger = lambda handler: None
        app, build = await self.check_start(address='127.0.0.1', max_body_size=123, log_function=logger)
        app.listen.assert_called_once_with(1234, address='127.0.0.1', max_body_size=123)
        self.assertIs(build.call_args.kwargs['log_function'], logger)
