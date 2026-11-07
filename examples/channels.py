"""Local public demo: pip install -e '.[webrtc]'; python examples/channels.py.

Open http://127.0.0.1:4000/ and connect two tabs using different transports.
This example intentionally permits anonymous commands. Production services should
use the authenticated classes and authorize rooms and every application command.
"""
import asyncio
from pathlib import Path

from pycronado import core
from pycronado.websocket import PublicChannelSocketHandler
from pycronado.sse import PublicChannelSSEHandler
from pycronado.webrtc import PublicChannelPeer, PublicChannelOfferHandler


class Commands:
    def can_publish(self, channel):
        return channel == 'demo'

    def on_channel_message(self, channel, data):
        self._hub().publish(channel, {'type': 'command', 'data': data})


class Socket(Commands, PublicChannelSocketHandler):
    pass


class Peer(Commands, PublicChannelPeer):
    def on_state_message(self, channel, data):
        self.manager.publish_state(channel, data)


class Offer(PublicChannelOfferHandler):
    peer_class = Peer


class Index(core.PublicJSONHandler):
    async def get(self):
        await self.afile(Path(__file__).with_name('channels.html'))


if __name__ == '__main__':
    asyncio.run(core.start('channels-demo', 4000,
        [('/', Index), ('/ws', Socket), ('/events', PublicChannelSSEHandler), ('/offer', Offer)],
        websocket=True, sse=True, webrtc=True, address='127.0.0.1', max_body_size=128 * 1024))
