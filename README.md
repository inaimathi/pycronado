# Pycronado

A small Python 3.11+ framework around Tornado for JSON microservices, JWT
identities, permissions, rate limiting, NDJSON streaming, and WebSocket pub/sub.
Version 1.1 adds a shared channel hub, SSE subscriptions, and optional WebRTC
data channels while preserving the existing WebSocket imports.

## Install and run

```sh
pip install pycronado
# Optional shared rate-limit storage:
pip install 'pycronado[redis]'
```

```python
import asyncio
from pycronado import core

class Hello(core.PublicJSONHandler):
    def get(self):
        self.ok(message="hello", name=self.param("name", "world"))

asyncio.run(core.start("example", 4000, [(r"/hello", Hello)]))
```

`start()` serves until cancelled and shuts down its HTTP server, channel hub,
and WebRTC peers. Optional arguments include `address`, `max_body_size`, `debug`, static file
settings, and `log_function`. Set a request body limit appropriate to the service;
`file_param()` checks an uploaded file only after Tornado has buffered the request.
For full control, construct a Tornado `Application` directly using these handlers.

## HTTP handlers and parameters

- `PublicJSONHandler`: no authentication requirement.
- `JSONHandler`: requires a valid bearer JWT, except for OPTIONS preflight.
- `param(name, default=None)`: JSON object body, legacy form field `json`, then
  ordinary form/query parameters. A body key (even null) takes precedence.
  Malformed JSON or a non-object JSON payload produces HTTP 400.
- `file_param(name, default=None, max_filesize=...)`: first uploaded file's bytes;
  oversized uploads raise HTTP 413 and stop normal handler execution.
- `json(data, status=None)`, `ok(**fields)`, and `jsonerr(message, status=500)`:
  JSON responses. `jsonerr()` finishes the response; callers must return afterward.
- `get_json_serializer()` / `json_serializer`: customize serialization of objects
  not handled by JSON. Dates and times become ISO strings by default.

`origin_whitelist=None` allows non-credentialed CORS from all origins. Set an
explicit sequence of HTTP(S) origins, or override `get_origin_whitelist()`, to
restrict browser access. Allowed explicit origins receive credentialed CORS.
CORS does not replace authentication or CSRF protection for cookie-based apps.
Use HTTPS at your reverse proxy in production.

## Authentication and permissions

Configure `JWT_PUBKEY` and `JWT_ISSUER` before importing the package. Optional
`JWT_AUDIENCE` enables audience validation. The verifier accepts RS256 only,
requires `exp` and `iat`, and rejects special-purpose tokens by default.
A handler can override `jwt_purposes` for a narrowly scoped flow.
Issuer/audience policy and startup validation remain the application's choice.

The usual user claim looks like:

```json
{"user":{"username":"alice","permissions":[{"user_group":"team-a","group_ability":"schedule"}]}}
```

```python
class ReadSchedule(core.JSONHandler):
    @core.requires("team", permissions="schedule")
    async def get(self, team):
        # Explicit group check when the operation targets a caller-selected team.
        if not self.has_permission("schedule", group=team):
            return self.jsonerr("forbidden", 403)
        self.ok(team=team)
```

`requires()` injects required parameters as keyword arguments. A list of
permissions means **any** of them, not all. Its permission checks are intentionally
unscoped: an ability in any group satisfies the decorator. `has_permission()`
accepts a group for resource-specific checks and supports `*` wildcards.
`user()`, `username()`, `issuer()`, `permissions()`, and `user_id()` expose identity.
These permission semantics have not changed.

A future backwards-compatible extension could add optional `group=` (a value or
resolver evaluated after required parameters) and `permission_match="any"|"all"`
to `requires()`. A failed resolver should deny access, not fall back to an
unscoped check. These options are recommendations, not current APIs.

Services issuing JWTs can use `token.ensureKeysFrom(file_path=...)` and
`token.encode()`. A new key is fully written and fsynced before it is atomically
published without replacing an existing identity. Corrupt/unreadable existing
keys fail startup. The destination filesystem must support same-directory hard
links. `hasValidSig()` is not an authentication or authorization check.

## WebSocket channels

```python
from pycronado.websocket import ChannelSocketHandler

class TeamSocket(ChannelSocketHandler):
    origin_whitelist = ("https://app.example.com",)

    def can_subscribe(self, channel):
        # The default permits every channel. Override for private rooms.
        return self.has_permission("schedule", group=channel)

asyncio.run(core.start("example", 4000, [(r"/ws", TeamSocket)], websocket=True))
```

`BaseChannelSocketHandler` provides channel commands; `PublicChannelSocketHandler`
allows anonymous connections; `ChannelSocketHandler` authenticates connections.
Subscription defaults remain permissive for all three. Override `can_connect()`
and `can_subscribe(channel)` as needed, keeping authenticated superclass checks
when overriding `can_connect()` on `ChannelSocketHandler`.

The initial subscription comes from the route capture or `?channel=...`, with
`default` as the fallback. Successful connection emits:

```json
{"type":"ready","channels":["default"]}
```

A rejected initial subscription emits an error followed by `ready` with an empty
list. Client commands:

```json
{"action":"subscribe","channel":"team-a"}
{"action":"unsubscribe","channel":"team-a"}
{"action":"ping"}
```

Subscriptions acknowledge with `subscribed`/`unsubscribed`; ping replies `pong`.
The stock handler does not republish arbitrary client messages. Application
commands require `can_publish(channel)` and an `on_channel_message()` override. The shared session rate budget covers all incoming messages, including malformed
ones. Use the application command hooks below instead of replacing `on_message()`
when possible.

`self.application.channel_hub.publish(channel, message)` is the preferred publisher;
`ws_hub` remains an alias.
`publish_ws(channel, message)` remains a single-application convenience, using
the most recently registered global hub. Do not use the global helper to choose
between multiple apps; use their explicit hubs. Starting a non-WebSocket app no
longer clears another app's global hub. A stopping app only clears its own global
registration. Hubs and subscriptions are process-local; a multi-process service
needs an application-level broker if events must reach other workers.

`publish()` and `broadcast()` are thread-safe and return `False` if the hub is
closed or its ingress queue is full (`publish()` also rejects invalid channels).
Handle that result according to the event's importance: retry/backoff, reject
work, or explicitly drop replaceable state. Enqueuing is not delivery confirmation.
Do not mutate message objects after publishing. Other hub methods run on the
owning IOLoop. `broadcast()` intentionally reaches all subscribed clients,
regardless of channel, and deduplicates clients subscribed to multiple channels.

### Limits and slow consumers

Pass `websocket_hub_options={...}` to `start()` or keywords to `WebSocketHub()`:

| Option | Default | Meaning |
| --- | ---: | --- |
| `max_subscriptions` | 128 | Distinct subscriptions per client |
| `max_channel_length` | 256 | Characters per channel name |
| `max_pending_messages` | 256 | Per-client messages, including the active write |
| `max_pending_bytes` | 1048576 | Per-client UTF-8 payload bytes, including the active write |
| `max_publications` | 1024 | Queued publish/broadcast calls awaiting fan-out |
| `max_clients` | 10000 | Registered connections, including unsubscribed connections |
| `write_timeout` | 30 | Seconds allowed for an outstanding write |

All limits must be positive. The ingress limit counts message objects, not their
in-memory size; bound application-generated payloads as well. There is one active
write per client, with FIFO delivery and independent queues. Control responses
use the same limits. Overflow/timeouts close the slow client with code 1013 and
remove its subscriptions. Write failures close with 1011; shutdown uses 1001.
The common JSON codec is reused within a publication; custom codecs remain
client-specific. Serialization errors are logged without unsubscribing recipients.
Use handler `write_json()` for bounded control messages; direct `write_message()`
calls bypass the hub queue.

Handler attributes `message_rate=100` and `message_burst=200` configure the inbound
token bucket; exceeding it closes with code 1008. Tune these for game traffic.
`start()` also exposes `websocket_max_message_size` (1 MiB),
`websocket_ping_interval` (30 seconds), and `websocket_ping_timeout` (30 seconds).
If constructing your own Application, pass the corresponding Tornado settings.

JWT sources are Authorization bearer header, the `jwt` query parameter, then an
optional cookie named by `ws_jwt_cookie_name`. Set `ws_jwt_query_param=None` to
disable query authentication. Tokens expire on a timer and are checked again
before hub sends. WebSockets use origin checks rather than CORS: by default
Tornado's same-origin policy applies; override `origin_whitelist` for other sites.
Origin checks do not authenticate non-browser clients. Cookie-based cross-origin
access requires careful policy; avoid broad allowlists.

## NDJSON streaming

```python
class Events(core.JSONHandler, core.NDJSONMixin):
    async def get(self):
        async def events():
            for number in range(10):
                yield {"number": number}
                await asyncio.sleep(0.1)
        await self.andjson_pump(events())
```

Use `andjson_start()`, `andjson()`, and `andjson_end()` for manual control, or
`andjson_pump()` for an async iterator. Each write awaits flushing. Disconnects
stop the pump, task cancellation propagates, and pumps close their owned iterator
on exit. Pass `close_iterator=False` only when the caller owns that lifecycle.
Standalone write helpers propagate disconnect errors to their caller. When using
these pumps with PublicJSONHandler/JSONHandler, disconnects also cancel a pump
waiting for its next item. Subclasses overriding `on_connection_close()` must
call `super()`. Custom non-Pycronado handlers must provide their own disconnect
cancellation hook.

An unexpected producer error is logged and emits a generic error event; an
optional synchronous `on_error(exception)` callback can format the event. A
background job that must survive disconnects should run independently and feed
subscriber-specific iterators. Cancellation does not send a final event.

Legacy `ndjson_*` methods support small synchronous streams, but cannot yield
while waiting for network backpressure. Outstanding output is bounded by
`ndjson_max_pending_bytes` (1 MiB); direct writes raise `BufferError` on overflow,
while the pump logs the error and ends the stream. Use async streaming for large
or long-running output. Raw string events must already be valid single-line JSON.

## Occasional file responses

Prefer `await self.afile(path, filename="download.txt")` or
`await self.afilebytes(data, extension="txt")`. Disk operations are offloaded and
network writes await flushing. Unknown MIME types use `application/octet-stream`.
Files must be trusted application-selected paths and remain unchanged during
transmission; these helpers are not a sandbox for user-supplied paths.

Legacy `file()` and `filebytes()` now schedule an async response and return its
task. Existing synchronous handlers can call them without awaiting; they keep
that response open until transmission finishes. Return after calling them and
do not write a second response. They no longer complete the response inline.
`filebytes()` still requires its input bytes to be resident in memory.

## Logging

`self.logger.info(...)` and legacy `self.logger().info(...)` use a stable logger
name derived from the handler class. Each message includes `[METHOD /path]`, so
specific routes remain visible without creating a permanent logger per URL.
Framework access/exception logs omit query strings and whole-request dumps.
Invalid JWTs are logged at DEBUG without exception tracebacks.

Do not put credentials in URL paths or explicitly log tokens, request bodies,
headers, or credential-bearing exception messages. Custom access loggers, Tornado
handlers not inheriting these mixins, reverse proxies, and application logging
remain responsible for their own redaction. Query tokens are retained for browser
compatibility; short-lived connection tickets are preferable when available.

## Rate limiting

```python
from pycronado.rate_limiter import RateLimiter

limiter = RateLimiter(window="1 hour", submit={"tier.free": 10, "tier.plus": 100})

class Submit(core.JSONHandler):
    @limiter.limited("submit")
    async def post(self):
        self.ok()
```

The default backend is a bounded, thread-safe process-local token bucket. Redis
can provide shared enforcement via `RateLimiter(client=redis_client, ...)`.
Use a synchronous redis-py client; async decorated handlers offload Redis calls.
The legacy synchronous decorated method still performs synchronous Redis I/O:
prefer `async def` handlers when using Redis to avoid blocking the event loop.
Backend failures deny work with 503; exhausted quota returns 429. Global `*/*`
permissions bypass quota denial but still count attempts. Unregistered actions
remain intentionally unmetered. `report()` is a live snapshot, not a billing log.

Memory entries expire after inactivity; cleanup runs at most once per minute.
When full, a store may reject new identities until the next sweep instead of
rescanning its entire table on every request. Existing live identities keep
working; live entries are never evicted to make room.

## Development

```sh
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -v
python -m pip install build
python -m build
```

Tests cover real HTTP/WebSocket/SSE connections, auth/CORS, stream cleanup, slow
consumers, queue limits, logging redaction, rate buckets, and atomic key writes.
For the full suite including real loopback ICE/DTLS/SCTP connections:

```sh
python -m pip install -e '.[test,webrtc]'
python -m unittest discover -s tests -v
```

Without aiortc, WebRTC integration tests are explicitly skipped. The local tests
use actual UDP on loopback without STUN/TURN, and do not establish browser or
Internet/NAT interoperability. CI installs the optional transport for these tests.


## Shared channels (1.1)

```python
from pycronado.channel import ChannelHub, ChannelEvent, publish
from pycronado.sse import ChannelSSEHandler
from pycronado.webrtc import ChannelOfferHandler

# Pass these flags alongside your existing routes in core.start(...):
# websocket=True, sse=True, webrtc=True,
# channel_hub_options={"max_subscriptions": 128}
```

Flags enable one shared hub; they do not install routes automatically. Route the
handler classes explicitly. `websocket_hub_options` is a backwards-compatible
alias for `channel_hub_options`; passing both raises ValueError. `webrtc_options`
is forwarded to `WebRTCManager`. The `webrtc` flag is the only startup option
that loads the optional aiortc dependency.

| Transport | Base | Anonymous | Authenticated |
| --- | --- | --- | --- |
| WebSocket | `BaseChannelSocketHandler` | `PublicChannelSocketHandler` | `ChannelSocketHandler` |
| SSE | `BaseChannelSSEHandler` | `PublicChannelSSEHandler` | `ChannelSSEHandler` |
| WebRTC peer | `BaseChannelPeer` | `PublicChannelPeer` | `ChannelPeer` |
| WebRTC signaling | `BaseChannelOfferHandler` | `PublicChannelOfferHandler` | `ChannelOfferHandler` |

`WebSocketHub` is an alias for `ChannelHub`. `publish_ws`, `get_ws_hub`, and
`set_ws_hub` alias the shared helpers. A publication now reaches every transport
subscribed to that logical channel. Existing WebSocket payloads and
subscribe/unsubscribe/ping commands are unchanged. Subscription defaults remain
permissive and can be overridden with `can_subscribe(channel)`.

### Client commands

WebSocket and WebRTC control channels accept:

```json
{"action":"message","channel":"room-123","data":{"action":"fire","tick":123}}
```

The client must already be subscribed and `can_publish(channel)` must allow the
command. This hook defaults to **False**, separately from permissive subscription
policy. Implement `on_channel_message(channel, data)` (sync or async) to validate
and apply commands; nothing is automatically republished.

```python
class GameSocket(ChannelSocketHandler):
    def can_publish(self, channel):
        return self.has_permission("play", group=channel)

    async def on_channel_message(self, channel, data):
        # Validate schema, authority and game rules before updating state.
        result = await game.apply_command(self.username(), channel, data)
        self._hub().publish(channel, result)
```

The same hooks work on WebRTC peers. There is one async command worker per
session, bounded by `max_incoming_messages=128` and `max_incoming_bytes=1048576`,
including the in-flight command. Overflow closes the client. Commands are handled
serially; closing a session cancels its active callback. Use independent jobs for
work that must outlive a connection. Hooks run on the event loop; offload blocking
work yourself. Authorization and expiry are rechecked when queued commands run.

## Server-Sent Events

```python
from pycronado.sse import PublicChannelSSEHandler, ChannelSSEHandler

# Routes for core.start(..., sse=True):
routes = [(r"/events", ChannelSSEHandler)]
```

Use GET `/events?channel=room-123`; repeated `channel` parameters subscribe to
multiple logical channels within the hub limit. A route capture also selects one
channel. Subscription/authentication failures return HTTP errors before streaming
starts. This is one-way communication; reconnect with new parameters to change
subscriptions. Application writes use a separate authorized HTTP endpoint or a
bidirectional transport.

Messages are UTF-8 SSE `data:` frames. Every payload line is framed independently,
so embedded newlines cannot inject SSE fields. Standard headers prevent caching
and request that proxies disable buffering. Named `heartbeat` events every 15
seconds keep idle connections active (`heartbeat_interval` is overrideable).
Slow-consumer limits, disconnect cleanup and JWT expiry use the shared hub.

```python
from pycronado.channel import ChannelEvent
hub.publish("room-123", {"score": 3})
hub.publish("room-123", ChannelEvent({"score": 4}, event="score", id="revision-4", retry=3000))
```

`ChannelEvent` adds optional SSE event name, ID, and retry milliseconds; WebSocket
and WebRTC recipients receive only its `data`, with their usual serialization.
Metadata cannot contain newline/NUL characters. SSE `Last-Event-ID` is exposed as
`handler.last_event_id` before session start. **There is no replay store or
exactly-once delivery.** Reconnecting subscriptions see current live events and a
new `ready` response, not missed events. Reconcile with an application snapshot or
implement durable cursors separately; supplying an ID alone does not add replay.

Native browser `EventSource` reconnects automatically and does not expose custom
Authorization headers. Public example:

```javascript
const events = new EventSource("/events?channel=room-123");
events.onmessage = event => console.log(JSON.parse(event.data));
events.addEventListener("score", event => console.log(event.lastEventId, event.data));
// events.close() stops reconnecting.
```

For protected streams, use authenticated fetch streaming, or explicitly configure
`sse_jwt_cookie_name` and an appropriate origin policy. `sse_jwt_query_param` is
None by default; enable it only deliberately, preferably for short-lived
connection tickets. Reverse proxies must redact credential-bearing URLs.

## WebRTC data channels

```sh
pip install 'pycronado[webrtc]'
```

The optional dependency is aiortc 1.16.0. Core HTTP, WebSocket and SSE support does
not import or require it. WebRTC is a browser-to-authoritative-server transport;
this release does not provide browser-to-browser matchmaking or media streaming.

```python
from pycronado.webrtc import ChannelPeer, ChannelOfferHandler

class GamePeer(ChannelPeer):
    def can_subscribe(self, channel):
        return self.has_permission("play", group=channel)

    def can_publish(self, channel):
        return self.has_permission("play", group=channel)

    async def on_channel_message(self, channel, data):
        await game.command(self.username(), channel, data)

    def on_state_message(self, channel, data):
        game.accept_snapshot(self.username(), channel, data)

class Offer(ChannelOfferHandler):
    peer_class = GamePeer

# core.start(..., routes=[(r"/rtc/offer", Offer)], webrtc=True)
```

POST `{type: "offer", sdp: "...", channel: "room-123"}` to the offer route using
an Authorization bearer header. Identity is verified before SDP parsing and peer
allocation, copied into the persistent peer, and remains bound until expiry.
Cookies/query tokens are not used by stock signaling. Server hooks can provide
additional immutable context via `get_peer_context()`; peers never retain the
whole HTTP handler/request. `WebRTCManager.accept(claims=...)` is a trusted
in-process API: custom signaling must verify those claims itself.

The client must create the data channels before its offer and **wait for ICE
gathering to complete** before posting SDP. The response is `{type: "answer",
sdp: "..."}`, also complete. There is no trickle-ICE endpoint, renegotiation, or
ICE restart API in this release; reconnect with a fresh peer/offer when needed.

| Data channel | Required configuration | Purpose |
| --- | --- | --- |
| `control` | `protocol: "pycronado.v1"`, ordered and reliable (defaults) | Subscriptions, pings, reliable publications and commands |
| `state` (optional) | `protocol: "pycronado.v1"`, `ordered: false`, `maxRetransmits: 0` | Replaceable snapshots/input with no retransmission |

Duplicate, unexpected, or incorrectly configured data channels close the peer.
There is no media negotiation; SDP must contain exactly one data section and
actual gathered candidates. An abandoned negotiation or missing control channel
has a deadline and releases its resources.

Reliable output uses `hub.publish(...)`. WebRTC's default maximum message size is
16 KiB and the transport buffer cap is 256 KiB, configurable as peer attributes
`max_message_size` and `max_transport_buffer_bytes`. Oversized reliable output
closes the peer; split application messages when needed. A drained transport
buffer is not a remote application acknowledgement.

Use `peer.send_state(channel, data)` or
`app.webrtc_manager.publish_state(channel, data)` for best-effort output. These
are **event-loop-only** methods; schedule them with the IOLoop when publishing from
workers. The manager returns the number of WebRTC peers accepting the snapshot;
individual sends return False if unavailable, oversized or congested. This path
sends nothing to WebSocket/SSE subscribers and never silently changes reliability.
It sends `{channel, data}` on `state`; inbound state messages use the same
`{action: "message", channel, data}` envelope as commands and dispatch to
`on_state_message()`. State packets cannot change subscriptions.

Include sequence/tick numbers and reject obsolete state in application code.
The transport does not implement prediction, interpolation, reconciliation,
authoritative game logic, coalescing, or reliable event deduplication.

### Deployment and limits

`webrtc_options` accepts:

| Option | Default |
| --- | ---: |
| `max_peers` | 256 |
| `max_pending_offers` | 32 |
| `offer_timeout` | 15 seconds |
| `connection_timeout` | 30 seconds from admission |
| `max_sdp_bytes` | 65536 |
| `offers_per_minute` | 20 per remote IP (token bucket) |
| `total_offers_per_minute` | 300 per manager (token bucket) |
| `ice_servers` | Empty list; host candidates only |

These budgets are process-local and must be tuned for the service. The signaling
body is bounded after Tornado receives it, so also set HTTP `max_body_size` (for
example 128 KiB) and edge rate limits. Behind proxies, configure trusted client-IP
handling when building your HTTP server or tune the shared proxy-IP budget.

ICE/STUN/TURN must match the deployment. Supply operator-configured `ice_servers`
using aiortc `RTCIceServer` objects or dictionaries with `urls`, `username`, and
`credential`; the request body cannot choose server ICE credentials/configuration.
Browser ICE servers are configured separately, ideally with short-lived TURN
credentials. Candidate handling makes outbound network traffic: isolate the
service and apply an appropriate egress policy for untrusted public signaling.
HTTP reverse proxying alone does not route UDP data channels. Plan firewall,
container networking, reachable candidate addresses and TURN capacity explicitly.
TURN may relay traffic; WebRTC does not guarantee higher throughput than WebSocket.

On application shutdown, hub sessions close and the manager awaits peer cleanup.
When constructing applications manually, attach `app.channel_hub` and
`app.webrtc_manager`, close the hub, then `await manager.aclose()` during shutdown.

### Runnable example

```sh
pip install -e '.[webrtc]'
python examples/channels.py
```

Open `http://127.0.0.1:4000/` in two tabs. Choose WebSocket, SSE or WebRTC; reliable
commands reach every subscribed tab, while snapshots reach only WebRTC tabs.
The example is deliberately anonymous and binds only to loopback. It contains
complete browser offer/answer and ICE-gathering code in `examples/channels.html`.

Protocol references: [aiortc API](https://aiortc.readthedocs.io/en/latest/api.html),
[WebRTC data channels (RFC 8831)](https://www.rfc-editor.org/rfc/rfc8831.html), and
[HTML Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html).
