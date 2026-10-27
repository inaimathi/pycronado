"""Atomic token buckets. Local locks never span an await or network I/O.

MemoryBucketStore is process-local (including its reports); use RedisBucketStore
for shared enforcement. Redis uses server time and a single Lua operation.
Only synchronous redis-py clients are accepted; async handlers offload Redis I/O.
"""
import json
import threading
import time


class BucketUnavailable(RuntimeError):
    pass


def advance(state, now, window, limit):
    state = state or {}
    used = int(state.get("used", 0)) + 1
    if limit is None:
        tokens, allowed = -1, True
    elif limit == 0:
        tokens, allowed = 0, False
    else:
        prior = state.get("tokens", limit)
        tokens = min(limit, (limit if prior == -1 else float(prior)) +
                     max(0, now - state.get("last_update", now)) * limit / window)
        allowed = tokens >= 1
        if allowed:
            tokens -= 1
    return {"tokens": tokens, "last_update": now, "used": used}, allowed


class MemoryBucketStore:
    """Thread-safe, coroutine-safe bounded storage; no await inside its lock.

    Sharing one store between limiter instances shares counters. Idle entries
    expire; live entries are never evicted to make room (which would reset quota).
    """
    def __init__(self, max_entries=100000, clock=time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._entries = {}
        self.max_entries = max_entries
        self._next_cleanup = 0

    def consume(self, key, window, limit):
        with self._lock:
            now = self._clock()
            if now >= self._next_cleanup or len(self._entries) >= self.max_entries:
                self._entries = {k: v for k, v in self._entries.items() if v[1] > now}
                self._next_cleanup = now + 60
            entry = self._entries.get(key)
            state = entry[0] if entry and entry[1] > now else None
            if state is None and len(self._entries) >= self.max_entries:
                raise BucketUnavailable("Local bucket capacity exhausted")
            state, allowed = advance(state, now, window, limit)
            self._entries[key] = (state, now + max(window * 2, 60))
            return allowed

    def snapshot(self):
        with self._lock:
            now = self._clock()
            return {k: dict(v[0]) for k, v in self._entries.items() if v[1] > now}


_CONSUME = r'''
local raw = redis.call('GET', KEYS[1])
local state = raw and cjson.decode(raw) or {}
local ts = redis.call('TIME')
local now = tonumber(ts[1]) + tonumber(ts[2]) / 1000000
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local used = (state.used or 0) + 1
local tokens, allowed
if limit == -1 then
  tokens, allowed = -1, 1
elseif limit == 0 then
  tokens, allowed = 0, 0
else
  local prior = state.tokens or limit
  if prior == -1 then prior = limit end
  tokens = math.min(limit, prior + math.max(0, now - (state.last_update or now)) * limit / window)
  allowed = tokens >= 1 and 1 or 0
  if allowed == 1 then tokens = tokens - 1 end
end
redis.call('SET', KEYS[1], cjson.encode({tokens=tokens,last_update=now,used=used}), 'EX', math.max(math.ceil(window*2),60))
return allowed
'''


class RedisBucketStore:
    def __init__(self, client):
        import inspect
        if inspect.iscoroutinefunction(client.eval):
            raise TypeError("Use a synchronous redis-py client; async handlers offload I/O")
        self.client = client

    def consume(self, key, window, limit):
        try:
            return bool(self.client.eval(_CONSUME, 1, key, window, -1 if limit is None else limit))
        except Exception as exc:
            raise BucketUnavailable("Redis bucket unavailable") from exc

    def snapshot(self):
        try:
            result = {}
            for key in self.client.scan_iter(match="rate|*", count=100):
                raw = self.client.get(key)
                if raw is not None:
                    name = key.decode() if isinstance(key, bytes) else key
                    result[name] = json.loads(raw)
            return result
        except Exception as exc:
            raise BucketUnavailable("Redis reporting unavailable") from exc
