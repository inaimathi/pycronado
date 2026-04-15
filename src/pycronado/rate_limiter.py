# src/pycronado/rate_limiter.py
import asyncio
import functools
import json
import re
import time
from collections import defaultdict
from collections.abc import Mapping

import fakeredis

DEFAULT_TIER_PRIORITY = [
    "tier.power",
    "tier.premium",
    "tier.plus",
    "tier.free",
]

# Sentinel stored in Redis to represent an unlimited tier bucket.
# We avoid float("inf") because it serialises to `Infinity`, which is not
# valid JSON and breaks non-Python consumers (Redis CLI, log aggregators, etc.).
_UNLIMITED_SENTINEL = -1


def _parse_window_to_seconds(window):
    """
    Accept "1 hour", "1h", "3600s", 3600, etc.
    Returns integer seconds.
    """
    if isinstance(window, (int, float)):
        return int(window)

    if not isinstance(window, str):
        raise ValueError(f"Unsupported window spec {window!r}")

    w = window.strip().lower()

    # common shorthands
    if w in ("1h", "1hr", "1 hour", "hour", "per hour"):
        return 3600
    if w in ("1m", "1min", "1 minute", "minute", "per minute"):
        return 60
    if w in ("1d", "1day", "1 day", "day", "per day"):
        return 86400

    # generic "<num><unit>" / "<num> <unit>"
    m = re.match(r"^\s*(\d+)\s*([a-z]+)\s*$", w)
    if not m:
        raise ValueError(f"Can't parse window spec {window!r}")

    num = int(m.group(1))
    unit = m.group(2)

    if unit.startswith("s"):
        return num
    # "mo" / "month" must be checked before plain "m" (minutes)
    if unit.startswith("mo"):
        raise ValueError(
            f"Month-based windows are ambiguous; use explicit days instead "
            f"(e.g. '30 days') in window spec {window!r}"
        )
    if unit.startswith("m"):
        return num * 60
    if unit.startswith("h"):
        return num * 3600
    if unit.startswith("d"):
        return num * 86400

    raise ValueError(f"Unknown unit in window spec {window!r}")


class RateLimiter:
    """
    Per-user, per-action, per-window rate limiter with:
      - tier-based limits
      - global god bypass (for *:*), but still accounting usage
      - reporting hooks

    Internals:
      - Uses a Python-implemented token bucket per (user, action):
          capacity    = limit
          refill_rate = limit / window_seconds  (tokens per second)
        where "limit per window" is defined by your tier map and window.

      - Unlimited tiers (limit=None) always pass, but still have their
        usage counted.

    Storage backend:
      - By default uses fakeredis.FakeStrictRedis() which lives in-memory
        for this process (great for dev / simple single-instance setups).
      - In production you can pass a real redis.StrictRedis-like client.
        The logic is still pure Python; Redis is just key/value storage.

    Concurrency note:
      - The load/modify/save cycle in _increment_and_check is NOT atomic.
        Under high concurrency, two requests for the same user can race and
        both see tokens available. For strict enforcement at scale, replace
        _increment_and_check with a Redis Lua script (EVAL) so the
        check-and-decrement is atomic server-side.
    """

    def __init__(
        self,
        client=None,
        window="1 hour",
        tier_priority=None,
        org_group="coherentvolition",
        **counters,
    ):
        """
        Args:
            client:
              redis-like client (StrictRedis, fakeredis).
              Default is fakeredis.FakeStrictRedis() (in-memory).
            window:
              default window size for counters (e.g. "1 hour" or 3600).
              Interpreted as: "tier limits are per `window` seconds".
            tier_priority:
              list of tier.* abilities from highest -> lowest.
              Used to pick the "best" tier if the user somehow has multiple.
              Defaults to DEFAULT_TIER_PRIORITY.
            org_group:
              logical org/group this limiter is protecting.
              We *do* still use this to decide which 'tier.*' abilities apply.
              Admin bypass, however, is global only now.
            **counters:
              optional initial counter definitions:
                  RateLimiter(
                      window="1 hour",
                      org_group="coherentvolition",
                      audio={
                          "tier.power":   None,   # unlimited (but still counted)
                          "tier.premium": 50,
                          "tier.plus":    10,
                          "tier.free":    5,
                      }
                  )
        """
        if "max_requests" in counters or "window_seconds" in counters:
            raise TypeError(
                "RateLimiter no longer uses `max_requests`/`window_seconds`.\n"
                "Use tier-based config, e.g. RateLimiter(window='1 day', my_action={"
                "'tier.free': 5, 'tier.plus': 20, ...})."
            )
        self.client = client or fakeredis.FakeStrictRedis()
        self.default_window_seconds = _parse_window_to_seconds(window)
        self.tier_priority = tier_priority or list(DEFAULT_TIER_PRIORITY)
        self.org_group = org_group

        # action_name -> {
        #   "window_seconds": int,
        #   "tiers": { "tier.free": 5, "tier.plus": 10, ..., "tier.power": None }
        # }
        self.counters = {}
        for action_name, tier_map in counters.items():
            self.add_counter(action_name, tier_map, window=None)

    # ------------------------------------------------------------------------
    # Public configuration surface
    # ------------------------------------------------------------------------
    def add_counter(self, action_name, tier_limits, window=None):
        """
        Register or update a metered action.

        action_name: "audio", "audio_retry", etc.

        tier_limits: dict mapping tier.* -> int|None
            e.g. {
                "tier.power":   None,
                "tier.premium": 50,
                "tier.plus":    10,
                "tier.free":    5
            }
            None means unlimited for that tier (but still counted).

        window: override window just for this counter.
                Defaults to the limiter's default window.
        """
        win = (
            self.default_window_seconds
            if window is None
            else _parse_window_to_seconds(window)
        )

        if not isinstance(tier_limits, Mapping):
            raise TypeError(
                f"tier_limits for action {action_name!r} must be a mapping "
                f"of tier_name -> limit, got {type(tier_limits).__name__}: {tier_limits!r}"
            )

        self.counters[action_name] = {
            "window_seconds": win,
            "tiers": dict(tier_limits),
        }

    def limited(self, action_name):
        """
        Decorator that enforces per-user quota for `action_name`.

        Works with both synchronous and asynchronous handler methods.

        Usage in a handler:

            L = RateLimiter(..., audio={...})
            class SpeakHandler(core.JSONHandler):
                @core.requires("text", permissions="audio")
                @L.limited("audio")
                def post(self, text):
                    ...

        Or with async handlers:

            class SomeHandler(core.JSONHandler, core.NDJSONMixin):
                @L.limited("job_audit")
                async def post(self):
                    ...

        Runtime behavior:
        - Always records usage for this user/action.
        - Checks a token bucket for their tier:
            * capacity     = tier_limit
            * refill_rate  = tier_limit / window_seconds
        - If they're over limit (bucket empty):
            - If they're a global god (*:*), ALLOW (but we still counted).
            - Otherwise jsonerr("rate limit exceeded", 429) and stop.
        """

        def decorator(method):
            def _enforce(handler_self):
                """
                Shared prelude: check quota, return True if the request should
                be blocked (i.e. caller should return the jsonerr result).
                """
                admin = self._is_global_admin(handler_self)
                tier_ability = self._get_highest_tier(handler_self)
                user_id = handler_self.user_id()
                allowed = self.check_and_increment(user_id, tier_ability, action_name)

                if not allowed and not admin:
                    return True  # blocked
                return False  # allowed

            if asyncio.iscoroutinefunction(method):

                @functools.wraps(method)
                async def async_wrapper(handler_self, *args, **kwargs):
                    if _enforce(handler_self):
                        return handler_self.jsonerr("rate limit exceeded", 429)
                    return await method(handler_self, *args, **kwargs)

                return async_wrapper

            else:

                @functools.wraps(method)
                def sync_wrapper(handler_self, *args, **kwargs):
                    if _enforce(handler_self):
                        return handler_self.jsonerr("rate limit exceeded", 429)
                    return method(handler_self, *args, **kwargs)

                return sync_wrapper

        return decorator

    # ------------------------------------------------------------------------
    # Internal helpers used by the decorator / counters
    # ------------------------------------------------------------------------

    def _redis_key(self, user_id, action_name):
        """
        Unique storage key for (user, action).

        IMPORTANT: user_id includes colons (issuer::username), so we cannot
        safely split on ":" later. We use '|' as a field separator.

        Format (token-bucket mode):
            rate|<user_id>|<action_name>
        """
        return f"rate|{user_id}|{action_name}"

    # -- token bucket state helpers -----------------------------------------

    def _load_bucket_state(self, key):
        """
        Load a token-bucket state dict from the backing store.

        Returns:
            dict(tokens: float, last_update: float, used: int) or None
        """
        try:
            raw = self.client.get(key)
        except Exception:
            return None

        if raw is None:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")

        try:
            state = json.loads(raw)
        except Exception:
            return None

        if not isinstance(state, dict):
            return None

        return state

    def _save_bucket_state(self, key, state, window_seconds):
        """
        Persist the given token-bucket state dict to the backing store,
        with a best-effort TTL to eventually clean up idle users.
        """
        # Let the key live for roughly 2 windows of inactivity.
        ttl = max(int(window_seconds * 2), 60)

        try:
            payload = json.dumps(state, separators=(",", ":"))
            self.client.set(key, payload, ex=ttl)
        except Exception:
            # If Redis/fakeredis is unavailable, we just drop state changes.
            # Rate limiting may become more permissive, but we won't crash.
            pass

    def _increment_and_check(self, key, window_seconds, limit):
        """
        Token-bucket based accounting for a single (user, action) key.

        Semantics:
          - For finite limits:
              * capacity    = limit
              * refill_rate = limit / window_seconds (tokens/sec)
              * This method:
                  - refills the bucket based on elapsed time
                  - consumes 1 token if available
                  - increments 'used' (total calls)
              * Returns True if the token was acquired, False otherwise.
          - For unlimited (limit is None):
              * Always allowed.
              * 'used' is still incremented.

        Note: this read-modify-write is not atomic. See class docstring.
        """
        now = time.time()

        # Unlimited tier: always allowed, just count usage.
        if limit is None:
            state = self._load_bucket_state(key) or {}
            used = int(state.get("used", 0)) + 1
            state.update(
                {
                    "tokens": _UNLIMITED_SENTINEL,
                    "last_update": now,
                    "used": used,
                }
            )
            self._save_bucket_state(key, state, window_seconds)
            return True

        # Finite limit → real token bucket.
        capacity = int(limit)

        # "No usage" tier: always disallow but still count.
        if capacity <= 0:
            state = self._load_bucket_state(key) or {}
            used = int(state.get("used", 0)) + 1
            state.update(
                {
                    "tokens": 0.0,
                    "last_update": now,
                    "used": used,
                }
            )
            self._save_bucket_state(key, state, window_seconds)
            return False

        # Load existing state or initialize a fresh bucket.
        state = self._load_bucket_state(key) or {}
        raw_tokens = state.get("tokens", capacity)
        # Treat the unlimited sentinel as a full bucket if the tier changed.
        tokens = float(capacity if raw_tokens == _UNLIMITED_SENTINEL else raw_tokens)
        last_update = float(state.get("last_update", now))
        used = int(state.get("used", 0))

        # Refill tokens based on elapsed time.
        delta = max(0.0, now - last_update)
        rate = float(capacity) / float(window_seconds)  # tokens per second
        tokens = min(float(capacity), tokens + rate * delta)

        # Try to acquire a token.
        allowed = tokens >= 1.0
        if allowed:
            tokens -= 1.0

        used += 1

        new_state = {
            "tokens": tokens,
            "last_update": now,
            "used": used,
        }
        self._save_bucket_state(key, new_state, window_seconds)

        return allowed

    def _is_global_admin(self, handler_self):
        """
        Returns True if this user has the global "*:*" ability.

        A global god is defined as any permission row with:
            user_group == "*" and group_ability == "*"

        Global gods can exceed limits (no 429),
        but we STILL count their usage.
        """
        if not hasattr(handler_self, "permissions") or not callable(
            handler_self.permissions
        ):
            return False

        for perm in handler_self.permissions():
            p_group = perm.get("user_group")
            p_ability = perm.get("group_ability")

            if p_group == "*" and p_ability == "*":
                return True

        return False

    def _get_highest_tier(self, handler_self):
        """
        Return the user's highest tier.* ability (e.g. 'tier.premium'),
        considering ONLY permissions for this limiter's org_group.

        If they have multiple tiers (unlikely but could happen briefly during upgrade),
        we return the first one in self.tier_priority.

        If we can't find any tier.* at all for this org_group,
        we fall back to the lowest priority, typically 'tier.free'.
        """
        perms = (
            handler_self.permissions() if hasattr(handler_self, "permissions") else []
        )

        tier_abilities = [
            p.get("group_ability")
            for p in perms
            if (
                p.get("user_group") == self.org_group
                and isinstance(p.get("group_ability"), str)
                and p["group_ability"].startswith("tier.")
            )
        ]

        for tier_name in self.tier_priority:
            if tier_name in tier_abilities:
                return tier_name

        return self.tier_priority[-1]  # fallback, expected "tier.free"

    def check_and_increment(self, user_id, tier_ability, action_name):
        """
        Record usage for this (user, action_name) and return True if usage
        is still within their tier's quota.

        Uses a token-bucket internally:

          - limit = tier-specific limit (calls per `window_seconds`)
          - capacity = limit
          - refill_rate = limit / window_seconds (tokens/sec)

        If this action_name was never registered with add_counter(), it's not metered:
        we treat it as always allowed and we do NOT create counters for it.
        """
        cfg = self.counters.get(action_name)
        if cfg is None:
            return True  # unmetered

        window_seconds = cfg["window_seconds"]
        tier_limits = cfg["tiers"]

        # Find their limit. If their exact tier isn't present, fallback to tier.free.
        limit = tier_limits.get(
            tier_ability,
            tier_limits.get("tier.free", None),
        )

        key = self._redis_key(user_id, action_name)
        return self._increment_and_check(key, window_seconds, limit)

    # ------------------------------------------------------------------------
    # Reporting / introspection
    # ------------------------------------------------------------------------

    def report(self):
        """
        Return a nested dict summarizing current usage counts
        for all live buckets in the backing store.

        Shape:
            {
              "<action_name>": {
                 "<user_id>": <total_used>,
                 ...
              },
              ...
            }

        Notes:
        - Under the token-bucket scheme, we aggregate the "used" field for
          each (user, action) key:
              rate|<user_id>|<action_name>
        - This is *not* a strict billing ledger. It's an ops/debug snapshot.
        - Uses SCAN rather than KEYS to avoid blocking Redis on large keyspaces.
        """
        summary = defaultdict(lambda: defaultdict(int))

        try:
            # SCAN iterates incrementally; safe on large keyspaces unlike KEYS.
            cursor = 0
            while True:
                cursor, keys = self.client.scan(cursor, match="rate|*", count=100)
                for raw_key in keys:
                    if isinstance(raw_key, bytes):
                        raw_key = raw_key.decode("utf-8", "replace")

                    # Expect "rate|<user_id>|<action_name>"
                    parts = raw_key.split("|", 2)
                    if len(parts) != 3:
                        continue

                    _prefix, user_id, action_name = parts

                    state = self._load_bucket_state(raw_key)
                    if not state:
                        continue

                    used_val = state.get("used")
                    try:
                        count = int(used_val)
                    except (TypeError, ValueError):
                        continue

                    summary[action_name][user_id] += count

                if cursor == 0:
                    break

        except Exception:
            pass

        # convert defaultdicts back to plain dicts for cleanliness
        return {action: dict(users) for action, users in summary.items()}
