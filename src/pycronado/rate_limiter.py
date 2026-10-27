# src/pycronado/rate_limiter.py
import asyncio
import functools
import re
from collections import defaultdict
from collections.abc import Mapping

from .buckets import BucketUnavailable, MemoryBucketStore, RedisBucketStore

DEFAULT_TIER_PRIORITY = [
    "tier.power",
    "tier.premium",
    "tier.plus",
    "tier.free",
]

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
    """Tier-based quotas with atomic local buckets by default.

    Pass client=<synchronous redis-py client> for shared Redis/Lua buckets.
    Local buckets are process-local and reset on restart. Storage failures deny
    work with 503; exhausted buckets return 429. Unregistered actions remain
    intentionally unmetered. has_permission semantics are unchanged.
    """

    def __init__(
        self,
        client=None,
        window="1 hour",
        tier_priority=None,
        org_group="coherentvolition",
        backend=None,
        **counters,
    ):
        """
        Args:
            client:
              redis-like client (StrictRedis, fakeredis).
              Default is an atomic MemoryBucketStore (process-local).
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
        if backend is not None and client is not None:
            raise ValueError("Pass backend or client, not both")
        self.backend = backend if backend is not None else (RedisBucketStore(client) if client is not None else MemoryBucketStore())
        self.client = client
        self.default_window_seconds = _parse_window_to_seconds(window)
        if self.default_window_seconds <= 0:
            raise ValueError("Rate-limit window must be positive")
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

        if win <= 0:
            raise ValueError("Rate-limit window must be positive")
        if not tier_limits or any(v is not None and (type(v) is not int or v < 0) for v in tier_limits.values()):
            raise ValueError("Tier limits must be nonnegative integers or None")
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
                    try:
                        blocked = await asyncio.to_thread(_enforce, handler_self) if isinstance(self.backend, RedisBucketStore) else _enforce(handler_self)
                    except BucketUnavailable:
                        return handler_self.jsonerr("rate limiter unavailable", 503)
                    if blocked:
                        return handler_self.jsonerr("rate limit exceeded", 429)
                    return await method(handler_self, *args, **kwargs)

                return async_wrapper

            else:

                @functools.wraps(method)
                def sync_wrapper(handler_self, *args, **kwargs):
                    try:
                        blocked = _enforce(handler_self)
                    except BucketUnavailable:
                        return handler_self.jsonerr("rate limiter unavailable", 503)
                    if blocked:
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

        # Missing configuration must not accidentally grant unlimited access.
        limit = tier_limits.get(tier_ability, tier_limits.get("tier.free", 0))
        key = self._redis_key(user_id, action_name)
        return self.backend.consume(key, window_seconds, limit)

    # ------------------------------------------------------------------------
    # Reporting / introspection
    # ------------------------------------------------------------------------

    def report(self):
        """Live usage snapshot, not a persistent billing ledger."""
        summary = defaultdict(dict)
        for key, state in self.backend.snapshot().items():
            if key.startswith("rate|"):
                uid, action = key[5:].rsplit("|", 1)
                summary[action][uid] = int(state["used"])
        return dict(summary)
