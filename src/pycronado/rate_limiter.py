import re
import time
from collections import defaultdict

import fakeredis

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

    This class:
      - tracks "counters" (actions) like "audio", "audio_retry", etc.
      - enforces per-tier quotas per action
      - exposes @L.limited("audio") for handlers
      - exposes report() / dumpReport() for introspection/ops

    Storage backend:
      - by default uses fakeredis.FakeStrictRedis(), which is just in-memory
        for this process (great for dev / single instance)
      - in production you can pass a real redis.StrictRedis(...)
        and everything still works the same way across replicas.
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
            self.addCounter(action_name, tier_map, window=None)

    # ------------------------------------------------------------------------
    # Public configuration surface
    # ------------------------------------------------------------------------

    def addCounter(self, action_name, tier_limits, window=None):
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

        self.counters[action_name] = {
            "window_seconds": win,
            "tiers": dict(tier_limits),
        }

    def limited(self, action_name):
        """
        Decorator that enforces per-user quota for `action_name`.

        Usage in a handler:

            L = RateLimiter(..., audio={...})
            class SpeakHandler(core.JSONHandler):
                @core.requires("text", permissions="audio")
                @L.limited("audio")
                def post(self, text):
                    ...

        Runtime behavior:
        - Always increments usage for this user/action in the current window.
        - Checks if usage <= their tier limit.
        - If they're over limit:
            - If they're a global god (*:*), ALLOW (but we still counted).
            - Otherwise jsonerr("rate limit exceeded", 429) and stop.

        Either way, this decorator returns/blocks in pycronado style.
        """

        def decorator(method):
            def wrapper(handler_self, *args, **kwargs):
                # figure out if they're a global god (*:*)
                admin = self._is_global_admin(handler_self)

                # pick their best tier.* (tier.power > tier.premium > ...)
                tier_ability = self._get_highest_tier(handler_self)

                # record usage and see if they'd normally be allowed
                user_id = handler_self.userId()
                allowed = self.check_and_increment(
                    user_id,
                    tier_ability,
                    action_name,
                )

                if not allowed and not admin:
                    # normal user over quota → block
                    return handler_self.jsonerr("rate limit exceeded", 429)

                # either allowed, or admin override → proceed
                return method(handler_self, *args, **kwargs)

            return wrapper

        return decorator

    # ------------------------------------------------------------------------
    # Internal helpers used by the decorator / counters
    # ------------------------------------------------------------------------

    def _bucket_start(self, window_seconds, now=None):
        """
        Floor current timestamp to the start of this window.
        For a 1h window, that's the top-of-hour epoch.
        """
        if now is None:
            now = int(time.time())
        return (now // window_seconds) * window_seconds

    def _redis_key(self, user_id, action_name, bucket_start):
        """
        Unique storage key for (user, action, window bucket).

        IMPORTANT: user_id includes colons (issuer::username), so we cannot
        safely split on ":" later. We use '|' as a field separator.

        Format:
            rate|<user_id>|<action_name>|<bucket_start_epoch>
        """
        return f"rate|{user_id}|{action_name}|{bucket_start}"

    def _increment_and_check(self, key, window_seconds, limit):
        """
        Atomically record usage for this key and decide if it's in quota.

        We ALWAYS increment, even if limit is None or the caller is a god.
        That means:
          - reporting sees everyone's traffic
          - unlimited tiers are still measured

        After increment:
          - if limit is None: always allowed
          - else: allowed if count_after <= limit
        """
        pipe = self.client.pipeline()
        pipe.incr(key)
        pipe.expire(key, window_seconds)
        count_after, _ = pipe.execute()

        if limit is None:
            return True

        return int(count_after) <= int(limit)

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
        Increment usage for this (user, action_name, current window),
        and return True if usage is still within their tier's quota.

        If this action_name was never registered with addCounter(), it's not metered:
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

        bucket_start = self._bucket_start(window_seconds)
        key = self._redis_key(user_id, action_name, bucket_start)

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
                 "<user_id>": <count_sum_across_live_buckets>,
                 ...
              },
              ...
            }

        Notes:
        - We aggregate across all non-expired buckets we can see.
          (Because of how TTL works, that usually means "this hour",
           maybe plus a tail from the previous bucket if it hasn't expired yet.)
        - This is *not* a strict billing ledger. It's an ops/debug snapshot.
        """
        summary = defaultdict(lambda: defaultdict(int))

        # fakeredis and redis.StrictRedis both support .keys()
        for raw_key in self.client.keys("rate|*"):
            # raw_key may be bytes (redis) or str (fakeredis). Normalize to str.
            if isinstance(raw_key, bytes):
                raw_key = raw_key.decode("utf-8", "replace")

            # Expect "rate|<user_id>|<action_name>|<bucket_start>"
            parts = raw_key.split("|", 3)
            if len(parts) != 4:
                continue
            _prefix, user_id, action_name, _bucket = parts

            # fetch current count for that key
            val = self.client.get(raw_key)
            if val is None:
                continue
            if isinstance(val, bytes):
                val = val.decode("utf-8", "replace")
            try:
                count = int(val)
            except ValueError:
                continue

            # aggregate per action then per user
            summary[action_name][user_id] += count

        # convert defaultdicts back to plain dicts for cleanliness
        outer = {}
        for action_name, users_map in summary.items():
            outer[action_name] = dict(users_map)

        return outer
