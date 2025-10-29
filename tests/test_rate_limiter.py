import os
import tempfile
import unittest

from src.pycronado.rate_limiter import RateLimiter


class DummyHandler:
    """
    Minimal stub that looks enough like a core.JSONHandler for the limiter
    to exercise logic. We don't pull in tornado or pycronado.core here;
    we just fake the surface the limiter calls:

      - userId()
      - permissions()
      - jsonerr(msg, status)

    And we track how many times the "real" handler body ran via .called.
    """

    def __init__(self, uid, perms):
        self._uid = uid
        self._perms = perms
        self.called = 0
        self.last_jsonerr = None

    def userId(self):
        return self._uid

    def permissions(self):
        # Matches UserMixin.permissions() contract:
        # returns a list of permission dicts, each with
        # "user_group" and "group_ability"
        return self._perms

    def jsonerr(self, message, status=500):
        # pycronado.core.PublicJSONHandler.jsonerr() writes a JSON error
        # and finishes the request. For tests, we just remember what
        # would have been sent to the client and return a dummy payload.
        self.last_jsonerr = (message, status)
        return {
            "status": "error",
            "message": message,
            "status_code": status,
        }

    # This simulates the real handler body.
    def do_work(self):
        self.called += 1
        return "ok"


class RateLimiterTests(unittest.TestCase):
    def test_basic_limit_enforcement_normal_user(self):
        """
        A normal user on a low tier hits a counter with a small limit.
        After exceeding the limit, they should get a 429 and the handler
        body should NOT run for that call.

        Also: attempts that get blocked still count toward accounting.
        """
        limiter = RateLimiter(
            window="1 hour",
            org_group="coherentvolition",
            audio={
                "tier.free": 2,  # allow 2 per hour
            },
        )

        # user has coherentvolition tier.free
        perms = [
            {"user_group": "coherentvolition", "group_ability": "tier.free"},
        ]
        h = DummyHandler("auth.example::alice", perms)

        wrapped = limiter.limited("audio")(DummyHandler.do_work)

        # First call: allowed
        res1 = wrapped(h)
        self.assertEqual(res1, "ok")
        self.assertEqual(h.called, 1)
        self.assertIsNone(h.last_jsonerr)

        # Second call: still allowed, still under limit 2
        res2 = wrapped(h)
        self.assertEqual(res2, "ok")
        self.assertEqual(h.called, 2)
        self.assertIsNone(h.last_jsonerr)

        # Third call: now over the limit (limit=2)
        res3 = wrapped(h)
        # We expect a jsonerr() style dict, with 429
        self.assertIsInstance(res3, dict)
        self.assertEqual(res3.get("status_code"), 429)
        self.assertEqual(h.last_jsonerr, ("rate limit exceeded", 429))

        # Handler body should NOT have run for the over-limit call
        self.assertEqual(h.called, 2)

        # Accounting: all 3 attempts are counted in redis/fakeredis,
        # including the blocked one.
        rep = limiter.report()
        self.assertIn("audio", rep)
        self.assertIn(h.userId(), rep["audio"])
        self.assertEqual(rep["audio"][h.userId()], 3)

    def test_unlimited_tier_allows_but_counts(self):
        """
        A user on an unlimited tier (tier.power -> limit None)
        should never be 429-blocked, even after many calls,
        but their usage should still be counted in report().
        """
        limiter = RateLimiter(
            window="1 hour",
            org_group="coherentvolition",
            audio={
                "tier.power": None,  # unlimited but still counted
                "tier.free": 1,
            },
        )

        perms = [
            {"user_group": "coherentvolition", "group_ability": "tier.power"},
        ]
        h = DummyHandler("auth.example::poweruser", perms)

        wrapped = limiter.limited("audio")(DummyHandler.do_work)

        # Call multiple times, well past the 'tier.free' baseline.
        for _ in range(3):
            res = wrapped(h)
            self.assertEqual(res, "ok")
            self.assertIsNone(h.last_jsonerr)

        # Handler ran every time
        self.assertEqual(h.called, 3)

        # Report shows all usage counted
        rep = limiter.report()
        self.assertEqual(rep["audio"][h.userId()], 3)

    def test_global_admin_bypass_but_counted(self):
        """
        A global admin (*:*) should never be 429-blocked even if they blow past
        their nominal tier limit, but their usage should still contribute to
        accounting.

        We'll give them tier.free (limit=1) plus global '*:*', then call 3 times.
        They should never get a 429, and handler should run every time.
        """
        limiter = RateLimiter(
            window="1 hour",
            org_group="coherentvolition",
            audio={
                "tier.free": 1,  # limit 1 per hour
            },
        )

        perms = [
            {"user_group": "coherentvolition", "group_ability": "tier.free"},
            {"user_group": "*", "group_ability": "*"},  # global god
        ]
        h = DummyHandler("auth.example::root", perms)

        wrapped = limiter.limited("audio")(DummyHandler.do_work)

        # First call: under limit, allowed
        res1 = wrapped(h)
        self.assertEqual(res1, "ok")
        self.assertIsNone(h.last_jsonerr)

        # Second + Third calls: definitely over the limit=1, but global admin means
        # they SHOULD NOT get blocked. Should still run handler.
        res2 = wrapped(h)
        res3 = wrapped(h)

        self.assertEqual(res2, "ok")
        self.assertEqual(res3, "ok")
        self.assertIsNone(h.last_jsonerr)

        # Handler ran all 3 times
        self.assertEqual(h.called, 3)

        # Accounting should show 3 total invocations
        rep = limiter.report()
        self.assertIn("audio", rep)
        self.assertEqual(rep["audio"][h.userId()], 3)

    def test_org_admin_is_not_bypassed(self):
        """
        A user with org-scoped '<org>:*' but not global '*:*'
        should NOT get special bypass anymore.

        We'll set limit to 1, give them coherentvolition:* and tier.free,
        and call twice. Second call should 429.
        """
        limiter = RateLimiter(
            window="1 hour",
            org_group="coherentvolition",
            audio={
                "tier.free": 1,  # limit 1
            },
        )

        perms = [
            {"user_group": "coherentvolition", "group_ability": "tier.free"},
            {"user_group": "coherentvolition", "group_ability": "*"},  # org "admin"
        ]
        h = DummyHandler("auth.example::orgadmin", perms)

        wrapped = limiter.limited("audio")(DummyHandler.do_work)

        # First call: allowed
        res1 = wrapped(h)
        self.assertEqual(res1, "ok")
        self.assertIsNone(h.last_jsonerr)
        self.assertEqual(h.called, 1)

        # Second call: over their tier.free limit (1). Not global god.
        res2 = wrapped(h)
        self.assertIsInstance(res2, dict)
        self.assertEqual(res2.get("status_code"), 429)
        self.assertEqual(h.last_jsonerr, ("rate limit exceeded", 429))

        # Handler body should NOT have run for second call
        self.assertEqual(h.called, 1)

        # Accounting still counts both attempts
        rep = limiter.report()
        self.assertIn("audio", rep)
        self.assertEqual(rep["audio"][h.userId()], 2)

    def test_report(self):
        """
        Multiple users hit a metered action. We then:
        - verify report() structure/action grouping
        """
        limiter = RateLimiter(
            window="1 hour",
            org_group="coherentvolition",
            audio={
                "tier.free": 10,  # generous so we don't trigger 429
            },
        )

        perms_a = [
            {"user_group": "coherentvolition", "group_ability": "tier.free"},
        ]
        perms_b = [
            {"user_group": "coherentvolition", "group_ability": "tier.free"},
        ]

        h_a = DummyHandler("auth.example::a", perms_a)
        h_b = DummyHandler("auth.example::b", perms_b)

        wrapped = limiter.limited("audio")(DummyHandler.do_work)

        # a hits twice
        wrapped(h_a)
        wrapped(h_a)

        # b hits three times
        wrapped(h_b)
        wrapped(h_b)
        wrapped(h_b)

        rep = limiter.report()
        self.assertIn("audio", rep)
        self.assertIn(h_a.userId(), rep["audio"])
        self.assertIn(h_b.userId(), rep["audio"])

        self.assertEqual(rep["audio"][h_a.userId()], 2)
        self.assertEqual(rep["audio"][h_b.userId()], 3)


if __name__ == "__main__":
    unittest.main()
