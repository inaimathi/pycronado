import unittest

from src.pycronado import core


class TestBasics(unittest.TestCase):
    def test_basics(self):
        core.getLogger
        self.assertTrue(True)
        self.assertIsNone(None)
        self.assertEqual("a", "a")
