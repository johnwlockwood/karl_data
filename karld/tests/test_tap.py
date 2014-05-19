from collections import deque
import unittest
from karld.tap import Spigot
from karld.tap import stream_tap


def get_odd(item):
    if item%2 != 0:
        return item

def get_company_crowd(item):
    if item >= 3:
        return "{} >= 3".format(item)

class TestTap(unittest.TestCase):
    def setUp(self):
        self.stream = iter([ 1, 2, 3, 4])

    def test_spigot_collects(self):
        """
        Test Spigot collects it's results.
        """
        odd_spigot = Spigot(get_odd)

        for item in self.stream:
            odd_spigot(item)

        odds = odd_spigot.results()

        self.assertEqual((1, 3), tuple(odds))

        self.assertNotEqual(deque(), odd_spigot.results())

    def test_spigot_flush(self):
        """
        Test Spigot can flush it's results.
        """
        odd_spigot = Spigot(get_odd)

        for item in self.stream:
            odd_spigot(item)

        odds = odd_spigot.flush_results()

        self.assertEqual((1, 3), tuple(odds))

        self.assertEqual(deque(), odd_spigot.results())

    def test_stream_tap_one_spigot(self):
        """
        Test stream_tap with one spigot.
        """
        odd_spigot = Spigot(get_odd)
        items = stream_tap((odd_spigot,), self.stream)
        out_flow = tuple(items)
        self.assertEqual((1, 2, 3, 4), out_flow)

        self.assertEqual((1, 3), tuple(odd_spigot.results()))

    def test_stream_tap_two_spigot(self):
        """
        Test stream_tap with one spigot.
        """
        odd_spigot = Spigot(get_odd)
        company_spigot = Spigot(get_company_crowd)
        items = stream_tap((odd_spigot, company_spigot), self.stream)
        out_flow = tuple(items)
        self.assertEqual((1, 2, 3, 4), out_flow)

        self.assertEqual((1, 3), tuple(odd_spigot.results()))

        self.assertEqual(("3 >= 3", "4 >= 3"), tuple(company_spigot.results()))
