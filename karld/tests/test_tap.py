from collections import deque
import unittest
from karld.tap import Bucket
from karld.tap import stream_tap


def get_odd(item):
    """Get odd items.

    :returns: item if it is odd."""
    if item%2 != 0:
        return item


def get_company_crowd(item):
    """Get string based upon item if it is greater than two.

    :returns: A string if item is greater or equal to three."""
    if item >= 3:
        return "{} >= 3".format(item)


class TestTap(unittest.TestCase):
    """
    Test the tap Bucket and stream_tap.
    """
    def setUp(self):
        self.stream = iter([ 1, 2, 3, 4])

    def test_spigot_collects(self):
        """
        Test Spigot collects it's contents.
        """
        odd_spigot = Bucket(get_odd)

        for item in self.stream:
            odd_spigot(item)

        odds = odd_spigot.contents()

        self.assertEqual((1, 3), tuple(odds))

        self.assertNotEqual(deque(), odd_spigot.contents())

    def test_bucket_drain(self):
        """
        Test Bucket can drain it's contents.
        """
        odd_bucket = Bucket(get_odd)

        for item in self.stream:
            odd_bucket(item)

        odds = odd_bucket.drain_contents()

        self.assertEqual((1, 3), tuple(odds))

        self.assertEqual(deque(), odd_bucket.contents())

    def test_stream_tap_one_bucket(self):
        """
        Test stream_tap with one bucket.
        """
        odd_bucket = Bucket(get_odd)
        items = stream_tap((odd_bucket,), self.stream)
        out_flow = tuple(items)
        self.assertEqual((1, 2, 3, 4), out_flow)

        self.assertEqual((1, 3), tuple(odd_bucket.contents()))

    def test_stream_tap_two_bucket(self):
        """
        Test stream_tap with two buckets.
        """
        odd_bucket = Bucket(get_odd)
        company_bucket = Bucket(get_company_crowd)
        items = stream_tap((odd_bucket, company_bucket), self.stream)
        out_flow = tuple(items)
        self.assertEqual((1, 2, 3, 4), out_flow)

        self.assertEqual((1, 3), tuple(odd_bucket.contents()))

        self.assertEqual(("3 >= 3", "4 >= 3"), tuple(company_bucket.contents()))
