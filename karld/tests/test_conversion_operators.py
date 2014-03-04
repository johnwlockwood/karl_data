from operator import itemgetter

import unittest
from ..conversion_operators import join_stripped_values
from ..conversion_operators import join_stripped_gotten_value


class TestValueJoiner(unittest.TestCase):
    def test_join_stripped_values(self):
        """
        Ensure joiner gets the values from
        data with the getter, coerce to str,
        strips padding whitespace and join
        with the separator.
        """
        getter = itemgetter(0, 1, 2, 3)
        data = (" A", "B ", 2, "D")
        separator = "+"
        self.assertEqual(join_stripped_values(separator, getter, data),
                         "A+B+2+D")


class TestGettersValueJoiner(unittest.TestCase):
    def test_join_stripped_gotten_value(self):
        """
        Ensure joiner gets the values from
        data with the getters, coerce to str,
        strips padding whitespace and join
        with the separator.
        """
        getters = (itemgetter(0), itemgetter(1), itemgetter(2), itemgetter(3))
        data = (" A", "B ", 2, "D")
        separator = "+"
        self.assertEqual(join_stripped_gotten_value(separator, getters, data),
                         "A+B+2+D")
