from operator import itemgetter

import unittest
from ..conversion_operators import apply_conversion_map
from ..conversion_operators import apply_conversion_map_map
from ..conversion_operators import get_number_as_int
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


class TestConversion(unittest.TestCase):
    def test_get_number_as_int(self):
        """
        Ensure a string with a number prefix returns
        an int.
        """
        self.assertEqual(2, get_number_as_int('2sdfgsd'))

    def test_must_int(self):
        """
        Ensure a string not castable to a number
        raises a ValueError
        """
        self.assertRaises(ValueError, get_number_as_int, 'sdfgsd')

    def test_apply_conversion_map(self):
        """
        Ensure all the items in the conversion_map
        is applied.
        """
        conversion_map = [
            ('first', itemgetter(1)),
            ('last', itemgetter(0)),
        ]

        result = apply_conversion_map(conversion_map, ['bruce', 'lara'])

        self.assertEqual(('lara', 'bruce'), result)

    def test_apply_conversion_map_map(self):
        """
        Ensure all the items in the conversion_map
        is applied.
        """
        conversion_map = [
            ('first', itemgetter(1)),
            ('last', itemgetter(0)),
        ]

        result = apply_conversion_map_map(conversion_map, ['bruce', 'lara'])

        self.assertEqual({'first': 'lara', 'last': 'bruce'}, result)
