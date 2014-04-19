from operator import methodcaller

try:
    from itertools import imap
except ImportError:
    imap = map

import logging
import re
from collections import OrderedDict

from karld import is_py3


if is_py3():
    unicode = str


NOT_NUMBER_REG = re.compile(r'\D')


str_strip = methodcaller('strip')


def apply_conversion_map(conversion_map, entity):
    """
    returns tuple of conversions
    """
    return tuple([conversion(entity) for key, conversion in conversion_map])


def apply_conversion_map_map(conversion_map, entity):
    """
    returns ordered dict of keys and converted values
    """
    return OrderedDict([(key, conversion(entity))
                        for key, conversion in conversion_map])


def get_number_as_int(number):
    """Returns the first number from a string."""
    number_parts = NOT_NUMBER_REG.split(number)
    if number_parts:
        try:
            return int(number_parts[0])
        except ValueError:
            logging.exception("Couldn't convert {0} "
                              "to an int".format(number_parts))
            raise


def join_stripped_gotten_value(sep, getters, data):
    """
    Join the values, coerced to str and stripped of whitespace padding,
    from entity, gotten with collection of getters,
    with the separator.

    :param sep: Separator of values.
    :type sep: str
    :param getters: collection of callables takes that data and returns value.
    :param data: argument for the getters
    """
    return sep.join(
        filter(
            None,
            imap(str_strip,
                 imap(unicode,
                      filter(None, [getter(data) for getter in getters])))))


def join_stripped_values(sep, collection_getter, data):
    """
    Join the values, coerced to str and stripped of whitespace padding,
    from entity, gotten with collection_getter,
    with the separator.

    :param sep: Separator of values.
    :type sep: str
    :param collection_getter: callable takes that data and returns collection.
    :param data: argument for the collection_getter
    """
    return sep.join(
        filter(
            None,
            imap(str_strip,
                 imap(unicode, filter(None, collection_getter(data))))))
