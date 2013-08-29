from itertools import imap
import logging
import re
import string
from collections import OrderedDict

NOT_NUMBER_REG = re.compile(r'\D')


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


def join_stripped_gotten_value(sep, getters, entity):
    return sep.join(
        filter(bool,
               imap(string.strip,
                    imap(str,
                         [getter(entity) for getter in getters]))))
