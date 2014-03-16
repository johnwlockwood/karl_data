from functools import partial
from itertools import imap
from operator import itemgetter


def yield_getter_of(getter_maker, iterator):
    """
    Iteratively map iterator over the result of getter_maker.

    :param getter_maker: function that returns a getter function.
    :param iterator: An iterator.
    """
    return imap(getter_maker(), iterator)


def yield_nth_of(nth, iterator):
    """
    For an iterator that returns sequences,
    yield the nth value of each.

    :param nth: :class: `int` index desired column of each sequence.
    :param iterator: iterator of sequences.
    """
    return yield_getter_of(partial(itemgetter, nth), iterator)
