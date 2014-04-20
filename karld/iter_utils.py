from functools import partial

try:
    from itertools import ifilter
    from itertools import imap
except ImportError:
    imap = map
    ifilter = filter

from itertools import islice
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

    :param nth: Index desired column of each sequence.
    :type nth: int
    :param iterator: iterator of sequences.
    """
    return yield_getter_of(partial(itemgetter, nth), iterator)


def i_batch(max_size, iterable):
    """
    Generator that iteratively batches items
    to a max size and consumes the items iterable
    as each batch is yielded.

    :param max_size: Max size of each batch.
    :type max_size: int
    :param iterable: An iterable
    :type iterable: iter
    """
    iterable_items = iter(iterable)
    for items_batch in iter(lambda: tuple(islice(iterable_items, max_size)),
                            tuple()):
        yield items_batch
