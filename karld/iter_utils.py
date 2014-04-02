from functools import partial
from itertools import chain
from itertools import imap
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


def i_batch(max_size, items):
    """
    Generator that iteratively batches items
    to a max size and consumes the first item of a
    batch it is yielded.

    :param max_size: Max size of each batch.
    :type max_size: int
    :param items: An iterable
    :type items: iter
    """
    iterable_items = iter(items)
    while True:
        items_batch = islice(iterable_items, max_size)
        yield chain((items_batch.next(),), items_batch)
