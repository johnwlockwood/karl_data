from operator import itemgetter


def yield_nth_of(nth, iterator):
    """
    For an iterator that returns sequences,
    yield the nth value of each.

    :param nth: :class: `int` index desired column of each sequence.
    :param iterator: iterator of sequences.
    """
    nth_getter = itemgetter(nth)
    for value in iterator:
        yield nth_getter(value)
