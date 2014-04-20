from functools import partial
from itertools import groupby
from operator import attrgetter

try:
    from itertools import ifilter
    from itertools import imap
except ImportError:
    imap = map
    ifilter = filter


import logging
import heapq

from karld import is_py3

#generator that gets sorted iterator


def merge(*iterables, **kwargs):
    """Merge multiple sorted inputs into a single sorted output.

   Similar to sorted(itertools.chain(\*iterables)) but returns a generator,
   does not pull the data into memory all at once, and assumes that each of
   the input streams is already sorted (smallest to largest).

   >>> list(merge([[2,1],[2,3],[2,5],[2,7]],
   [[2,0],[2,2],[2,4],[2,8]],
   [[2,5],[2,10],[2,15],[2,20]],
   [], [[2,25]]), key=itemgetter(-1))
   [0, 1, 2, 3, 4, 5, 5, 7, 8, 10, 15, 20, 25]

   """
    key = kwargs.get('key')
    _heappop, _heapreplace, _StopIteration = heapq.heappop, heapq.heapreplace, StopIteration
    if is_py3():
        next_method = attrgetter('__next__')
    else:
        next_method = attrgetter('next')

    h = []
    h_append = h.append
    key_is_None = key is None
    for itnum, it in enumerate(map(iter, iterables)):
        try:
            nnext = next_method(it)
            v = nnext()
            h_append([v if key_is_None else key(v), itnum, v, nnext])
        except _StopIteration:
            pass
    heapq.heapify(h)

    while 1:
        try:
            while 1:
                # raises IndexError when h is empty
                k, itnum, v, nnext = s = h[0]
                yield v
                v = nnext()                  # raises StopIteration when exhausted
                s[0] = v if key_is_None else key(v)
                s[2] = v
                _heapreplace(h, s)          # restore heap condition
        except _StopIteration:
            _heappop(h)                     # remove empty iterator
        except IndexError:
            return


def sorted_by(key, items):
    return sorted(items, key=key)


def sort_iterables(iterables, key=None):
    assert key is not None
    sorted_by_key = partial(sorted_by, key)
    return list(map(sorted_by_key, iterables))


def i_merge_group_sorted(iterables, key=None):
    assert key is not None
    all_sorted = merge(*iterables, key=key)
    grouped = groupby(all_sorted, key=key)
    grouped_voters = ((key_value, list(grouped))
                      for key_value, grouped in grouped)
    return grouped_voters


def sort_merge_group(iterables, key=None):
    assert key is not None
    return list(i_merge_group_sorted(
        sort_iterables(iterables, key=key),
        key=key))


def get_first_if_any(values):
    if values:
        return values[0]


def get_first_type_instance_of_group(instance_type, group):
    key_value, items = group
    try:
        return get_first_if_any(
            list(filter(lambda vs: isinstance(vs, instance_type), items)))
    except ValueError:
        logging.exception("couldn't unpack {0}".format(group))


def i_get_multi_groups(iterables, key=None):
    assert key is not None
    return ifilter(lambda v: len(v[1]) > 1,
                   sort_merge_group(iterables, key=key))
