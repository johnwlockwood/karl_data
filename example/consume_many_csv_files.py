#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from __future__ import print_function
from itertools import chain

try:
    from itertools import imap
except ImportError:
    # if python 3
    imap = map

import karld

from karld.path import i_walk_csv_paths


def main():
    """
    Consume many csv files as if one.
    """
    import pathlib

    input_dir = pathlib.Path('test_data/things_kinds')

    # # Use a generator expression
    # iterables = (karld.io.i_get_csv_data(data_path)
    #              for data_path in i_walk_csv_paths(str(input_dir)))

    # # or a generator map.
    iterables = imap(karld.io.i_get_csv_data,
                     i_walk_csv_paths(str(input_dir)))

    items = chain.from_iterable(iterables)

    for item in items:
        print(item[0], item[1])


if __name__ == "__main__":
    main()
