#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from __future__ import print_function
from operator import itemgetter

import karld


def main():
    """
    Iterate over a the row of a csv file, extracting the data
    you desire.
    """
    import pathlib

    data_file_path = pathlib.Path('test_data/things_kinds/data_0.csv')

    rows = karld.io.i_get_csv_data(str(data_file_path))

    kinds = set(map(itemgetter(1), rows))

    for kind in kinds:
        print(kind)


if __name__ == "__main__":
    main()
