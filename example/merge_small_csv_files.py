from __future__ import print_function
from operator import itemgetter

import pathlib

import karld
from karld.merger import sort_merge_group
from karld.path import i_walk_csv_paths


def main():
    """
    Merge a number of homogeneous small csv files on a key.
    Small means they all together fit in
    your computer's memory.
    """

    input_dir = pathlib.Path('test_data/things_kinds')

    data_items_iter = (karld.io.i_get_csv_data(data_path)
                       for data_path in i_walk_csv_paths(str(input_dir)))

    KINDS = 1

    groups = sort_merge_group(data_items_iter, itemgetter(KINDS))

    for group in groups:
        print(group[0])
        for item in group[1]:
            print('\t' + item[0])
        print()


if __name__ == "__main__":
    main()
