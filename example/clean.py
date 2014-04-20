import argparse
from functools import partial
from itertools import chain
from operator import itemgetter
import os

from karld import is_py3


if is_py3():
    unicode = str

from karld.loadump import is_file_csv
from karld.run_together import csv_file_to_file
from karld.run_together import pool_run_files_to_files
from karld.run_together import serial_run_files_to_files


def contrived_cleaner(data_items):
    """
    Sort the data by the second row, enumerate it,
    apply title case to every field and include
    the original index and sorted in the in the row.

    :param data_items: A sequence of unicode strings
    """
    original_index_added = (
        (unicode(o_index), item[0].title(), item[1].title())
        for o_index, item in enumerate(data_items)

    )
    ROWTWO = 2
    rowtwo_getter = itemgetter(ROWTWO)

    items = tuple(tuple(chain([index], row))
            for index, row in
            enumerate(
                sorted(
                    original_index_added
                    , key=rowtwo_getter
                )
            )
    )

    return items


def run(in_dir, out_dir, pool):
    """

    """
    files_to_files_runner = serial_run_files_to_files

    if pool:
        print("multi-processing")
        files_to_files_runner = pool_run_files_to_files

    files_to_files_runner(
        partial(csv_file_to_file,
                contrived_cleaner,
                "",
                out_dir),
        in_dir, filter_func=is_file_csv)


def main(*args):
    """
    Try it::

        python clean.py

    or::

        python clean.py --pool True

    or::

        python clean.py --in-dir split_data_ml/data --out-dir my_clean_data

    or::

        python clean.py --pool True --in-dir split_data_ml/data
    """
    parser = argparse.ArgumentParser(*args)
    parser.add_argument("--in-dir",
                        default=os.path.join("split_data_ml", "data"),
                        help="Data source directory")
    parser.add_argument("--out-dir", default="clean_data",
                        help="Data output directory")
    parser.add_argument("--pool", default=False)
    args = parser.parse_args()

    run(args.in_dir, args.out_dir, args.pool)


if __name__ == "__main__":
    main()
