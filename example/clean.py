import argparse
from functools import partial
from itertools import chain
from operator import itemgetter
from operator import methodcaller
import os

from karld.loadump import is_file_csv
from karld.run_together import csv_file_to_file
from karld.run_together import pool_run_files_to_files
from karld.run_together import serial_run_files_to_files


def titlize(data_items):
    """
    Sort the data by the second row, enumerate it,
    apply title case to every field and include
    the index in the in the row.

    :param data_items: A sequence of unicode strings
    """
    return (tuple(chain([index], map(methodcaller('title'), row)))
            for index, row in enumerate(sorted(data_items, key=itemgetter(1))))


def run(in_dir, out_dir, pool):
    """

    """
    files_to_files_runner = serial_run_files_to_files

    if pool:
        print("multi-processing")
        files_to_files_runner = pool_run_files_to_files

    files_to_files_runner(
        partial(csv_file_to_file,
                titlize,
                "",
                out_dir),
        in_dir, filter_func=is_file_csv)


def main(*args):
    """
    Try it::

        python clean.py

    or::

        python clean.py --in-dir split_data_ml/unsplit --out-dir titlized

    or::

        python clean.py --pool True --in-dir split_data_ml/unsplit --out-dir titlized
    """
    parser = argparse.ArgumentParser(*args)
    parser.add_argument("--in-dir",
                        default=os.path.join("split_data_ml", "unsplit"),
                        help="Data source directory")
    parser.add_argument("--out-dir", default="clean_data",
                        help="Data output directory")
    parser.add_argument("--pool", default=False)
    args = parser.parse_args()

    run(args.in_dir, args.out_dir, args.pool)


if __name__ == "__main__":
    main()
