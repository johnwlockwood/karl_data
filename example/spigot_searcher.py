import argparse
from functools import partial
import os

from karld import is_py3
from karld.iter_utils import i_batch


if is_py3():
    unicode = str

from karld.loadump import is_file_csv
from karld.run_together import csv_file_consumer
from karld.run_together import pool_run_files_to_files
from karld.run_together import serial_run_files_to_files
from karld.tap import Spigot
from karld.tap import stream_tap


def get_fruit(item):
    if len(item) == 2 and item[1] == u"fruit":
        return item[0]


def get_metal(item):
    if len(item) == 2 and item[1] == u"metal":
        return item[0]


def information_tap(data_items):
    """
    As the stream of data items go by get different
    kinds of information from them, in this case
    the things that are fruit and metal, collecting
    each kind with a different spigot.

    stream_tap doesn't consume the iterator(data_items)
    by itself, it's a generator and must be consumed
    by something else. In this case, it's consuming
    the items by casting the iterator to a tuple,
    but doing it in batches.

    Since each batch is not referenced by anything
    the garbage collector is free to free the memory,
    so no matter the size of the data_items, only a little
    memory is needed. The only things retained
    are the results, which should just be a subset
    of the items and in this case, the getter functions
    only return a portion of each item it matches.


    :param data_items: A sequence of unicode strings
    """
    fruit_spigot = Spigot(get_fruit)
    metal_spigot = Spigot(get_metal)

    items = stream_tap((fruit_spigot, metal_spigot), data_items)

    for batch in i_batch(100, items):
        tuple(batch)

    return fruit_spigot.results(), metal_spigot.results()


def run(in_dir, pool):
    """
    Run the composition of csv_file_consumer and information tap
    with the csv files in the input directory, and collect
    the results from each file and merge them together,
    printing both kinds of results.
    """
    files_to_files_runner = serial_run_files_to_files

    if pool:
        print("multi-processing")
        files_to_files_runner = pool_run_files_to_files

    results = files_to_files_runner(
        partial(csv_file_consumer,
                information_tap),
        in_dir, filter_func=is_file_csv)

    fruit_results = []
    metal_results = []

    for fruits, metals in results:
        for fruit in fruits:
            fruit_results.append(fruit)

        for metal in metals:
            metal_results.append(metal)

    print("=== fruits ===")
    for fruit in fruit_results:
        print(fruit)

    print("=== metals ===")
    for metal in metal_results:
        print(metal)


def main(*args):
    """
    Try it::

        python spigot_searcher.py

    or::

        python spigot_searcher.py --pool True

    or::

        python spigot_searcher.py --in-dir test_data/things_kinds

    or::

        python spigot_searcher.py --pool True --in-dir test_data/things_kinds
    """
    parser = argparse.ArgumentParser(*args)
    parser.add_argument("--in-dir",
                        default=os.path.join("test_data", "things_kinds"),
                        help="Data source directory")
    parser.add_argument("--pool", default=False)
    args = parser.parse_args()

    run(args.in_dir, args.pool)


if __name__ == "__main__":
    main()
