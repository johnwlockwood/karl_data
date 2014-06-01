import argparse
from functools import partial
import os

from karld import is_py3
from karld.iter_utils import i_batch


if is_py3():
    unicode = str

from karld.loadump import is_file_csv
from karld.loadump import i_get_csv_data
from karld.run_together import csv_file_consumer
from karld.run_together import pool_run_files_to_files
from karld.run_together import serial_run_files_to_files
from karld.run_together import distribute_run_to_runners
from karld.run_together import distribute_multi_run_to_runners
from karld.tap import Bucket
from karld.tap import stream_tap


def get_fruit(item):
    """Get things that are fruit.

    :returns: thing of item if it's a fruit"""
    if len(item) == 2 and item[1] == u"fruit":
        return item[0]


def get_metal(item):
    """Get things that are metal.

    :returns: thing of item if it's metal"""
    if len(item) == 2 and item[1] == u"metal":
        return item[0]


def certain_kind_tap(data_items):
    """
    As the stream of data items go by, get different
    kinds of information from them, in this case,
    the things that are fruit and metal, collecting
    each kind with a different spigot.

    stream_tap doesn't consume the data_items iterator
    by itself, it's a generator and must be consumed
    by something else. In this case, it's consuming
    the items by casting the iterator to a tuple,
    but doing it in batches.

    Since each batch is not referenced by anything
    the memory can be freed by the garbage collector,
    so no matter the size of the data_items, only a little
    memory is needed. The only things retained
    are the results, which should just be a subset
    of the items and in this case, the getter functions
    only return a portion of each item it matches.


    :param data_items: A sequence of unicode strings
    """
    fruit_spigot = Bucket(get_fruit)
    metal_spigot = Bucket(get_metal)

    items = stream_tap((fruit_spigot, metal_spigot), data_items)

    for batch in i_batch(100, items):
        tuple(batch)

    return fruit_spigot.contents(), metal_spigot.contents()


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
                certain_kind_tap),
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


def run_distribute(in_path):
    """
    Run the composition of csv_file_consumer and information tap
    with the csv files in the input directory, and collect
    the results from each file and merge them together,
    printing both kinds of results.
    """

    results = distribute_run_to_runners(
        certain_kind_tap,
        in_path, reader=i_get_csv_data, batch_size=3)

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


def run_distribute_multi(in_dir):
    """
    Run the composition of csv_file_consumer and information tap
    with the csv files in the input directory, and collect
    the results from each file and merge them together,
    printing both kinds of results.
    """

    results = distribute_multi_run_to_runners(
        certain_kind_tap,
        in_dir, reader=i_get_csv_data, batch_size=3, filter_func=is_file_csv)

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

        python stream_searcher.py

    or::

        python stream_searcher.py --pool True

    or::

        python stream_searcher.py --in-dir test_data/things_kinds

    or::

        python stream_searcher.py --pool True --in-dir test_data/things_kinds
    """
    parser = argparse.ArgumentParser(*args)
    parser.add_argument("--in-dir",
                        default=os.path.join("test_data", "things_kinds"),
                        help="Data source directory")
    parser.add_argument("--in-file",
                        default=None,
                        help="Data source file")
    parser.add_argument("--pool", default=False)
    args = parser.parse_args()

    if args.in_file:
        run_distribute(args.in_file)
    else:
        run_distribute_multi(args.in_dir)
        # run(args.in_dir, args.pool)


if __name__ == "__main__":
    main()
