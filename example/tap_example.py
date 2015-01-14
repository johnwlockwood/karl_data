#!/usr/bin/python
# _*_ coding: utf-8 _*_
from functools import partial
import os

from karld.iter_utils import i_batch

from karld.loadump import is_file_csv
from karld.run_together import csv_file_consumer
from karld.run_together import pool_run_files_to_files
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
    :param data_items: A sequence of unicode strings
    """
    fruit_spigot = Bucket(get_fruit)
    metal_spigot = Bucket(get_metal)

    items = stream_tap((fruit_spigot, metal_spigot), data_items)

    for batch in i_batch(100, items):
        tuple(batch)

    return fruit_spigot.contents(), metal_spigot.contents()


def run(in_dir):
    """
    Run the composition of csv_file_consumer and information tap
    with the csv files in the input directory, and collect
    the results from each file and merge them together,
    printing both kinds of results.

    :param in_dir: directory of input csv files.
    """
    files_to_files_runner = pool_run_files_to_files

    results = files_to_files_runner(
        partial(csv_file_consumer, certain_kind_tap),
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

if __name__ == "__main__":
    run(os.path.join("test_data", "things_kinds"))
