from __future__ import print_function
import csv
from itertools import takewhile
import json
import os
from itertools_recipes import grouper

LINE_BUFFER_SIZE = 5000


def i_get_csv_data(file_name, *args, **kwargs):
    """A generator for reading a csv file.
    """
    with open(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file, *args, **kwargs)
        for row in reader:
            yield row


def write_as_csv(items, file_name, append=False, line_buffer_size=None):
    """
    Writes out tuples to a csv file
    """
    if line_buffer_size is None:
        line_buffer_size = LINE_BUFFER_SIZE
    if append:
        mode = 'w+'
    else:
        mode = 'wt'
    with open(file_name, mode) as csv_file:
        writer = csv.writer(csv_file)
        line_groups = grouper(line_buffer_size, items, None)
        for line_group in line_groups:
            writer.writerows(takewhile(lambda x: x is not None, line_group))


def dump_dicts_to_json_file(file_name, dicts):
    """writes each dictionary in the dicts iterable
    to a line of the file as json."""
    with open(file_name, 'w+') as json_file:
        for item in dicts:
            json_file.write(json.dumps(item)+"\n")


def split_file_output_json(filename, dict_list, max_lines=1100):
    """
    Split an iterable of JSON serializable rows of data
     into groups and write each to a shard.
    """
    fill_object = object()
    groups = grouper(max_lines, dict_list, fillvalue=fill_object)
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    for index, group in enumerate(groups):
        dict_group = takewhile(lambda x: x is not fill_object, group)
        dump_dicts_to_json_file(
            os.path.join(dirname, "{0}_{1}".format(index, basename)),
            dict_group)


def split_file_output_csv(filename, data, max_lines=1100, out_dir=None):
    """
    Split an iterable of csv serializable rows of data
     into groups and write each to a csv shard.
    """
    fill_object = object()
    groups = grouper(max_lines, data, fillvalue=fill_object)
    dirname = os.path.abspath(os.path.dirname(filename))
    if out_dir is None:
        out_dir = dirname
    basename = os.path.basename(filename)
    for index, group in enumerate(groups):
        dict_group = takewhile(lambda x: x is not fill_object, group)
        write_as_csv(
            dict_group,
            os.path.join(out_dir, "{0}_{1}".format(index, basename)))


def split_file_output(name, data, out_dir, max_lines=1100):
    """
    Split an iterable lines into groups and write each to
    a shard.
    :param name: Each shard will use this in it's name.
    :param data: `iterable` of data to write.
    :param out_dir: Path to directory to write the shards.
    :param max_lines: Max number of lines per shard.
    """
    fill_object = object()
    groups = grouper(max_lines, data, fillvalue=fill_object)
    for index, group in enumerate(groups):
        lines = takewhile(lambda x: x is not fill_object, group)
        with open(os.path.join(out_dir, "{0}_{1}".format(
                index, name)), 'wt') as shard_file:
            shard_file.write("".join(lines))


def split_file(file_path, out_dir=None, max_lines=200000):
    """
    Opens then shards the file.
    :param file_path: `str` Path to the large input file.
    :param max_lines: `int` Max number of lines in each shard.
    :param out_dir: `str` Path of directory to put the shards.
    """
    dir_name = os.path.abspath(os.path.dirname(file_path))

    # Use the name of the file to name the output
    base_name = os.path.basename(file_path)
    if out_dir is None:
        # Default the output directory to the same directory of
        # input file.
        out_dir = dir_name
    else:
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

    with open(file_path, 'r') as data_file:
        data = (line for line in data_file)
        split_file_output(base_name, data, out_dir, max_lines=max_lines)


def i_walk_dir_for_filepaths_names(root_dir):
    """
    Walks a directory yielding the paths and names
    of files.

    :param root_dir: :class: `str` path to a directory.
    """
    for subdir, dirs, files in os.walk(root_dir):
        for fi in files:
            yield os.path.join(subdir, fi), fi
