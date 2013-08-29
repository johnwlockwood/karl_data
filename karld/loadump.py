import csv
from itertools import ifilter, izip_longest
import logging
import os


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def i_get_csv_data(file_name, *args, **kwargs):
    with open(file_name, 'r') as csv_file:
        reader = csv.reader(csv_file, *args, **kwargs)
        for row in reader:
            yield row


def write_as_csv(items, file_name, append=False):
    """
    Writes out tuples to a csv file
    """
    if append:
        mode = 'w+'
    else:
        mode = 'w'
    with open(file_name, mode) as csv_file:
        writer = csv.writer(csv_file)
        for item in items:
            writer.writerow(item)


def dump_dicts_to_json_file(file_name, dicts):
    """writes each dictionary in the dicts iterable
    to a line of the file as json."""
    with open(file_name, 'w+') as json_file:
        for item in dicts:
            json_file.write(json.dumps(item)+"\n")


def split_file_output_json(filename, dict_list, max_lines=1100):
    groups = grouper(dict_list, max_lines, fillvalue=None)
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    for index, group in enumerate(groups):
        dict_group = ifilter(None, group)
        dump_dicts_to_json_file(
            os.path.join(dirname, "{0}_{1}".format(index, basename)),
            dict_group)


def split_file_output_csv(filename, dict_list, max_lines=1100):
    groups = grouper(dict_list, max_lines, fillvalue=None)
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    for index, group in enumerate(groups):
        dict_group = ifilter(None, group)
        write_as_csv(
            dict_group,
            os.path.join(dirname, "{0}_{1}".format(index, basename)))
