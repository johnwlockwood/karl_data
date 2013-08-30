import csv
from itertools import ifilter, takewhile
import os
from karld.itertools_utils import grouper

LINE_BUFFER_SIZE = 5000


def i_get_csv_data(file_name, *args, **kwargs):
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
        line_groups = grouper(items, line_buffer_size, None)
        for line_group in line_groups:
            writer.writerows(takewhile(lambda x: x is not None, line_group))


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
        dict_group = takewhile(lambda x: x is not None, group)
        dump_dicts_to_json_file(
            os.path.join(dirname, "{0}_{1}".format(index, basename)),
            dict_group)


def split_file_output_csv(filename, dict_list, max_lines=1100):
    groups = grouper(dict_list, max_lines, fillvalue=None)
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)
    for index, group in enumerate(groups):
        dict_group = takewhile(lambda x: x is not None, group)
        write_as_csv(
            dict_group,
            os.path.join(dirname, "{0}_{1}".format(index, basename)))
