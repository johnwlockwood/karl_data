import os
import json

from itertools import chain
from itertools import count
from itertools import imap
from itertools import repeat
from itertools import starmap

from functools import partial

from operator import itemgetter

from karld.iter_utils import i_batch
from karld.unicode_io import csv_reader
from karld.unicode_io import get_csv_row_writer

LINE_BUFFER_SIZE = 5000
FILE_BUFFER_SIZE = 10485760  # -1  # 419430400
WALK_SUB_DIR = 0
WALK_FILES = 2


def ensure_dir(directory):
    """
    If directory doesn't exist, make it.

    :param directory: path to directory
    :type directory: str
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def ensure_file_path_dir(file_path):
    """
    Ensure the parent directory of the file path.

    :param file_path: Path to file.
    :type file_path: str
    """
    ensure_dir(os.path.abspath(os.path.dirname(file_path)))


def i_get_csv_data(file_name, *args, **kwargs):
    """A generator for reading a csv file.
    """
    buffering = kwargs.get('buffering', FILE_BUFFER_SIZE)
    with open(file_name, 'rb', buffering=buffering) as csv_file:
        reader = csv_reader(csv_file, *args, **kwargs)
        for row in reader:
            yield row


def write_as_csv(items, file_name, append=False,
                 line_buffer_size=None, buffering=FILE_BUFFER_SIZE,
                 get_csv_row_writer=get_csv_row_writer):
    """
    Writes out items to a csv file in groups.

    :param items: An iterable collection of collections.
    :param file_name: path to the output file.
    :param append: whether to append or overwrite the file.
    :param line_buffer_size: number of lines to write at a time.
    :param buffering: number of bytes to buffer files
    :type buffering: int
    :param get_csv_row_writer: callable that returns a csv row writer function,
     customize this for non-default options:
     `custom_writer = partial(get_csv_row_writer, delimiter="|");`
     `write_as_csv(items, 'my_out_file', get_csv_row_writer=custom_writer)`
    """
    if line_buffer_size is None:
        line_buffer_size = LINE_BUFFER_SIZE
    if append:
        mode = 'ab'
    else:
        mode = 'wtb'
    with open(file_name, mode, buffering=buffering) as csv_file:
        write_row = get_csv_row_writer(csv_file)
        batches = i_batch(line_buffer_size, items)
        for batch in batches:
            for row in batch:
                write_row(row)


def is_file_csv(file_path_name):
    """
    Is the file a csv file? Identify by extension.

    :param file_path_name:
    :type file_path_name: str
    """
    _, file_name = file_path_name
    return file_name[-4:].lower() == '.csv'


def dump_dicts_to_json_file(file_name, dicts, buffering=FILE_BUFFER_SIZE):
    """writes each dictionary in the dicts iterable
    to a line of the file as json.

    :param buffering: number of bytes to buffer files
    :type buffering: int
    """
    with open(file_name, 'w+', buffering=buffering) as json_file:
        for item in dicts:
            json_file.write(json.dumps(item) + "\n")


def split_file_output_json(filename, dict_list, max_lines=1100,
                           buffering=FILE_BUFFER_SIZE):
    """
    Split an iterable of JSON serializable rows of data
     into groups and write each to a shard.

    :param buffering: number of bytes to buffer files
    :type buffering: int
    """
    dirname = os.path.dirname(filename)
    basename = os.path.basename(filename)

    batches = i_batch(max_lines, dict_list)

    index = count()
    for group in batches:
        dump_dicts_to_json_file(
            os.path.join(dirname, "{0}_{1}".format(next(index), basename)),
            group,
            buffering=buffering)


def split_file_output_csv(filename, data, out_dir=None, max_lines=1100,
                          buffering=FILE_BUFFER_SIZE,
                          write_as_csv=write_as_csv):
    """
    Split an iterable of csv serializable rows of data
     into groups and write each to a csv shard.

    :param buffering: number of bytes to buffer files
    :type buffering: int
    """
    batches = i_batch(max_lines, data)

    dirname = os.path.abspath(os.path.dirname(filename))
    if out_dir is None:
        out_dir = dirname
    basename = os.path.basename(filename)

    index = count()
    for group in batches:
        write_as_csv(
            group,
            os.path.join(out_dir, "{0}_{1}".format(next(index), basename)),
            buffering=buffering
        )


def split_file_output(name, data, out_dir, max_lines=1100,
                      buffering=FILE_BUFFER_SIZE):
    """
    Split an iterable lines into groups and write each to
    a shard.

    :param name: Each shard will use this in it's name.
    :type name: str
    :param data: Iterable of data to write.
    :type data: iter
    :param out_dir: Path to directory to write the shards.
    :type out_dir: str
    :param max_lines: Max number of lines per shard.
    :type max_lines: int
    :param buffering: number of bytes to buffer files
    :type buffering: int
    """
    batches = i_batch(max_lines, data)

    index = count()
    for group in batches:
        file_path = os.path.join(out_dir,
                                 "{0}_{1}".format(next(index), name))
        with open(file_path, 'wt', buffering=buffering) as shard_file:
            shard_file.write("".join(group))


def raw_line_reader(file_object):
    return (line for line in file_object)


def split_file(file_path, out_dir=None, max_lines=200000,
               buffering=FILE_BUFFER_SIZE, line_reader=raw_line_reader,
               split_file_writer=split_file_output):
    """
    Opens then shards the file.

    :param file_path: Path to the large input file.
    :type file_path: str
    :param max_lines: Max number of lines in each shard.
    :type max_lines: int
    :param out_dir: Path of directory to put the shards.
    :type out_dir: str
    :param buffering: number of bytes to buffer files
    :type buffering: int
    """
    dir_name = os.path.abspath(os.path.dirname(file_path))

    # Use the name of the file to name the output
    base_name = os.path.basename(file_path)
    if out_dir is None:
        # Default the output directory to the same directory of
        # input file.
        out_dir = dir_name
    else:
        ensure_dir(out_dir)

    with open(file_path, 'r', buffering=buffering) as data_file:
        data = line_reader(data_file)
        split_file_writer(base_name, data, out_dir, max_lines=max_lines,
                          buffering=buffering)


split_multi_line_csv_file = partial(split_file,
                                    line_reader=csv_reader,
                                    split_file_writer=split_file_output_csv)
split_multi_line_csv_file.__doc__ = """
Split a large csv file without separating newlines in quotes. Runs slower
than split_file.
the csv reader and writer use the default dialect.
    customize this for non-default options:
     `custom_reader = partial(csv_reader, delimiter="|");`
     `split_multi_line_csv_file('input_file.csv', line_reader=custom_reader)`

     Writing the csv data with a non-default dialect requires defining
     a split_file_writer with a custom write_as_csv with a custom
     csv row writer factory.

     ```my_split_file_writer = partial(
            split_file_output_csv,
            write_as_csv=partial(
                write_as_csv,
                get_csv_row_writer=partial(
                    get_csv_row_writer, delimiter="|")))```
     `split_multi_line_csv_file('input_file.csv',
      split_file_writer=my_split_file_writer)`

"""


def file_path_and_name(path, base_name):
    """
    Join the path and base_name and yield it and the base_name.

    :param path: Directory path
    :type path: str
    :param base_name: File name
    :type base_name: str

    :return: `tuple` of file path and file name.
    """
    return os.path.join(path, base_name), base_name


def i_walk_dir_for_paths_names(root_dir):
    """
    Walks a directory yielding the directory of files
    and names of files.

    :param root_dir: path to a directory.
    :type root_dir: str
    """
    return chain.from_iterable(
        (
            imap(None, repeat(subdir), files)
            for subdir, files
            in imap(itemgetter(WALK_SUB_DIR, WALK_FILES), os.walk(root_dir))
        )
    )


def i_walk_dir_for_filepaths_names(root_dir):
    """
    Walks a directory yielding the paths and names
    of files.

    :param root_dir: path to a directory.
    :type root_dir: str
    """
    return starmap(file_path_and_name,
                   i_walk_dir_for_paths_names(root_dir))
