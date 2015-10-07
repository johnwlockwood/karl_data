from functools import partial
from operator import methodcaller

try:
    from itertools import chain
    from itertools import ifilter
    from itertools import imap
except ImportError:
    imap = map
    ifilter = filter

import os

from iter_karld_tools import i_batch
from iter_karld_tools import yield_nth_of

from karld.loadump import ensure_dir
from karld.loadump import i_get_csv_data
from karld.loadump import i_walk_dir_for_filepaths_names
from karld.loadump import i_read_buffered_binary_file
from karld.loadump import write_as_csv


def csv_file_consumer(csv_rows_consumer, file_path_name):
    """
    Consume the file at file_path_name as a csv file, passing
    it through csv_rows_consumer.

    :param csv_rows_consumer: consumes data_items yielding collection for each
    :type csv_rows_consumer: callable
    :param file_path_name: path to input csv file
    :type file_path_name: str, str

    """
    data_path, data_file_name = file_path_name
    data_items = i_get_csv_data(data_path)
    return csv_rows_consumer(data_items)


def csv_file_to_file(csv_rows_consumer, out_prefix, out_dir, file_path_name):
    """
    Consume the file at file_path_name as a csv file, passing
    it through csv_rows_consumer, writing the results
    as a csv file into out_dir as the same name, lowered, and prefixed.

    :param csv_rows_consumer: consumes data_items yielding collection for each
    :type csv_rows_consumer: callable
    :param out_prefix: prefix out_file_name
    :type out_prefix: str
    :param out_dir: directory to write output file to
    :type out_dir: str
    :param file_path_name: path to input csv file
    :type file_path_name: str, str

    """
    data_path, data_file_name = file_path_name
    data_items = i_get_csv_data(data_path)
    ensure_dir(out_dir)
    out_filename = os.path.join(
        out_dir, '{}{}'.format(
            out_prefix, data_file_name.lower()))
    write_as_csv(csv_rows_consumer(data_items), out_filename)


def multi_in_single_out(rows_reader,
                        rows_writer,
                        rows_iter_consumer,
                        out_url,
                        in_urls_func):
    """
    Multi input combiner.

    :param rows_reader: function to read a file path and returns an iterator
    :param rows_writer: function to write values
    :param rows_iter_consumer: function takes iter. of iterators returns iter.
    :param out_url: url for the rows_writer to write to.
    :param in_urls_func: function generates iterator of input urls.
    """
    data_items_iter = (rows_reader(data_path) for data_path in in_urls_func())
    rows_writer(rows_iter_consumer(data_items_iter), out_url)


def csv_files_to_file(csv_rows_consumer,
                      out_prefix,
                      out_dir,
                      out_file_name,
                      file_path_names):
    """
    Consume the file at file_path_name as a csv file, passing
    it through csv_rows_consumer, writing the results
    as a csv file into out_dir as the same name, lowered, and prefixed.

    :param csv_rows_consumer: consumes data_items yielding collection for each
    :param out_prefix: prefix out_file_name
    :type out_prefix: str
    :param out_dir: Directory to write output file to.
    :type out_dir: str
    :param out_file_name: Output file base name.
    :type out_file_name: str
    :param file_path_names: tuple of paths and basenames to input csv files
    :type file_path_names: str, str
    """
    ensure_dir(out_dir)
    out_filename = os.path.join(
        out_dir, '{}{}'.format(
            out_prefix, out_file_name.lower()))

    in_urls_func = partial(yield_nth_of, 0, file_path_names)

    multi_in_single_out(i_get_csv_data,
                        write_as_csv,
                        csv_rows_consumer,
                        out_filename,
                        in_urls_func)


def pool_run_files_to_files(file_to_file, in_dir, filter_func=None):
    """
    With a multi-process pool, map files in in_dir over
    file_to_file function.

    :param file_to_file: callable that takes file paths.
    :param in_dir: path to process all files from.
    :param filter_func: Takes a tuple of path and base \
    name of a file and returns a bool.
    :returns: A list of return values from the map.
    """
    from concurrent.futures import ProcessPoolExecutor

    results = i_walk_dir_for_filepaths_names(in_dir)
    if filter_func:
        results_final = ifilter(filter_func, results)
    else:
        results_final = results

    with ProcessPoolExecutor() as pool:
        return list(pool.map(file_to_file, results_final))


def distribute_run_to_runners(items_func, in_url, reader=None, batch_size=1100):
    """
    With a multi-process pool, map batches of items from
    file to an items processing function.

    The reader callable should be as fast as possible to
    reduce data feeder cpu usage. It should do the minimal
    to produce discrete units of data, save any decoding
    for the items function.

    :param items_func: Callable that takes multiple items of the data.
    :param reader: URL reader callable.
    :param in_url: Url of content
    :param batch_size: size of batches.
    """
    from concurrent.futures import ProcessPoolExecutor

    if not reader:
        reader = i_read_buffered_binary_file

    stream = reader(in_url)
    batches = i_batch(batch_size, stream)

    with ProcessPoolExecutor() as pool:
        return list(pool.map(items_func, batches))


def distribute_multi_run_to_runners(items_func, in_dir,
                                    reader=None,
                                    walker=None,
                                    batch_size=1100,
                                    filter_func=None):
    """
    With a multi-process pool, map batches of items from
    multiple files to an items processing function.

    The reader callable should be as fast as possible to
    reduce data feeder cpu usage. It should do the minimal
    to produce discrete units of data, save any decoding
    for the items function.

    :param items_func: Callable that takes multiple items of the data.
    :param reader: URL reader callable.
    :param walker: A generator that takes the in_dir URL and emits
     url, name tuples.
    :param batch_size: size of batches.
    :param filter_func: a function that returns True for desired paths names.
    """
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import cpu_count

    if not reader:
        reader = i_read_buffered_binary_file

    if not walker:
        walker = i_walk_dir_for_filepaths_names

    paths_names = walker(in_dir)
    if filter_func:
        paths_names_final = ifilter(filter_func, paths_names)
    else:
        paths_names_final = paths_names

    stream = chain.from_iterable(
        (reader(in_url) for in_url, name in paths_names_final))
    batches = i_batch(batch_size, stream)

    n_cpus = cpu_count()
    max_workers = (n_cpus-1) or 1
    max_in_queue = int(n_cpus * 1.5)
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        while True:
            if len(pool._pending_work_items) < max_in_queue:
                try:
                    batch = next(batches)
                    futures.append(pool.submit(items_func, batch))
                except StopIteration:
                    break

    def results():
        """Generator that yield results of futures
        that are done. If not done yet, it skips it.
        """
        while futures:
            for index, future in enumerate(futures):
                if future.done():
                    yield future.result()
                    del futures[index]
                    break

    return results()


def serial_run_files_to_files(file_to_file, in_dir, filter_func=None):
    """
    With a map files in in_dir over the file_to_file function.

    Using this to debug your file_to_file function can
    make it easier.

    :param file_to_file: callable that takes file paths.
    :param in_dir: path to process all files from.
    :param filter_func: Takes a tuple of path and base \
    name of a file and returns a bool.
    :returns: A list of return values from the map.
    """
    results = i_walk_dir_for_filepaths_names(in_dir)
    if filter_func:
        results_final = ifilter(filter_func, results)
    else:
        results_final = results

    return list(map(file_to_file, results_final))
