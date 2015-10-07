from iter_karld_tools import yield_nth_of

from karld.loadump import is_file_csv
from karld.loadump import is_file_json
from karld.loadump import i_walk_dir_for_filepaths_names
from karld.merger import ifilter


def i_walk_csv_paths(input_dir):
    """
    Generator to yield the paths of csv files in the input directory.

    :param input_dir: path to the input directory
    """
    # Iterator of the filepaths and file names in the input directory
    file_path_names = i_walk_dir_for_filepaths_names(str(input_dir))

    # Iterator of just the csv files.
    csv_file_path_names = ifilter(is_file_csv, file_path_names)

    # Generator function that will yield just the paths
    return yield_nth_of(0, csv_file_path_names)


def i_walk_json_paths(input_dir):
    """
    Generator to yield the paths of json files in the input directory.

    :param input_dir: path to the input directory
    """
    # Iterator of the filepaths and file names in the input directory
    file_path_names = i_walk_dir_for_filepaths_names(str(input_dir))

    # Iterator of just the csv files.
    json_file_path_names = ifilter(is_file_json, file_path_names)

    # Generator function that will yield just the paths
    return yield_nth_of(0, json_file_path_names)
