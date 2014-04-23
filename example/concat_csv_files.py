from __future__ import print_function
from itertools import ifilter
from itertools import chain

import karld
from karld.loadump import i_walk_dir_for_filepaths_names
from karld.run_together import csv_files_to_file


def main():
    """
    Concatenate csv files together in no particular order.
    """
    import pathlib

    input_dir = pathlib.Path('test_data/things_kinds')

    file_path_names = i_walk_dir_for_filepaths_names(str(input_dir))

    csv_file_path_names = ifilter(karld.io.is_file_csv, file_path_names)

    out_prefix = ""
    out_dir = pathlib.Path('out_data/things_kinds')
    out_filename = "combined_things.csv"

    csv_files_to_file(
        chain.from_iterable,
        out_prefix,
        str(out_dir),
        out_filename,
        csv_file_path_names)


if __name__ == "__main__":
    main()
