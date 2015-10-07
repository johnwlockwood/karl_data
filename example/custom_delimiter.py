#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from functools import partial
import os

import karld

from karld.unicode_io import get_csv_row_writer


def main():
    """
    When split_csv_file is writing out csv data, you may
    need to use custom settings such as a different
    field delimiter. This is achieved by giving it a custom
    write_as_csv function which is given a custom get_csv_row_writer
    which is where you set the custom delimiter.
    """
    big_file_names = [
        "data.csv"
    ]
    data_path = os.path.join('multiline')

    # Set the delimiter keyword argument on the csv row writer
    my_row_writer = partial(get_csv_row_writer, delimiter="|")

    # Set the write_as_csv with the custom row writer.
    my_write_as_csv = partial(karld.io.write_as_csv,
                              get_csv_row_writer=my_row_writer)

    # Split csv file writing with custom a write_as_csv which does
    #  so with the '|' delimiter
    my_split_file_writer = partial(karld.io.split_file_output_csv,
                                   write_as_csv=my_write_as_csv)

    for filename in big_file_names:
        in_file_path = os.path.join(data_path, filename)

        out_dir = os.path.join(os.path.dirname(__file__),
                               'split_data_ml_pipe',
                               filename.replace('.csv', ''))

        karld.io.split_csv_file(in_file_path, out_dir, max_lines=2,
                                split_file_writer=my_split_file_writer)


if __name__ == "__main__":
    main()
