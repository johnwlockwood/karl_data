from functools import partial
import os

from karld.loadump import split_file_output_csv
from karld.loadump import split_multi_line_csv_file
from karld.loadump import write_as_csv
from karld.unicode_io import get_csv_row_writer

big_file_names = [
    "data.csv"
]

data_path = os.path.join('multiline')


def main():
    for filename in big_file_names:
        # Name the directory to write the split files into.
        # I'll make it after the name of the file, removing the extension.
        out_dir = os.path.join(os.path.dirname(__file__),
                               'split_data_ml', filename.replace('.csv', ''))

        in_file_path = os.path.join(data_path, filename)

        # Split the file, with a default max_lines=2 per shard of the file.
        split_multi_line_csv_file(in_file_path, out_dir, max_lines=2)

        # Split csv file writing with custom a delimiter
        my_split_file_writer = partial(
            split_file_output_csv,
            write_as_csv=partial(
                write_as_csv,
                get_csv_row_writer=partial(
                    get_csv_row_writer, delimiter="|")))

        out_dir = os.path.join(os.path.dirname(__file__),
                               'split_data_ml_pipe',
                               filename.replace('.csv', ''))

        split_multi_line_csv_file(in_file_path, out_dir, max_lines=2,
                                  split_file_writer=my_split_file_writer)


if __name__ == "__main__":
    main()