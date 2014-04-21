import os

import karld

big_file_names = [
    "data.csv"
]

data_path = os.path.join('multiline')


def main():
    for filename in big_file_names:
        # Name the directory to write the split files into based
        # on the name of the file.
        out_dir = os.path.join(os.path.dirname(__file__),
                               'split_data_ml', filename.replace('.csv', ''))

        in_file_path = os.path.join(data_path, filename)

        # Split the file, with a default max_lines=2 per shard of the file.
        karld.io.split_csv_file(in_file_path, out_dir, max_lines=2)


if __name__ == "__main__":
    main()
