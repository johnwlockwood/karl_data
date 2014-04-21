import os

import karld

big_file_names = [
    "data.csv"
]

data_path = os.path.join('non-multiline')


def main():
    for filename in big_file_names:
        # Name the directory to write the split files into.
        # I'll make it after the name of the file, removing the extension.
        out_dir = os.path.join(os.path.dirname(__file__),
                               'split_data_non_multiline', filename.replace('.csv', ''))

        # Split the file, with a default max_lines=200000 per shard of the file.
        karld.io.split_file(os.path.join(data_path, filename), out_dir, max_lines=2)


if __name__ == "__main__":
    main()
