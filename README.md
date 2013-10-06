How To Data
======================

    1.) Split data.
    2.) Create a generator that will take the data as an iterator, yielding key,value pairs.
    3.) Sort each list of key/value pairs by the key.
    4.) Use heap to merge lists of key/value pairs by the key.
    5.) Group key/value pairs by the key.
    6.) Reduce each key grouped values to one value yielding a single key/value pair.

In lieu of a key, you may use a key function as long as it produces the
same key throughout the map-sort-merge-group phases.

Split data
----------------------

Use split_file to split up your data files.

    import os
    from karld.loadump import split_file

    big_file_names = [
        "bigfile1.csv",
        "bigfile2.csv",
        "bigfile3.csv"
    ]

    data_path = os.path.join('path','to','data', 'root')


    def main():
        for filename in big_file_names:
            # Name the directory to write the split files into.
            # I'll make it after the name of the file, removing the extension.
            out_dir = os.path.join(data_path, 'split_data', filename.replace('.csv', ''))

            # Split the file, with a default max_lines=200000 per shard of the file.
            split_file(os.path.join(data_path, filename), out_dir)


    if __name__ == "__main__":
        main()
