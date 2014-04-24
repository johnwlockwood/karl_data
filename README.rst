How To Data
======================

1. Split data.
2. Create a generator that will take the data as an iterator, yielding key,value pairs.
3. Sort each list of key/value pairs by the key.
4. Use heap to merge lists of key/value pairs by the key.
5. Group key/value pairs by the key.
6. Reduce each key grouped values to one value yielding a single key/value pair.

In lieu of a key, you may use a key function as long as it produces the
same key throughout the map-sort-merge-group phases.

Split data
----------------------

Use split_file to split up your data files or use split_csv_file to split up
csv files which may have multi-line fields to ensure they are not broken up.::

    import os

    import karld

    big_file_names = [
        "bigfile1.csv",
        "bigfile2.csv",
        "bigfile3.csv"
    ]

    data_path = os.path.join('path','to','data', 'root')


    def main():
        for filename in big_file_names:
            # Name the directory to write the split files into based
            # on the name of the file.
            out_dir = os.path.join(data_path, 'split_data', filename.replace('.csv', ''))

            # Split the file, with a default max_lines=200000 per shard of the file.
            karld.io.split_csv_file(os.path.join(data_path, filename), out_dir)


    if __name__ == "__main__":
        main()


When you're generating data and want to shard it out to files based on quantity, use
one of the split output functions such as ``split_file_output_csv``, ``split_file_output`` or
``split_file_output_json``::

    import os
    import pathlib

    import karld


    def main():
        """
        Python 2 version
        """

        items = (str(x) + os.linesep for x in range(2000))

        out_dir = pathlib.Path('shgen')
        karld.io.ensure_dir(str(out_dir))

        karld.io.split_file_output('big_data', items, str(out_dir))


    if __name__ == "__main__":
        main()

CSV serializable data::

    import pathlib

    import karld


    def main():
        """
        From a source of data, shard it to csv files.
        """
        if karld.is_py3():
            third = chr
        else:
            third = unichr

        # Your data source
        items = ((x, x + 1, third(x + 10)) for x in range(2000))

        out_dir = pathlib.Path('shard_out_csv')

        karld.io.ensure_dir(str(out_dir))

        karld.io.split_file_output_csv('big_data.csv', items, str(out_dir))


    if __name__ == "__main__":
        main()


Rows of json serializable data::

    import pathlib

    import karld


    def main():
        """
        From a source of data, shard it to csv files.
        """
        if karld.is_py3():
            third = chr
        else:
            third = unichr

        # Your data source
        items = ((x, x + 1, third(x + 10)) for x in range(2000))

        out_dir = pathlib.Path('shard_out_json')

        karld.io.ensure_dir(str(out_dir))

        karld.io.split_file_output_json('big_data.json', items, str(out_dir))


    if __name__ == "__main__":
        main()


Documentation
===============================

Read the docs: http://karld.readthedocs.org/en/latest/

Expanded Getting Started at http://karld.readthedocs.org/en/latest/getting-started.html.

More examples are documented at http://karld.readthedocs.org/en/latest/source/example.html. View
the source of the example files, for examples...


Contributing:
==================
Submit any issues or questions here: https://github.com/johnwlockwood/karl_data/issues.

Make pull requests to **development** branch of
 https://github.com/johnwlockwood/karl_data.

**Documentation** is written in reStructuredText and currently uses the
 Sphinx style for field
 lists http://sphinx-doc.org/domains.html#info-field-lists

Check out closed pull requests to see the flow of development, as almost
every change to master is done via a pull request on **GitHub**. Code Reviews
are welcome, even on merged Pull Requests. Feel free to ask questions about
the code.
