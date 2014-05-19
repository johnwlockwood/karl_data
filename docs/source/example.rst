example scripts
================

:mod:`split_multiline` Module
----------------------------------

Run this script first to split the example data, which has multiple lines in some fields.

.. automodule:: split_multiline
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`clean` Module
----------------------------------

Run this script to 'clean' the split up data.

.. automodule:: clean
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`split_non_multiline` Module
----------------------------------

Run this script first to split the example data that does not have any multiple line in fields.

.. automodule:: split_non_multiline
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`shard_data` Module
----------------------------------

Shard out data to files.

.. automodule:: shard_data
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`shard_to_csv` Module
----------------------------------

Shard out data to csv files.

.. automodule:: shard_to_csv
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`shard_to_json` Module
----------------------------------

Shard out data to files as rows of JSON.

.. automodule:: shard_to_json
    :members:
    :undoc-members:
    :show-inheritance:

:mod:`consume_csv_file` Module
----------------------------------

Iteratively consume csv file.

.. automodule:: consume_csv_file
    :members:
    :undoc-members:
    :show-inheritance:


:mod:`consume_many_csv_files` Module
-------------------------------------------

Consume the items of a directory of csv files as if they were
one file.

.. automodule:: consume_many_csv_files
    :members:
    :undoc-members:
    :show-inheritance:


:mod:`concat_csv_files` Module
-------------------------------------------

Concatenate all the csv files in a directory together.

.. automodule:: concat_csv_files
    :members:
    :undoc-members:
    :show-inheritance:


:mod:`merge_small_csv_files` Module
-------------------------------------------

Merge a number of homogeneous small csv files on a key.
    Small means they all together fit in
    your computer's memory.

.. automodule:: merge_small_csv_files
    :members:
    :undoc-members:
    :show-inheritance:


:mod:`tap_example` Module
-------------------------------------------

Uses tap to get information from a stream
of data in csv files.

.. automodule:: tap_example
    :members:
    :undoc-members:
    :show-inheritance:



:mod:`stream_searcher` Module
-------------------------------------------

Uses tap to get information from a stream
of data in csv files in designated directory with
optional multi-processing.

.. automodule:: stream_searcher
    :members:
    :undoc-members:
    :show-inheritance:
