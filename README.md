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


Organize the work
-----------------------

Some kinds of work to separate:
    Row transforms
    Sorting, Grouping, Merging
    Building Lookups
    Using Lookups. Flattening looked up values into rows
    Filtering
    Reducing
    Writing out

Row Transforms can be spread across as many processors you have,
because they are handled independently.

If you are sorting more data than can fit into memory,
you could build an index of the key:
    for each split file,
        extract the key from each
            sort that list with it's position in the file indexed
        sort each on a key and write it out to a file
            get the file object of each file
            start a generator feeding each into a heapq merge
            write out the results, splitting at key boundary after
            a size limit(guideline).
    Indexes of these files can be made by reading over
    them and extracting a key with it's file offset.
        Could OT(operational transforms) help update an index?
    or simply grouping on a key with specific small data for lookup.
        person 1
        person 2

        person cases
        ------
             1: (case 1($10), case 2($100), case 3($20))
             2: (case 5($15), case 45($50), case 56($85))



        person 1 case_total: $130
        person 2 case_total: $150

    The merge data can be in it's own files sorted on the key.
    What if it's merged with a non-primary key of the main record? Column
    4 is a color code and it needs to fill in the name of the color from
    a table. Well that lookup could just be in memory, most likely an enum.
    If the process for a record needs to aggregate data in a bag under
    it's key, it can access the part of the file with that data, or have
    the bag loaded into memory concurrent with the main record.
    load a number of persons, and load the corresponding bags for that range
    of person keys, merge, group and aggregate.

