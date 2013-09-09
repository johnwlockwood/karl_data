How To Big Data
======================

    1.) Split data.
    2.) Create a generator that will take the data as an iterator, yielding key,value pairs.
    3.) Sort each list of key/value pairs by the key.
    4.) Use heap to merge lists of key/value pairs by the key.
    5.) Group key/value pairs by the key.
    6.) Reduce each key grouped values to one value yielding a single key/value pair.

In lieu of a key, you may use a key function as long as it produces the
same key throughout the map-sort-merge-group phases.
