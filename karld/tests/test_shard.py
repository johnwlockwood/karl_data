# -*- coding: utf-8 -*-
from karld.path import i_walk_csv_paths

try:
    from itertools import ifilter
    from itertools import imap
except ImportError:
    imap = map
    ifilter = filter

from itertools import chain
import os
import tempfile
import unittest

from nose.plugins.attrib import attr
from karld import is_py3

if is_py3():
    unicode = str

from karld.loadump import i_get_csv_data, ensure_dir
from karld.loadump import split_file_output

@attr('integration')
class TestFileSharding(unittest.TestCase):
    def test_split_file_output(self):
        """
        Ensure when data is split then reassembled, it's
        all there.
        """
        data = list(map(lambda x: (unicode(x)+os.linesep).encode('utf-8'),
                        range(10000)))
        name = "big_data.csv"
        out_dir = os.path.join(tempfile.gettempdir(),
                               "karld_test_large_shard_data")
        for path in i_walk_csv_paths(str(out_dir)):
            if os.path.exists(path):
                os.remove(path)

        ensure_dir(out_dir)
        split_file_output(name, iter(data), out_dir)

        # Walk the sharded files numerically ordered.
        sorted_file_paths = sorted(
            i_walk_csv_paths(str(out_dir)),
            key=lambda x: int(os.path.basename(x).split("_")[0]))
        iterables = imap(i_get_csv_data, sorted_file_paths)

        items = list(map(lambda x: x[0]+os.linesep,
                         chain.from_iterable(iterables)))

        unicode_data = [value.decode('utf-8') for value in data]

        self.assertEqual(unicode_data, items)

        for path in i_walk_csv_paths(str(out_dir)):
            if os.path.exists(path):
                os.remove(path)

