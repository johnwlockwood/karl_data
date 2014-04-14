# -*- coding: utf-8 -*-
from datetime import datetime
from itertools import ifilter
from operator import itemgetter
import os
import tempfile
import unittest

from mock import patch
from nose.plugins.attrib import attr

from karld.loadump import ensure_dir
from karld.loadump import i_walk_dir_for_filepaths_names
from karld.loadump import is_file_csv
from karld.merger import sort_merge_group


class TestDirectoryFunctions(unittest.TestCase):
    """
    Test directory handling functions.
    """

    @patch('os.walk')
    def test_i_walk_dir_for_filepaths_names(self, mock_walk):
        """
        Ensure the file names paired with their paths
        are yielded for all the results returned by os.walk
        for the given root dir.
        """

        def fake_walk(root_dir):
            """
            Yield results like os.walk
            """
            yield ("dir0", ["dir1"], ["cat", "hat", "bat"])
            yield ("dir1", [], ["tin", "can"])

        mock_walk.side_effect = fake_walk

        walker = i_walk_dir_for_filepaths_names("fake")

        self.assertEqual(next(walker), ("dir0/cat", "cat"))
        self.assertEqual(next(walker), ("dir0/hat", "hat"))
        self.assertEqual(next(walker), ("dir0/bat", "bat"))
        self.assertEqual(next(walker), ("dir1/tin", "tin"))
        self.assertEqual(next(walker), ("dir1/can", "can"))

        mock_walk.assert_called_once_with("fake")

    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_ensure_dir_makesdirs_with_none(self, mock_makedirs, mock_exists):
        """
        When a directory doesn't exist according to os.path.exists
        call makedirs with it.
        """
        mock_exists.return_value = False

        directory = "a/directory"

        ensure_dir(directory)

        mock_exists.assert_called_once_with(directory)
        mock_makedirs.assert_called_once_with(directory)

    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_ensure_dir_not_makesdirs_when_exists(self,
                                                  mock_makedirs,
                                                  mock_exists):
        """
        When a directory exists according to os.path.exists
        do not call makedirs with it.
        """
        mock_exists.return_value = True

        directory = "a/directory"

        ensure_dir(directory)

        mock_exists.assert_called_once_with(directory)
        self.assertFalse(mock_makedirs.call_count)


def combine_things(iterables):
    """
    Example iterables combiner function.
    The data files contain rows such as:
    pear, fruit
    cat, animal

    Where the first column is the thing and the second is kind.

    This will sort the rows of each file by kind,
    then merge all the rows of each file into one sorted
    iterable, then group them by kind and finally
    yield each item from each group.

    :param iterables: An iterable of iterable values.
    """
    THING_KIND = 1
    grouped_items = sort_merge_group(iterables,
                                     key=itemgetter(THING_KIND))
    for group in grouped_items:
        for item in sorted(group[1]):
            yield item


@attr('integration')
class TestSplitData(unittest.TestCase):
    def test_data_splits(self):

        from karld.loadump import split_file

        input_data_path = os.path.join(os.path.dirname(__file__),
                                 "test_data",
                                 "things_kinds",
                                 "data_0.csv")

        out_dir = os.path.join(tempfile.gettempdir(),
                               "karld_test_split_data")

        split_file(input_data_path, out_dir, max_lines=5)

        self.assertTrue(os.path.exists(os.path.join(out_dir, "0_data_0.csv")))
        self.assertTrue(os.path.exists(os.path.join(out_dir, "1_data_0.csv")))






@attr('integration')
class TestFileSystemIntegration(unittest.TestCase):
    """
    Integration tests against the filesystem.
    """

    def test_sort_merge_csv_files_to_file(self):
        """
        Ensure csv_files_to_file will read multiple
        csv files and write one csv file
        with the contents as yielded from the
        given combiner function.

        Ensure i_walk_dir_for_filepaths_names produces
        the paths and basenames of the files in the
        test_data directory.
        """
        from karld.run_together import csv_files_to_file

        out_dir = os.path.join(tempfile.gettempdir(),
                               "karld_test_sort_merge")

        prefix = str(datetime.now())

        out_filename = "things_combined.csv"
        input_dir = os.path.join(os.path.dirname(__file__),
                                 "test_data",
                                 "things_kinds")

        file_path_names = i_walk_dir_for_filepaths_names(input_dir)

        csv_file_path_names = ifilter(
            is_file_csv,
            file_path_names)

        csv_files_to_file(
            combine_things,
            prefix,
            out_dir,
            out_filename,
            csv_file_path_names)

        expected_file = os.path.join(out_dir,
                                     "{}{}".format(prefix, out_filename))

        self.assertTrue(os.path.exists(expected_file))

        with open(expected_file) as result_file:
            contents = result_file.read()
            self.assertEqual(
                ['cat,animal',
                 'cheese,dairy',
                 'apple,fruit',
                 'orange,fruit',
                 'peach,fruit',
                 'pear,fruit',
                 'tomato,fruit',
                 'mushroom,fungus',
                 'iron,metal',
                 'titanium,metal',
                 'ruby,mineral',
                 'topaz,mineral',
                 'WĄŻ,utf-8 sample',
                 'dróżką,utf-8 sample',
                 'celery,vegetable'],
                contents.splitlines())
