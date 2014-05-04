# -*- coding: utf-8 -*-
from collections import deque
from itertools import islice
import unittest
import os
from karld.record_reader.app_engine import log_reader


class TestMultiLineRecords(unittest.TestCase):
    """
    Test use of multi_line_records.
    """
    def setUp(self):
        self.root_data = os.path.join(os.path.dirname(__file__),
                                            "test_data",
                                            "logs")

        self.input_data_path = os.path.join(self.root_data,
                                            "example.logs")

        self.last_start_single = os.path.join(self.root_data,
                                              "last_line_single.logs")


    def test_read_logs(self):
        """
        Ensure a deque is yielded for each log record.

        Ensure last line of last record is included
        when not a start line.
        """
        records = log_reader(self.input_data_path)

        results = list(records)

        self.assertEqual(4, len(results))

        self.assertEqual(
            deque([u'record 1\n', u'\tline 1\n',
                   u'\tline 2\n', u'\tline 3\n']),
            results[0])

        self.assertEqual(
            deque([u'record 4\n', u'\tline 1\n', u'\tline 2\n']),
            results[3])

    def test_last_start_single(self):
        """
        Ensure the last line is included as a record
        if it is a start line.
        """
        records = log_reader(self.last_start_single)

        results = list(islice(records, 3))

        self.assertEqual(deque([u'record 4\n']), next(records))
