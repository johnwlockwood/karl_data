from itertools import islice
import string
import unittest
from mock import patch, Mock

from ..run_together import csv_file_to_file


class TestCSVFileToFile(unittest.TestCase):
    def setUp(self):
        self.csv_contents = iter([
            'a,b',
            'c,d',
            'e,f',
        ])
        self.csv_list = (
            [u'a', u'b'],
            [u'c', u'd'],
            [u'e', u'f'],
        )

    @patch('karld.run_together.ensure_dir')
    @patch('karld.run_together.write_as_csv')
    @patch('karld.run_together.i_read_buffered_file')
    @patch('karld.run_together.csv_reader')
    def test_csv_file_to_file(self,
                              mock_csv_reader,
                              mock_file_reader,
                              mock_out_csv,
                              mock_ensure_dir):
        """
        Ensure csv_file_to_file ensures the out directory,
        then writes as csv to a filename the same as the input
        filename, but lowercase with a prefix and to the out directory
        the data from the input file as called with
        the csv_row_consumer.

        """
        def out_csv(rows, out_file):
            return list(islice(rows, 3))

        mock_out_csv.side_effect = out_csv
        mock_file_reader.return_value = self.csv_contents
        mock_csv_reader.return_value = self.csv_list

        def row_consumer(rows):
            for row in rows:
                yield map(string.upper, row)

        mock_row_consumer = Mock(side_effect=row_consumer)

        out_prefix = "yeah_"
        out_dir = "out"
        file_path_name = ("in/File.csv", "File.csv")
        csv_file_to_file(mock_row_consumer,
                         out_prefix,
                         out_dir,
                         file_path_name)

        mock_ensure_dir.assert_called_once_with("out")

        self.assertIn('out/yeah_file.csv', mock_out_csv.call_args[0])

        mock_row_consumer.assert_called_once_with(self.csv_list)
