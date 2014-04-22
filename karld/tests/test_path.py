import os
import unittest
from karld.path import i_walk_csv_paths

from nose.plugins.attrib import attr


@attr('integration')
class TestIWalkCsv(unittest.TestCase):
    def test_csv_only(self):
        """
        Ensure only the paths of the csv files
        are returned.
        """

        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        csv_paths = list(i_walk_csv_paths(input_path))
        self.assertEqual(2, len(csv_paths))
        for path in csv_paths:
            self.assertIn('.csv', path)

