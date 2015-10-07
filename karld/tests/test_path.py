import os
import unittest
from karld.path import i_walk_csv_paths
from karld.path import i_walk_json_paths

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


@attr('integration')
class TestIWalkJson(unittest.TestCase):
    def test_json_only(self):
        """
        Ensure only the paths of the json files
        are returned.
        """

        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        json_paths = list(i_walk_json_paths(input_path))
        self.assertEqual(2, len(json_paths))
        for path in json_paths:
            self.assertIn('.json', path)

