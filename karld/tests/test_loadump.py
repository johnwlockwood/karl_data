import unittest
from mock import patch
from karld.loadump import i_walk_dir_for_filepaths_names


class TestDirectoryWalker(unittest.TestCase):
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

