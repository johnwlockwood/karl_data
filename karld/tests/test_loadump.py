import unittest
from mock import patch
from ..loadump import ensure_dir
from ..loadump import i_walk_dir_for_filepaths_names


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
