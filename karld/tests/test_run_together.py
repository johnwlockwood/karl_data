from operator import methodcaller
import os
import tempfile
import unittest
from datetime import datetime

from nose.plugins.attrib import attr
from karld import is_py3

str_upper = methodcaller('upper')


class TestCSVFileToFile(unittest.TestCase):
    @attr('integration')
    def test_csv_file_to_file_integration(self):
        """
        Ensure
        """
        from karld.loadump import file_path_and_name
        from ..run_together import csv_file_to_file

        out_dir = os.path.join(tempfile.gettempdir(),
                               "karld_test_csv_file_to_file")

        prefix = str(datetime.now())

        out_filename = "data_0.csv"
        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        def combiner(items):
            return items

        expected_file = os.path.join(out_dir,
                                     "{}{}".format(prefix, out_filename))

        if os.path.exists(expected_file):
            os.remove(expected_file)

        csv_file_to_file(combiner, prefix, out_dir, file_path_and_name(input_path, "data_0.csv"))

        self.assertTrue(os.path.exists(expected_file))

        expected_data = (b'mushroom,fungus\ntomato,fruit\ntopaz,mineral\n'
                         b'iron,metal\ndr\xc3\xb3\xc5\xbck\xc4\x85,'
                         b'utf-8 sample\napple,fruit\ncheese,dairy\n'
                         b'peach,fruit\ncelery,vegetable\n'.decode('utf-8'))

        if is_py3():
            with open(expected_file, 'rt') as result_file:
                contents = result_file.read()
                self.assertEqual(expected_data, contents)
        else:
            with open(expected_file, 'r') as result_file:
                contents = result_file.read()
                self.assertEqual(expected_data.splitlines(), contents.decode('utf-8').splitlines())

        if os.path.exists(expected_file):
            os.remove(expected_file)

    @attr('integration')
    def test_csv_file_consumer(self):
        """
        Ensure csv_file_consumer calls it's csv_rows_consumer
        with the csv rows from the file.
        """
        from ..run_together import csv_file_consumer
        from karld.loadump import file_path_and_name

        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        def combiner(items):
            return tuple(items)

        results = csv_file_consumer(combiner, file_path_and_name(
            input_path, "data_0.csv"))

        self.assertEqual(([u'mushroom', u'fungus'],
                         [u'tomato', u'fruit'],
                         [u'topaz', u'mineral'],
                         [u'iron', u'metal'],
                         [u'dr\xf3\u017ck\u0105', u'utf-8 sample'],
                         [u'apple', u'fruit'],
                         [u'cheese', u'dairy'],
                         [u'peach', u'fruit'],
                         [u'celery', u'vegetable']), results)


