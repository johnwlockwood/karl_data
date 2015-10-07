from operator import methodcaller
import os
import tempfile
import unittest
from datetime import datetime
import json

from itertools import chain

from nose.plugins.attrib import attr
from karld import is_py3

str_upper = methodcaller('upper')


def collect_first(items):
    results = []
    for item in items:
        results.append(item[0])

    return results


class TestDistributeMulti(unittest.TestCase):
    @attr('integration')
    def test_default_reader(self):
        """
        Ensure results have given items_func, collect_first, applied to them.
        """
        import karld
        from karld.run_together import distribute_multi_run_to_runners
        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        test_results = distribute_multi_run_to_runners(
            collect_first,
            in_dir=input_path,
            batch_size=10,
            filter_func=karld.io.is_file_csv)

        list_results = list(test_results)

        self.assertEqual(len(list_results[0]), 10)
        self.assertEqual(len(list_results[1]), 5)

        if is_py3():
            self.assertEqual(
                sorted(chain.from_iterable(list_results)),
                [87, 97, 99, 99, 99, 100, 105,
                 109, 111, 112, 112, 114, 116, 116, 116]
            )
        else:
            self.assertEqual(
                sorted(chain.from_iterable(list_results)),
                ['W', 'a', 'c', 'c', 'c', 'd',
                 'i', 'm', 'o', 'p', 'p', 'r', 't', 't', 't'],
            )

    @attr('integration')
    def test_csv_reader(self):
        """
        Ensure results have the function applied to them.

        Ensure only csv files are read.

        Ensure the batches of results match batch_size.

        Ensure the given function is applied to the items
        of a batch.
        """
        import karld
        from karld.run_together import distribute_multi_run_to_runners
        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds")

        test_results = distribute_multi_run_to_runners(
            collect_first,
            in_dir=input_path,
            reader=karld.io.i_get_csv_data,
            batch_size=3,
            filter_func=karld.io.is_file_csv)
        list_results = list(test_results)

        self.assertEqual(len(list_results), 5)
        self.assertEqual(len(list_results[0]), 3)

        self.assertEqual(
            sorted(chain.from_iterable(list_results)),
            [u'W\u0104\u017b', u'apple', u'cat',
             u'celery', u'cheese', u'dr\xf3\u017ck\u0105',
             u'iron', u'mushroom', u'orange',
             u'peach', u'pear', u'ruby',
             u'titanium', u'tomato', u'topaz']
        )


def json_values(items):
    results = []
    for item in items:
        results.append(json.loads(item.decode()).get('first'))

    return results


class TestDistribute(unittest.TestCase):
    @attr('integration')
    def test_default_reader(self):
        """
        Ensure results have the items_func applied to them
        and were processed in batches of batch_size.
        """
        from karld.run_together import distribute_run_to_runners
        input_path = os.path.join(os.path.dirname(__file__),
                                  "test_data",
                                  "things_kinds",
                                  "people.json")

        test_results = distribute_run_to_runners(
            json_values,
            input_path,
            batch_size=1)

        self.assertEqual(
            test_results,
            [
                [u'John'],
                [u'Sally']
            ])


class TestCSVFileToFile(unittest.TestCase):
    @attr('integration')
    def test_csv_file_to_file_integration(self):
        """
        Ensure
        """
        from karld.loadump import file_path_and_name
        from karld.run_together import csv_file_to_file

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
                self.assertEqual(expected_data.splitlines(),
                                 contents.decode('utf-8').splitlines())

        if os.path.exists(expected_file):
            os.remove(expected_file)

    @attr('integration')
    def test_csv_file_consumer(self):
        """
        Ensure csv_file_consumer calls it's csv_rows_consumer
        with the csv rows from the file.
        """
        from karld.run_together import csv_file_consumer
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


