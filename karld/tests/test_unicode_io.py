# -*- coding: utf-8 -*-
import unittest

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from karld.unicode_io import csv_unicode_reader
from karld.unicode_io import csv_reader
from karld.unicode_io import get_csv_row_writer


class TestUnicodeToUnicodeCSVReader(unittest.TestCase):
    """
    Ensure the unicode csv reader is iterative,
    and consumes an iterator of unicode strings
    that are delimited and split them into
    sequences of unicode strings.
    """

    def test_unicode_to_csv_unicode(self):
        """
        Ensure that a stream of unicode strings
        are converted to sequences by parsing
        with the csv module.
        """
        data = u'this,is,"a\na",test\nmy,name,is,john'
        stream = StringIO(data)

        self.assertEqual(
            [
                [u'this', u'is', u'a\na', u'test'],
                [u'my', u'name', u'is', u'john'],
            ],
            list(csv_unicode_reader(stream)))

    def test_iterative(self):
        """
        Ensure csv_unicode_reader consumes iteratively.
        """

        data = iter([u"this,is,a,test",
                     u"my,name,is,john"])
        reader = csv_unicode_reader(data)
        first_row = next(reader)
        self.assertEqual([u'this', u'is', u'a', u'test'], first_row)
        self.assertEqual(u"my,name,is,john", next(data))


class TestCSVToUnicodeReader(unittest.TestCase):
    """
    Ensure unicode reader reads contents of
    a file iteratively and produces unicode sequences.
    """

    def setUp(self):
        self.data = ["WĄŻ,utf-8 sample",
                     "dróżką,utf-8 sample"]

    def test_unicoded(self):
        """
        Ensure utf-8 strings in the data
        are converted to unicode sequences.
        """
        rows = list(csv_reader(self.data))

        self.assertEqual(
            [
                [u"WĄŻ", u"utf-8 sample"],
                [u"dróżką", u"utf-8 sample"]
            ],
            rows)

    def test_iterative(self):
        """Ensure unicode reader consumes the data
        iteratively.
        """
        data = iter(self.data)
        rows = csv_reader(data)
        first_row = next(rows)
        self.assertEqual([u"WĄŻ", u"utf-8 sample"],
                         first_row)

        remaining_data = next(data)
        self.assertEqual("dróżką,utf-8 sample",
                         remaining_data)


class TestUnicodeCSVRowWriter(unittest.TestCase):
    """
    Ensure get_csv_row_writer returns a function
    that will write a row of data to a stream.
    """

    def setUp(self):
        self.data = [
            [u"WĄŻ", 2, u"utf-8 sample"],
            [u"dróżką", u"utf-8 sample"]
        ]

        self.file_stream = StringIO()

    def test_write_unicode(self):
        """
        Ensure the function returned from get_csv_row_writer
        will write a row to the io stream.
        """

        unicode_row_writer = get_csv_row_writer(self.file_stream)
        for row in self.data:
            unicode_row_writer(row)

        self.assertEqual("WĄŻ,2,utf-8 sample\r\ndróżką,utf-8 sample\r\n",
                         self.file_stream.getvalue())
