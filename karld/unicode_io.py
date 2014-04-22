# -*- coding: utf-8 -*-
import sys
import csv
import codecs

from karld import is_py3

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from functools import partial
try:
    from itertools import imap
except ImportError:
    imap = map

from operator import methodcaller

__all__ = ['csv_reader',
           'get_csv_row_writer',
           'csv_unicode_reader']

#Unicode IO

__doc__ = """
How To Encoding
=================
If you've tried something like ``unicode('က')`` or  ``u'hello ' + 'wကrld'
or ``str(u'wörld')`` you will have seen UnicodeDecodeError
and UnicodeEncodeError. Likely, you've tried to
read csv data from a file and mixed the data with unicode
and everything went fine until it got to the line with
some word with an accent character and it broke and showed
``UnicodeDecodeError: 'ascii' codec can't decode byte ...``
What do you do?.
You've tried to write sequences of unicode strings
to a csv file and gotten
``UnicodeEncodeError: 'ascii' codec can't encode character u'\\xf6' in position 1: ordinal not in range(128)``
What do you do?

Unicode handles characters used by different languages
around the world, emojis, curly quotes and other *glyphs*.
The textual data in different parts of the world
can have various encodings designed to specifically
handle their glyphs and unicode can represent them all,
but the data must be decoded from that encoding to unicode.

The data was written to the file in a specific encoding,
either deliberately or because that was the default for
the software. Unfortunately, it's up to the reader of the
data to know what the data was encoded in. It can be
connected to the language or locale it was created in.
Sometimes it can be inferred by the data. Many times
it's written in utf-8, which can handle encoding all
the different chars that can be in a unicode string.
It does this by saving chars like ``'¥'``, or in unicode, ``u'\\xa5'``,
as ``'\\xc2\\xa5'``.  ``u'\\xa5'.encode('utf-8')`` results in ``'\\xc2\\xa5'``.
It uses more space, but can do it. By the way, ``'¥'``
is possible in this code because the encoding is declared
at the top of this file.

String transformation methods, such as upper() or lower()
don't work on these chars, like ``'î'`` or ``'ê'`` if they are
encoded as a utf-8 string, but will work if they are
decoded from utf-8 to unicode.

>>> print 'î'.upper()
î
>>> print u'î'.upper()
Î
>>> print 'ê'.upper()
ê
>>> print 'ê'.decode('utf-8').upper()
Ê

The python 2.7 csv module doesn't work with unicode,
so the text it parses must be encoded from unicode
to a str using an encoding that will handle all the
chars in the text. utf-8 is good choice, and thus is
default.

The purpose of this module is to facilitate reading
and writing csv data in whatever encoding your data
is in.
"""

encode_utf8 = methodcaller('encode', "utf-8")
decode_utf8 = methodcaller('decode', "utf-8")


def not_implemented(*args, **kwargs):
    raise NotImplementedError()


if is_py3():
    unicode = str
    decode_utf8_to_unicode = not_implemented
    map_decode_utf8_to_unicode = not_implemented
else:
    decode_utf8_to_unicode = partial(unicode, encoding="utf-8")
    map_decode_utf8_to_unicode = partial(map, decode_utf8_to_unicode)


def csv_unicode_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Generator the reads serialized unicode csv data.
    Use this if you have a stream of data
    in unicode and you want to access the rows
    of the data as sequences encoded as unicode.

    Unicode in, unicode out.

    :param unicode_csv_data: An iterable of unicode strings.
    :param dialect: csv dialect
    """
    if is_py3():
        return csv.reader(unicode_csv_data, dialect=dialect, **kwargs)
    else:
        encoded_utf8_data = imap(encode_utf8, unicode_csv_data)

        reader = csv.reader(encoded_utf8_data, dialect=dialect, **kwargs)

        return imap(map_decode_utf8_to_unicode, reader)

unicode_csv_unicode_reader = csv_unicode_reader


def _utf8_iter_recoder(stream, encoding):
    """Generator re-encodes input file's lines from a given
    encoding to utf-8.

    :param stream: file handle.
    :param encoding: str of encoding.
    """
    return codecs.iterencode(codecs.iterdecode(stream, encoding), "utf-8")


def csv_reader(csv_data, dialect=csv.excel, encoding="utf-8", **kwargs):
    """
    Csv row generator that re-encodes to
    unicode from csv data with a given encoding.

    Utf-8 data in, unicode out. You may specify a different
     encoding of the incoming data.

    :param csv_data: An iterable of str of the specified encoding.
    :param dialect: csv dialect
    :param encoding: The encoding of the given data.
    """
    if is_py3():
        return csv.reader(csv_data, dialect=csv.excel, **kwargs)

    reader = csv.reader(
        _utf8_iter_recoder(csv_data, encoding),
        dialect=dialect, **kwargs
    )

    return imap(map_decode_utf8_to_unicode, reader)

csv_to_unicode_reader = csv_reader


def _encode_unicode_or_identity(value):
    """
    Encode a value to utf-8 only if
    it's unicode.
    """
    if isinstance(value, unicode):
        return encode_utf8(value)
    return value


def _encode_write_row(stream, queue, writer, encoder, row):
    """
    Write a row, of unicode data to a cStringIO.StringIO
    then get the csv row value from the queue
    and decode from utf-8 to unicode, then to the target
    encoding and write to the stream.

    """
    writer.writerow(map(_encode_unicode_or_identity, row))
    stream.write(
        encoder.encode(
            decode_utf8(
                queue.getvalue()
            )
        )
    )
    # empty queue
    queue.truncate(0)


def get_csv_row_writer(stream, dialect=csv.excel, encoding="utf-8", **kwargs):
    """
    Create a csv, encoding from unicode, row writer.

    Use returned callable to write rows of unicode data
    to a stream, such as a file opened in write mode,
    in utf-8(or another) encoding.

    ::

        my_row_data = [
            [u'one', u'two'],
            [u'three', u'four'],
        ]

        with open('myfile.csv', 'wt') as myfile:
            unicode_row_writer = get_unicode_row_writer(myfile)
            for row in my_row_data:
                unicode_row_writer(row)
    """
    if is_py3():
        writer = csv.writer(stream, dialect=dialect, **kwargs)
        return writer.writerow

    else:
        queue = StringIO()
        writer = csv.writer(queue, dialect=dialect, **kwargs)
        encoder = codecs.getincrementalencoder(encoding)()
        return partial(_encode_write_row, stream, queue, writer, encoder)


get_unicode_row_writer = get_csv_row_writer
