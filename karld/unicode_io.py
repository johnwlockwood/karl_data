# -*- coding: utf-8 -*-

import csv
import codecs
import cStringIO
from functools import partial
from itertools import imap
from operator import methodcaller

#Unicode IO

__doc__ = """
Unfortunately, as you probably already know, encoding.
======================================================

Y U NO LIKE MY CHARACTERS?

Not all encodings are like that.

How to encoding
****************
Unicode is supposed to handle
a wider range of chars used by different languages
around the world and also emojis and other symbols
or curly quotes.

The texts in these different parts of the world
can have various encodings designed to specifically
handle their chars, but not others.

Data will contain these chars. So reading them
from the data as an encoding that does not account
for them is when you get problems like UnicodeEncodeError
or UnicodeDecodeError.

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

Unicode is how python can deal with all the chars in
memory, but when writing to a file, it has to encode
it to a specific encoding.

String transformation methods, such as upper() or lower()
don't work on these chars, like 'î' or 'ê' if they are
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
decode_utf8_to_unicode = partial(unicode, encoding="utf-8")
map_decode_utf8_to_unicode = partial(map, decode_utf8_to_unicode)


def unicode_csv_unicode_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Generator the reads a unicode csv data.
    Use this if you have a stream of data
    in unicode and you want to access the rows
    of the data as sequences encoded as unicode.

    Unicode in, unicode out.

    :param unicode_csv_data: An iterable of unicode strings.
    :param dialect: csv dialect
    """
    encoded_utf8_data = imap(encode_utf8, unicode_csv_data)

    reader = csv.reader(encoded_utf8_data, dialect=dialect, **kwargs)

    return imap(map_decode_utf8_to_unicode, reader)


def utf8_iter_recoder(stream, encoding):
    """Generator re-encodes input file's lines from a given
    encoding to utf-8.

    :param stream: file handle.
    :param encoding: str of encoding.
    """
    return codecs.iterencode(codecs.iterdecode(stream, encoding), "utf-8")


def csv_to_unicode_reader(csv_data,
                          dialect=csv.excel,
                          encoding="utf-8", **kwargs):
    """
    Csv row generator that re-encodes to
    unicode from csv data with a given encoding.

    Utf-8 data in, unicode out. You may specify a different
     encoding of the incoming data.

    :param csv_data: An iterable of str of the specified encoding.
    :param dialect: csv dialect
    :param encoding: The encoding of the given data.
    """
    reader = csv.reader(
        utf8_iter_recoder(csv_data, encoding),
        dialect=dialect, **kwargs
    )

    return imap(map_decode_utf8_to_unicode, reader)


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


def get_unicode_row_writer(stream, dialect=csv.excel, encoding="utf-8", **kwargs):
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
    queue = cStringIO.StringIO()
    writer = csv.writer(queue, dialect=dialect, **kwargs)
    encoder = codecs.getincrementalencoder(encoding)()
    return partial(_encode_write_row, stream, queue, writer, encoder)
