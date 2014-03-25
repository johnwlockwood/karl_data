import csv
import codecs
import cStringIO
from functools import partial
from itertools import imap
from operator import methodcaller

#Unicode IO


encode_utf8 = methodcaller('encode', "utf-8")
decode_utf8 = methodcaller('decode', "utf-8")
decode_utf8_to_unicode = partial(unicode, encoding="utf-8")
map_decode_utf8_to_unicode = partial(map, decode_utf8_to_unicode)


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Generator the reads a unicode csv data.
    """
    encoded_utf8_data = imap(encode_utf8, unicode_csv_data)

    reader = csv.reader(encoded_utf8_data, dialect=dialect, **kwargs)

    return imap(map_decode_utf8_to_unicode, reader)


def utf8_iter_recoder(fi, encoding):
    """Generator re-encodes input file's lines from a given
    encoding to utf-8.

    :param fi: file handle.
    :param encoding: str of encoding.
    """
    return codecs.iterencode(codecs.iterdecode(fi, encoding), "utf-8")


def unicode_reader(csv_data, dialect=csv.excel, encoding="utf-8", **kwargs):
    """
    csv row generator that re-encodes to
    unicode from csv data with a given encoding.
    """
    reader = csv.reader(
        utf8_iter_recoder(csv_data, encoding),
        dialect=dialect, **kwargs
    )

    return imap(map_decode_utf8_to_unicode, reader)


def _encode_write_row(stream, queue, writer, encoder, row):
    """
    Write a row, of unicode data to a cStringIO.StringIO
    then get the csv row value from the queue
    and decode from utf-8 to unicode, then to the target
    encoding and write to the stream.

    """
    writer.writerow(map(encode_utf8, row))
    stream.write(
        encoder.encode(
            decode_utf8(
                queue.getvalue()
            )
        )
    )
    # empty queue
    queue.truncate(0)


def get_unicode_row_writer(fi, dialect=csv.excel, encoding="utf-8", **kwargs):
    """
    Create a csv, encoding, row writer.
    """
    queue = cStringIO.StringIO()
    writer = csv.writer(queue, dialect=dialect, **kwargs)
    stream = fi
    encoder = codecs.getincrementalencoder(encoding)()
    return partial(_encode_write_row, stream, queue, writer, encoder)
