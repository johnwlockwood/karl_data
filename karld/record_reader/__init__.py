"""
When your data can be divided into logical units, but is
each unit takes up varying amounts of multiple lines of
a file, use this to consume them in those units. Just provide
a function that takes a line and tells if it's a start line
or not.
"""

from collections import deque


def multi_line_records(lines, is_line_start=None):
    """
    Iterate over lines, yielding a sequence for group
    of lines that end where the next multi-line record
    begins. The beginning of the record is determined by
    calling the given is_line_delimiter function, which
    is called on the every line.

    :param lines: An iterator of unicode lines.
    :param is_line_start: determine the beginning line of a record.
    :type is_line_start: callable that returns if a line is the beginning
     of a record.
    :yields: deque of lines.
    """
    record = None
    if is_line_start:
        for line in lines:
            if record is None:
                record = deque([line])
            elif is_line_start(line):
                yield record
                record = deque([line])
            else:
                record.append(line)
        if record:
            yield record
    else:
        for line in lines:
            yield deque([line])
