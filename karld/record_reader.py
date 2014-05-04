from collections import deque
from karld.loadump import i_get_unicode_lines


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


def is_log_start_line(line):
    """
    Is the line the start of a request log from Google App Engine.
    :param line: A string.
    :returns: True if the line doesn't start with a tab.
    """
    if not line[:1] == u'\t':
        return True


def log_reader(file_path):
    """
    :param file_path: Path to an App Engine log file.
    :returns: An iterator of multi-line log records.
    """
    lines = i_get_unicode_lines(file_path)

    return multi_line_records(lines, is_line_start=is_log_start_line)
