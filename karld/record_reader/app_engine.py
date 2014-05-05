from karld.loadump import i_get_unicode_lines
from karld.record_reader import multi_line_records


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
    Iterate over request logs as written by a Google App Engine app.

    :param file_path: Path to an App Engine log file.
    :returns: An iterator of multi-line log records.
    """
    lines = i_get_unicode_lines(file_path)

    return multi_line_records(lines, is_line_start=is_log_start_line)
