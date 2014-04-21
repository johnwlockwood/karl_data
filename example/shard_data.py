import os
import pathlib
import karld


def main():
    """
    Python 2 version
    """

    items = (str(x) + os.linesep for x in range(2000))

    out_dir = pathlib.Path('shard_out')

    karld.io.ensure_dir(str(out_dir))

    karld.io.split_file_output('big_data', items, str(out_dir))


if __name__ == "__main__":
    main()
