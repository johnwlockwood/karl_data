import karld


def main():
    """
    From a source of data, shard it to csv files.
    """
    import pathlib

    if karld.is_py3():
        third = chr
    else:
        third = unichr

    # Your data source
    items = ((x, x + 1, third(x + 10)) for x in range(2000))

    out_dir = pathlib.Path('shard_out_json')

    karld.io.ensure_dir(str(out_dir))

    karld.io.split_file_output_json('big_data.json', items, str(out_dir))


if __name__ == "__main__":
    main()
