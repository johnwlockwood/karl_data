import argparse


def run(in_dir='dirty_data', out_dir='clean_data'):
    pass


def main(*args):
    parser = argparse.ArgumentParser(*args)
    parser.add_argument("in_dir", help="Data source directory")
    parser.add_argument("out_dir", help="Data output directory")
    args = parser.parse_args()

    in_dir = args.in_dir or 'dirty_data'
    out_dir= args.out_dir or 'clean_data'
    run(in_dir, out_dir)


if __name__ == "__main__":
    main()
