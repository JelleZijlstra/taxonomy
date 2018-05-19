import argparse

from . import lib


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    parser.add_argument("-s", "--single-column", nargs="*", type=int)
    args = parser.parse_args()

    lines = lib.get_text(lib.Source(args.file, ""))
    pages = lib.extract_pages(lines)
    pages = lib.align_columns(pages, single_column_pages=set(args.single_column))
    for page, lines in pages:
        print(f"\x0c{page}")
        for line in lines:
            print(lib.clean_string(line))


if __name__ == "__main__":
    main()
