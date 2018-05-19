import re

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("acomys.txt", "Acomys-distribution (Bates 1994).pdf")


def extract_names(pages: PagesT) -> DataT:

    for page, lines in pages:
        for line in lines:
            if line.strip() == "REFERENCES":
                return
            if line.strip():
                yield {"pages": [page], "raw_text": line}


def split_fields(names: DataT) -> DataT:
    for name in names:
        match = re.match(
            r"^(?P<orig_name_author>[^,]+), (?P<year>\d{4}) ?[a-d]?: (?P<page_described>[^;]+); (?P<loc>.*)$",
            name["raw_text"],
        )
        if match:
            name.update(match.groupdict())
        else:
            print(f'failed to match {name["raw_text"]}')
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names, clean_labels=False)
    names = split_fields(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=False)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, try_manual=True)
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
