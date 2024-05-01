import re

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("africa8808.txt", "Africa 1988-2008.pdf")


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    starting_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                in_remarks = False
                if current_name:
                    yield {"raw_text": current_name, "pages": [starting_page]}
                    current_name = []
                continue
            leading_spaces = lib.initial_count(line, " ")
            if leading_spaces == 0:
                if line.startswith("Remarks:"):
                    in_remarks = True
                elif not in_remarks and " " in line:
                    if current_name:
                        yield {"raw_text": current_name, "pages": [starting_page]}
                        current_name = []
                    current_name = [line]
                    starting_page = page
            elif current_name:
                current_name.append(line)


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        match = re.match(
            (
                r"^(?P<orig_name_author>\D+), (?P<year>\d{4}):"
                r" (?P<page_described>[^\.]+)\. (?P<loc>.*?)( \[[^\]]+\])?$"
            ),
            text,
        )
        if match:
            for k, v in match.groupdict().items():
                if v:
                    name[k] = v
        else:
            print(f"failed to match {text}")
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "USNM", SOURCE, verbose=True)
    names = lib.associate_names(names, use_taxon_match=True, try_manual=True)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
