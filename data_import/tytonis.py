import re

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("tytonis.txt", "Amblysomus tytonis nov.pdf")

RGX = re.compile(
    r"""
    ^(?P<year>\d{4})\.\s+(?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?(\s[a-z]+){0,2})(?P<colon>:)?\s
    (?P<authority>[A-Z][^,]+),\s(?P<rest>.*)$
""",
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    starting_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            leading_spaces = lib.initial_count(line, " ")
            can_be_continuation = 5 < leading_spaces < 10
            if current_name and not can_be_continuation:
                # flush the active name
                yield {"raw_text": current_name, "pages": [starting_page]}
                current_name = []

            if leading_spaces == 0 and re.match(r"^\d{4}\. ", line):
                starting_page = page
                current_name = [line]
            elif can_be_continuation and current_name:
                current_name.append(line)


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        match = RGX.match(text)
        if match:
            if match.group("colon"):
                continue
            name.update(match.groupdict())
            text = name["rest"]
            match = re.match(r"^(.*\d)[;\.] (.*)$", text)
            if match:
                name["verbatim_citation"] = match.group(1)
                if " " in name["original_name"] and match.group(2) != ".":
                    name["loc"] = match.group(2)
                else:
                    name["verbatim_type"] = match.group(2)
            else:
                print(f"failed to match {text!r}")
        else:
            print(f"failed to match {text!r}")

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "USNM", SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, start_at="Chrysochloris duthiae")
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
