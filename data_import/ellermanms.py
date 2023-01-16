import re
from typing import List, Tuple
from collections.abc import Iterable

from taxonomy.db.constants import NomenclatureStatus

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("ellermanms1966-pavlinov.txt", "Palearctic, India.pdf")
SPLIT_REGEXES = [
    re.compile(r"(^.*)\$(.*)$"),
    re.compile(r"(^.*: +\d+)\. (.*)$"),
    re.compile(r"(^.*, +\d+)\. (.*)$"),
    re.compile(r"(^.*\. +\d+)\. (.*)$"),
    re.compile(r"(^.*; +\d+)\. (.*)$"),
]


def extract_pages(lines: Iterable[str]) -> Iterable[tuple[int, list[str]]]:
    """Split the text into pages."""
    current_page = 0
    current_lines: list[str] = []
    for line in lines:
        if line.startswith("\x0c"):
            yield current_page, current_lines
            current_lines = []
            current_page += 1
            if current_page == 742:
                return
        else:
            current_lines.append(line)
    # last page
    yield current_page, current_lines


def extract_names(pages: PagesT) -> DataT:
    current_name: list[str] = []
    start_page = 0
    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if re.match(r"^ *(\(\?\) {2,})(\d{4})\. ", line):
                if current_name:
                    yield {"raw_text": current_name, "pages": [start_page]}
                start_page = page
                current_name = [line]
            elif not line or not line.startswith(" "):
                if current_name:
                    yield {"raw_text": current_name, "pages": [start_page]}
                current_name = []
            elif current_name:
                current_name.append(line)


def split_fields(names: DataT) -> DataT:
    for name in names:
        text = name["raw_text"]
        match = re.match(r"^ *(\(\?\) )(?P<year>\d{4})\. (?P<rest>.*$)", text)
        assert match, f"failed to match {text}"
        name["year"] = match.group("year")
        text = match.group("rest")
        if "|" in text:
            match = re.match(r"^(?P<head>[^\|]+\|[^,]+), (?P<tail>.*$)", text)
        else:
            match = re.match(r"^(?P<head>[^,]+), (?P<tail>.*$)", text)
        assert match, f"failed to match {text}"
        name["orig_name_author"] = match.group("head")
        text = match.group("tail")
        for rgx in SPLIT_REGEXES:
            match = rgx.match(text)
            if match:
                name["verbatim_citation"] = match.group(1)
                name["rest"] = match.group(2)
                break
        else:
            print(f'failed to match {name["raw_text"]}')
        yield name


def identify_rest(names: DataT) -> DataT:
    for name in names:
        if name["rest"] == "nom. nud.":
            name["nomenclature_status"] = NomenclatureStatus.nomen_nudum
        elif name["rest"]:
            is_spec = " " in name["original_name"]
            if is_spec:
                name["loc"] = name["rest"]
            else:
                name["verbatim_type"] = name["rest"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False, check=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.clean_text(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = identify_rest(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = lib.associate_names(names, try_manual=True)
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    main()
