import re
from typing import Any

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("texastypes.txt", "TTU, TCWC-types.pdf")
NAME_LINE_RGX = re.compile(
    r"""
    ^(?P<age_gender>[^,]+),\s(?P<body_parts>.+?),\s(?P<type_specimen>(TTU|TCWC)\s\d+),\s
    from\s(?P<loc>[^;]+);\sobtained\s[io]n\s(?P<date>.*?)\sby\s(?P<collector>.*),\s(original\sno|prepared\sby).*$
""",
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: dict[str, Any] = {}
    current_label: str | None = None
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_label = label
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                current_label = None
                continue
            if re.match(r"^ {8,}[A-Za-z\^]+$", line):
                continue
            if (
                current_label is None
                and not line.startswith(" ")
                and (not current_name or "verbatim_citation" in current_name)
            ):
                # new name
                if current_name:
                    yield current_name
                current_name = {"pages": [page], "orig_name_author": line}
            elif not line.startswith(" "):
                if current_label:
                    current_lines.append(line)
                else:
                    start_label("verbatim_citation", line)
            else:
                match = re.match(r"^ +([A-Z][a-z]+)\. ?—", line)
                assert match, f"failed to match {line}"
                start_label(match.group(1), line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        match = NAME_LINE_RGX.match(name["Holotype"])
        assert match, f'failed to match {name["Holotype"]}'
        name.update(match.groupdict())
        name["species_type_kind"] = constants.SpeciesGroupType.holotype
        name["collection"] = models.Collection.by_label(
            name["type_specimen"].split()[0]
        )
        name["specimen_detail"] = name["Holotype"]

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True)
    names = lib.associate_names(names)
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
