import re
from typing import Any, Dict, List

from taxonomy.db import constants

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("cmntypes-nocolumns.txt", "CMN-types.pdf")
NAME_LINE_RGX = re.compile(
    r"""
    (?P<original_name>[A-Z][a-z]+(\s\([A-Z]?[a-z]+\))?(\s[a-zü]+){1,2})(\s\[[a-z]+\])?\s
    (?P<authority>[A-Z][a-zü]+(\sde\sBalsac|-Edwards)?),\s
    (?P<year>\d{4})$
""",
    re.VERBOSE,
)


def extract_names(pages: PagesT) -> DataT:
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name, f"cannot start {label} with {line!r} on an empty name"
        assert (
            label not in current_name
        ), f"duplicate label {label} for {current_name} (line: {line})"
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                if current_name:
                    yield current_name
                current_name = {}
                continue
            if not current_name:
                current_name = {"orig_name_author": line, "pages": [page]}
            elif "verbatim_citation" not in current_name:
                start_label("verbatim_citation", line)
            elif line.startswith("Holotype"):
                start_label("Holotype", line)
            elif re.match(r"^Neotype \d+", line):
                start_label("Neotype", line)
            elif (
                "Holotype" in current_name or "Neotype" in current_name
            ) and "loc" not in current_name:
                start_label("loc", line)
            elif (
                "loc" in current_name
                and "specimen_detail" not in current_name
                and "Paratype" not in current_name
                and re.match(r"^\d{4}", line)
            ):
                start_label("specimen_detail", line)
            elif line.startswith("Paratype"):
                start_label("Paratype", line)
            else:
                current_lines.append(line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)

        if "Holotype" in name:
            name["type_specimen"] = f'CMN {name["Holotype"]}'
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
        elif "Neotype" in name:
            name["type_specimen"] = f'CMN {name["Neotype"]}'
            name["species_type_kind"] = constants.SpeciesGroupType.neotype

        if "specimen_detail" in name:
            match = re.match(
                r"^(?P<date>\d{4}), (?P<collector>.*)$", name["specimen_detail"]
            )
            assert match, name["specimen_detail"]
            name.update(match.groupdict())

        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "CMN", SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True)
    names = lib.associate_names(names)
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
