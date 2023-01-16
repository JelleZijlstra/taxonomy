import re
from typing import Any, Dict, Iterable, List

from taxonomy.db import constants

from . import lib
from .lib import DataT

SOURCE = lib.Source("nmbtypes.txt", "NMB-types (2).pdf")
MARKER = "==="


def extract_names(lines: Iterable[str]) -> DataT:
    current_name: Dict[str, Any] = {}
    current_lines: List[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name is not None
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_lines = [line]
        current_name[label] = current_lines

    for line in lines:
        line = line.rstrip()
        if not line:
            continue
        initial_spaces = lib.initial_count(line, " ")
        if initial_spaces == 0:
            if current_name:
                yield current_name
                current_name = {}
            if re.search(r" [A-Z]{3,}[, ]", line):
                current_name = {"name_line": line}
        elif initial_spaces == 4:
            if len(current_name) == 1:
                start_label("verbatim_citation", line)
            elif len(current_name) == 2:
                start_label("type_specimens", line)
            elif len(current_name) == 3 and line.startswith("    Comments: "):
                start_label("Comments", line)
            else:
                current_lines.append(MARKER)
                current_lines.append(line)
        elif initial_spaces >= 6 and current_lines:
            current_lines.append(line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        match = re.match(r"(.*?) ([A-Z]{3,}[ A-Za-z]*), (\d{4})", name["name_line"])
        if match:
            name["original_name"] = match.group(1)
            name["authority"] = match.group(2).replace(" and ", " & ").title()
            name["year"] = match.group(3)
        else:
            print(f'failed to match {name["name_line"]}')

        type_kind = (
            name["type_specimens"].split()[0].replace("[", "").replace("]", "").lower()
        )
        if type_kind == "holotype":
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
        elif type_kind == "lectotype":
            name["species_type_kind"] = constants.SpeciesGroupType.lectotype
        elif type_kind in ("syntype", "syntypes"):
            name["species_type_kind"] = constants.SpeciesGroupType.syntypes
        if type_kind in ("holotype", "lectotype"):
            text = name["type_specimens"].split(MARKER)[0].strip()
            match = re.match(
                (
                    r"^[^ ]+ (?P<type_specimen>NMB ?[\dM\-]+)( \([^\)]+\)){0,2}"
                    r" 1(?P<gender>[MF\?]) (?P<body_parts>[^:]+): (?P<loc>.*)$"
                ),
                text,
            )
            if match:
                name.update(match.groupdict())
            else:
                print(f'failed to match {name["type_specimens"]}')
        name["specimen_detail"] = lib.clean_string(
            name["type_specimens"].replace(MARKER, " ")
        )
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = extract_names(lines)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "NMB", SOURCE, verbose=False)
    names = lib.associate_names(
        names,
        name_config=lib.NameConfig(
            authority_fixes={"Heim De Balsac": "Heim de Balsac"}
        ),
        start_at="Mus xanthurus orientalis",
    )
    names = lib.write_to_db(
        names, SOURCE, dry_run=False, edit_if_no_holotype=True, always_edit=True
    )
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
