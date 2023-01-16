import re
from typing import Any, Dict, List

from taxonomy.db import constants

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("zsitypes.txt", "ZSI-types.pdf")


def extract_names(pages: PagesT) -> DataT:
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name is not None
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if re.match(r"^ {5,}(Order|Family) [A-Z]+$", line):
                continue
            match = re.match(r"^ *(\d{1,3})\. +(.*)$", line)
            if match:
                if current_name:
                    yield current_name
                current_name = {
                    "pages": [page],
                    "no": int(match.group(1)),
                    "orig_name_author": match.group(2),
                }
            elif re.match(r"^ *\d{4}\. ", line):
                start_label("citation_line", line)
            else:
                match = re.match(r"^ +([A-Z][a-z\?]+( +name)?) *:", line)
                if match:
                    start_label(match.group(1), line)
                else:
                    current_lines.append(line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        match = re.match(r"(\d+)\. (.*)$", name["citation_line"])
        if match:
            name["year"] = match.group(1)
            name["verbatim_citation"] = match.group(2)
        else:
            print(f'failed to match {name["name_line"]}')
        name["specimen_detail"] = "\n".join(
            value for key, value in name.items() if "type" in key.lower()
        )

        if "Holotype" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
            parts = name["Holotype"].split("; ")
            if len(parts) >= 4:
                name["collector"] = re.sub(r" collectors?\.?$", "", parts[-1].strip())
                name["date"] = parts[-2]
                name["loc"] = parts[-3]
            else:
                print(name["Holotype"])
            match = re.match(r"^ *Holotype *: *Reg\. No\. ([\d, ]+)", name["Holotype"])
            if match:
                name["type_specimen"] = f"ZSI {match.group(1).strip()}"
            else:
                print(f'failed to match {name["Holotype"]}')

        if "syntype" in name or "syntypes" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.syntypes
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names, clean_labels=False)
    names = split_fields(names)
    names = lib.translate_to_db(names, "ZSI", SOURCE, verbose=False)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(
        names,
        name_config=lib.NameConfig(
            original_name_fixes={
                "Sciurus phayrei": "Sciurus pygerythrus, var. Phayrei",
                "Mus andamanensis": "Mus (Leggada) andamanensis",
            }
        ),
    )
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
