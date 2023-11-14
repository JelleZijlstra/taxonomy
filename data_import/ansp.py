import re
from collections.abc import Iterable
from typing import Any

from taxonomy.db import constants

from . import lib
from .lib import DataT

SOURCE = lib.Source("ansp.txt", "Mammalia-ANSP Recent types.pdf")


def extract_names(pages: Iterable[tuple[int, list[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: dict[str, Any] | None = None
    current_label: str | None = None
    current_lines: list[str] = []

    first_lines = "ORDER MARSUPIALIA", "FAMILY VESPERTILIONIDAE"
    saw_first = False

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert current_label is not None
        if current_label in current_name:
            current_label = current_lines[0]
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            if not saw_first:
                if line.strip() in first_lines:
                    saw_first = True
                else:
                    continue
            if line.strip() == "ADDENDUM":
                saw_first = False
            if not line:
                continue
            # ignore family/genus headers and blank lines
            saw_family = re.match(r"^\s*(FAMILY|ORDER)", line)
            if saw_family:
                previous_was_family = True
                continue
            if re.match(r"^ *[=\-]", line):
                assert not previous_was_family, line
                start_label("synonymy", line)
            elif re.match(r"^ *ANSP ", line):
                assert not previous_was_family, line
                start_label("specimen_detail", line)
            elif previous_was_family or re.search(r"  .{2,6}types?$", line):
                if (
                    current_name is not None
                    and "specimen_detail" not in current_name
                    and current_label == "name"
                    and line.startswith(" ")
                ):
                    current_lines.append(line)
                    continue
                # new name
                if current_name is not None:
                    assert current_label is not None
                    if current_label in current_name:
                        current_label = current_lines[0]
                    current_name[current_label] = current_lines
                    yield current_name
                current_name = {"pages": [page]}
                current_label = "name"
                current_lines = [line]
            else:
                assert line.startswith(" "), line
                current_lines.append(line)
            previous_was_family = False
    assert current_label is not None
    assert current_name is not None
    current_name[current_label] = current_lines
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        if any("ANSP" in key for key in name):
            detail = "\n".join([
                name["specimen_detail"],
                *[value for key, value in name.items() if "ANSP" in key],
            ])
            name["specimen_detail"] = detail
        elif "specimen_detail" in name:
            match = re.match(
                r"^ *(ANSP [^\.]+)\. ([^\.\(]*)(.*)$", name["specimen_detail"]
            )
            assert match, f'failed to match {name["specimen_detail"]}'
            name["raw_type_specimen"] = match.group(1)
            if match.group(2):
                name["raw_body_parts"] = match.group(2)
            name["rest"] = match.group(3)
        match = re.match(r"^(.*) +(\??[A-Z][a-z]{1,10}types?) +(.*)$", name["name"])
        assert match, f'failed to match {name["name"]} {name}'
        name["orig_name_author"] = match.group(1)
        type_kind = match.group(2).lower()
        name["type_kind"] = type_kind
        name["verbatim_citation"] = match.group(3)
        if type_kind in ("holotype", "lectotype"):
            name["species_type_kind"] = getattr(constants.SpeciesGroupType, type_kind)
            if "raw_type_specimen" in name:
                name["type_specimen"] = name["raw_type_specimen"]
            if "raw_body_parts" in name:
                name["body_parts"] = name["raw_body_parts"]
            if "rest" in name:
                print(name["rest"])
                match = re.match(
                    (
                        r"^\. ([^,]+), collected by (.*?) at (.*), (in )?([A-Z][a-z]+"
                        r" \d+, \d{4})\.? *$"
                    ),
                    name["rest"],
                )
                if match:
                    name["age_gender"] = match.group(1)
                    name["collector"] = match.group(2)
                    name["loc"] = match.group(3)
                    name["date"] = match.group(5)
        elif "specimen_detail" in name:
            name["specimen_detail"] = f'{type_kind}: {name["specimen_detail"]}'
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    pages = lib.align_columns(pages)
    names = extract_names(pages)
    names = lib.clean_text(names, clean_labels=False)
    names = split_fields(names)
    names = lib.translate_to_db(names, "ANSP", SOURCE, verbose=False)
    config = lib.NameConfig(
        authority_fixes={"LeConte": "Le Conte", "DuChaillu": "Du Chaillu"},
        original_name_fixes={"Sciurus fremontii": "Sciurus Frémonti"},
    )
    names = lib.associate_names(names, config, start_at="Onychomys ruidosae")
    lib.write_to_db(
        names, SOURCE, edit_if_no_holotype=False, always_edit=True, dry_run=False
    )

    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for n in main():
        print(n)
