import re
from typing import Any

from taxonomy.db import constants

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("fmnhtypes.txt", "FMNH-types (Sanborn 1947).pdf")


def extract_names(pages: PagesT) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        assert current_name is not None
        if label in current_name:
            assert line not in current_name, f"duplicate label {label}"
            label = line
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if re.match(r"\s+(Order|Family) +[A-Z]+$", line):
                continue
            match = re.match(
                r"^(?P<original_name>[A-Z][a-z]+( +\([A-Z][a-z]+\))?("
                r" +[a-z\-]+(\[[a-z]+\])?){1,2}) +"
                r"(?P<authority>([A-Z]\. +)*[A-Z][a-z]+( +and +[A-Z][a-z]+)?)"
                r" *(?P<type_kind>Cotypes|Lectotype)?$",
                line,
            )
            if match:
                if current_name:
                    assert (
                        "specimen_detail" in current_name
                    ), f"missing specimen for {current_name} on page {page}"
                    yield current_name
                current_name = {"pages": [page], "raw_text": [line]}
                for key, value in match.groupdict().items():
                    if value:
                        current_name[key] = re.sub(r" +", " ", value)
                start_label("verbatim_citation", "")
            elif re.match(r"^ *[=\-]", line):
                start_label("synonymy", line)
            elif re.match(r"^\d{3,}\. ", line):
                start_label("specimen_detail", line)
            elif line.startswith("  "):
                current_lines.append(line)
            else:
                assert (
                    "specimen_detail" not in current_name
                ), f"{line}: already saw specimen detail for {current_name} at {page}"
                start_label("synonymy", line)
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        text = name["specimen_detail"]
        match = re.match(
            r"^(?P<specimen>\d+)\. +(?P<body_parts>[^\.]+)\."
            r" +(?P<age_gender>[^\.]+)\. +"
            r"(?P<loc>.*?)\. +(Altitude +(?P<altitude>[\d,\-]+ +(feet|meters))\."
            r" +)?((?P<date>([A-Z][a-z]+( \d+)?, )?\d{4})\. +)?"
            r"(Received from |Presented by |Collected by|Purchased |Found by)",
            text,
        )
        if match:
            for k, v in match.groupdict().items():
                if k == "specimen":
                    name["type_specimen"] = f"FMNH {v}"
                if v:
                    name[k] = v
        if "type_specimen" in name and "type_kind" not in name:
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "FMNH", SOURCE, verbose=False)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
