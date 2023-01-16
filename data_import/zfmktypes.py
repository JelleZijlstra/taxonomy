import re
from typing import Any, Dict, List

from taxonomy.db import constants

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("zfmktypes.txt", "ZFMK-types.pdf")
LABELS = {
    "Holotype",
    "Paratypes",
    "Current name",
    "Comments",
    "Paratype",
    "Material",
    "Lectotype",
    "Neotype",
    "Syntypes",
    "Paralectotype",
    "Paralectotypes",
    "Current status",
}


def extract_names(pages: PagesT) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: dict[str, Any] = {}
    current_lines: list[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_lines
        assert current_name is not None
        assert label not in current_name, f"duplicate label {label} for {current_name}"
        current_lines = [line]
        current_name[label] = current_lines

    for page, lines in pages:
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if re.match(r"^[A-Z][a-zA-Z]{4,}$", line):
                continue
            match = re.match(
                (
                    r"^(?P<original_name>[A-Z][a-z]+( \([A-Za-z][a-z]+\))?("
                    r" [a-zü]+){1,2}) (?P<authority>[A-Z].*), (?P<year>\d{4})$"
                ),
                line,
            )
            if match:
                if current_name:
                    assert (
                        "Current name" in current_name
                        or "Current status" in current_name
                    ), f"missing comments for {current_name} on page {page}"
                    yield current_name
                current_name = {"pages": [page], **match.groupdict()}
                start_label("verbatim_citation", "")
            else:
                for label in LABELS:
                    if line.startswith(label + ". "):
                        start_label(label, line)
                        break
                else:
                    current_lines.append(line)
    yield current_name


def split_fields(names: DataT, verbose: bool = True) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        if "Holotype" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
            name["specimen_detail"] = name["Holotype"]
        elif "Lectotype" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.lectotype
            name["specimen_detail"] = name["Lectotype"]
        elif "Syntypes" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.syntypes
            name["specimen_detail"] = name["Syntypes"]
        elif "Neotype" in name:
            name["species_type_kind"] = constants.SpeciesGroupType.neotype
            name["specimen_detail"] = name["Neotype"]
        if "specimen_detail" in name and "Syntypes" not in name:
            text = name["specimen_detail"]
            match = re.match(
                r"^(?P<type_specimen>ZFMK [\d\.]+)( \([^\)]+\))?,"
                r" (?P<age_gender>[^,]+), (?P<body_parts>[^;]+); "
                r"(?P<loc>.*), (collected|caught|coll\.) by (?P<collector>.*),"
                r" (?P<date>(\d{1,2} )?[A-Z][a-z]{2,3} \d{4})"
                r"(, field no.*|, Coll\. .*|, in captivity.*|, collection .*|\.)$",
                text,
            )
            if not match:
                match = re.match(
                    (
                        r"^(?P<type_specimen>ZFMK [\d\.]+)( \([^\)]+\))?,"
                        r" (?P<age_gender>[^,]+), (?P<body_parts>[^;]+); (?P<loc>.*),"
                        r" (collected|caught|coll\.) by .*$"
                    ),
                    text,
                )
                if not match:
                    if verbose:
                        print(f"failed to match {text}")
                else:
                    name.update(match.groupdict())
            else:
                name.update(match.groupdict())
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names, verbose=False)
    names = lib.translate_to_db(names, "ZFMK", SOURCE, verbose=False)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(names, try_manual=True)
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
