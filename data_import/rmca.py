import itertools
import re
from typing import Any

from taxonomy.db import constants, models

from . import lib
from .lib import DataT, PagesT

SOURCE = lib.Source("rmcatypes.txt", "RMCA-types.pdf")

RGX = re.compile(
    r"""
    ^(?P<year>\d{4})\.\s+(?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?(\s[a-z]+){0,2})(?P<colon>:)?\s
    (?P<authority>[A-Z][^,]+),\s(?P<rest>.*)$
""",
    re.VERBOSE,
)


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
        # this was for the first run where I missed names with an irregular number of blank lines in the beginning
        if re.match(r"^\d+ +[A-Z][a-z]+ +[A-Z][a-z]+$", lines[1]):
            continue

        lines = list(itertools.dropwhile(lambda line: not line.strip(), lines))
        if not re.match(r"^\d+ +[A-Z][a-z]+ +[A-Z][a-z]+$", lines[0]):
            continue
        current_name = {
            "pages": [page],
            "raw_text": lines,
            "name_line": lines[1],
            "verbatim_citation": lines[2],
        }
        assert current_name["name_line"].strip(), (current_name, lines)
        for i in itertools.count(3):
            if not lines[i].rstrip():
                break
        type_line = lines[i + 1].strip()
        if type_line == "Type":
            current_name["species_type_kind"] = constants.SpeciesGroupType.holotype
            current_name["type_line"] = lines[i + 2]
            for line in lines[i + 3 :]:
                line = line.rstrip()
                if not line:
                    break
                if re.match(r"^[A-Z][a-z][a-z]: ", line):
                    label = line[:3]
                    start_label(label, line)
                else:
                    current_lines.append(line)
        if "Prp" in current_name:
            current_name["Prp"] = "; ".join(line for line in current_name["Prp"])
        yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        match = re.match(
            (
                r"([A-Z][a-z]+( [a-z-]+){1,2}(, f\. fulva)?) ([A-Z][a-zA-Z \.,&]+),"
                r" (\d{4})"
            ),
            name["name_line"],
        )
        if match:
            name["original_name"] = match.group(1)
            name["authority"] = match.group(4)
            name["year"] = match.group(5)
        else:
            print(f'failed to match {name["name_line"]}')

        if "type_line" in name:
            parts = name["type_line"].split()
            name["type_specimen"] = " ".join(parts[:2])
            try:
                name["collection"] = models.Collection.by_label(parts[0])
            except ValueError:
                pass
            if parts[2:]:
                name["age_gender"] = " ".join(parts[2:])
        if "Loc" in name:
            name["loc"] = name["Loc"]
        if "Prp" in name:
            name["specimen_detail"] = name["Prp"]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, None, SOURCE, verbose=True)
    names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(
        names,
        use_taxon_match=True,
        name_config=lib.NameConfig(
            authority_fixes={"Elliott": "Elliot", "Verheyen": "W. Verheyen"}
        ),
    )
    names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=True)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
