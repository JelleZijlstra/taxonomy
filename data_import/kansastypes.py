import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from taxonomy.db import constants, models

from . import lib
from .lib import DataT

SOURCE = lib.Source("kansastypes.txt", "Kansas-mammal types.pdf")


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: Optional[Dict[str, Any]] = None
    current_label: Optional[str] = None
    current_lines: List[str] = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert current_label is not None
        assert (
            current_label not in current_name
        ), f"duplicate label {current_label} in {current_name}"
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name["pages"].append(page)
        for line in lines:
            # ignore blank lines
            if not line:
                continue
            if not re.search(r"^ {3,8}[^ ]", line):
                current_lines.append(line)
            else:
                first_word = line.split(".")[0].strip()
                if first_word not in ("Holotype", "Remarks", "Allotype"):
                    # new name
                    if current_name is not None:
                        assert current_label is not None
                        current_name[current_label] = current_lines
                        yield current_name
                    current_name = {"pages": [page]}
                    current_label = "name"
                    current_lines = [line]
                else:
                    start_label(first_word, line)
    assert current_label is not None
    assert current_name is not None
    current_name[current_label] = current_lines
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        match = re.match(
            r"^([A-Z][a-z ]+) \(?([A-Z][a-zé A-Z\.-]+)\)?, (.*)$", name["name"]
        )
        assert match is not None, f"failed to match {name}"
        name["original_name"] = match.group(1)
        name["authority"] = match.group(2)
        name["verbatim_citation"] = match.group(3)
        if "Holotype" in name and "KU" in name["Holotype"]:
            name["species_type_kind"] = constants.SpeciesGroupType.holotype
            match = re.match(
                r"[-—]?(.*), (KU \d+)( \(originally [^\)]+\))?, from (.*)[,;] obtained (.*?)\.?$",
                name["Holotype"],
            )
            assert match is not None, f'failed to match {name["Holotype"]}'
            front = match.group(1)
            name["type_specimen"] = match.group(2)
            name["loc"] = match.group(4)
            rear = match.group(5)

            match = re.match(r"^([^,]+), (.*)$", front)
            assert match, f"failed to match {front} (name: {name})"
            name["gender_age"] = match.group(1)
            name["body_parts"] = match.group(2)

            rear = re.sub(r"(, original .*)$", "", rear).strip()
            if ", by " in rear:
                try:
                    date, name["collector"] = rear.split(", by ")
                except ValueError:
                    raise ValueError(rear)
                if date.strip():
                    name["date"] = date.strip()
            else:
                name["date"] = rear
        else:
            print("handle manually", name)
        yield name


def associate_names(names: DataT) -> DataT:
    yield from lib.associate_names(
        names,
        lib.NameConfig(
            {
                "Murie": "A. Murie",
                "Villa-R. & Hall": "Villa & Hall",
                "Anderson": "S. Anderson",
                "Schanz": "Schantz",
            }
        ),
    )


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, "KU", SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True)
    names = associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False)
    lib.print_counts_if_no_tag(names, "Holotype", models.TypeTag.Date)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for _ in main():
        pass  # print(name)
