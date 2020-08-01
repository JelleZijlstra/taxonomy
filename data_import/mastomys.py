import re
from typing import Iterable

from taxonomy.db import constants, models

from . import lib
from .lib import DataT

SOURCE = lib.Source("mastomys.txt", "Mastomys (Van der Straeten & Robbins 1997).pdf")


def extract_pages(lines: Iterable[str]) -> DataT:
    """Split the text into pages."""
    for line in lines:
        if re.match(r"^ *\d+ ", line):
            yield {"pages": ["table 2"], "raw_text": line}


def extract_names(names: DataT) -> DataT:
    for name in names:
        line = name["raw_text"]
        match = re.match(
            r"\s*\d+ +(?P<original_name>[A-Z][a-zA-Z\(\) ]+? [a-z]{3,}) (?P<authority>[^\da-z]+)(?P<year>\d{4}) {2,}(?P<type_specimen>.*)$",
            line,
        )
        assert match, line
        name.update(match.groupdict())
        name["authority"] = re.sub(
            r"\s+",
            " ",
            name["authority"]
            .strip()
            .rstrip(",")
            .title()
            .replace("Van Der Straeten", "Van der Straeten")
            .replace("Thomas", "O. Thomas")
            .replace("Dewinton", "de Winton")
            .replace("Heim De Balsac", "Heim de Balsac"),
        )
        name["species_type_kind"] = constants.SpeciesGroupType.holotype
        coll = (
            name["type_specimen"]
            .split()[0]
            .replace("CM", "CM (Carnegie)")
            .replace("MRAC", "RMCA")
        )
        name["collection"] = models.Collection.by_label(coll)
        name["type_tags"] = [
            models.TypeTag.SpecimenDetail(
                "[Holotype] " + name["type_specimen"],
                models.Article.get(name=SOURCE.source),
            )
        ]
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = extract_pages(lines)
    names = extract_names(names)

    names = lib.associate_names(names)
    names = lib.write_to_db(names, SOURCE, dry_run=False)
    # lib.print_counts_if_no_tag(names, 'loc', models.TypeTag.Coordinates)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for n in main():
        print(n)
