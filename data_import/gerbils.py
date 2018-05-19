import re
from typing import Iterable

from . import lib
from .lib import DataT

SOURCE = lib.Source("gerbillus.txt", "Gerbillus (Lay 1983).pdf")


def split_fields(lines: Iterable[str]) -> DataT:
    for line in lines:
        line = line.strip()
        if not line:
            continue
        name = {"raw_text": line}
        match = re.match(
            r"""
            ^(?P<name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?\s[a-z]+(\s[a-z]{4,})?)\s
            (?P<author>[^\d]+),\s
            (?P<year>\d{4})[\.,]\s
            (?P<citation>.*\da?\)?)\.\s
            (?P<loc>[A-Z][a-z\sA-Z]+:\s.+)\.$
        """,
            line,
            flags=re.VERBOSE,
        )
        if not match:
            print(f"failed to match {line!r}")
            continue
        name["loc"] = match.group("loc")
        name["verbatim_citation"] = match.group("citation")
        name["year"] = match.group("year")
        name["authority"] = match.group("author").replace(" and ", " & ")
        name["original_name"] = match.group("name")
        yield name


def translate_to_db(names: DataT, source: lib.Source) -> DataT:
    yield from lib.translate_to_db(names, "USNM", source)


def translate_type_localities(names: DataT) -> DataT:
    for name in names:
        if "loc" in name:
            text = name["loc"].rstrip(".")
            country = text.split(":")[0]
            type_loc = lib.extract_region([[country]])
            if type_loc is not None:
                name["type_locality"] = type_loc
            else:
                print("could not extract type locality from", text)
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = split_fields(lines)
    names = translate_to_db(names, SOURCE)
    names = translate_type_localities(names)
    names = lib.associate_names(
        names,
        lib.NameConfig(
            {
                "De Winton": "de Winton",
                "von Lehmann": "Lehmann",
                "Cockrum, Vaughn & Vaughn": "Cockrum, Vaughan & Vaughan",
            },
            {
                "Dipodillus campestris rozsikae": "Dipodillus campestris roszikae",
                "Dipodillus watersi": "Gerbillus (Dipodillus) watersi",
            },
        ),
    )
    lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    # lib.print_counts(names, 'original_name')
    # lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for _ in main():
        print(_)
