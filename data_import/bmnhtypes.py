import csv
import re
from collections.abc import Iterable

from taxonomy.db import models
from taxonomy.db.models import Name

from . import arctostypes, lib
from .lib import DataT

SOURCE = lib.Source("BMNH-types.tsv", "VertNet-catalog")
IGNORED: set[str] = set()
DISABLED = True


def extract_names(lines: Iterable[str]) -> DataT:
    yield from csv.DictReader(lines, delimiter="\t")


def split_fields(names: DataT) -> DataT:
    for name in names:
        if not DISABLED:
            name["raw_text"] = dict(name)
            name["type_specimen"] = re.sub(
                r"^([^:]+):[^:]+:([^:]+)$", r"\1 \2", name["GUID"]
            )
            name["original_name"] = name["SCIENTIFIC_NAME"]
            name["authority"] = ""
            name["collection_name"] = name["type_specimen"].split()[0]
        yield name


def filter_known(names: DataT, verbose: bool = False) -> DataT:
    types = {
        nam.type_specimen: nam
        for nam in Name.filter(Name.collection == models.Collection.by_label("FMNH"))
    }
    for name in names:
        if name["original_name"] in IGNORED:
            continue
        types = arctostypes.types_of_collection(name["collection_name"])
        if name["type_specimen"] in types:
            if verbose:
                print(
                    f'ignoring {types[name["type_specimen"]]} (type ='
                    f' {name["type_specimen"]})'
                )
            continue
        else:
            yield name


def try_manual(names: DataT) -> DataT:
    for name in names:
        print(name["original_name"])
        if name.get("name_obj"):
            nam = name["name_obj"]
            if nam.original_citation and int(nam.year) > 1900:
                models.fill_data.fill_data_from_paper(nam.original_citation)
        yield name


def print_names(names: DataT) -> DataT:
    for name in names:
        print(f'{name["scientificname"]} (type = {name["catalognumber"]})')
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = extract_names(lines)
    names = split_fields(names)
    # names = filter_known(names, verbose=False)
    # names = lib.translate_to_db(names, None, SOURCE, verbose=False)
    # names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    # names = lib.associate_names(names, try_manual=False, use_taxon_match=True, quiet=True)
    # names = try_manual(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    names = sorted(names, key=lambda name: name["scientificname"])
    names = print_names(names)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
