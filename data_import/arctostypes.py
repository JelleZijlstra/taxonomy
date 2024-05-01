import csv
import functools
import re
from collections.abc import Iterable

from taxonomy.db import models
from taxonomy.db.models import Name

from . import lib
from .lib import DataT

SOURCE = lib.Source("arctostypes.csv", "Arctos-catalog")
IGNORED = {
    "Drymoreomys albimaculatus",  # transferred to MZUSP
    "Proechimys echinothrix",  # transferred to INPA
    "Proechimys gardneri",  # transferred to INPA
    "Proechimys kulinae",  # transferred to INPA
    "Proechimys pattoni",  # transferred to INPA
    "Predicrostonyx hopkinsi",  # strange numbering
    "Pliomys deeringensis",
    "Microtus deceitensis",
    "Ochotona whartoni",
    "Xeronycteris vieirai",  # type in MZUSP
    "Sorex jacksoni",  # different number in {MVZ-types}
    "Scolomys ucayalensis",  # type in MUSM
    "Neacomys minutus",  # type in INPA
    "Mesomys occultus",  # type in INPA
    "Tapecomys primus",  # type in CBF
    "Thomasomys andersoni",  # type in AMNH
    "Monodelphis sanctaerosae",  # type in AMNH
    "Thomomys bottae fulvus",  # type in USNM
    "Panthera onca",  # not a holotype
}

#   <div class="views-row views-row-1 views-row-odd views-row-first">
#   <div class="views-field views-field-solr-document-6">    <span class="views-label views-label-solr-document-6">Sex: </span>    <span class="field-content">Female</span>  </div>  </div>


def extract_names(lines: Iterable[str]) -> DataT:
    yield from csv.DictReader(lines)


@functools.lru_cache
def types_of_collection(collection_name: str) -> dict[str, models.Name]:
    if collection_name == "UAM":
        collection_name = "UAM (Alaska)"
    elif collection_name == "UCM":
        collection_name = "UCM (Colorado)"
    return {
        nam.type_specimen: nam
        for nam in Name.filter(
            Name.collection == models.Collection.by_label(collection_name)
        )
    }


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        name["type_specimen"] = re.sub(
            r"^([^:]+):[^:]+:([^:]+)$", r"\1 \2", name["GUID"]
        )
        name["original_name"] = name["SCIENTIFIC_NAME"]
        name["authority"] = ""
        name["collection_name"] = name["type_specimen"].split()[0]
        yield name


def filter_known(names: DataT, *, verbose: bool = False) -> DataT:
    types = {
        nam.type_specimen: nam
        for nam in Name.filter(Name.collection == models.Collection.by_label("FMNH"))
    }
    for name in names:
        if name["original_name"] in IGNORED:
            continue
        types = types_of_collection(name["collection_name"])
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
        print(f'{name["original_name"]} (type = {name["type_specimen"]})')
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = extract_names(lines)
    names = split_fields(names)
    names = filter_known(names, verbose=False)
    # names = lib.translate_to_db(names, None, SOURCE, verbose=False)
    # names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    # names = lib.associate_names(names, try_manual=False, use_taxon_match=True, quiet=True)
    # names = try_manual(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    names = sorted(
        names, key=lambda name: (name["collection_name"], name["original_name"])
    )
    names = print_names(names)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
