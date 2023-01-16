import re
from typing import Any
from collections.abc import Iterable

from taxonomy.db import models
from taxonomy.db.models import Name

from . import lib
from .lib import DataT

SOURCE = lib.Source("fmnh-holotypes.txt", "FMNH-catalog")
IGNORED = {
    "Thalpomys cerradensis",  # number is wrong
    "Bubalus cebuensis",  # holotype was moved elsewhere
    "Sus cebifrons negrinus",  # number is wrong
    "Rhagomys longilingua",  # most of holotype is elsewhere
    "Reithrodon auritus pachycephalus",  # holotype is probably of another name but can't be sure
    "Platyrrhinus ismaeli",  # most of holotype is elsewhere
    "Paucidentomys vermidax",  # moved to MZB
    "Myomyscus angolensis",  # holotype is probably of another name but can't be sure
    "Isothrix barbarabrownae",  # FMNH only has tissue samples
    "Acomys russatus harrisoni",  # ignoring because there is insufficient published information
    "Calomys callosus callosus",  # apparently not a type
    "Crocidura abscondita",  # to MZB
    "Funisciurus isabella isabella",  # not a type
}

#   <div class="views-row views-row-1 views-row-odd views-row-first">
#   <div class="views-field views-field-solr-document-6">    <span class="views-label views-label-solr-document-6">Sex: </span>    <span class="field-content">Female</span>  </div>  </div>


def extract_names(lines: Iterable[str]) -> DataT:
    current_name: dict[str, Any] = {}

    for line in lines:
        line = line.strip()
        if not line:
            continue
        elif re.match(r'^<div class="views-row', line):
            if current_name:
                yield current_name
            current_name = {}
        elif "catalog-multimedia" in line or "Available Media" in line:
            continue
        else:
            match = re.match(
                (
                    r'^.*<span class="views-label[^"]+">([^<]+)</span> +<span'
                    r' class="field-content">(<a href=[^>]+>)?([^<]+)(</a>)?</span>'
                ),
                line,
            )
            if match:
                label = match.group(1).strip().rstrip(":")
                assert label not in current_name, (current_name, line)
                current_name[label] = match.group(3)
            else:
                assert False, line
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name["raw_text"] = dict(name)
        name["type_specimen"] = f'FMNH {name["Catalog number"]}'
        name["original_name"] = name["Taxonomic Name"]
        name["authority"] = ""
        yield name


def filter_known(names: DataT, verbose: bool = False) -> DataT:
    types = {
        nam.type_specimen: nam
        for nam in Name.filter(Name.collection == models.Collection.by_label("FMNH"))
    }
    for name in names:
        if name["original_name"] in IGNORED:
            continue
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
            if nam.original_citation and int(nam.year) > 1945:
                models.fill_data.fill_data_from_paper(nam.original_citation)
        yield name


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    names = extract_names(lines)
    names = split_fields(names)
    names = filter_known(names)
    # names = lib.translate_to_db(names, None, SOURCE, verbose=False)
    # names = lib.translate_type_locality(names, start_at_end=True, quiet=True)
    names = lib.associate_names(
        names, try_manual=False, use_taxon_match=True, quiet=True
    )
    names = try_manual(names)
    # names = lib.write_to_db(names, SOURCE, dry_run=False, edit_if_no_holotype=False)
    lib.print_field_counts(names)
    return names


if __name__ == "__main__":
    for p in main():
        print(p)
