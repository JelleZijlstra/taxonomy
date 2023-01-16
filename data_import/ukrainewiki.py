import functools
import re
from typing import Set

import wikitextparser

from taxonomy import getinput
from taxonomy.db.models import Name, Taxon

from . import readwiki

NAME = "Список_видів_ссавців,_описаних_у_XXI_ст."


@functools.lru_cache()
def all_names() -> Set[str]:
    return {
        nam.corrected_original_name
        for nam in Name.select_for_field("corrected_original_name")
    }


@functools.lru_cache()
def all_taxa() -> Set[str]:
    return {taxon.valid_name for taxon in Taxon.select_for_field("valid_name")}


def run() -> None:
    text = readwiki.get_text(NAME, "uk")
    parsed = wikitextparser.parse(text)
    table = parsed.tables[0]
    for row in table.data():
        name = re.sub(r"['\[\]]", "", row[0])
        ref = wikitextparser.parse(row[-1])
        if name in all_names() or name in all_taxa():
            continue
        getinput.print_header(name)
        print(ref)
