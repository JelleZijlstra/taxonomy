import json
import re
from typing import Any, Counter, Dict, Iterable, List, Optional, Tuple

from taxonomy import getinput
from taxonomy.db import constants, helpers, models

from . import lib
from .lib import DataT

SOURCE = lib.Source('utahtypes.txt', 'Utah-types.pdf')


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> Iterable[Dict[str, Any]]:
    current_name: Dict[str, Any] = {'pages': []}
    current_lines: List[str] = []

    for page, lines in pages:
        current_name['pages'].append(page)
        for line in lines:
            line = line.strip()
            if not line or line.isalpha():
                if current_lines:
                    current_name['lines'] = current_lines
                    yield current_name
                    current_lines = []
                    current_name = {'pages': [page]}
            else:
                current_lines.append(line)


def split_text(names: DataT) -> DataT:
    for name in names:
        text = name['lines']
        name['raw_text'] = text
        match = re.match(r'^(.*?)[\. ]+HOLOTYPE[\.\- —]+(.*?)(REMARKS[\.\- ]+(.*))?$', text)
        if match:
            name['name_author'] = match.group(1)
            name['holotype'] = match.group(2)
            name['remarks'] = match.group(4)
            name['species_type_kind'] = constants.SpeciesGroupType.holotype
        else:
            assert False, f'failed to match {text}'
        yield name


def split_fields(names: DataT) -> DataT:
    for name in names:
        match = re.match(r'^([A-Z][a-z]+( [a-z]+){1,2}) ([A-Z][a-zA-Z &]+), (.*)$', name['name_author'])
        assert match is not None, f'failed to match {name["name_author"]}'
        name['original_name'] = match.group(1)
        name['authority'] = match.group(3)
        name['verbatim_citation'] = match.group(4)

        match = re.match(r'^([A-Za-z ]+), ([A-Za-z, ]+), (UU \d+), from ([^;]+); obtained (.*?) by (.*?), original number .*$', name['holotype'])
        assert match is not None, f'failed to match {name["holotype"]}'
        name['age_gender'] = match.group(1)
        name['body_parts'] = match.group(2)
        name['type_specimen'] = match.group(3)
        name['loc'] = match.group(4)
        name['date'] = match.group(5)
        name['collector'] = match.group(6)
        yield name


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'UU', SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True)
    names = lib.associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False)
    lib.print_field_counts(names)


if __name__ == '__main__':
    main()
