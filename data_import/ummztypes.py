import re
from typing import Iterable, List, Tuple

from taxonomy.db import constants, models

from . import lib
from .lib import DataT

FILE_PATH = lib.DATA_DIR / 'ummztypes-layout.txt'
SOURCE = 'UMMZ-types.pdf'


def get_text() -> Iterable[str]:
    with FILE_PATH.open() as f:
        yield from f


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name = None
    current_label = None
    current_lines = []

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_label not in current_name, f'duplicate label {current_label} in {current_name}'
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name['pages'].append(page)
        for line in lines:
            # ignore family/genus headers
            if re.match(r'^\s*[A-Z]+\s*$', line):
                continue
            # ignore blank lines
            if not line:
                continue
            if not line.startswith(' '):
                current_lines.append(line)
            else:
                first_word = line.split('.')[0].strip()
                if ' ' in first_word:
                    # new name
                    if current_name is not None:
                        current_name[current_label] = current_lines
                        yield current_name
                    current_name = {'pages': [page]}
                    current_label = 'name'
                    current_lines = [line]
                else:
                    start_label(first_word, line)
    current_name[current_label] = current_lines
    yield current_name


def clean_text(names: DataT) -> DataT:
    for name in names:
        yield {
            'text': ' '.join(line.strip() for line in name['lines']),
            'pages': name['pages'],
        }


def split_fields(names: DataT) -> DataT:
    for name in names:
        name['raw_name'] = dict(name)
        match = re.match(r'^([A-Z][a-z ]+) \(?([A-Z][a-zé A-Z\.]+)\)?, (.*)$', name['name'])
        assert match is not None, f'failed to match {name}'
        name['original_name'] = match.group(1)
        name['authority'] = match.group(2)
        name['verbatim_citation'] = match.group(3)
        if 'Holotype' in name and 'UMMZ' in name['Holotype']:
            match = re.match(r'(.*), (UMMZ \d+), (.*)[,;] obtained (.*?)\.?$', name['Holotype'])
            assert match is not None, f'failed to match {name["Holotype"]}'
            front = match.group(1)
            name['type_specimen'] = match.group(2)
            name['loc'] = match.group(3)
            rear = match.group(4)

            if ', CMNH' in front:
                front = re.sub(r', CMNH.*$', '', front)
            match = re.match(r'^([^,]+), (.*)$', front)
            assert match, f'failed to match {front}'
            name['gender_age'] = match.group(1)
            name['body_parts'] = match.group(2)

            rear = re.sub(r'(, (original|Church Coll\.) .*)$', '', rear).strip()
            if 'by ' in rear:
                date, name['collector'] = rear.split('by ')
                if date.strip():
                    name['date'] = date.strip()
            else:
                name['date'] = rear
        else:
            print('handle manually', name)
        yield name


def translate_to_db(names: DataT) -> DataT:
    ummz = models.Collection.by_label('UMMZ')
    for name in names:
        if 'Holotype' in name:
            name['collection'] = ummz
            name['type_specimen_source'] = SOURCE
            name['species_type_kind'] = constants.SpeciesGroupType.holotype
        type_tags = []
        if 'gender_age' in name:
            type_tags += lib.extract_gender_age(name['gender_age'])
        if 'body_parts' in name:
            body_parts = lib.extract_body_parts(name['body_parts'])
            if body_parts:
                type_tags += body_parts
            else:
                type_tags.append(models.TypeTag.SpecimenDetail(name['body_parts'], SOURCE))
        if 'loc' in name:
            type_tags.append(models.TypeTag.LocationDetail(name['loc'], SOURCE))
            parts = [re.sub(r' \([^\(]+\)$', '', part) for part in name['loc'].split(', ')]
            type_loc = lib.extract_region(parts)
            if type_loc is not None:
                name['type_locality'] = type_loc
            else:
                print('could not extract type locality from', name['loc'])
        if 'collector' in name:
            type_tags.append(models.TypeTag.Collector(name['collector']))
        if 'date' in name:
            type_tags.append(models.TypeTag.Date(name['date']))

        if type_tags:
            name['type_tags'] = type_tags
        yield name


def associate_names(names: DataT) -> DataT:
    yield from lib.associate_names(names, {'Murie': 'A. Murie'})


def main():
    lines = get_text()
    pages = lib.extract_pages(lines)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = translate_to_db(names)
    names = associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False)
    return names


if __name__ == '__main__':
    for name in main():
        pass  #print(name)