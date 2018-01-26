import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from taxonomy.db import constants, models

from . import lib
from .lib import DataT


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    found_first = False
    current_name: Optional[Dict[str, Any]] = None
    current_label: Optional[str] = None
    current_lines: List[str] = []
    in_headings = True

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None
        assert current_label is not None
        if label in current_name:
            if label == 'Syntype':
                label = f'Syntype {line}'
            assert label not in current_name, f'duplicate label {label} in {current_name}'
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name['pages'].append(page)
        for line in lines:
            if not found_first:
                if line.strip() == 'TYPE SPECIMENS':
                    found_first = True
                continue
            # ignore family/genus headers
            if re.match(r'^\s*(Genus|Family|Subfamily|Order) [A-Z][a-zA-Z]+ [a-zA-Z\.’, \-]+(, \d{4})?$', line):
                in_headings = True
                continue
            # ignore blank lines
            if not line:
                continue
            if in_headings:
                if line.startswith(' '):
                    continue
                else:
                    in_headings = False
            if line.startswith(' '):
                current_lines.append(line)
            elif line.startswith('This specimen'):
                start_label('comments', line)
            elif line.startswith('Secondary junior') or line.startswith('Primary junior'):
                start_label('comments', line)
            elif re.match(r'^[A-Z][A-Z a-z]+: ', line):
                start_label(line.split(':')[0], line)
            elif line.startswith('USNM'):
                start_label(line.split('.')[0], line)
            elif current_label not in ('name', 'verbatim_citation', 'comments') and ':' not in line:
                # new name
                if current_name is not None:
                    assert current_label is not None
                    current_name[current_label] = current_lines
                    assert 'Type Locality' in current_name or 'Holotype' in current_name, current_name
                    yield current_name
                current_name = {'pages': [page]}
                current_label = 'name'
                current_lines = [line]
            elif current_label == 'name':
                if re.search(r'\d|\b[A-Z][a-z]+\.|\baus\b|\bDas\b|\bPreliminary\b|\., ', line):
                    start_label('verbatim_citation', line)
                else:
                    # probably continuation of the author
                    current_lines.append(line)
            elif current_label == 'verbatim_citation' or current_label == 'comments':
                start_label('synonymy', line)
            else:
                assert False, line
    assert current_label is not None
    assert current_name is not None
    current_name[current_label] = lines
    yield current_name


def split_fields(names: DataT) -> DataT:
    tried = succeeded = 0
    for name in names:
        name['raw_name'] = dict(name)
        name.update(lib.extract_name_and_author(name['name']))
        if 'Type Locality' in name:
            name['loc'] = name['Type Locality']
        for field in 'Holotype', 'Lectotype', 'Neotype':
            if field in name:
                tried += 1
                name['species_type_kind'] = constants.SpeciesGroupType[field.lower()]
                data = name[field]
                match = re.match(r'^(USNM [\d/]+)\. ([^\.]+)\. ([^\.]+)\. (Collected|Leg\. \(Collected\)) (.*) by (.*)\. (Original [Nn]umbers? .+|No original number.*)\.$', data)
                if match is None:
                    # print(f'failed to match {data!r}')
                    match = re.match(r'^((USNM |ANSP )?[\d/]+)', data)
                    if not match:
                        print(f'failed to match {data} at all')
                    else:
                        name['type_specimen'] = match.group(1)
                else:
                    succeeded += 1
                    name['type_specimen'] = match.group(1)
                    name['body_parts'] = match.group(2)
                    name['gender_age'] = match.group(3)
                    name['date'] = match.group(5)
                    name['collector'] = match.group(6)
                name['specimen_detail'] = data
                break
        yield name
    print(f'succeeded in splitting field: {succeeded}/{tried}')


def translate_to_db(names: DataT, source: lib.Source) -> DataT:
    yield from lib.translate_to_db(names, 'USNM', source)


def translate_type_localities(names: DataT) -> DataT:
    for name in names:
        if 'loc' in name:
            text = name['loc'].rstrip('.')
            text = re.sub(r'\[.*?: ([^\]]+)\]', r'\1', text)
            text = text.replace('[', '').replace(']', '')
            parts = [list(filter(None, re.split(r'[()]', part))) for part in text.split(', ')]
            type_loc = lib.extract_region(list(reversed(parts)))
            if type_loc is not None:
                name['type_locality'] = type_loc
            else:
                print('could not extract type locality from', name['loc'])
                pass
        yield name


def main() -> DataT:
    if len(sys.argv) > 1 and sys.argv[1] == 'ahm':
        source = lib.Source('usnmtypesahm-layout.txt', 'Anomaluromorpha, Hystricomorpha, Myomorpha-USNM types.pdf')
    else:
        source = lib.Source('usnmtypes-layout.txt', 'USNM-types (Fisher & Ludwig 2015).pdf')

    lines = lib.get_text(source)
    pages = lib.extract_pages(lines)
    pages = lib.align_columns(pages)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = translate_to_db(names, source)
    names = translate_type_localities(names)
    names = lib.associate_names(names, {
        'Deleuil & Labbe': 'Deleuil & Labbé',
        'Tavares, Gardner, Ramirez-Chaves & Velazco': 'Tavares, Gardner, Ramírez-Chaves & Velazco',
        'Miller & Allen': 'Miller & G.M. Allen',
        'Robinson & Lyon': 'W. Robinson & Lyon',
    }, {
        'Tana tana besara': 'Tupaia tana besara',
    })
    names = list(names)
    #lib.print_counts(names, 'original_name')
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for name in main():
        pass
