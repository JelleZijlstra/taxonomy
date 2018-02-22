import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from taxonomy.db import constants

from . import lib
from .lib import DataT

SOURCE = lib.Source('mcztypes-bhl.txt', 'MCZ-types.pdf')
LABELS = [
    'Holotype',
    'Lectotype',
    'Collector',
    'Condition',
    'Type Series',
    'Comments',
    'Neotype',
    'Syntypes',
    'Locality',
    'Syntype',
]
LABEL_RGX = re.compile(r'^\s*({})[\.:\s]+'.format('|'.join(LABELS)))
NAME_RGX = re.compile(r'''
    ^(?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?((\svar\.)?\s[a-z-]+){1,2})
    \s\(?(?P<authority>[A-Z][a-zé A-Z\.-]+)\)?,\s(?P<year>\d{4})[a-zA-Z]?\s(?P<verbatim_citation>.*)$
''', re.VERBOSE)


def extract_pages(lines: Iterable[str]) -> Iterable[Tuple[int, List[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines: List[str] = []
    for line in lines:
        match = re.search(r'^(\d+) Bulletin Museum of Comparative', line)
        if match:
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            current_page = int(match.group(1))
        else:
            match = re.search(r'^Type Specimens.*den (\d+) *$', line)
            if match:
                if current_page is not None:
                    yield current_page, current_lines
                    current_lines = []
                current_page = int(match.group(1))
            else:
                current_lines.append(line)
    # last page
    assert current_page is not None
    yield current_page, current_lines


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> DataT:
    """Extracts names from the text, as dictionaries."""
    current_name: Optional[Dict[str, Any]] = None
    current_label: Optional[str] = None
    current_lines: List[str] = []
    last_line_blank = False
    last_line_header = False
    last_line_is_genus = False

    def start_label(label: str, line: str) -> None:
        nonlocal current_label, current_lines
        assert current_name is not None, (label, line)
        assert current_label is not None, (label, line)
        assert current_label not in current_name, f'duplicate label {current_label} in {current_name}'
        current_name[current_label] = current_lines
        current_label = label
        current_lines = [line]

    for page, lines in pages:
        if current_name is not None:
            current_name['pages'].append(page)
        for line in lines:
            is_blank = not line.strip()
            is_header = line.startswith(('Genus', 'Family', 'Order', 'Subfamily'))
            # ignore blank lines
            if not is_blank and not is_header and not (last_line_header and not last_line_is_genus):
                match = LABEL_RGX.search(line)
                if match:
                    start_label(match.group(1), line)
                elif line.startswith('='):
                    start_label('synonymy', line)
                else:
                    if last_line_header and last_line_blank:
                        is_new_name = True
                    elif not last_line_blank:
                        is_new_name = False
                    elif current_label not in ('Comments', 'Type Series'):
                        is_new_name = False
                    else:
                        is_new_name = line.lstrip()[0].isupper() and bool(re.search(r'[a-z\)] [a-z]', line))
                    if is_new_name:
                        if current_name is not None:
                            assert current_label is not None
                            current_name[current_label] = current_lines
                            yield current_name
                        current_name = {'pages': [page]}
                        current_label = 'name'
                        current_lines = [line]
                    else:
                        current_lines.append(line)

            last_line_blank = is_blank
            if not is_blank:
                # If this line is blank, just continue with the previous value.
                last_line_header = is_header
                last_line_is_genus = is_header and line.startswith('Genus')
    assert current_label is not None
    assert current_name is not None
    current_name[current_label] = current_lines
    yield current_name


def split_fields(names: DataT) -> DataT:
    for name in names:
        name['raw_text'] = dict(name)
        match = NAME_RGX.match(name['name'])
        assert match is not None, f'failed to match {name}'
        for group, value in match.groupdict().items():
            name[group] = value

        if 'Holotype' in name or 'Lectotype' in name:
            if 'Holotype' in name:
                name['species_type_kind'] = constants.SpeciesGroupType.holotype
                text = name['Holotype']
            else:
                name['species_type_kind'] = constants.SpeciesGroupType.lectotype
                text = name['Lectotype']
            match = re.match(r'^((MCZ |VP |B)\d+)\. ([^\.]+)\.( ([^\.]+)\.)?$', text)
            assert match is not None, f'failed to match {name["Holotype"]}'
            type_specimen = match.group(1)
            if not type_specimen.startswith('MCZ '):
                type_specimen = f'MCZ {type_specimen}'
            name['type_specimen'] = type_specimen
            name['body_parts'] = match.group(3)
            if match.group(4):
                name['gender_age'] = match.group(5)
        elif 'Syntypes' not in name and 'Syntype' not in name:
            print('handle manually', name)
        if 'Condition' in name:
            name['specimen_detail'] = name['Condition']
        if 'Locality' in name:
            text = name['Locality']
            match = re.match(r'^(.*?)\.? +((\d{1,2} +)?[A-Z][a-z]{2,8} +\d{4})[\.,]?$', text)
            if match:
                name['date'] = match.group(2)
                name['loc'] = match.group(1)
            else:
                match = re.match(r'^(.*)\.? +(\d{4})[\.,]?$', text)
                if match:
                    name['date'] = match.group(2)
                    name['loc'] = match.group(1)
                else:
                    name['loc'] = text
        if 'Collector' in name:
            text = name['Collector']
            match = re.match(r'^(.*) Original number +\d+\.?$', text)
            if match:
                name['collector'] = match.group(1)
            else:
                name['collector'] = text
        yield name


def translate_type_locality(names: DataT) -> DataT:
    for name in names:
        if 'loc' in name:
            parts = [[part.strip()] for part in re.split(r'[,\(:\);]+', name['loc'].rstrip('.')) if part.strip()]
            type_loc = lib.extract_region(parts)
            if type_loc is not None:
                name['type_locality'] = type_loc
            else:
                print('could not extract type locality from', name['loc'])
        yield name


def associate_names(names: DataT) -> DataT:
    yield from lib.associate_names(names, lib.NameConfig({
        'Savage & Wyman': 'Savage',
    }, {
        'Hesperomys sonoriensis nebrascensis': 'Hesperomys sonoriensis var. nebrascensis',
    }))


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = extract_pages(lines)
    # lib.validate_pages(pages)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'MCZ', SOURCE)
    names = translate_type_locality(names)
    names = associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False)
    # lib.print_counts_if_no_tag(names, 'Holotype', models.TypeTag.Date)
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for _ in main():
        pass  # print(name)
