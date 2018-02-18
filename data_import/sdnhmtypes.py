import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from taxonomy.db import constants, models

from . import lib
from .lib import DataT

SOURCE = lib.Source('sdnhmtypes-bhl.txt', 'SDNHM-types.pdf')
LABELS = [
    'Holotype',
    'Remarks',
    'Type Locality',
    'Type locality',
]
LABEL_RGX = re.compile(r'^\s*({})[\.:\s]+'.format('|'.join(LABELS)))
NAME_RGX = re.compile(r'''
    ^(?P<original_name>[A-Z][a-z]+(\s\([A-Z][a-z]+\))?((\svar\.)?\s[a-z-]+){1,2})
    \s(?P<authority>[A-Z][a-zé A-Z\.\-&]+)$
''', re.VERBOSE)
TYPE_RGX = re.compile(r'''
    ^(?P<gender_age>[^\.]+)\.\sCollected\sby\s(?P<collector>[^,]+),\s(?P<date>[^\.]+)\.\s
    (Original\sno\.\s.*?,\snow\s)?SDSNH\sno\.\s(?P<number>\d+)['"]?\.$
''', re.VERBOSE)
LATLONG_RGX = re.compile(r'''
    lat\.\s+(?P<latitude>\d+[°""]\d+'\s*[NS]).*
    long\.\s+(?P<longitude>\d+[°""]\d+'\s*[WE])
''', re.VERBOSE)


def extract_pages(lines: Iterable[str]) -> Iterable[Tuple[int, List[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines = []
    for line in lines:
        match = re.search(r'^(\d+) SAN DIEGO SOCIETY OF NATURAL HISTORY', line, re.IGNORECASE)
        if match:
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            current_page = int(match.group(1))
        else:
            match = re.search(r'TYPE SPECIMENS Of MAMMALS +(\d+) *$', line, re.IGNORECASE)
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
            is_header = line.startswith('FAMILY')
            # ignore blank lines
            if not is_blank and not is_header:
                match = LABEL_RGX.search(line)
                if match:
                    start_label(match.group(1), line)
                elif line.startswith('='):
                    start_label('synonymy', line)
                elif current_label == 'name':
                    start_label('verbatim_citation', line)
                else:
                    if last_line_header and last_line_blank:
                        is_new_name = True
                    elif not last_line_blank:
                        is_new_name = False
                    elif current_label not in ('Remarks', 'Type locality'):
                        is_new_name = False
                    else:
                        is_new_name = line.lstrip()[0].isupper() and re.search(r'[a-z\)] [a-z]', line)
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

        if 'Holotype' in name:
            name['species_type_kind'] = constants.SpeciesGroupType.holotype
            match = TYPE_RGX.match(name['Holotype'])
            assert match is not None, f'failed to match {name["Holotype"]}'
            type_specimen = match.group('number')
            name['type_specimen'] = f'SDSNH {type_specimen}'
            for field in ('gender_age', 'collector', 'date'):
                name[field] = match.group(field)
        if 'Type locality' in name:
            text = name['Type locality']
            name['loc'] = text
            if LATLONG_RGX.search(text):
                pass
        yield name


def associate_names(names: DataT) -> DataT:
    yield from lib.associate_names(names, {
        'Savage & Wyman': 'Savage',
    }, {
        'Hesperomys sonoriensis nebrascensis': 'Hesperomys sonoriensis var. nebrascensis',
    })


def main() -> DataT:
    lines = lib.get_text(SOURCE)
    pages = extract_pages(lines)
    # lib.validate_pages(pages)
    names = extract_names(pages)
    names = lib.clean_text(names)
    names = split_fields(names)
    names = lib.translate_to_db(names, 'SDNHM', SOURCE)
    names = lib.translate_type_locality(names, start_at_end=True)
    names = associate_names(names)
    lib.write_to_db(names, SOURCE, dry_run=False)
    # lib.print_counts_if_no_tag(names, 'loc', models.TypeTag.Coordinates)
    lib.print_field_counts(names)
    return names


if __name__ == '__main__':
    for _ in main():
        pass  # print(name)
