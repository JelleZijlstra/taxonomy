import json
import re
from typing import Any, Counter, Dict, Iterable, List, Optional, Tuple

from taxonomy import getinput
from taxonomy.db import constants, helpers, models

from . import lawrence1993
from .lawrence1993 import DataT

FILE_PATH = lawrence1993.DATA_DIR / 'mexicotypes-layout.txt'
SOURCE = 'Mexico-type localities.pdf'


def get_text() -> Iterable[str]:
    with FILE_PATH.open() as f:
        yield from f


def extract_pages(lines: Iterable[str]) -> Iterable[Tuple[int, List[str]]]:
    """Split the text into pages."""
    current_page = None
    current_lines = []
    for line in lines:
        if line.startswith('\x0c'):
            if current_page is not None:
                yield current_page, current_lines
                current_lines = []
            line = line[1:].strip()
            if re.search(r'^\d+ ', line):
                # page number on the left
                current_page = int(line.split()[0])
            else:
                # or the right
                current_page = int(line.split()[-1])
        else:
            current_lines.append(line)
    # last page
    assert current_page is not None
    yield current_page, current_lines


def extract_names(pages: Iterable[Tuple[int, List[str]]]) -> Iterable[Dict[str, Any]]:
    current_name: Optional[Dict[str, Any]] = None
    current_lines: List[str] = []

    for page, lines in pages:
        if current_name is not None:
            current_name['pages'].append(page)
        for line in lines:
            line = line.rstrip()
            # ignore family/genus headers
            if re.match(r'^(ORDEN|FAMILIA) [A-Z]+$', line.strip()):
                continue
            # ignore blank lines
            if not line:
                continue
            if line.startswith(' '):
                current_lines.append(line)
            else:
                if current_name is not None:
                    current_name['lines'] = current_lines
                    yield current_name
                current_name = {'pages': [page]}
                current_lines = [line]

    assert current_name is not None
    current_name['lines'] = current_lines
    yield current_name


def clean_text(names: DataT) -> DataT:
    for name in names:
        yield {
            'text': ' '.join(line.strip() for line in name['lines']),
            'pages': name['pages'],
        }


def split_text(names: DataT) -> DataT:
    for name in names:
        text = name['text']
        try:
            name_author, rest = re.split(r'(?<=[,\.] \d{4})\. ', text, maxsplit=1)
        except ValueError:
            raise ValueError(f'failed to parse {text}')
        match = re.search(r'\. \(([A-Za-z ]+)\)\.?$', rest)
        if match:
            current_name = match.group(1)
            rest = rest[:match.start()]
        else:
            current_name = None
        try:
            citation, loc = re.split(r'(?<=[: \d]\d)\. ', rest, maxsplit=1)
        except ValueError:
            print(f'failed to parse {rest}')
            continue

        yield {
            'text': text,
            'pages': name['pages'],
            'name_author': name_author,
            'current_name': current_name,
            'citation': citation,
            'loc': loc.rstrip('.'),
        }


def split_fields(names: DataT) -> DataT:
    for name in names:
        match = re.match(r'^(\[?[A-Z].*( [a-z-\[\],]{3,})) ((de )?[A-Z].*?)[,\.] (\d{4})$', name['name_author'])
        assert match is not None, f'failed to match {name}'
        name['scientific_name'] = match.group(1)
        name['author'] = match.group(3)
        name['year'] = match.group(5)
        match = re.search(r', ([A-Za-záéíóñ \[\]]+)$', name['loc'])
        if match:
            name['state'] = match.group(1)
        yield name


def associate_names(names: DataT) -> DataT:
    total = 0
    found = 0
    for name in names:
        name['raw_name'] = {k: v for k, v in name.items() if k not in ('text', 'name_author')}
        name_obj = None
        total += 1
        author = name['author'].replace(' y ', ' & ').replace('G. M.', 'G.M.').replace('J. A.', 'J.A.').replace('Tshudi', 'Tschudi').replace('Bachmann', 'Bachman')
        if author == 'Goldman & Gardner':
            author = 'Goldman & M.C. Gardner'
        if author == 'Rhen':
            author = 'Rehn'
        if author == 'La Val':
            author = 'LaVal'
        if author == 'Menegaux':
            author = 'Ménègaux'
        if author == 'Ferrari Perez':
            author = 'Ferrari-Pérez'
        if author == 'Bryant':
            author = 'W. Bryant'
        if author == 'Kelson':
            author = 'Goldman'
        if author == 'Robertson':
            author = 'Robertson & Musser'
        if author == 'Findley':
            author = 'Finley'
        if author == 'Major':
            author = 'Forsyth Major'
        if author == 'Linneaus':
            author = 'Linnaeus'
        orig_name = name['scientific_name']
        if orig_name == 'Antrozous pallidus packardi':
            # citation for this name is wrong; unclear what is meant
            continue
        orig_name = orig_name.replace(' [sic]', '')
        orig_name = re.sub(r'[\[\]\.,]', '', orig_name)
        if orig_name == 'Myrmecophaga tamandua var mexicana':
            orig_name = 'Myrmecophaga tamandua Var. Mexicana'
        if orig_name == 'Sciurus? aureogaster':
            orig_name = 'Sciurus aureogaster'
            author = 'F. Cuvier'
        if orig_name == 'Spermophilus beecheyi rupinarum':
            orig_name = 'Citellus beecheyi rupinarum'
        if orig_name == 'Spermophilus spilosoma ammophilus':
            orig_name = 'Citellus spilosoma ammophilus'
        if orig_name == 'Tamias obscurus meridionalis':
            orig_name = 'Eutamias merriami meridionalis'
        if orig_name == 'Dipodomys philipsii':
            orig_name = 'Dipodomys Phillipii'
        if orig_name == 'Microtus mexicanus ocotensis':
            author = 'T. Alvarez & Hernández-Chávez'

        for original_name, authority in lawrence1993.name_variants(orig_name, author):
            name_obj = lawrence1993.find_name(original_name, authority)
            if name_obj is not None:
                break
        if name_obj:
            found += 1
        else:
            print(f'could not find name {orig_name} {author}')
        name['name_obj'] = name_obj
        if orig_name != 'Perognathus longimembris internationalis' and 'state' in name:
            state = 'Mexico State' if name['state'] == 'México' else name['state']
            region = lawrence1993.get_region_from_name([state])
            if region is not None:
                name['type_locality'] = region.get_location()
        yield name
    print(f'found: {found}/{total}')


def write_to_db(names: DataT, dry_run: bool = True) -> None:
    name_discrepancies = []
    num_changed: Counter[str] = Counter()
    for name in names:
        nam = name['name_obj']
        if nam is None:
            continue
        print(f'--- processing {nam} ---')
        pages = '-'.join(map(str, name['pages']))
        if nam.verbatim_citation is None and nam.original_citation is None:
            if not dry_run:
                nam.verbatim_citation = name['citation']
            num_changed['verbatim_citation'] += 1
        if 'type_locality' in name:
            num_changed['has_type_locality'] += 1
            if nam.type_locality != name['type_locality']:
                if nam.type_locality is not None:
                    print(f'value for type locality differs: (new) {name["type_locality"]} vs. (current) {nam.type_locality}')
                    print(name['loc'])
                    print(name['state'])
                    print(nam.type_tags, nam.type_locality_description)
                if not dry_run:
                    nam.type_locality = name['type_locality']
                num_changed['type_locality'] += 1
        if not dry_run:
            nam.add_type_tag(models.TypeTag.LocationDetail(name['loc'], SOURCE))
        if nam.original_name is None:
            if not dry_run:
                nam.original_name = name['scientific_name']
            num_changed['original_name'] += 1
        if nam.original_name is not None and nam.original_name != name['scientific_name']:
            num_changed['discrepancies'] += 1
            new_root_name = helpers.root_name_of_name(name['scientific_name'], constants.Rank.species)
            if helpers.root_name_of_name(nam.original_name, constants.Rank.species).lower() != new_root_name.lower():
                name_discrepancies.append((nam, nam.original_name, name['scientific_name']))

        if not dry_run:
            nam.add_comment(constants.CommentKind.structured_quote, json.dumps(name['raw_name']), SOURCE, pages)
            nam.save()

    for nam, current, new in name_discrepancies:
        print('----------')
        print(f'discrepancy for {nam}')
        print(f'current: {current}')
        print(f'new: {new}')
        if not dry_run:
            nam.open_description()
            getinput.get_line('press enter to continue> ')

    for attr, value in num_changed.most_common():
        print(f'{attr}: {value}')


def main() -> None:
    lines = get_text()
    pages = extract_pages(lines)
    names = extract_names(pages)
    names = clean_text(names)
    names = split_text(names)
    names = split_fields(names)
    names = associate_names(names)
    #yield from names
    write_to_db(names, dry_run=False)


if __name__ == '__main__':
    main()
