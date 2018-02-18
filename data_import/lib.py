import enum
import functools
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import (Any, Counter, Dict, Iterable, List, Mapping, NamedTuple,
                    Optional, Sequence, Set, Tuple, Type)

import Levenshtein
import unidecode

from taxonomy import getinput
from taxonomy.db import constants, helpers, models
from taxonomy.db.models import TypeTag

DATA_DIR = Path(__file__).parent / 'data'
NAME_SYNONYMS = {
    "Costa 'Rica": 'Costa Rica',
    'Bahama Islands': 'Bahamas',
    'British Guiana': 'Guyana',
    'Burma': 'Myanmar',
    'Cape York': 'Queensland',
    'Celebes': 'Sulawesi',
    'French Congo': 'Rep Congo',
    'Fukien': 'Fujian',
    'Hainan Island': 'Hainan',
    'Irian Jaya': 'Western New Guinea',
    'Kazakstan': 'Kazakhstan',
    'Matto Grosso do Sul': 'Mato Grosso do Sul',
    'Matto Grosso': 'Mato Grosso',
    'Netherlands New Guinea': 'Western New Guinea',
    'Newfoundland': 'Newfoundland and Labrador',
    'Nicaraugua': 'Nicaragua',
    'Northwest Territory': 'Northwest Territories',
    'Philippine Islands': 'Philippines',
    'Russian Federation': 'Russia',
    'Shensi': 'Shaanxi',
    'Siam': 'Thailand',
    'Sulawesi Selatan': 'Sulawesi',
    'Timor Island': 'West Timor',
    'Vera Cruz': 'Veracruz',
    'Zaire': 'DR Congo',
    'Baja California [Sur]': 'Baja California Sur',
    'Estado de México': 'Mexico State',
    'Panama Canal Zone': 'Panama',
    'Labrador': 'Newfoundland and Labrador',
    'Greater Antilles': 'North America',
    'Lesser Antilles': 'North America',
    'West Indies': 'North America',
    'Federated States of Micronesia': 'Micronesia',
    'Marocco': 'Morocco',
    'Tchad': 'Chad',
    'New Britain': 'Papua New Guinea',
    'Bismarck Archipelago': 'Papua New Guinea',
    'Kei Islands': 'Kai Islands',
    'North-East New Guinea': 'Papua New Guinea',
    'Papua': 'New Guinea',
    'Batchian': 'Batjan',
    'Ceram': 'Seram',
    'Amboina': 'Ambon',
    'Banda Islands': 'Moluccas',
    'Sicily': 'Italy',
    'Malay States': 'Peninsular Malaysia',
    'New Ireland': 'Papua New Guinea',
    'Admiralty Islands': 'Papua New Guinea',
    'Trobriand Islands': 'Papua New Guinea',
    "D'Entrecasteaux Archipelago": 'Papua New Guinea',
    'Waigeu': 'Waigeo',
    'Maldive Islands': 'Maldives',
    'U.S.A.': 'United States',
    'Tanganyika Territory': 'Tanzania',
    'Tanganyika Territoiy': 'Tanzania',
    'Anglo-Egyptian Sudan': 'Africa',
    'Malagasy Republic': 'Madagascar',
    'British East Africa': 'Kenya',
    'Belgian Congo': 'DR Congo',
    'Nyasaland': 'Malawi',
    'Cameroun': 'Cameroon',
    'Cameroons': 'Cameroon',
    'Asia Minor': 'Turkey',
    'British Honduras': 'Belize',
    'Anglo- Egyptian Sudan': 'Africa',
}
REMOVE_PARENS = re.compile(r' \([A-Z][a-z]+\)')
LATLONG = re.compile(r'''
    (?P<latitude>\d+°\s*(\d+')?\s*[NS])[,\s\[\]]+
    (long\.\s)?(?P<longitude>\d+°\s*(\d+')?\s*[EW])
''', re.VERBOSE)

DataT = Iterable[Dict[str, Any]]


class Source(NamedTuple):
    inputfile: str
    source: str


def get_text(source: Source) -> Iterable[str]:
    with (DATA_DIR / source.inputfile).open() as f:
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


def validate_pages(pages: Iterable[Tuple[int, List[str]]]):
    current_page: Optional[int] = None
    for page, _ in pages:
        if current_page is not None:
            assert page == current_page + 1, f'missing {current_page + 1}'
        current_page = page


def align_columns(pages: Iterable[Tuple[int, List[str]]]) -> Iterable[Tuple[int, List[str]]]:
    """Rearrange the text to separate the two columns on each page."""
    for page, lines in pages:
        # find a position that is blank in every line
        max_len = max(len(line) for line in lines)
        best_blank = -1
        for i in range(max_len):
            if not all(len(line) <= i or line[i] == ' ' for line in lines):
                continue
            num_lines = len([line for line in lines if len(line) > i])
            if num_lines < 10:
                continue
            best_blank = i
        assert best_blank != -1, f'failed to find split for {page}'
        first_column = [line[:best_blank].rstrip() for line in lines]
        second_column = [line[best_blank + 1:].rstrip() for line in lines]
        yield page, first_column + second_column


def clean_text(names: DataT) -> DataT:
    """Puts each field into a single line and undoes line breaks within words."""
    for name in names:
        new_name = {}
        for key, value in name.items():
            if key == 'pages':
                new_name[key] = value
            else:
                text = '\n'.join(value)
                text = text.replace(' \xad ', '')
                text = text.replace('\xad', '')
                text = re.sub(r'- *\n+ *', '', text)
                text = re.sub(r'\s+', ' ', text)
                if isinstance(key, str):
                    text = re.sub(r'^\s*' + key + r'[-:\. ]+', '', text)
                new_name[key] = text.strip()
        yield new_name


def clean_text_simple(names: DataT) -> DataT:
    for name in names:
        yield {
            'text': ' '.join(line.strip() for line in name['lines']),
            'pages': name['pages'],
        }


def translate_to_db(names: DataT, collection_name: str, source: Source, verbose: bool = False) -> DataT:
    coll = models.Collection.by_label(collection_name)
    for name in names:
        if 'authority' in name and ' and ' in name['authority']:
            name['authority'] = name['authority'].replace(' and ', ' & ')
        if 'species_type_kind' in name:
            name['collection'] = coll
            name['type_specimen_source'] = source.source
        type_tags: List[models.TypeTag] = []
        for field in ('age_gender', 'gender_age'):
            if field in name:
                gender_age = extract_gender_age(name[field])
                type_tags += gender_age
                if not gender_age:
                    print(f'failed to parse gender age {name[field]!r}')
        if 'body_parts' in name:
            body_parts = extract_body_parts(name['body_parts'])
            if body_parts:
                type_tags += body_parts
            else:
                if verbose:
                    print(f'failed to parse body parts {name["body_parts"]!r}')
                type_tags.append(models.TypeTag.SpecimenDetail(name['body_parts'], source.source))
        if 'loc' in name:
            text = name['loc']
            type_tags.append(models.TypeTag.LocationDetail(text, source.source))
            match = LATLONG.search(text)
            if match:
                type_tags.append(models.TypeTag.Coordinates(match.group('latitude'), match.group('longitude')))
        if 'collector' in name:
            type_tags.append(models.TypeTag.Collector(name['collector']))
        if 'date' in name:
            date = name['date']
            try:
                date = helpers.standardize_date(date)
            except ValueError:
                if 'specimen_detail' in name and date in name['specimen_detail']:
                    pass  # it will be included elsewhere
                else:
                    type_tags.append(models.TypeTag.SpecimenDetail(f'Collected: "{date}"', source.source))
            else:
                if date is not None:
                    type_tags.append(models.TypeTag.Date(date))
        if 'specimen_detail' in name:
            type_tags.append(models.TypeTag.SpecimenDetail(name['specimen_detail'], source.source))

        if type_tags:
            name['type_tags'] = type_tags
        yield name


def translate_type_locality(names: DataT, start_at_end: bool = False) -> DataT:
    for name in names:
        if 'loc' in name:
            loc = name['loc']
            loc = re.sub(r'[ \[]lat\. .*$', '', loc)
            loc = re.sub(r'[\.,;:\[ ]+$', '', loc)
            parts = [[re.sub(r' \([^\(]+\)$', '', part)] for part in loc.split(', ')]
            if start_at_end:
                parts = list(reversed(parts))
            type_loc = extract_region(parts)
            if type_loc is not None:
                name['type_locality'] = type_loc
            else:
                print('could not extract type locality from', name['loc'])
        yield name


AUTHOR_NAME_RGX = re.compile(r'''
    (?P<name>[A-Z][a-z]+(\s\([A-Z][a-z]+\??\))?(\s\([a-z]+\??\))?(\s[a-z\'-]{3,})?(\svar\.)?\s[a-z\'-]{3,})
    \s
    \(?(?P<authority>([A-Z]\.\s)*[a-zA-Z,\-\. ]+)(,\s\d+)?\)?$
''', re.VERBOSE)


def extract_name_and_author(text: str) -> Dict[str, str]:
    if text == 'Sus oi Miller':
        return {'original_name': 'Sus oi', 'authority': 'Miller'}
    text = re.sub(r' \[sic\.?\]', '', text)
    text = re.sub(r'\[([A-Za-z]+)\]\.?', r'\1', text)
    text = text.replace('\xad', '').replace('œ', 'oe')
    match = AUTHOR_NAME_RGX.match(text)
    assert match, f'failed to match {text!r}'
    authority = match.group('authority').replace(', and ', ' & ').replace(' and ', ' & ').replace(', in', '')
    authority = re.sub(r'(?<=\.) (?=[A-Z]\.)', '', authority)
    return {'original_name': match.group('name'), 'authority': authority}


def enum_has_member(enum_cls: Type[enum.Enum], member: str) -> bool:
    try:
        enum_cls[member]
    except KeyError:
        return False
    else:
        return True


def extract_gender_age(text: str) -> List[TypeTag]:
    text = re.sub(r'\[.*?: ([^\]]+)\]', r'\1', text)
    text = text.strip().lower()
    out = []
    if enum_has_member(constants.SpecimenAge, text):
        out.append(TypeTag.Age(constants.SpecimenAge[text]))
    elif enum_has_member(constants.SpecimenGender, text):
        out.append(TypeTag.Gender(constants.SpecimenGender[text]))
    elif text == 'unsexed adult' or text == 'adult, sex not given' or text == 'adult, sex unknown' or text == 'adult unsexed':
        out.append(TypeTag.Age(constants.SpecimenAge.adult))
        out.append(TypeTag.Gender(constants.SpecimenGender.unknown))
    elif text.endswith(', age not given'):
        gender = text.split(',')[0]
        if enum_has_member(constants.SpecimenGender, gender):
            out.append(TypeTag.Gender(constants.SpecimenGender[gender]))
    elif ' ' in text:
        age, gender = text.rsplit(maxsplit=1)
        if enum_has_member(constants.SpecimenAge, age):
            out.append(TypeTag.Age(constants.SpecimenAge[age]))
        elif age == 'immature' or age == 'young':
            out.append(TypeTag.Age(constants.SpecimenAge.juvenile))
        elif age == 'young adult':
            out.append(TypeTag.Age(constants.SpecimenAge.subadult))
        elif age == 'old' or age == 'old adult' or age == 'aged':
            out.append(TypeTag.Age(constants.SpecimenAge.adult))
        if enum_has_member(constants.SpecimenGender, gender):
            out.append(TypeTag.Gender(constants.SpecimenGender[gender]))
    return out


SKIN = TypeTag.Organ(constants.Organ.skin, '', '')
SKULL = TypeTag.Organ(constants.Organ.skull, '', '')
IN_ALCOHOL = TypeTag.Organ(constants.Organ.in_alcohol, '', '')
SKELETON = TypeTag.Organ(constants.Organ.postcranial_skeleton, '', '')


def extract_body_parts(organs: str) -> List[TypeTag]:
    organs = organs.lower().replace('[', '').replace(']', '')
    organs = re.sub(r'sk..?ll', 'skull', organs).replace('skufl', 'skull')
    if organs in ('skin and skull', 'skin and cranium', 'study skin and skull', 'skull and skin', 'mounted skin and skull', 'skull and head skin') or '(skin and skull)' in organs:
        tags = [SKIN, SKULL]
    elif organs == 'skin and skeleton':
        tags = [SKIN, SKULL, SKELETON]
    elif organs.startswith('skin, skull,'):
        tags = [SKIN, SKULL]
        if 'skeleton' in organs:
            tags.append(SKELETON)
        elif 'in alcohol' in organs:
            tags.append(IN_ALCOHOL)
    elif 'in alcohol' in organs or 'alcoholic' in organs or 'in spirits' in organs:
        tags = [IN_ALCOHOL]
        if 'skull' in organs:
            tags.append(SKULL)
    elif organs.startswith(('skull only', 'cranium only')):
        tags = [SKULL]
    elif organs.startswith('skin only'):
        tags = [SKIN]
    elif organs == 'skin':
        tags = [SKIN]
    elif organs in ('skull', 'cranium'):
        tags = [SKULL]
    elif organs == 'skull and postcranial skeleton':
        tags = [SKULL, SKELETON]
    elif 'mandible' in organs or 'ramus' in organs:
        tags = [TypeTag.Organ(constants.Organ.mandible, organs, '')]
    else:
        tags = []
    return tags


def get_possible_names(names: Iterable[str]) -> Iterable[str]:
    for name in names:
        yield name
        yield NAME_SYNONYMS.get(name, name)
        if name.endswith(' Island'):
            fixed = name[:-len(' Island')]
            yield fixed
            yield NAME_SYNONYMS.get(fixed, fixed)
        without_direction = re.sub(r'^(North|South|West|East|NE|SE|NW|SW|Republic of|Central|Middle)-?(west|east)?(ern)? (central )?', '', name, flags=re.IGNORECASE)
        if without_direction != name:
            yield without_direction
            yield NAME_SYNONYMS.get(without_direction, without_direction)
        without_diacritics = unidecode.unidecode(name)
        if name != without_diacritics:
            yield without_diacritics


def get_region_from_name(raw_names: Sequence[str]) -> Optional[models.Region]:
    for name in get_possible_names(raw_names):
        name = NAME_SYNONYMS.get(name, name)
        try:
            return models.Region.get(models.Region.name == name)
        except models.Region.DoesNotExist:
            pass
    return None


def extract_region(components: Sequence[Sequence[str]]) -> Optional[models.Location]:
    possible_region = get_region_from_name(components[0])
    if possible_region is None:
        # print(f'could not extract region from {components}')
        return None
    region = possible_region
    if len(components) > 1 and region.children.count() > 0:
        for name in get_possible_names(components[1]):
            name = NAME_SYNONYMS.get(name, name)
            try:
                region = region.children.filter(models.Region.name == name).get()
                break
            except models.Region.DoesNotExist:
                pass
        else:
            # Child regions for these are just for some outlying islands, so we don't care
            # if we can't extract a child.
            if region.name not in ('Colombia', 'Ecuador', 'Honduras'):
                pass  # print(f'could not extract subregion from {components}')
    return region.get_location()


def genus_name_of_name(name: models.Name) -> Optional[str]:
    try:
        return name.taxon.parent_of_rank(constants.Rank.genus).valid_name
    except ValueError:
        return None


def genus_of_name(name: models.Name) -> Optional[models.Taxon]:
    try:
        return name.taxon.parent_of_rank(constants.Rank.genus)
    except ValueError:
        return None


def find_name(original_name: str, authority: str) -> Optional[models.Name]:
    # Exact match
    try:
        return models.Name.get(models.Name.original_name == original_name, models.Name.authority == authority)
    except models.Name.DoesNotExist:
        pass

    # Names without original names, but in the same genus or subgenus
    root_name = original_name.split()[-1]
    genus_name = helpers.genus_name_of_name(original_name)
    possible_genus_names = [genus_name]
    # try subgenus
    match = re.search(r'\(([A-Z][a-z]+)\)', original_name)
    if match:
        possible_genus_names.append(match.group(1))
    all_names = models.Name.filter(models.Name.root_name == root_name, models.Name.authority == authority)
    for genus in possible_genus_names:
        names = [name for name in all_names if genus_name_of_name(name) == genus]
        if len(names) == 1:
            return names[0]

    # If the genus name is a synonym, try its valid equivalent.
    genus_nams = list(models.Name.filter(models.Name.group == constants.Group.genus,
                                         models.Name.root_name == genus_name))
    if len(genus_nams) == 1:
        txn = genus_nams[0].taxon.parent_of_rank(constants.Rank.genus)
        names = [name for name in all_names if genus_of_name(name) == txn]
        if len(names) == 1:
            return names[0]
    # Fuzzy match on original name
    matches = [
        name
        for name in models.Name.filter(models.Name.original_name != None, models.Name.authority == authority)
        if Levenshtein.distance(original_name, name.original_name) < 3 or
        REMOVE_PARENS.sub('', original_name) == REMOVE_PARENS.sub('', name.original_name)
    ]
    if len(matches) == 1:
        return matches[0]

    # Find names without an original name in similar genera.
    name_genus_pairs, genus_to_orig_genera = build_original_name_map(root_name)
    matches = []
    for nam, genus in name_genus_pairs:
        if genus_name in genus_to_orig_genera[genus]:
            matches.append(nam)
    if len(matches) == 1:
        return matches[0]
    return None


@functools.lru_cache(maxsize=1024)
def build_original_name_map(root_name: str) -> Tuple[List[Tuple[models.Name, models.Taxon]], Dict[models.Taxon, Set[str]]]:
    nams: List[Tuple[models.Name, models.Taxon]] = []
    genus_to_orig_genera: Dict[models.Taxon, Set[str]] = {}
    for nam in models.Name.filter(models.Name.group == constants.Group.species, models.Name.original_name >> None, models.Name.root_name == root_name):
        try:
            genus = nam.taxon.parent_of_rank(constants.Rank.genus)
        except ValueError:
            continue
        nams.append((nam, genus))
        if genus not in genus_to_orig_genera:
            genus_to_orig_genera[genus] = get_original_genera_of_genus(genus)
    return nams, genus_to_orig_genera


@functools.lru_cache(maxsize=1024)
def get_original_genera_of_genus(genus: models.Taxon) -> Set[str]:
    return {
        helpers.genus_name_of_name(nam.original_name)
        for nam in genus.all_names()
        if nam.group == constants.Group.species and nam.original_name is not None
    }


def name_variants(original_name: str, authority: str) -> Iterable[Tuple[str, str]]:
    yield original_name, authority
    original_authority = authority
    unspaced = re.sub(r'([A-Z]\.) (?=[A-Z]\.)', r'\1', authority).strip()
    if original_authority != unspaced:
        yield original_name, unspaced
    authority = re.sub(r'([A-Z]\.)+ ', '', authority).strip()
    if authority != original_authority:
        yield original_name, authority
    if ' in ' in authority:
        authority = re.sub(r',? in .*$', '', authority)
        yield original_name, authority
        yield original_name, re.sub(r'^.* in ', '', original_authority)
    # This should be generalized (initials are in the DB but not the source)
    if authority.startswith('Allen'):
        yield original_name, 'J.A. ' + authority
        yield original_name, 'J.A. ' + authority + ' & Chapman'
        yield original_name, 'G.M. ' + authority
    if authority.startswith('Anthony'):
        yield original_name, 'H.E. ' + authority
    if authority == 'Andersen':
        yield original_name, 'K. Andersen'
    if authority == 'Gray':
        yield original_name, 'J.E. Gray'
    if authority == 'Bonaparte':
        yield original_name, 'C.L. Bonaparte'
    if authority == 'Cuvier':
        yield original_name, 'F. Cuvier'
    if authority == 'Major':
        yield original_name, 'Forsyth Major'
    if authority.startswith('Bailey'):
        yield original_name, 'V. Bailey'
    if authority.startswith('Howell'):
        yield original_name, 'A.H. Howell'
    if authority == 'Hill':
        yield original_name, 'J. Eric Hill'
    if 'ue' in original_name:
        yield original_name.replace('ue', 'ü'), authority
    if authority in ('Wied', 'Wied-Neuwied'):
        yield original_name, 'Wied-Neuwied'
    if authority == 'Geoffroy':
        yield original_name, 'É. Geoffroy Saint-Hilaire'
        yield original_name, 'I. Geoffroy Saint-Hilaire'
    if authority == 'Schwartz':
        yield original_name, 'Schwarz'
    if authority == 'Fischer':
        yield original_name, 'J.B. Fischer'
    if authority == 'Linné':
        yield original_name, 'Linnaeus'
    if authority == 'Mjoberg':
        yield original_name, 'Mjöberg'
    if authority == 'Forster':
        yield original_name, 'Förster'
    if authority == 'Rummler':
        yield original_name, 'Rümmler'
    if authority in ('Müller & Schlegel', 'Schlegel & Müller'):
        # many names that were previously attributed to M & S were earlier described by M alone
        yield original_name, 'Müller'


def associate_types(names: DataT, author_fixes: Mapping[str, str] = {}, original_name_fixes: Mapping[str, str] = {}) -> DataT:
    for name in names:
        if 'type_name' in name and 'type_authority' in name:
            typ = identify_name(name['type_name'], name['type_authority'], author_fixes, original_name_fixes)
            if typ:
                name['type'] = typ
        yield name


def identify_name(orig_name: str, author: str, author_fixes: Mapping[str, str] = {}, original_name_fixes: Mapping[str, str] = {}) -> Optional[models.Name]:
    name_obj = None
    author = author_fixes.get(author, author)
    orig_name = original_name_fixes.get(orig_name, orig_name)

    for original_name, authority in name_variants(orig_name, author.strip()):
        name_obj = find_name(original_name, authority)
        if name_obj is not None:
            break
    if name_obj:
        return name_obj
    else:
        print(f'could not find name {orig_name} -- {author} (tried variants {list(name_variants(orig_name, author))})')
        return None


def associate_names(names: DataT, author_fixes: Mapping[str, str] = {}, original_name_fixes: Mapping[str, str] = {},
                    start_at: Optional[str] = None) -> DataT:
    total = 0
    found = 0
    found_first = start_at is None
    for name in names:
        if not found_first:
            if 'original_name' in name and name['original_name'] == start_at:
                found_first = True
            else:
                continue
        total += 1
        if 'original_name' in name and 'authority' in name:
            name_obj = identify_name(name['original_name'], name['authority'], author_fixes, original_name_fixes)
            if name_obj:
                found += 1
                name['name_obj'] = name_obj
        yield name
    print(f'found: {found}/{total}')


def write_to_db(names: DataT, source: Source, dry_run: bool = True, edit_if_no_holotype: bool = True) -> None:
    name_discrepancies = []
    num_changed: Counter[str] = Counter()
    for name in names:
        if 'name_obj' not in name:
            continue
        nam = name['name_obj']
        print(f'--- processing {nam} ({name["pages"] if "pages" in name else ""}) ---')
        for attr in ('type_locality', 'type_tags', 'collection', 'type_specimen', 'species_type_kind', 'verbatim_citation', 'original_name', 'type_specimen_source', 'type'):
            if attr not in name or name[attr] is None:
                continue
            if attr == 'verbatim_citation' and nam.original_citation is not None:
                continue
            current_value = getattr(nam, attr)
            new_value = name[attr]
            if current_value == new_value:
                continue
            elif current_value is not None:
                if attr == 'type_locality':
                    # if the new TL is a parent of the current, ignore it
                    if new_value.region in current_value.region.all_parents():
                        continue
                if attr == 'type_tags':
                    new_tags = set(new_value) - set(current_value)
                    existing_types = tuple({type(tag) for tag in current_value})
                    tags_of_new_types = {
                        tag for tag in new_tags
                        # Always add LocationDetail tags, because it has a source field and it's OK to have multiple tags
                        if (not isinstance(tag, existing_types)) or isinstance(tag, (TypeTag.LocationDetail, TypeTag.SpecimenDetail))
                    }
                    if not tags_of_new_types:
                        continue
                    print(f'adding tags: {tags_of_new_types}')
                    if not dry_run:
                        nam.type_tags = sorted(nam.type_tags + tuple(tags_of_new_types))
                    new_tags -= tags_of_new_types
                    if new_tags:
                        print(f'new tags: {new_tags}')
                        if not dry_run:
                            nam.fill_field('type_tags')
                    continue

                if attr == 'verbatim_citation':
                    new_value = f'{current_value} [From {{{source.source}}}: {new_value}]'
                else:
                    print(f'value for {attr} differs: (new) {new_value} vs. (current) {current_value}')

                if attr == 'type_specimen_source':
                    nam.display(full=True)
                    if not dry_run:
                        should_replace = getinput.yes_no('Replace type_specimen_source? ')
                        if not should_replace:
                            continue
                elif attr == 'original_name':
                    new_root_name = helpers.root_name_of_name(new_value, constants.Rank.species)
                    if helpers.root_name_of_name(nam.original_name, constants.Rank.species).lower() != new_root_name.lower():
                        if not dry_run and not getinput.yes_no(f'Is the source\'s spelling {new_value} correct?'):
                            continue
                        try:
                            existing = models.Name.filter(models.Name.original_name == new_value).get()
                        except models.Name.DoesNotExist:
                            print(f'creating ISS with orig name={new_value}')
                            if not dry_run:
                                nam.open_description()
                                if getinput.yes_no(f'Is the original spelling {nam.original_name} correct? '):
                                    if 'pages' in name:
                                        page_described = name['pages'][0]
                                    else:
                                        page_described = None
                                    nam.add_variant(new_root_name, constants.NomenclatureStatus.incorrect_subsequent_spelling,
                                                    paper=source.source, page_described=page_described, original_name=new_value)
                                    continue
                        else:
                            if existing.original_citation == source.source:
                                continue
                    name_discrepancies.append((nam, current_value, new_value))
                    continue
                elif attr != 'verbatim_citation':
                    if not dry_run:
                        nam.display()
                        nam.fill_field(attr)
                    continue
            num_changed[attr] += 1
            if not dry_run:
                setattr(nam, attr, new_value)

        if 'pages' not in name:
            pages = ''
        elif len(name['pages']) == 1:
            pages = str(name['pages'][0])
        else:
            pages = f'{name["pages"][0]}-{name["pages"][-1]}'
        if not dry_run:
            if edit_if_no_holotype and ('species_type_kind' not in name or name['species_type_kind'] != constants.SpeciesGroupType.holotype):
                print(f'{nam} does not have a holotype: {name}')
                nam.fill_field('type_tags')

            if nam.comments.filter(models.NameComment.source == source.source).count() == 0:
                nam.add_comment(constants.CommentKind.structured_quote, json.dumps(name['raw_text']), source.source, pages)

    for nam, current, new in name_discrepancies:
        print('----------')
        print(f'discrepancy for {nam} (p. {nam.page_described})')
        print(f'current: {current}')
        print(f'new: {new}')
        if not dry_run:
            nam.open_description()
            getinput.get_line('press enter to continue> ')

    for attr, value in num_changed.most_common():
        print(f'{attr}: {value}')


def print_counts(names: DataT, field: str) -> None:
    counts: Counter[Any] = Counter(name[field] for name in names if field in name)
    for value, count in counts.most_common():
        print(count, value)


def print_counts_if_no_tag(names: DataT, field: str, tag_cls: TypeTag) -> None:
    counts: Counter[Any] = Counter()
    for name in names:
        if field in name and ('type_tags' not in name or not any(isinstance(tag, tag_cls) for tag in name['type_tags'])):
            counts[name[field]] += 1
    for value, count in counts.most_common():
        print(count, value)


def print_field_counts(names: DataT) -> None:
    counts: Counter[str] = Counter()
    for name in names:
        for field, value in name.items():
            counts[field] += 1
            if field == 'type_tags':
                tags = sorted({type(tag).__name__ for tag in value})
                for tag in tags:
                    counts[tag] += 1

    for value, count in counts.most_common():
        print(count, value)
