from collections import defaultdict
import enum
import functools
import json
import Levenshtein
from pathlib import Path
import re
from typing import Any, Counter, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple, Type
import unidecode

from taxonomy.db import constants, helpers, models
from taxonomy.db.models import TypeTag
from taxonomy import getinput

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
}
REMOVE_PARENS = re.compile(r' \([A-Z][a-z]+\)')

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
                text = re.sub(r'-\n+ *', '', text)
                text = re.sub(r'\s+', ' ', text)
                if isinstance(key, str):
                    text = re.sub(r'^\s*' + key + r'[-:\. ]+', '', text)
                new_name[key] = text.strip()
        yield new_name


def translate_to_db(names: DataT, collection_name: str, source: Source) -> DataT:
    ummz = models.Collection.by_label(collection_name)
    for name in names:
        if 'species_type_kind' in name:
            name['collection'] = ummz
            name['type_specimen_source'] = source.source
        type_tags: List[models.TypeTag] = []
        if 'gender_age' in name:
            type_tags += extract_gender_age(name['gender_age'])
        if 'body_parts' in name:
            body_parts = extract_body_parts(name['body_parts'])
            if body_parts:
                type_tags += body_parts
            else:
                type_tags.append(models.TypeTag.SpecimenDetail(name['body_parts'], source.source))
        if 'loc' in name:
            type_tags.append(models.TypeTag.LocationDetail(name['loc'], source.source))
        if 'collector' in name:
            type_tags.append(models.TypeTag.Collector(name['collector']))
        if 'date' in name:
            type_tags.append(models.TypeTag.Date(name['date']))
        if 'specimen_detail' in name:
            type_tags.append(models.TypeTag.SpecimenDetail(name['specimen_detail'], source.source))

        if type_tags:
            name['type_tags'] = type_tags
        yield name


def translate_type_locality(names: DataT) -> DataT:
    for name in names:
        if 'loc' in name:
            parts = [re.sub(r' \([^\(]+\)$', '', part) for part in name['loc'].split(', ')]
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
    if organs in ('skin and skull', 'skin and cranium', 'study skin and skull') or '(skin and skull)' in organs:
        tags = [SKIN, SKULL]
    elif organs == 'skin and skeleton':
        tags = [SKIN, SKULL, SKELETON]
    elif organs.startswith('skin, skull,'):
        tags = [SKIN, SKULL]
        if 'skeleton' in organs:
            tags.append(SKELETON)
        elif 'in alcohol' in organs:
            tags.append(IN_ALCOHOL)
    elif 'in alcohol' in organs or 'alcoholic' in organs:
        tags = [IN_ALCOHOL]
        if 'skull' in organs:
            tags.append(SKULL)
    elif organs.startswith(('skull only', 'cranium only')):
        tags = [SKULL]
    elif organs.startswith('skin only'):
        tags = [SKIN]
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
        without_direction = re.sub(r'^(North|South|West|East|NE|SE|NW|SW|Republic of)(west|east)?(ern)? ', '', name, flags=re.IGNORECASE)
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
    if region.children.count() > 0:
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


def genus_name_of_name(name: models.Name) -> Optional[models.Taxon]:
    try:
        return name.taxon.parent_of_rank(constants.Rank.genus).valid_name
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
    for genus in possible_genus_names:
        names = models.Name.filter(models.Name.root_name == root_name, models.Name.authority == authority)
        names = [name for name in names if genus_name_of_name(name) == genus]
        if len(names) == 1:
            return names[0]
    # Fuzzy match on original name
    matches = [
        name
        for name in models.Name.filter(models.Name.original_name != None, models.Name.authority == authority)
        if Levenshtein.distance(original_name, name.original_name) < 3
        or REMOVE_PARENS.sub('', original_name) == REMOVE_PARENS.sub('', name.original_name)
    ]
    if len(matches) == 1:
        return matches[0]

    # Find names without an original name in similar genera.
    root_name_to_names, genus_to_orig_genera = build_original_name_map()
    if root_name in root_name_to_names:
        matches = []
        for nam, genus in root_name_to_names[root_name]:
            if genus_name in genus_to_orig_genera[genus]:
                matches.append(nam)
        if len(matches) == 1:
            return matches[0]
    return None


@functools.lru_cache()
def build_original_name_map() -> Tuple[Dict[str, List[Tuple[models.Name, models.Taxon]]], Dict[models.Taxon, Set[str]]]:
    root_name_to_names: Dict[str, List[Tuple[models.Name, models.Taxon]]] = defaultdict(list)
    genus_to_orig_genera: Dict[models.Taxon, Set[str]] = defaultdict(set)
    return root_name_to_names, genus_to_orig_genera
    for nam in models.Name.filter(models.Name.group == constants.Group.species):
        try:
            genus = nam.taxon.parent_of_rank(constants.Rank.genus)
        except ValueError:
            continue
        if nam.original_name is None:
            root_name_to_names[nam.root_name].append((nam, genus))
        else:
            genus_name = helpers.genus_name_of_name(nam.original_name)
            genus_to_orig_genera[genus].add(genus_name)
    return root_name_to_names, genus_to_orig_genera


def name_variants(original_name: str, authority: str) -> Iterable[Tuple[str, str]]:
    yield original_name, authority
    original_authority = authority
    authority = re.sub(r'([A-Z]\.)+ ', '', authority).strip()
    if authority != original_authority:
        yield original_name, authority
    if ' in ' in authority:
        authority = re.sub(r' in .*$', '', authority)
        yield original_name, authority
        yield original_name, re.sub(r'^.* in ', '', original_authority)
    # This should be generalized (initials are in the DB but not the source)
    if authority.startswith('Allen'):
        yield original_name, 'J.A. ' + authority
        yield original_name, 'J.A. ' + authority + ' & Chapman'
    if authority.startswith('Anthony'):
        yield original_name, 'H.E. ' + authority
    if authority == 'Andersen':
        yield original_name, 'K. Andersen'
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
    if authority == 'Schwartz':
        yield original_name, 'Schwarz'
    if authority == 'Fischer':
        yield original_name, 'J.B. Fischer'
    if authority == 'Linné':
        yield original_name, 'Linnaeus'


def associate_names(names: DataT, author_fixes: Mapping[str, str] = {}, original_name_fixes: Mapping[str, str] = {}) -> DataT:
    total = 0
    found = 0
    for name in names:
        name_obj = None
        total += 1
        if ' and ' in name['authority']:
            name['authority'] = name['authority'].replace(' and ', ' & ')
        author = name['authority']
        author = author_fixes.get(author, author)
        orig_name = name['original_name']
        orig_name = original_name_fixes.get(orig_name, orig_name)

        for original_name, authority in name_variants(orig_name, author):
            name_obj = find_name(original_name, authority)
            if name_obj is not None:
                break
        if name_obj:
            found += 1
        else:
            print(f'could not find name {orig_name} -- {author} (tried variants {list(name_variants(orig_name, author))})')
        name['name_obj'] = name_obj
        yield name
    print(f'found: {found}/{total}')


def write_to_db(names: DataT, source: Source, dry_run: bool = True) -> None:
    name_discrepancies = []
    num_changed: Counter[str] = Counter()
    for name in names:
        nam = name['name_obj']
        print(f'--- processing {nam} ---')
        for attr in ('type_locality', 'type_tags', 'collection', 'type_specimen', 'species_type_kind', 'verbatim_citation', 'original_name', 'type_specimen_source'):
            if attr not in name or name[attr] is None:
                continue
            if attr == 'verbatim_citation' and nam.original_citation is not None:
                continue
            current_value = getattr(nam, attr)
            new_value = name[attr]
            if current_value == new_value:
                continue
            elif current_value is not None:
                if attr == 'verbatim_citation':
                    new_value = f'{current_value} [From {{{source.source}}}: {new_value}]'
                else:
                    print(f'value for {attr} differs: (new) {new_value} vs. (current) {current_value}')
                if attr == 'type_tags':
                    new_tags = set(new_value) - set(current_value)
                    existing_types = {type(tag) for tag in current_value}
                    tags_of_new_types = {
                        tag for tag in new_tags
                        # Always add LocationDetail tags, because it has a source field and it's OK to have multiple tags
                        if type(tag) not in existing_types or isinstance(tag, (TypeTag.LocationDetail, TypeTag.SpecimenDetail))
                    }
                    print(f'adding tags: {tags_of_new_types}')
                    if not dry_run:
                        nam.type_tags = sorted(nam.type_tags + tuple(tags_of_new_types))
                    new_tags -= tags_of_new_types
                    if new_tags:
                        print(f'new tags: {new_tags}')
                        if not dry_run:
                            nam.fill_field('type_tags')
                    continue
                elif attr == 'type_specimen_source':
                    nam.display(full=True)
                    if not dry_run:
                        should_replace = getinput.yes_no('Replace type_specimen_source? ')
                        if not should_replace:
                            continue
                elif attr == 'original_name':
                    new_root_name = helpers.root_name_of_name(new_value, constants.Rank.species)
                    if helpers.root_name_of_name(nam.original_name, constants.Rank.species).lower() != new_root_name.lower():
                        try:
                            existing = models.Name.filter(models.Name.original_name == new_value).get()
                        except models.Name.DoesNotExist:
                            print(f'creating ISS with orig name={new_value}')
                            if not dry_run:
                                nam.open_description()
                                if getinput.yes_no(f'Is the original spelling {nam.original_name} correct? '):
                                    nam.add_variant(new_root_name, constants.NomenclatureStatus.incorrect_subsequent_spelling,
                                                    paper=source.source, page_described=name['pages'][0], original_name=new_value)
                                    continue
                        else:
                            if existing.original_citation == source.source:
                                continue
                    name_discrepancies.append((nam, current_value, new_value))
                    continue
            num_changed[attr] += 1
            if not dry_run:
                setattr(nam, attr, new_value)

        if not dry_run:
            if len(name['pages']) == 1:
                pages = str(name['pages'][0])
            else:
                pages = f'{name["pages"][0]}-{name["pages"][-1]}'
            nam.add_comment(constants.CommentKind.structured_quote, json.dumps(name['raw_text']), source.source, pages)
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
