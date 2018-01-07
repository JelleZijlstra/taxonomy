from .db import constants, definition, detection, ehphp, helpers, models
from .db.constants import Age, Group, Rank
from .db.models import Name, Taxon, database
from . import events
from . import getinput

import collections
import functools
import IPython
import os.path
import re
import requests
from typing import cast, Any, Callable, Counter, Dict, Generic, Iterable, Iterator, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple, Type, TypeVar
from traitlets.config.loader import Config
import unidecode

T = TypeVar('T')


class _ShellNamespace(dict):  # type: ignore
    def __missing__(self, key: str) -> object:
        try:
            return getattr(__builtins__, key)
        except AttributeError:
            # make names accessible
            return taxon(key)

    def keys(self) -> Set[str]:  # type: ignore
        keys = set(super().keys())
        keys |= set(dir(__builtins__))
        if not hasattr(self, '_names'):
            self._names = set(
                getinput.encode_name(taxon.valid_name)
                for taxon in Taxon.select(Taxon.valid_name)
                if taxon.valid_name is not None
            )
        return keys | self._names

    def __delitem__(self, key: str) -> None:
        if super().__contains__(key):
            super().__delitem__(key)

    def clear_cache(self) -> None:
        del self._names

    def add_name(self, taxon: Taxon) -> None:
        if hasattr(self, '_names') and taxon.valid_name is not None:
            self._names.add(taxon.valid_name.replace(' ', '_'))


def _reconnect() -> None:
    database.close()
    database.connect()


ns = _ShellNamespace({
    'constants': constants,
    'helpers': helpers,
    'definition': definition,
    'Branch': definition.Branch,
    'Node': definition.Node,
    'Apomorphy': definition.Apomorphy,
    'Other': definition.Other,
    'N': Name.getter('root_name'),
    'L': models.Location.getter('name'),
    'P': models.Period.getter('name'),
    'R': models.Region.getter('name'),
    'O': Name.getter('original_name'),
    'NC': models.NameComplex.getter('label'),
    'SC': models.SpeciesNameComplex.getter('label'),
    'C': models.Collection.getter('label'),
    'reconnect': _reconnect,
})
ns.update(constants.__dict__)

for model in models.BaseModel.__subclasses__():
    ns[model.__name__] = model


CallableT = TypeVar('CallableT', bound=Callable[..., Any])


def command(fn: CallableT) -> CallableT:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except getinput.StopException:
            return None

    ns[fn.__name__] = wrapper
    return cast(CallableT, wrapper)


def generator_command(fn: Callable[..., Iterable[T]]) -> Callable[..., List[T]]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Optional[List[T]]:
        try:
            return list(fn(*args, **kwargs))
        except getinput.StopException:
            return []
    ns[fn.__name__] = wrapper
    return wrapper


# Shell internal commands

@command
def clear_cache() -> None:
    """Clears the autocomplete cache."""
    ns.clear_cache()


# Lookup

@command
def taxon(name: str) -> Taxon:
    """Finds a taxon with the given name."""
    name = name.replace('_', ' ')
    try:
        return Taxon.filter(Taxon.valid_name == name)[0]
    except IndexError:
        raise LookupError(name)


@generator_command
def n(name: str) -> Iterable[Name]:
    """Finds names with the given root name or original name."""
    return Name.filter((Name.root_name % name) | (Name.original_name % name))


@generator_command
def h(authority: str, year: str) -> Iterable[Name]:
    return Name.filter(Name.authority % '%{}%'.format(authority), Name.year == year)


# Maintenance
_MissingDataProducer = Callable[..., Iterable[Tuple[Name, str]]]


def _add_missing_data(attribute: str) -> Callable[[_MissingDataProducer], Callable[..., None]]:
    def decorator(fn: _MissingDataProducer) -> Callable[..., None]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            for nam, message in fn(*args, **kwargs):
                value = getinput.get_line(
                    message + '> ', handlers={'o': lambda _: bool(nam.open_description())}
                )
                if value:
                    setattr(nam, attribute, value)
                    nam.save()
        return wrapper
    return decorator


@command
@_add_missing_data('authority')
def fix_bad_ampersands() -> Iterable[Tuple[Name, str]]:
    for name in Name.filter(Name.authority % '%&%&%'):
        yield name, 'Name {} has bad authority format'.format(name.description())


@command
@_add_missing_data('authority')
def fix_et_al() -> Iterable[Tuple[Name, str]]:
    for name in (Name.filter(Name.authority % '%et al%', Name.original_citation != None)  # noqa: E711
                     .order_by(Name.original_name, Name.root_name)):
        yield name, 'Name {} uses et al.'.format(name.description())


@command
@_add_missing_data('original_name')
def add_original_names() -> Iterable[Tuple[Name, str]]:
    for name in Name.filter(Name.original_citation != None, Name.original_name >> None).order_by(Name.original_name):  # noqa: E711
        message = 'Name {} is missing an original name, but has original citation {{{}}}:{}'.format(
            name.description(), name.original_citation, name.page_described)
        yield name, message


@command
@_add_missing_data('page_described')
def add_page_described() -> Iterable[Tuple[Name, str]]:
    for name in Name.filter(Name.original_citation != None, Name.page_described >> None,  # noqa: E711
                            Name.year != 'in press').order_by(Name.original_citation, Name.original_name):
        if name.year in ('2015', '2016'):
            continue  # recent JVP papers don't have page numbers
        message = 'Name %s is missing page described, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        yield name, message


@command
def add_types() -> None:
    for name in Name.filter(Name.original_citation != None, Name.type >> None, Name.year > '1930',  # noqa: E711
                            Name.group == Group.genus).order_by(Name.original_citation):
        name.taxon.display(full=True, max_depth=1)
        message = 'Name %s is missing type, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        verbatim_type = getinput.get_line(
            message + '> ', handlers={'o': lambda _: name.open_description()}, should_stop=lambda line: line == 's'
        )
        if verbatim_type:
            name.detect_and_set_type(verbatim_type, verbose=True)


@generator_command
def find_rank_mismatch() -> Iterable[Taxon]:
    for taxon in Taxon.select():
        expected_group = helpers.group_of_rank(taxon.rank)
        if expected_group != taxon.base_name.group:
            rank = taxon.rank.name
            group = taxon.base_name.group.name
            print("Group mismatch for %s: rank %s but group %s" % (taxon, rank, group))
            yield taxon


@command
def detect_types(max_count: Optional[int] = None, verbose: bool = False) -> None:
    """Converts verbatim_types into references to the actual names."""
    count = 0
    successful_count = 0
    group = (Group.family, Group.genus)
    for name in Name.filter(Name.verbatim_type != None, Name.type >> None, Name.group << group).limit(max_count):  # noqa: E711
        count += 1
        if name.detect_and_set_type(verbatim_type=name.verbatim_type, verbose=verbose):
            successful_count += 1
    print("Success: %d/%d" % (successful_count, count))


@command
def detect_types_from_root_names(max_count: Optional[int] = None) -> None:
    """Detects types for family-group names on the basis of the root_name."""
    def detect_from_root_name(name: Name, root_name: str) -> bool:
        candidates = Name.filter(Name.group == Group.genus, (Name.stem == root_name) | (Name.stem == root_name + 'i'))
        candidates = list(filter(lambda c: c.taxon.is_child_of(name.taxon), candidates))
        if len(candidates) == 1:
            print("Detected type for name %s: %s" % (name, candidates[0]))
            name.type = candidates[0]
            name.save()
            return True
        else:
            if candidates:
                print(f'found multiple candidates for {name} using root {root_name}: {candidates}')
            return False

    count = 0
    successful_count = 0
    for name in Name.filter(Name.group == Group.family, Name.type >> None).order_by(Name.id.desc()).limit(max_count):
        if name.is_unavailable():
            continue
        count += 1
        if detect_from_root_name(name, name.root_name):
            successful_count += 1
        else:
            for stripped in helpers.name_with_suffixes_removed(name.root_name):
                if detect_from_root_name(name, stripped):
                    successful_count += 1
                    break
            else:
                print("Could not detect type for name %s (root_name = %s)" % (name, name.root_name))
    print("Success: %d/%d" % (successful_count, count))


@command
def endswith(end: str) -> List[Name]:
    return list(Name.filter(Name.group == Group.genus, Name.root_name % ('%%%s' % end)))


@command
def detect_stems() -> None:
    for name in Name.filter(Name.group == Group.genus, Name.stem >> None):
        inferred = detection.detect_stem_and_gender(name.root_name)
        if inferred is None:
            continue
        if not inferred.confident:
            print('%s: stem %s, gender %s' % (name.description(), inferred.stem, inferred.gender))
            if not getinput.yes_no('Is this correct? '):
                continue
        print("Inferred stem and gender for %s: %s, %s" % (name, inferred.stem, inferred.gender))
        name.stem = inferred.stem
        name.gender = inferred.gender
        name.save()


@command
def detect_complexes() -> None:
    endings = list(models.NameEnding.select())
    for name in Name.filter(Name.group == Group.genus, Name._name_complex_id >> None):
        inferred = find_ending(name, endings)
        if inferred is None:
            continue
        stem = inferred.get_stem_from_name(name.root_name)
        if name.stem is not None and name.stem != stem:
            print(f'ignoring {inferred} for {name} because {inferred.stem} != {stem}')
            continue
        if name.gender is not None and name.gender != inferred.gender:
            print(f'ignoring {inferred} for {name} because {inferred.gender} != {name.gender}')
            continue
        print(f'Inferred stem and complex for {name}: {stem}, {inferred}')
        name.stem = stem
        name.gender = inferred.gender
        name.name_complex = inferred
        name.save()


@command
def detect_species_name_complexes(dry_run: bool = False) -> None:
    endings_tree: SuffixTree[models.SpeciesNameEnding] = SuffixTree()
    full_names: Dict[str, models.SpeciesNameComplex] = {}
    for ending in models.SpeciesNameEnding.select():
        for form in ending.name_complex.get_forms(ending.ending):
            if ending.full_name_only:
                full_names[form] = ending.name_complex
            else:
                endings_tree.add(form, ending)
    for snc in models.SpeciesNameComplex.filter(models.SpeciesNameComplex.kind == constants.SpeciesNameKind.adjective):
        for form in snc.get_forms(snc.stem):
            full_names[form] = snc
    success = 0
    total = 0
    for name in Name.filter(Name.group == Group.species, Name._name_complex_id >> None):
        total += 1
        if name.root_name in full_names:
            inferred = full_names[name.root_name]
        else:
            endings = endings_tree.lookup(name.root_name)
            try:
                inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
            except ValueError:
                continue
        print(f'inferred complex for {name}: {inferred}')
        success += 1
        if not dry_run:
            name.name_complex = inferred
            name.save()
    print(f'{success}/{total} inferred')


class SuffixTree(Generic[T]):
    def __init__(self) -> None:
        self.children: Dict[str, SuffixTree[T]] = collections.defaultdict(SuffixTree)
        self.values: List[T] = []

    def add(self, key: str, value: T) -> None:
        self._add(iter(reversed(key)), value)

    def lookup(self, key: str) -> Iterable[T]:
        yield from self._lookup(iter(reversed(key)))

    def _add(self, key: Iterator[str], value: T) -> None:
        try:
            char = next(key)
        except StopIteration:
            self.values.append(value)
        else:
            self.children[char]._add(key, value)

    def _lookup(self, key: Iterator[str]) -> Iterable[T]:
        yield from self.values
        try:
            char = next(key)
        except StopIteration:
            pass
        else:
            if char in self.children:
                yield from self.children[char]._lookup(key)


@command
def find_patronyms(dry_run: bool = True, min_length: int = 4) -> Dict[str, int]:
    """Finds names based on patronyms of authors in the database."""
    authors = set()
    species_name_to_names: Dict[str, List[Name]] = collections.defaultdict(list)
    for name in Name.select():
        if name.authority:
            for author in name.get_authors():
                author = re.sub(r'^([A-Z]\.)+ ', '', author)
                author = unidecode.unidecode(author.replace('-', '').replace(' ', '').replace("'", '')).lower()
                authors.add(author)
        if name.group == Group.species:
            species_name_to_names[name.root_name].append(name)
    masculine = models.SpeciesNameComplex.of_kind(constants.SpeciesNameKind.patronym_masculine)
    feminine = models.SpeciesNameComplex.of_kind(constants.SpeciesNameKind.patronym_feminine)
    latinized = models.SpeciesNameComplex.of_kind(constants.SpeciesNameKind.patronym_latin)
    count = 0
    names_applied: Counter[str] = Counter()
    for author in authors:
        masculine_name = author + 'i'
        feminine_name = author + 'ae'
        latinized_name = author + 'ii'
        for snc, name in [(masculine, masculine_name), (feminine, feminine_name), (latinized, latinized_name)]:
            for nam in species_name_to_names[name]:
                if nam.name_complex is None:
                    print(f'set {nam} to {snc} patronym')
                    count += 1
                    names_applied[name] += 1
                    if not dry_run and len(author) >= min_length:
                        sne = snc.make_ending(name, full_name_only=True)
                elif nam.name_complex != snc:
                    print(f'{nam} has {nam.name_complex} but expected {snc}')
    print(f'applied {count} names')
    if not dry_run:
        detect_species_name_complexes()
    return names_applied


@command
def find_first_declension_adjectives(dry_run: bool = True) -> Dict[str, int]:
    adjectives = get_pages_in_wiki_category('en.wiktionary.org', 'Latin first and second declension adjectives')
    species_name_to_names: Dict[str, List[Name]] = collections.defaultdict(list)
    for name in Name.filter(Name.group == Group.species, Name._name_complex_id >> None):
        species_name_to_names[name.root_name].append(name)
    count = 0
    names_applied: Counter[str] = Counter()
    for adjective in adjectives:
        if not adjective.endswith('us'):
            print('ignoring', adjective)
            continue
        for form in (adjective, adjective[:-2] + 'a', adjective[:-2] + 'um'):
            if form in species_name_to_names:
                print(f'apply {form} to {species_name_to_names[form]}')
                count += len(species_name_to_names[form])
                names_applied[adjective] += len(species_name_to_names[form])
                if not dry_run:
                    snc = models.SpeciesNameComplex.first_declension(adjective, auto_apply=False)
                    snc.make_ending(adjective, full_name_only=len(adjective) < 6)
    print(f'applied {count} names')
    if not dry_run:
        detect_species_name_complexes()
    return names_applied


@command
def get_pages_in_wiki_category(domain: str, category_name: str) -> Iterable[str]:
    cmcontinue = None
    url = f'https://{domain}/w/api.php'
    while True:
        params = {
            'action': 'query',
            'list': 'categorymembers',
            'cmtitle': f'Category:{category_name}',
            'cmlimit': 'max',
            'format': 'json',
        }
        if cmcontinue:
            params['cmcontinue'] = cmcontinue
        json = requests.get(url, params).json()
        for entry in json['query']['categorymembers']:
            if entry['ns'] == 0:
                yield entry['title']
        if 'continue' in json:
            cmcontinue = json['continue']['cmcontinue']
        else:
            break


def find_ending(name: Name, endings: Iterable[models.NameEnding]) -> Optional[models.NameComplex]:
    for ending in endings:
        if name.root_name.endswith(ending.ending):
            return ending.name_complex
    else:
        return None


@generator_command
def root_name_mismatch() -> Iterable[Name]:
    for name in Name.filter(Name.group == Group.family, ~(Name.type >> None)):
        if name.is_unavailable():
            continue
        stem_name = name.type.stem
        if stem_name is None:
            continue
        if name.root_name == stem_name:
            continue
        for stripped in helpers.name_with_suffixes_removed(name.root_name):
            if stripped == stem_name or stripped + 'i' == stem_name:
                print('Autocorrecting root name: %s -> %s' % (name.root_name, stem_name))
                name.root_name = stem_name
                name.save()
                break
        if name.root_name != stem_name:
            print('Stem mismatch for %s: %s vs. %s' % (name, name.root_name, stem_name))
            yield name


def _duplicate_finder(fn: Callable[..., Iterable[Mapping[Any, Sequence[T]]]]) -> Callable[..., Optional[List[Sequence[T]]]]:
    @generator_command
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Iterable[Sequence[T]]:
        for dups_dict in fn(*args, **kwargs):
            for key, entries_list in dups_dict.items():
                if len(entries_list) > 1:
                    print("Duplicate:", key, len(entries_list))
                    yield entries_list
    return wrapper


@_duplicate_finder
def dup_taxa() -> List[Dict[str, List[Taxon]]]:
    taxa = collections.defaultdict(list)  # type: Dict[str, List[Taxon]]
    for txn in Taxon.select():
        if txn.rank == Rank.subgenus and len(taxa[txn.valid_name]) > 0:
            continue
        taxa[txn.valid_name].append(txn)
    return [taxa]


@_duplicate_finder
def dup_genus() -> List[Dict[str, List[Name]]]:
    names = collections.defaultdict(list)  # type: Dict[str, List[Name]]
    for name in Name.filter(Name.group == Group.genus):
        full_name = "%s %s, %s" % (name.root_name, name.authority, name.year)
        names[full_name].append(name)
    return [names]


@_duplicate_finder
def dup_names() -> List[Dict[Tuple[str, str], List[Name]]]:
    original_year = collections.defaultdict(list)  # type: Dict[Tuple[str, str], List[Name]]
    for name in Name.select():
        if name.original_name is not None and name.year is not None:
            original_year[(name.original_name, name.year)].append(name)
    return [original_year]


@command
def stem_statistics() -> None:
    stem = Name.filter(Name.group == Group.genus, ~(Name.stem >> None)).count()
    gender = Name.filter(Name.group == Group.genus, ~(Name.gender >> None)).count()
    total = Name.filter(Name.group == Group.genus).count()
    print("Genus-group names:")
    print("stem: %s/%s (%.02f%%)" % (stem, total, stem / total * 100))
    print("gender: %s/%s (%.02f%%)" % (gender, total, gender / total * 100))
    print("Family-group names:")
    total = Name.filter(Name.group == Group.family).count()
    typ = Name.filter(Name.group == Group.family, ~(Name.type >> None)).count()
    print("type: %s/%s (%.02f%%)" % (typ, total, typ / total * 100))


@generator_command
def name_mismatches(max_count: Optional[int] = None, correct: bool = False, correct_undoubted: bool = True) -> Iterable[Taxon]:
    count = 0
    for taxon in Taxon.select():
        computed = taxon.compute_valid_name()
        if computed is not None and taxon.valid_name != computed:
            print("Mismatch for %s: %s (actual) vs. %s (computed)" % (taxon, taxon.valid_name, computed))
            yield taxon
            count += 1
            # for species-group taxa with a known genus parent, the computed valid name is almost
            # always right (the mismatch will usually happen after a change in genus classification)
            # one area that isn't well-covered yet is autocorrecting gender endings
            if correct_undoubted and taxon.base_name.group == Group.species and \
                    taxon.has_parent_of_rank(Rank.genus):
                taxon.recompute_name()
            elif correct:
                taxon.recompute_name()
            if max_count is not None and count == max_count:
                return


@generator_command
def authorless_names(root_taxon: Taxon, attribute: str = 'authority',
                     predicate: Optional[Callable[[Name], bool]] = None) -> Iterable[Name]:
    for nam in root_taxon.names:
        if (not predicate) or predicate(nam):
            if getattr(nam, attribute) is None:
                print(nam)
                yield nam
    for child in root_taxon.children:
        yield from authorless_names(child, attribute=attribute, predicate=predicate)

yearless_names = functools.partial(authorless_names, attribute='year')


@generator_command
def complexless_genera(root_taxon: Taxon) -> Iterable[Name]:
    return authorless_names(root_taxon, 'name_complex', predicate=lambda n: n.group == Group.genus)


class LabeledName(NamedTuple):
    name: Name
    order: Optional[Taxon]
    family: Optional[Taxon]
    is_mammal: bool
    is_doubtful: bool


def label_name(name: Name) -> LabeledName:
    try:
        order = name.taxon.parent_of_rank(Rank.order)
    except ValueError:
        order = None
    try:
        family = name.taxon.parent_of_rank(Rank.family)
    except ValueError:
        family = None
    is_mammal = name.taxon.is_child_of(taxon('Mammalia'))
    is_doubtful = name.taxon.is_child_of(taxon('Doubtful'))
    return LabeledName(name, order, family, is_mammal, is_doubtful)


@command
def labeled_authorless_names(attribute: str = 'authority') -> List[LabeledName]:
    nams = Name.filter(getattr(Name, attribute) >> None)
    return [label_name(name) for name in nams]


@command
def correct_type_taxon(max_count: Optional[int] = None, dry_run: bool = True, only_if_child: bool = True) -> None:
    count = 0
    for nam in Name.select().where(Name.group << (Group.genus, Group.family)):
        if nam.type is None:
            continue
        if nam.taxon == nam.type.taxon:
            continue
        expected_taxon = nam.type.taxon.parent
        while expected_taxon.base_name.group != nam.group and expected_taxon != nam.taxon:
            expected_taxon = expected_taxon.parent
            if expected_taxon is None:
                break
        if expected_taxon is None:
            continue
        if nam.taxon != expected_taxon:
            count += 1
            print('changing taxon of %s from %s to %s' % (nam, nam.taxon, expected_taxon))
            if not dry_run:
                if only_if_child:
                    if not expected_taxon.is_child_of(nam.taxon):
                        print('dropping non-parent: %s' % nam)
                        continue
                nam.taxon = expected_taxon
                nam.save()
            if max_count is not None and count > max_count:
                return


# Statistics

@command
def print_percentages() -> None:
    attributes = ['original_name', 'original_citation', 'page_described', 'authority', 'year']
    parent_of_taxon = {}  # type: Dict[int, int]

    def _find_parent(taxon: Taxon) -> int:
        if taxon.id in parent_of_taxon:
            return parent_of_taxon[taxon.id]
        else:
            result: int
            if taxon.is_page_root:
                result = taxon.id
            else:
                result = _find_parent(taxon.parent)
            # cache the parent taxon too
            parent_of_taxon[taxon.id] = result
            return result

    for taxon in Taxon.select():
        _find_parent(taxon)

    print('Finished collecting parents for taxa')

    counts_of_parent = collections.defaultdict(lambda: collections.defaultdict(int))  # type: Dict[int, Dict[str, int]]
    for name in Name.select():
        parent_id = parent_of_taxon[name.taxon.id]
        counts_of_parent[parent_id]['total'] += 1
        for attribute in attributes:
            if getattr(name, attribute) is not None:
                counts_of_parent[parent_id][attribute] += 1

    print('Finished collecting statistics on names')

    parents = [
        (Taxon.filter(Taxon.id == parent_id)[0], data)
        for parent_id, data in counts_of_parent.items()
    ]

    for parent, data in sorted(parents, key=lambda i: i[0].valid_name):
        print("FILE", parent)
        total = data['total']
        del data['total']
        print("Total", total)
        for attribute in attributes:
            percentage = data[attribute] * 100.0 / total
            print("%s: %s (%.2f%%)" % (attribute, data[attribute], percentage))


@generator_command
def bad_base_names() -> Iterable[Taxon]:
    return Taxon.raw('SELECT * FROM taxon WHERE base_name_id IS NULL OR base_name_id NOT IN (SELECT id FROM name)')


@generator_command
def bad_taxa() -> Iterable[Name]:
    return Name.raw('SELECT * FROM name WHERE taxon_id IS NULL or taxon_id NOT IN (SELECT id FROM taxon)')


@generator_command
def bad_parents() -> Iterable[Name]:
    return Name.raw('SELECT * FROM taxon WHERE parent_id NOT IN (SELECT id FROM taxon)')


@generator_command
def parentless_taxa() -> Iterable[Taxon]:
    return Taxon.filter(Taxon.parent == None)  # noqa: E711


@generator_command
def childless_taxa() -> Iterable[Taxon]:
    return Taxon.raw('SELECT * FROM taxon WHERE rank > 5 AND id NOT IN (SELECT parent_id FROM taxon WHERE parent_id IS NOT NULL)')


@generator_command
def labeled_childless_taxa() -> Iterable[LabeledName]:
    return [label_name(taxon.base_name) for taxon in childless_taxa()]


@command
def fossilize(*taxa: Taxon, to_status: Age = Age.fossil, from_status: Age = Age.extant) -> None:
    for taxon in taxa:
        if taxon.age != from_status:
            continue
        taxon.age = to_status  # type: ignore
        taxon.save()
        for child in taxon.children:
            fossilize(child, to_status=to_status, from_status=from_status)


@command
def clean_up_verbatim(dry_run: bool = True) -> None:
    famgen_type_count = species_type_count = citation_count = 0
    for nam in Name.filter(Name.group << (Group.family, Group.genus), Name.verbatim_type != None, Name.type != None):
        print(f'{nam}: {nam.type}, {nam.verbatim_type}')
        famgen_type_count += 1
        if not dry_run:
            nam.add_data('verbatim_type', nam.verbatim_type)
            nam.verbatim_type = None
            nam.save()
    for nam in Name.filter(Name.group == Group.species, Name.verbatim_type != None, Name.type_specimen != None):
        print(f'{nam}: {nam.type_specimen}, {nam.verbatim_type}')
        species_type_count += 1
        if not dry_run:
            nam.add_data('verbatim_type', nam.verbatim_type)
            nam.verbatim_type = None
            nam.save()
    for nam in Name.filter(Name.verbatim_citation != None, Name.original_citation != None):
        print(f'{nam}: {nam.original_citation}, {nam.verbatim_citation}')
        citation_count += 1
        if not dry_run:
            nam.add_data('verbatim_citation', nam.verbatim_citation)
            nam.verbatim_citation = None
            nam.save()
    print(f'Family/genera type count: {famgen_type_count}')
    print(f'Species type count: {species_type_count}')
    print(f'Citation count: {citation_count}')


@command
def fill_data_from_paper(paper: Optional[str] = None) -> None:
    if paper is None:
        paper = models.BaseModel.get_value_for_article_field('paper')
    assert paper is not None, 'paper needs to be specified'
    models.fill_data_from_paper(paper)


@command
def fill_type_locality(extant_only: bool = True, start_at: Optional[Name] = None) -> None:
    started = start_at is None
    for nam in Name.filter(Name.type_locality_description != None, Name.type_locality >> None):
        if extant_only and nam.taxon.age != Age.extant:
            continue
        if not started:
            assert start_at is not None
            if nam.id == start_at.id:
                started = True
            else:
                continue
        print(nam)
        print(nam.type_locality_description)
        nam.fill_field('type_locality')


def run_shell() -> None:
    config = Config()
    config.InteractiveShell.confirm_exit = False
    config.TerminalIPythonApp.display_banner = False
    lib_file = os.path.join(os.path.dirname(__file__), 'lib.py')
    IPython.start_ipython(argv=[lib_file, '-i'], config=config, user_ns=ns)


if __name__ == '__main__':
    run_shell()
