from .db import constants, definition, detection, helpers, models
from .db.constants import Group, Rank
from .db.models import Age, Name, Taxon
from . import events
from . import getinput

import collections
import functools
import IPython
import os.path
import re
from typing import cast, Any, Callable, Dict, Generic, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Type, TypeVar

T = TypeVar('T')


# Encode and decode names so they can be used as identifiers. Spaces are replaced with underscores
# and any non-alphabetical characters are replaced with the character's ASCII code surrounded by
# underscores. TODO: we shouldn't replace accented characters like í, which are allowed in Python
# identifiers
_encode_re = re.compile(r'[^A-Za-z ]')
_decode_re = re.compile(r'_(\d+)_')


def _encode_name(name: str) -> str:
    return _encode_re.sub(lambda m: '_%d_' % ord(m.group()), name).replace(' ', '_')


def _decode_name(name: str) -> str:
    return _decode_re.sub(lambda m: chr(int(m.group(1))), name).replace('_', ' ')


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
                _encode_name(taxon.valid_name)
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

ModelT = TypeVar('ModelT', bound=models.BaseModel)


class _NameGetter(Generic[ModelT]):
    def __init__(self, cls: Type[ModelT], field: str) -> None:
        self.cls = cls
        self.field = field
        self.field_obj = getattr(cls, field)
        self._data = None  # type: Optional[Set[str]]

    def __dir__(self) -> Set[str]:
        result = set(super().__dir__())  # type: ignore
        if self._data is None:
            self._data = set()
            for obj in self.cls.select(self.field_obj):
                self._add_obj(obj)
        return result | self._data

    def __getattr__(self, name: str) -> ModelT:
        return self.cls.filter(self.field_obj == _decode_name(name)).get()

    def __call__(self, name: str) -> ModelT:
        return self.__getattr__(name)

    def clear_cache(self) -> None:
        self._data = None

    def add_name(self, nam: ModelT) -> None:
        if self._data is not None:
            self._add_obj(nam)

    def _add_obj(self, obj: ModelT) -> None:
        assert self._data is not None
        val = getattr(obj, self.field)
        if val is None:
            return
        self._data.add(_encode_name(val))


ns = _ShellNamespace({
    'constants': constants,
    'helpers': helpers,
    'definition': definition,
    'Branch': definition.Branch,
    'Node': definition.Node,
    'Apomorphy': definition.Apomorphy,
    'Other': definition.Other,
    'N': _NameGetter(Name, 'root_name'),
    'L': _NameGetter(models.Location, 'name'),
    'P': _NameGetter(models.Period, 'name'),
    'R': _NameGetter(models.Region, 'name'),
    'O': _NameGetter(Name, 'original_name'),
})
ns.update(constants.__dict__)

for model in models.BaseModel.__subclasses__():
    ns[model.__name__] = model

events.on_new_taxon.on(ns.add_name)
events.on_taxon_save.on(ns.add_name)
events.on_name_save.on(ns['N'].add_name)
events.on_name_save.on(ns['O'].add_name)
events.on_locality_save.on(ns['L'].add_name)
events.on_period_save.on(ns['P'].add_name)


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


def generator_command(fn: Callable[..., Iterable[T]]) -> Callable[..., Optional[List[T]]]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Optional[List[T]]:
        try:
            return list(fn(*args, **kwargs))
        except getinput.StopException:
            return None
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
                    message, handlers={'o': lambda _: bool(nam.open_description())}
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
    for name in (Name.filter(Name.authority % '%et al%', Name.original_citation != None)
                     .order_by(Name.original_name, Name.root_name)):
        yield name, 'Name {} uses et al.'.format(name.description())


@command
@_add_missing_data('original_name')
def add_original_names() -> Iterable[Tuple[Name, str]]:
    for name in Name.filter(Name.original_citation != None, Name.original_name >> None).order_by(Name.original_name):
        message = 'Name {} is missing an original name, but has original citation {{{}}}:{}'.format(
            name.description(), name.original_citation, name.page_described)
        yield name, message


@command
@_add_missing_data('page_described')
def add_page_described() -> Iterable[Tuple[Name, str]]:
    for name in Name.filter(Name.original_citation != None, Name.page_described >> None, Name.year != 'in press').order_by(Name.original_citation, Name.original_name):
        if name.year in ('2015', '2016'):
            continue  # recent JVP papers don't have page numbers
        message = 'Name %s is missing page described, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        yield name, message


@command
def add_types() -> None:
    for name in Name.filter(Name.original_citation != None, Name.type >> None, Name.year > '1930', Name.group == Group.genus).order_by(Name.original_citation):
        name.taxon.display(full=True, max_depth=1)
        message = 'Name %s is missing type, but has original citation {%s}' % \
            (name.description(), name.original_citation)
        verbatim_type = getinput.get_line(
            message, handlers={'o': lambda _: name.open_description()}, should_stop=lambda line: line == 's'
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
    for name in Name.filter(Name.verbatim_type != None, Name.type >> None, Name.group << group).limit(max_count):
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
def authorless_names(root_taxon: Taxon, attribute: str = 'authority') -> Iterable[Name]:
    for nam in root_taxon.names:
        if getattr(nam, attribute) is None:
            print(nam)
            yield nam
    for child in root_taxon.children:
        yield from authorless_names(child, attribute=attribute)  # type: ignore


@command
def labeled_authorless_names() -> List[Dict[str, Any]]:
    nams = Name.filter(Name.authority >> None)
    nams = [{'name': nam} for nam in nams]
    Mammalia = taxon('Mammalia')
    for nam in nams:
        try:
            order = nam['name'].taxon.parent_of_rank(Rank.order)
        except ValueError:
            order = None
        nam['order'] = order
        try:
            family = nam['name'].taxon.parent_of_rank(Rank.family)
        except ValueError:
            family = None
        nam['family'] = family
        nam['is_mammal'] = nam['name'].taxon.is_child_of(Mammalia)
    return nams


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
        if taxon.is_page_root:
            return taxon.id
        elif taxon.id in parent_of_taxon:
            return parent_of_taxon[taxon.id]
        else:
            return _find_parent(taxon.parent)

    for taxon in Taxon.select():
        parent_of_taxon[taxon.id] = _find_parent(taxon)

    counts_of_parent = collections.defaultdict(lambda: collections.defaultdict(int))  # type: Dict[int, Dict[str, int]]
    for name in Name.select():
        parent_id = parent_of_taxon[name.taxon.id]
        counts_of_parent[parent_id]['total'] += 1
        for attribute in attributes:
            if getattr(name, attribute) is not None:
                counts_of_parent[parent_id][attribute] += 1

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
    return Taxon.filter(Taxon.parent == None)


@generator_command
def childless_taxa() -> Iterable[Taxon]:
    return Taxon.raw('SELECT * FROM taxon WHERE rank > 5 AND id NOT IN (SELECT parent_id FROM taxon WHERE parent_id IS NOT NULL)')


@command
def fossilize(*taxa: Taxon, to_status: Age = Age.fossil, from_status: Age = Age.extant) -> None:
    for taxon in taxa:
        if taxon.age != from_status:
            return
        taxon.age = to_status  # type: ignore
        taxon.save()
        for child in taxon.children:
            fossilize(child, to_status=to_status, from_status=from_status)


def run_shell() -> None:
    config = IPython.config.loader.Config()
    config.InteractiveShellEmbed.confirm_exit = False
    lib_file = os.path.join(os.path.dirname(__file__), 'lib.py')
    IPython.start_ipython(argv=[lib_file, '-i'], config=config, user_ns=ns)


if __name__ == '__main__':
    run_shell()