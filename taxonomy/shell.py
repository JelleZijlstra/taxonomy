"""

Shell commands, functions that can be called directly from the shell.

This is mostly used for functions that check certain invariants in the database.

Possible ones to add:
- The valid name for a taxon should always be the oldest name that does not have some sort of
  nomenclatural trouble (preoccupied, unavailable, etc.).
- The root name should match the last part of the corrected original name, unless there is a
  justified emendation.
- Emendations and subsequent spellings should be synonyms of the name they are based on.
- Nomina nova and similar should not themselves have a type locality.

"""

import collections
import functools
import os.path
import re
from typing import (
    Any,
    Callable,
    Counter,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

import IPython
import requests
import unidecode
from traitlets.config.loader import Config

from . import getinput
from .db import constants, definition, detection, helpers, models
from .db.constants import Age, Group, NomenclatureStatus, Rank, PeriodSystem
from .db.models import Article, Collection, Name, Tag, Taxon, TypeTag, database

T = TypeVar("T")


class _ShellNamespace(dict):  # type: ignore
    def __missing__(self, key: str) -> object:
        try:
            return getattr(__builtins__, key)
        except AttributeError:
            # make names accessible
            return taxon_of_name(key)

    def keys(self) -> Set[str]:  # type: ignore
        keys = set(super().keys())
        keys |= set(dir(__builtins__))
        if not hasattr(self, "_names"):
            self._names = {
                getinput.encode_name(taxon.valid_name)
                for taxon in Taxon.select_valid(Taxon.valid_name)
                if taxon.valid_name is not None
            }
        return keys | self._names

    def __delitem__(self, key: str) -> None:
        if super().__contains__(key):
            super().__delitem__(key)

    def clear_cache(self) -> None:
        del self._names

    def add_name(self, taxon: Taxon) -> None:
        if hasattr(self, "_names") and taxon.valid_name is not None:
            self._names.add(taxon.valid_name.replace(" ", "_"))


def _reconnect() -> None:
    database.close()
    database.connect()


ns = _ShellNamespace(
    {
        "constants": constants,
        "helpers": helpers,
        "definition": definition,
        "Branch": definition.Branch,
        "Node": definition.Node,
        "Apomorphy": definition.Apomorphy,
        "Other": definition.Other,
        "N": Name.getter("root_name"),
        "O": Name.getter("original_name"),
        "reconnect": _reconnect,
        "Tag": models.Tag,
        "TypeTag": models.TypeTag,
        "Counter": collections.Counter,
        "defaultdict": collections.defaultdict,
    }
)
ns.update(constants.__dict__)

for model in models.BaseModel.__subclasses__():
    ns[model.__name__] = model
    if (
        hasattr(model, "call_sign")
        and hasattr(model, "label_field")
        and model is not Name
    ):
        ns[model.call_sign] = model.getter(model.label_field)


CallableT = TypeVar("CallableT", bound=Callable[..., Any])


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
def taxon_of_name(name: str) -> Taxon:
    """Finds a taxon with the given name."""
    name = name.replace("_", " ")
    try:
        return Taxon.select_valid().filter(Taxon.valid_name == name)[0]
    except IndexError:
        raise LookupError(name) from None


@generator_command
def n(name: str) -> Iterable[Name]:
    """Finds names with the given root name or original name."""
    return Name.select_valid().filter(
        (Name.root_name % name) | (Name.original_name % name)
    )


@generator_command
def h(authority: str, year: str) -> Iterable[Name]:
    return Name.select_valid().filter(
        Name.authority % f"%{authority}%", Name.year == year
    )


# Maintenance
_MissingDataProducer = Callable[..., Iterable[Tuple[Name, str]]]


def _add_missing_data(fn: _MissingDataProducer) -> Callable[..., None]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        for nam, message in fn(*args, **kwargs):
            print(message)
            nam.open_description()
            nam.display()
            nam.fill_required_fields()

    return wrapper


@command
@_add_missing_data
def fix_bad_ampersands() -> Iterable[Tuple[Name, str]]:
    for name in Name.select_valid().filter(Name.authority % "%&%&%"):
        yield name, "Name {} has bad authority format".format(name.description())


@command
@_add_missing_data
def fix_et_al() -> Iterable[Tuple[Name, str]]:
    for name in (
        Name.select_valid()
        .filter(Name.authority.contains("et al"), Name.original_citation != None)
        .order_by(Name.original_name, Name.root_name)
    ):
        yield name, "Name {} uses et al.".format(name.description())


@command
@_add_missing_data
def add_original_names() -> Iterable[Tuple[Name, str]]:
    for name in (
        Name.select_valid()
        .filter(Name.original_citation != None, Name.original_name >> None)
        .order_by(Name.original_name)
    ):
        message = "Name {} is missing an original name, but has original citation {{{}}}:{}".format(
            name.description(), name.original_citation.name, name.page_described
        )
        yield name, message


@command
@_add_missing_data
def add_page_described() -> Iterable[Tuple[Name, str]]:
    for name in (
        Name.select_valid()
        .filter(
            Name.original_citation != None,
            Name.page_described >> None,  # noqa: E711
            Name.year != "in press",
        )
        .order_by(Name.original_citation, Name.original_name)
    ):
        if name.year in ("2015", "2016"):
            continue  # recent JVP papers don't have page numbers
        message = (
            "Name %s is missing page described, but has original citation {%s}"
            % (name.description(), name.original_citation.name)
        )
        yield name, message


@command
def make_pleistocene_localities(dry_run: bool = False) -> None:
    pleistocene = models.Period.get(models.Period.name == "Pleistocene")
    for region in models.Region.select():
        name = f"{region.name} Pleistocene"
        try:
            loc = models.Location.get(models.Location.name == name)
        except models.Location.DoesNotExist:
            print(f"creating location {name}")
            if dry_run:
                continue
            else:
                loc = models.Location.make(name=name, region=region, period=pleistocene)
        if not loc.comment:
            comment = f"Undifferentiated Pleistocene localities in {region.name}"
            print(f"setting comment on {loc} to {comment}")
            if not dry_run:
                loc.comment = comment


@generator_command
def bad_stratigraphy(dry_run: bool = True) -> Iterable[models.Location]:
    for loc in models.Location.select():
        if loc.min_period is None and loc.max_period is not None:
            print(f"=== {loc.name}: missing min_period ===")
            loc.display()
            yield loc
        if loc.max_period is None and loc.min_period is not None:
            print(f"=== {loc.name}: missing max_period ===")
            loc.display()
            yield loc
        if loc.stratigraphic_unit is None and loc.min_period is None:
            print(f"=== {loc.name}: missing stratigraphic_unit and period ===")
            loc.display()
            yield loc
        periods = (loc.min_period, loc.max_period)
        has_stratigraphic = False
        for period in periods:
            # exclude Recent (171)
            if (
                period is not None
                and period.id != 171
                and period.system.is_stratigraphy()
            ):
                has_stratigraphic = True
        if has_stratigraphic:
            print(f"=== {loc.name} has stratigraphic period ===")
            loc.display()
            yield loc
            period = loc.min_period
            if period == loc.max_period and loc.stratigraphic_unit is None:
                print(f"autofixing {loc.name}")
                if not dry_run:
                    loc.min_period = period.min_period
                    loc.max_period = period.max_period
                    loc.stratigraphic_unit = period


@command
def add_types() -> None:
    for name in (
        Name.select_valid()
        .filter(
            Name.original_citation != None,
            Name.type >> None,
            Name.year > "1930",  # noqa: E711
            Name.group == Group.genus,
        )
        .order_by(Name.original_citation)
    ):
        if "type" not in name.get_required_fields():
            continue
        name.taxon.display(full=True, max_depth=1)
        print(
            f"Name {name} is missing type, but has original citation {name.original_citation.name}"
        )
        models.taxon.fill_data_from_paper(name.original_citation)


@generator_command
def find_rank_mismatch() -> Iterable[Taxon]:
    for taxon in Taxon.select_valid():
        expected_group = helpers.group_of_rank(taxon.rank)
        if expected_group != taxon.base_name.group:
            rank = taxon.rank.name
            group = taxon.base_name.group.name
            print(f"Group mismatch for {taxon}: rank {rank} but group {group}")
            yield taxon


@generator_command
def detect_corrected_original_names(
    dry_run: bool = False, interactive: bool = False, ignore_failure: bool = False
) -> Iterable[Name]:
    total = successful = 0
    for nam in Name.select_valid().filter(
        Name.original_name != None,
        Name.corrected_original_name == None,
        Name.group << (Group.genus, Group.species),
    ):
        if "corrected_original_name" not in nam.get_required_fields():
            continue
        total += 1
        inferred = nam.infer_corrected_original_name()
        if inferred:
            successful += 1
            print(
                f"{nam}: inferred corrected_original_name to be {inferred!r} from {nam.original_name!r}"
            )
            if not dry_run:
                nam.corrected_original_name = inferred
        elif not ignore_failure:
            print(
                f"{nam}: could not infer corrected original name from {nam.original_name!r}"
            )
            if interactive:
                nam.display()
                nam.fill_field("corrected_original_name")
            yield nam
    print(f"Success: {successful}/{total}")


@command
def detect_types(max_count: Optional[int] = None, verbose: bool = False) -> None:
    """Converts verbatim_types into references to the actual names."""
    count = 0
    successful_count = 0
    group = (Group.family, Group.genus)
    for name in (
        Name.select_valid()
        .filter(Name.verbatim_type != None, Name.type >> None, Name.group << group)
        .limit(max_count)
    ):  # noqa: E711
        count += 1
        if name.detect_and_set_type(verbatim_type=name.verbatim_type, verbose=verbose):
            successful_count += 1
    print("Success: %d/%d" % (successful_count, count))


@command
def detect_types_from_root_names(max_count: Optional[int] = None) -> None:
    """Detects types for family-group names on the basis of the root_name."""

    def detect_from_root_name(name: Name, root_name: str) -> bool:
        candidates = Name.select_valid().filter(
            Name.group == Group.genus,
            (Name.stem == root_name) | (Name.stem == root_name + "i"),
        )
        candidates = list(filter(lambda c: c.taxon.is_child_of(name.taxon), candidates))
        if len(candidates) == 1:
            print("Detected type for name {}: {}".format(name, candidates[0]))
            name.type = candidates[0]
            name.save()
            return True
        else:
            if candidates:
                print(
                    f"found multiple candidates for {name} using root {root_name}: {candidates}"
                )
            return False

    count = 0
    successful_count = 0
    for name in (
        Name.select_valid()
        .filter(Name.group == Group.family, Name.type >> None)
        .order_by(Name.id.desc())
        .limit(max_count)
    ):
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
                print(
                    "Could not detect type for name %s (root_name = %s)"
                    % (name, name.root_name)
                )
    print("Success: %d/%d" % (successful_count, count))


@command
def endswith(end: str) -> List[Name]:
    return list(
        Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name % ("%%%s" % end)
        )
    )


@command
def detect_stems() -> None:
    for name in Name.select_valid().filter(
        Name.group == Group.genus, Name.stem >> None
    ):
        inferred = detection.detect_stem_and_gender(name.root_name)
        if inferred is None:
            continue
        if not inferred.confident:
            print(
                "%s: stem %s, gender %s"
                % (name.description(), inferred.stem, inferred.gender)
            )
            if not getinput.yes_no("Is this correct? "):
                continue
        print(
            "Inferred stem and gender for %s: %s, %s"
            % (name, inferred.stem, inferred.gender)
        )
        name.stem = inferred.stem
        name.gender = inferred.gender
        name.save()


@command
def detect_complexes() -> None:
    endings = list(models.NameEnding.select())
    for name in Name.select_valid().filter(
        Name.group == Group.genus, Name.name_complex >> None
    ):
        inferred = find_ending(name, endings)
        if inferred is None:
            continue
        stem = inferred.get_stem_from_name(name.root_name)
        if name.stem is not None and name.stem != stem:
            print(f"ignoring {inferred} for {name} because {inferred.stem} != {stem}")
            continue
        if name.gender is not None and name.gender != inferred.gender:
            print(
                f"ignoring {inferred} for {name} because {inferred.gender} != {name.gender}"
            )
            continue
        print(f"Inferred stem and complex for {name}: {stem}, {inferred}")
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
    for snc in models.SpeciesNameComplex.filter(
        models.SpeciesNameComplex.kind == constants.SpeciesNameKind.adjective
    ):
        for form in snc.get_forms(snc.stem):
            full_names[form] = snc
    success = 0
    total = 0
    for name in Name.select_valid().filter(
        Name.group == Group.species, Name.species_name_complex >> None
    ):
        total += 1
        if name.root_name in full_names:
            inferred = full_names[name.root_name]
        else:
            endings = endings_tree.lookup(name.root_name)
            try:
                inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
            except ValueError:
                continue
        print(f"inferred complex for {name}: {inferred}")
        success += 1
        if not dry_run:
            name.species_name_complex = inferred
            name.save()
    print(f"{success}/{total} inferred")


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
    for name in Name.select_valid():
        if name.authority:
            for author in name.get_authors():
                author = re.sub(r"^([A-Z]\.)+ ", "", author)
                author = unidecode.unidecode(
                    author.replace("-", "").replace(" ", "").replace("'", "")
                ).lower()
                authors.add(author)
        if name.group == Group.species:
            species_name_to_names[name.root_name].append(name)
    masculine = models.SpeciesNameComplex.of_kind(
        constants.SpeciesNameKind.patronym_masculine
    )
    feminine = models.SpeciesNameComplex.of_kind(
        constants.SpeciesNameKind.patronym_feminine
    )
    latinized = models.SpeciesNameComplex.of_kind(
        constants.SpeciesNameKind.patronym_latin
    )
    count = 0
    names_applied: Counter[str] = Counter()
    for author in authors:
        masculine_name = author + "i"
        feminine_name = author + "ae"
        latinized_name = author + "ii"
        for snc, name in [
            (masculine, masculine_name),
            (feminine, feminine_name),
            (latinized, latinized_name),
        ]:
            for nam in species_name_to_names[name]:
                if nam.species_name_complex is None:
                    print(f"set {nam} to {snc} patronym")
                    count += 1
                    names_applied[name] += 1
                    if not dry_run and len(author) >= min_length:
                        snc.make_ending(name, full_name_only=True)
                elif nam.species_name_complex != snc:
                    print(f"{nam} has {nam.species_name_complex} but expected {snc}")
    print(f"applied {count} names")
    if not dry_run:
        detect_species_name_complexes()
    return names_applied


@command
def find_first_declension_adjectives(dry_run: bool = True) -> Dict[str, int]:
    adjectives = get_pages_in_wiki_category(
        "en.wiktionary.org", "Latin first and second declension adjectives"
    )
    species_name_to_names: Dict[str, List[Name]] = collections.defaultdict(list)
    for name in Name.select_valid().filter(
        Name.group == Group.species, Name.species_name_complex >> None
    ):
        species_name_to_names[name.root_name].append(name)
    count = 0
    names_applied: Counter[str] = Counter()
    for adjective in adjectives:
        if not adjective.endswith("us"):
            print("ignoring", adjective)
            continue
        for form in (adjective, adjective[:-2] + "a", adjective[:-2] + "um"):
            if form in species_name_to_names:
                print(f"apply {form} to {species_name_to_names[form]}")
                count += len(species_name_to_names[form])
                names_applied[adjective] += len(species_name_to_names[form])
                if not dry_run:
                    snc = models.SpeciesNameComplex.first_declension(
                        adjective, auto_apply=False
                    )
                    snc.make_ending(adjective, full_name_only=len(adjective) < 6)
    print(f"applied {count} names")
    if not dry_run:
        detect_species_name_complexes()
    return names_applied


@command
def get_pages_in_wiki_category(domain: str, category_name: str) -> Iterable[str]:
    cmcontinue = None
    url = f"https://{domain}/w/api.php"
    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_name}",
            "cmlimit": "max",
            "format": "json",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        json = requests.get(url, params).json()
        for entry in json["query"]["categorymembers"]:
            if entry["ns"] == 0:
                yield entry["title"]
        if "continue" in json:
            cmcontinue = json["continue"]["cmcontinue"]
        else:
            break


def find_ending(
    name: Name, endings: Iterable[models.NameEnding]
) -> Optional[models.NameComplex]:
    for ending in endings:
        if name.root_name.endswith(ending.ending):
            return ending.name_complex
    return None


@command
def generate_word_list() -> Set[str]:
    strings = set()
    for nam in Name.select_valid():
        for attr in ("original_name", "root_name", "authority", "verbatim_citation"):
            value = getattr(nam, attr)
            if value is not None:
                strings.add(value)
        if nam.type_tags:
            for tag in nam.type_tags:
                if isinstance(tag, TypeTag.LocationDetail):
                    strings.add(tag.text)

    print(f"Got {len(strings)} strings")
    words = set()
    for string in strings:
        for word in re.findall(r"[a-zA-Z\-]+", string):
            words.add(word)
    print(f"Got {len(words)} words")
    return words


@generator_command
def stem_mismatch(autofix: bool = False) -> Iterable[Name]:
    for nam in Name.select_valid().filter(
        Name.group == Group.genus, ~(Name.name_complex >> None)
    ):
        if nam.stem is None:
            continue
        if nam.stem != nam.get_stem():
            print(f"Stem mismatch for {nam}: {nam.stem} vs. {nam.get_stem()}")
            if autofix:
                nam.stem = nam.get_stem()
                nam.save()
            yield nam


@generator_command
def complexless_stems() -> Iterable[Name]:
    for nam in Name.select_valid().filter(
        Name.group == Group.genus, Name.name_complex == None, Name.stem != None
    ):
        if nam.nomenclature_status.requires_name_complex():
            yield nam


@generator_command
def correct_species_root_names(dry_run: bool = True) -> Iterable[Name]:
    for nam in Name.select_valid().filter(
        Name.group == Group.species, Name.species_name_complex != None
    ):
        if not nam.compute_gender(dry_run=dry_run):
            yield nam


@generator_command
def species_root_name_mismatch() -> Iterable[Name]:
    for nam in Name.select_valid().filter(
        Name.group == Group.species,
        Name.species_name_complex != None,
        Name.corrected_original_name != None,
    ):
        original_root_name = nam.corrected_original_name.split()[-1]
        if nam.species_name_complex.kind == constants.SpeciesNameKind.adjective:
            try:
                forms = list(nam.species_name_complex.get_forms(nam.root_name))
            except ValueError:
                print(
                    f"{nam}: {nam.root_name} does not match {nam.species_name_complex}"
                )
                yield nam
                continue
            if original_root_name not in forms:
                print(f"{nam}: {original_root_name} does not match {nam.root_name}")
                yield nam
        else:
            if original_root_name != nam.root_name:
                print(f"{nam}: {original_root_name} does not match {nam.root_name}")
                yield nam


@generator_command
def root_name_mismatch(interactive: bool = False) -> Iterable[Name]:
    for name in Name.select_valid().filter(
        Name.group == Group.family, ~(Name.type >> None)
    ):
        if name.is_unavailable():
            continue
        stem_name = name.type.stem
        if stem_name is None:
            continue
        if name.root_name == stem_name:
            continue
        if name.root_name + "id" == stem_name:
            # The Code allows eliding -id- from the stem.
            continue
        for stripped in helpers.name_with_suffixes_removed(name.root_name):
            if stripped == stem_name or stripped + "i" == stem_name:
                print(f"Autocorrecting root name: {name.root_name} -> {stem_name}")
                name.root_name = stem_name
                name.save()
                break
        if name.root_name != stem_name:
            print(f"Stem mismatch for {name}: {name.root_name} vs. {stem_name}")
            if interactive and getinput.yes_no("correct? "):
                name.root_name = stem_name
            yield name


def _duplicate_finder(
    fn: Callable[..., Iterable[Mapping[Any, Sequence[T]]]]
) -> Callable[..., Optional[List[Sequence[T]]]]:
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
def dup_collections() -> List[Dict[str, List[Collection]]]:
    colls: Dict[str, List[Collection]] = collections.defaultdict(list)
    for coll in Collection.select():
        colls[coll.label].append(coll)
    return [colls]


@_duplicate_finder
def dup_taxa() -> List[Dict[str, List[Taxon]]]:
    taxa: Dict[str, List[Taxon]] = collections.defaultdict(list)
    for txn in Taxon.select_valid():
        if txn.rank == Rank.subgenus and taxa[txn.valid_name]:
            continue
        taxa[txn.valid_name].append(txn)
    return [
        {
            label: [
                t
                for t in ts
                if t.base_name.nomenclature_status != NomenclatureStatus.preoccupied
            ]
            for label, ts in taxa.items()
            if len(ts) > 1
        }
    ]


@_duplicate_finder
def dup_genus() -> List[Dict[str, List[Name]]]:
    names: Dict[str, List[Name]] = collections.defaultdict(list)
    for name in Name.select_valid().filter(Name.group == Group.genus):
        full_name = f"{name.root_name} {name.authority}, {name.year}"
        names[full_name].append(name)
    return [names]


@_duplicate_finder
def dup_names() -> List[
    Dict[Tuple[str, str, constants.NomenclatureStatus], List[Name]]
]:
    original_year: Dict[
        Tuple[str, str, constants.NomenclatureStatus], List[Name]
    ] = collections.defaultdict(list)
    for name in Name.select_valid().filter(
        Name.original_name != None, Name.year != None
    ):
        original_year[(name.original_name, name.year, name.nomenclature_status)].append(
            name
        )
    return [original_year]


@command
def stem_statistics() -> None:
    stem = (
        Name.select_valid()
        .filter(Name.group == Group.genus, ~(Name.stem >> None))
        .count()
    )
    gender = (
        Name.select_valid()
        .filter(Name.group == Group.genus, ~(Name.gender >> None))
        .count()
    )
    total = Name.select_valid().filter(Name.group == Group.genus).count()
    print("Genus-group names:")
    print("stem: {}/{} ({:.02f}%)".format(stem, total, stem / total * 100))
    print("gender: {}/{} ({:.02f}%)".format(gender, total, gender / total * 100))
    print("Family-group names:")
    total = Name.select_valid().filter(Name.group == Group.family).count()
    typ = (
        Name.select_valid()
        .filter(Name.group == Group.family, ~(Name.type >> None))
        .count()
    )
    print("type: {}/{} ({:.02f}%)".format(typ, total, typ / total * 100))


class ScoreHolder:
    def __init__(self, data: Dict[Taxon, Dict[str, Any]]) -> None:
        self.data = data

    def by_field(self, field: str, min_count: int = 0, max_score: float = 101) -> None:
        items = (
            (key, value)
            for key, value in self.data.items()
            if value["total"] > min_count
            and value.get(field, (100, None, None))[0] < max_score
        )

        def sort_key(pair):
            _, data = pair
            percentage, required_count, count = data.get(field, (100, 0, 0))
            return (percentage, required_count, data["total"])

        sorted_items = sorted(items, key=sort_key)
        for taxon, data in sorted_items:
            if field in data:
                percentage, required_count, count = data[field]
            else:
                percentage, required_count, count = 100, 0, 0
            print(
                f'{taxon} {percentage:.2f} ({count}/{required_count}) {data["total"]}'
            )

    def completion_rate(self) -> None:
        fields = {field for data in self.data.values() for field in data} - {
            "total",
            "count",
            "score",
        }
        counts = collections.defaultdict(int)
        for data in self.data.values():
            for field in fields:
                if field not in data or data[field][0] == 100:
                    counts[field] += 1
        total = len(self.data)
        for field, count in sorted(counts.items(), key=lambda p: p[1]):
            print(f"{field}: {count * 100 / total:.2f} ({count}/{total})")


@command
def get_scores(
    rank: Rank,
    within_taxon: Optional[Taxon] = None,
    age: Optional[constants.Age] = None,
) -> ScoreHolder:
    data = {}
    if within_taxon is not None:
        taxa = within_taxon.children_of_rank(rank)
    else:
        taxa = Taxon.select_valid().filter(Taxon.rank == rank)
    for taxon in taxa:
        if age is not None and taxon.age > age:
            continue
        getinput.show(f"--- {taxon} ---")
        data[taxon] = taxon.stats(age=age)
    return ScoreHolder(data)


@generator_command
def name_mismatches(
    max_count: Optional[int] = None,
    correct: bool = False,
    correct_undoubted: bool = True,
) -> Iterable[Taxon]:
    count = 0
    for taxon in Taxon.select_valid():
        computed = taxon.compute_valid_name()
        if computed is not None and taxon.valid_name != computed:
            print(
                "Mismatch for %s: %s (actual) vs. %s (computed)"
                % (taxon, taxon.valid_name, computed)
            )
            yield taxon
            count += 1
            # For species-group taxa, we always trust the computed name. Usually these
            # have been reassigned to a different genus, or changed between species and
            # subspecies, or they have become nomina dubia (in which case we use the
            # corrected original name). For family-group names we don't always trust the
            # computed name, because stems may be arbitrary.
            if correct_undoubted and taxon.base_name.group == Group.species:
                taxon.recompute_name()
            elif correct:
                taxon.recompute_name()
            if max_count is not None and count == max_count:
                return


@generator_command
def authorless_names(
    root_taxon: Taxon,
    attribute: str = "authority",
    predicate: Optional[Callable[[Name], bool]] = None,
) -> Iterable[Name]:
    for nam in root_taxon.names:
        if (not predicate) or predicate(nam):
            if getattr(nam, attribute) is None:
                print(nam)
                yield nam
    for child in root_taxon.children:
        yield from authorless_names(child, attribute=attribute, predicate=predicate)


yearless_names = functools.partial(authorless_names, attribute="year")


@generator_command
def complexless_genera(root_taxon: Taxon) -> Iterable[Name]:
    return authorless_names(
        root_taxon, "name_complex", predicate=lambda n: n.group == Group.genus
    )


class LabeledName(NamedTuple):
    name: Name
    order: Optional[Taxon]
    family: Optional[Taxon]
    is_high_quality: bool
    is_doubtful: bool


HIGH_QUALITY = {
    "Probainognathia",  # including mammals
    "Cephalochordata",
    "Loricifera",
    "Cycliophora",
    "Micrognathozoa",
    "Gnathostomulida",
    "Gymnophiona",
    "Avemetatarsalia",  # pterosaurs and some dinosaurs
    "Eusuchia",  # crocodiles
    "Choristodera",
    "Ichthyosauria",
    "Rhynchocephalia",
    "Allocaudata",
}
LOW_QUALITY = {"Neornithes", "Ornithischia", "root"}


def is_high_quality(taxon: Taxon) -> bool:
    if taxon.valid_name in HIGH_QUALITY:
        return True
    elif taxon.valid_name in LOW_QUALITY:
        return False
    else:
        return is_high_quality(taxon.parent)


def label_name(name: Name) -> LabeledName:
    try:
        order = name.taxon.parent_of_rank(Rank.order)
    except ValueError:
        order = None
    try:
        family = name.taxon.parent_of_rank(Rank.family)
    except ValueError:
        family = None
    quality = is_high_quality(name.taxon)
    is_doubtful = name.taxon.is_child_of(taxon_of_name("Doubtful"))
    return LabeledName(name, order, family, quality, is_doubtful)


@command
def labeled_authorless_names(attribute: str = "authority") -> List[LabeledName]:
    nams = Name.select_valid().filter(getattr(Name, attribute) >> None)
    return [
        label_name(name) for name in nams if attribute in name.get_required_fields()
    ]


@command
def correct_type_taxon(
    max_count: Optional[int] = None, dry_run: bool = False, only_if_child: bool = True
) -> List[Name]:
    count = 0
    out = []
    doubtful = taxon_of_name("Doubtful")
    for nam in Name.select_valid().filter(
        Name.group << (Group.genus, Group.family), Name.type != None
    ):
        if nam.taxon == nam.type.taxon:
            continue
        expected_taxon = nam.type.taxon.parent
        while (
            expected_taxon.base_name.group != nam.group and expected_taxon != nam.taxon
        ):
            expected_taxon = expected_taxon.parent
            if expected_taxon is None:
                break
        if expected_taxon is None:
            continue
        if nam.taxon == doubtful:
            continue
        if nam.taxon != expected_taxon:
            count += 1
            print(f"maybe changing taxon of {nam} from {nam.taxon} to {expected_taxon}")
            if not dry_run:
                if only_if_child:
                    if not expected_taxon.is_child_of(nam.taxon):
                        print(f"skipping non-parent: {nam}")
                        out.append(nam)
                        continue
                nam.taxon = expected_taxon
                nam.save()
            if max_count is not None and count > max_count:
                break
    return out


# Statistics


@command
def type_locality_tree() -> None:
    earth = models.Region.get(name="Earth")
    _, lines = _tl_count(earth)
    for line in lines:
        print(line)


def _tl_count(region: models.Region) -> Tuple[int, List[str]]:
    print(f"processing {region}")
    count = 0
    lines = []
    for loc in region.locations.order_by(models.Location.name):
        tl_count = loc.type_localities.count()
        if tl_count:
            count += tl_count
            lines.append(f"{tl_count} - {loc.name}")
    for child in region.children.order_by(models.Region.name):
        child_count, child_lines = _tl_count(child)
        count += child_count
        lines += child_lines
    line = f"{count} - {region.name}"
    return count, [line, *["    " + l for l in lines]]


@command
def print_percentages() -> None:
    attributes = [
        "original_name",
        "original_citation",
        "page_described",
        "authority",
        "year",
    ]
    parent_of_taxon: Dict[int, int] = {}

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

    for taxon in Taxon.select_valid():
        _find_parent(taxon)

    print("Finished collecting parents for taxa")

    counts_of_parent: Dict[int, Dict[str, int]] = collections.defaultdict(
        lambda: collections.defaultdict(int)
    )
    for name in Name.select_valid():
        parent_id = parent_of_taxon[name.taxon.id]
        counts_of_parent[parent_id]["total"] += 1
        for attribute in attributes:
            if getattr(name, attribute) is not None:
                counts_of_parent[parent_id][attribute] += 1

    print("Finished collecting statistics on names")

    parents = [
        (Taxon.select_valid().filter(Taxon.id == parent_id)[0], data)
        for parent_id, data in counts_of_parent.items()
    ]

    for parent, data in sorted(parents, key=lambda i: i[0].valid_name):
        print("FILE", parent)
        total = data["total"]
        del data["total"]
        print("Total", total)
        for attribute in attributes:
            percentage = data[attribute] * 100.0 / total
            print("{}: {} ({:.2f}%)".format(attribute, data[attribute], percentage))


@generator_command
def bad_base_names() -> Iterable[Taxon]:
    return Taxon.raw(
        f"""
            SELECT * FROM taxon
            WHERE
                age != {constants.Age.removed.value} AND
                (
                    base_name_id IS NULL OR
                    base_name_id NOT IN (
                        SELECT id
                        FROM name
                        WHERE status IN (
                            {constants.Status.valid.value},
                            {constants.Status.nomen_dubium.value},
                            {constants.Status.species_inquirenda.value},
                            {constants.Status.spurious.value}
                        )
                    )
                )
        """
    )


@generator_command
def bad_taxa() -> Iterable[Name]:
    return Name.raw(
        "SELECT * FROM name WHERE taxon_id IS NULL or taxon_id NOT IN (SELECT id FROM taxon)"
    )


@generator_command
def bad_parents() -> Iterable[Name]:
    return Name.raw("SELECT * FROM taxon WHERE parent_id NOT IN (SELECT id FROM taxon)")


@generator_command
def parentless_taxa() -> Iterable[Taxon]:
    # exclude root
    return (t for t in Taxon.select_valid().filter(Taxon.parent >> None) if t.id != 1)


@generator_command
def bad_occurrences() -> Iterable[models.Occurrence]:
    return models.Occurrence.raw(
        "SELECT * FROM occurrence WHERE taxon_id NOT IN (SELECT id FROM taxon)"
    )


@generator_command
def bad_types() -> Iterable[Name]:
    return Name.raw(
        "SELECT * FROM name WHERE type_id IS NOT NULL AND type_id NOT IN (SELECT id FROM name)"
    )


ATTRIBUTES_BY_GROUP = {
    "stem": (Group.genus,),
    "gender": (Group.genus,),
    "name_complex": (Group.genus,),
    "species_name_complex": (Group.species,),
    "type": (Group.family, Group.genus),
    "type_locality": (Group.species,),
    "type_locality_description": (Group.species,),
    "type_specimen": (Group.species,),
    "collection": (Group.species,),
    "type_specimen_source": (Group.species,),
    "genus_type_kind": (Group.genus,),
    "species_type_kind": (Group.species,),
    "type_tags": (Group.genus, Group.species),
}


@generator_command
def disallowed_attribute() -> Iterable[Tuple[Name, str]]:
    for field, groups in ATTRIBUTES_BY_GROUP.items():
        for nam in Name.select_valid().filter(
            getattr(Name, field) != None, ~(Name.group << groups)
        ):
            yield nam, field


@command
def autoset_original_name() -> None:
    for nam in Name.select_valid().filter(
        Name.original_name >> None, Name.group << (Group.genus, Group.high)
    ):
        nam.original_name = nam.root_name


@generator_command
def childless_taxa() -> Iterable[Taxon]:
    return Taxon.raw(
        f"""
            SELECT *
            FROM taxon
            WHERE
                rank > 5 AND
                age != {Age.removed.value} AND
                id NOT IN (
                    SELECT parent_id
                    FROM taxon
                    WHERE
                        parent_id IS NOT NULL AND
                        age != {Age.removed.value}
                )
        """
    )


@generator_command
def labeled_childless_taxa() -> Iterable[LabeledName]:
    return [label_name(taxon.base_name) for taxon in childless_taxa()]


@command
def fossilize(
    *taxa: Taxon, to_status: Age = Age.fossil, from_status: Age = Age.extant
) -> None:
    for taxon in taxa:
        if taxon.age != from_status:
            continue
        taxon.age = to_status  # type: ignore
        taxon.save()
        for child in taxon.children:
            fossilize(child, to_status=to_status, from_status=from_status)


@generator_command
def check_age_parents() -> Iterable[Taxon]:
    """Extant taxa should not have fossil parents."""
    for taxon in Taxon.select_valid():
        if taxon.parent is not None and taxon.age < taxon.parent.age:
            print(
                f"{taxon} is {taxon.age}, but its parent {taxon.parent} is {taxon.parent.age}"
            )
            yield taxon


@command
def clean_up_verbatim(dry_run: bool = False, slow: bool = False) -> None:
    def _maybe_clean_verbatim(nam):
        print(f"{nam}: {nam.type}, {nam.verbatim_type}")
        if not dry_run:
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
            nam.save()

    famgen_type_count = species_type_count = citation_count = 0
    for nam in Name.select_valid().filter(
        Name.group << (Group.family, Group.genus),
        Name.verbatim_type != None,
        Name.type != None,
    ):
        famgen_type_count += 1
        _maybe_clean_verbatim(nam)
    if slow:
        for nam in Name.select_valid().filter(
            Name.group << (Group.family, Group.genus), Name.verbatim_type != None
        ):
            if "type" not in nam.get_required_fields():
                famgen_type_count += 1
                _maybe_clean_verbatim(nam)
    for nam in Name.select_valid().filter(
        Name.group == Group.species,
        Name.verbatim_type != None,
        Name.type_specimen != None,
    ):
        print(f"{nam}: {nam.type_specimen}, {nam.verbatim_type}")
        species_type_count += 1
        if not dry_run:
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
            nam.save()
    for nam in Name.select_valid().filter(
        Name.verbatim_citation != None, Name.original_citation != None
    ):
        print(f"{nam}: {nam.original_citation.name}, {nam.verbatim_citation}")
        citation_count += 1
        if not dry_run:
            nam.add_data(
                "verbatim_citation", nam.verbatim_citation, concat_duplicate=True
            )
            nam.verbatim_citation = None
            nam.save()
    print(f"Family/genera type count: {famgen_type_count}")
    print(f"Species type count: {species_type_count}")
    print(f"Citation count: {citation_count}")


@command
def clean_up_type_locality_description(dry_run: bool = False) -> None:
    count = removed = 0
    for nam in Name.select_valid().filter(
        Name.type_locality_description != None, Name.type_tags != None
    ):
        count += 1
        tags = [tag for tag in nam.type_tags if isinstance(tag, TypeTag.LocationDetail)]
        if not tags:
            continue
        print("----------------")
        print(nam)
        for tag in tags:
            print(tag)
        print(nam.type_locality_description)
        if not dry_run:
            removed += 1
            print("automatically emptying data")
            nam.add_data("type_locality_description", nam.type_locality_description)
            nam.type_locality_description = None
    print(f"removed: {removed}/{count}")


@command
def set_empty_to_none(
    model_cls: Type[models.BaseModel], field: str, dry_run: bool = False
) -> None:
    for obj in model_cls.filter(getattr(model_cls, field) == ""):
        print(f"{obj}: set {field} to None")
        if not dry_run:
            setattr(obj, field, None)
            obj.save()


@command
def fill_data_from_paper(
    paper: Optional[models.Article] = None, always_edit_tags: bool = False
) -> None:
    if paper is None:
        paper = models.BaseModel.get_value_for_foreign_class("paper", models.Article)
    assert paper is not None, "paper needs to be specified"
    models.taxon.fill_data_from_paper(paper, always_edit_tags=always_edit_tags)


@command
def fill_type_locality(
    extant_only: bool = True, start_at: Optional[Name] = None
) -> None:
    started = start_at is None
    for nam in Name.select_valid().filter(
        Name.type_locality_description != None, Name.type_locality >> None
    ):
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
        nam.fill_field("type_locality")


def names_with_location_detail_without_type_loc(
    taxon: Optional[Taxon] = None
) -> Iterable[Name]:
    if taxon is None:
        nams = Name.select_valid().filter(
            Name.type_tags != None, Name.type_locality >> None
        )
    else:
        nams = [
            nam
            for nam in taxon.all_names()
            if nam.type_tags is not None and nam.type_locality is None
        ]
    for nam in nams:
        tags = [tag for tag in nam.type_tags if isinstance(tag, TypeTag.LocationDetail)]
        if not tags:
            continue
        if "type_locality" not in nam.get_required_fields():
            continue
        nam.display()
        for tag in tags:
            print(tag)
        yield nam


@command
def fill_type_locality_from_location_detail(taxon: Optional[Taxon] = None) -> None:
    for nam in names_with_location_detail_without_type_loc(taxon):
        nam.fill_field("type_locality")


@command
def more_precise_type_localities(loc: models.Location) -> None:
    for nam in loc.type_localities:
        if not nam.type_tags:
            continue
        print("-------------------------")
        print(nam)
        for tag in nam.type_tags:
            if isinstance(tag, models.TypeTag.LocationDetail):
                print(tag)
        nam.fill_field("type_locality")


@generator_command
def type_locality_without_detail() -> Iterable[Name]:
    # All type localities should be supported by a LocationDetail tag. However, we probably shouldn't
    # worry about this until coverage of extant type localities is more comprehensive.
    for nam in Name.select_valid().filter(Name.type_locality != None):
        if not nam.type_tags or not any(
            isinstance(tag, models.TypeTag.LocationDetail) for tag in nam.type_tags
        ):
            print(f"{nam} has a type locality but no location detail")
            yield nam


@command
def most_common_comments(field: str = "other_comments") -> Counter[str]:
    return Counter(
        getattr(nam, field)
        for nam in Name.select_valid().filter(getattr(Name, field) != None)
    )


@command
def redundant_comments() -> None:
    for nam in Name.select_valid().filter(
        Name.nomenclature_comments == "Nomen nudum",
        Name.nomenclature_status == NomenclatureStatus.nomen_nudum,
    ):
        print(f"{nam}: remove {nam.nomenclature_comments!r}")
        nam.nomenclature_comments = None
    for nam in Name.select_valid().filter(
        Name.other_comments == "Nomen nudum",
        Name.nomenclature_status == NomenclatureStatus.nomen_nudum,
    ):
        print(f"{nam}: remove {nam.other_comments!r}")
        nam.other_comments = None
    for nam in Name.select_valid().filter(
        Name.nomenclature_comments == "Preoccupied",
        Name.nomenclature_status == NomenclatureStatus.preoccupied,
    ):
        print(f"{nam}: remove {nam.nomenclature_comments!r}")
        nam.nomenclature_comments = None
    for nam in Name.select_valid().filter(
        Name.other_comments == "Preoccupied",
        Name.nomenclature_status == NomenclatureStatus.preoccupied,
    ):
        print(f"{nam}: remove {nam.other_comments!r}")
        nam.other_comments = None
    for nam in Name.select_valid().filter(
        Name.other_comments == "Nomen dubium",
        Name.status == constants.Status.nomen_dubium,
    ):
        print(f"{nam}: remove {nam.other_comments!r}")
        nam.other_comments = None


@generator_command
def check_year() -> Iterable[Name]:
    single_year = re.compile(r"^\d{4}$")
    multi_year = re.compile(r"^\d{4}-\d{4}$")
    for nam in Name.select_valid().filter(Name.year != None, Name.year != "in press"):
        if single_year.match(nam.year):
            continue
        if multi_year.match(nam.year):
            continue
        print(f"{nam} has invalid year {nam.year!r}")
        yield nam


@generator_command
def check_expected_base_name() -> Iterable[Taxon]:
    for txn in Taxon.select_valid().filter(Taxon.rank <= Rank.superfamily):
        if not txn.check_expected_base_name():
            yield txn


@generator_command
def check_justified_emendations() -> Iterable[Tuple[Name, str]]:
    """Checks that justified emendations are treated correctly.

    See documentation in name.rst.

    """
    justified_emendations = Name.bfind(
        nomenclature_status=NomenclatureStatus.justified_emendation, quiet=True
    )
    as_emendeds = set(
        Name.bfind(nomenclature_status=NomenclatureStatus.as_emended, quiet=True)
    )
    for nam in justified_emendations:
        target = nam.get_tag_target(Tag.JustifiedEmendationOf)
        if target is None:
            yield nam, "justified_emendation without a JustifiedEmendationOf tag"
            continue
        ios_target = target.get_tag_target(Tag.IncorrectOriginalSpellingOf)
        if target.nomenclature_status is NomenclatureStatus.incorrect_original_spelling:
            if ios_target.nomenclature_status is not NomenclatureStatus.as_emended:
                yield ios_target, f"should be as_emended because {target} is an IOS"
            elif ios_target in as_emendeds:
                as_emendeds.remove(ios_target)
            if nam.root_name == target.root_name:
                yield nam, f"root_name should be different from {target}"
            if nam.root_name != ios_target.root_name:
                yield nam, f"root_name should match {ios_target}"
        elif target.nomenclature_status is NomenclatureStatus.available:
            if ios_target is not None:
                yield target, "unexpected IncorrectOriginalSpellingOf tag"
            if target.root_name != nam.root_name:
                yield nam, f"root_name does not match ({nam.root_name} vs. {target.root_name} in {target})"
            elif target.original_name == target.corrected_original_name:
                yield nam, f"justified emendation but there is nothing to emend in {target}"
        else:
            yield nam, f"unexpected status in target {target}"
    for nam in as_emendeds:
        yield nam, "as_emended without a justified_emendation"


@generator_command
def check_tags(dry_run: bool = True) -> Iterable[Tuple[Name, str]]:
    """Looks at all tags set on names and applies related changes."""
    status_to_priority = {}
    for priority, statuses in enumerate(NomenclatureStatus.hierarchy()):
        for status in statuses:
            status_to_priority[status] = priority

    def maybe_adjust_status(nam: Name, status: NomenclatureStatus, tag: object) -> None:
        current_priority = status_to_priority[nam.nomenclature_status]
        new_priority = status_to_priority[status]
        if current_priority > new_priority:
            comment = f"Status automatically changed from {nam.nomenclature_status.name} to {status.name} because of {tag}"
            print(f"changing status of {nam} and adding comment {comment!r}")
            if not dry_run:
                nam.add_comment(constants.CommentKind.automatic_change, comment, None)
                nam.nomenclature_status = status  # type: ignore
                nam.save()

    names_by_tag: Dict[Type[Any], Set[Name]] = collections.defaultdict(set)
    for nam in Name.select_valid().filter(Name.tags != None):
        try:
            tags = nam.tags
        except Exception:
            yield nam, "could not deserialize tags"
            continue
        for tag in tags:
            names_by_tag[type(tag)].add(nam)
            if isinstance(tag, Tag.PreoccupiedBy):
                maybe_adjust_status(nam, NomenclatureStatus.preoccupied, tag)
                if nam.group != tag.name.group:
                    print(
                        f"{nam} is of a different group than supposed senior name {tag.name}"
                    )
                    yield nam, "homonym of different group"
                if nam.effective_year() < tag.name.effective_year():
                    print(f"{nam} predates supposed senior name {tag.name}")
                    yield nam, "antedates homonym"
                # TODO apply this check to species too by handling gender endings correctly.
                if nam.group != Group.species:
                    if nam.root_name != tag.name.root_name:
                        print(
                            f"{nam} has a different root name than supposed senior name {tag.name}"
                        )
                        yield nam, "differently-named homonym"
            elif isinstance(
                tag,
                (
                    Tag.UnjustifiedEmendationOf,
                    Tag.IncorrectSubsequentSpellingOf,
                    Tag.VariantOf,
                    Tag.NomenNovumFor,
                    Tag.JustifiedEmendationOf,
                ),
            ):
                for status, tag_cls in models.STATUS_TO_TAG.items():
                    if isinstance(tag, tag_cls):
                        maybe_adjust_status(nam, status, tag)
                if nam.effective_year() < tag.name.effective_year():
                    print(f"{nam} predates supposed original name {tag.name}")
                    yield nam, "antedates original name"
                if nam.taxon != tag.name.taxon:
                    print(f"{nam} is not assigned to the same name as {tag.name}")
                    yield nam, "not synonym of original name"
            elif isinstance(tag, Tag.PartiallySuppressedBy):
                maybe_adjust_status(nam, NomenclatureStatus.partially_suppressed, tag)
            elif isinstance(tag, Tag.FullySuppressedBy):
                maybe_adjust_status(nam, NomenclatureStatus.fully_suppressed, tag)
            elif isinstance(tag, Tag.Conserved):
                if nam.nomenclature_status != NomenclatureStatus.available:
                    print(
                        f"{nam} is on the Official List, but is not marked as available."
                    )
                    yield nam, "unavailable listed name"
            # haven't handled TakesPriorityOf, NomenOblitum, MandatoryChangeOf

    for status, tag_cls in models.STATUS_TO_TAG.items():
        tagged_names = names_by_tag[tag_cls]
        for nam in Name.select_valid().filter(Name.nomenclature_status == status):
            if nam not in tagged_names:
                yield nam, f"has status {status.name} but no corresponding tag"


@generator_command
def check_type_tags(dry_run: bool = False) -> Iterable[Tuple[Name, str]]:
    for nam in Name.select_valid().filter(Name.type_tags != None):
        tags: List[TypeTag] = []
        original_tags = list(nam.type_tags)
        for tag in original_tags:
            if isinstance(tag, TypeTag.CommissionTypeDesignation):
                if nam.type != tag.type:
                    print(
                        f"{nam} has {nam.type} as its type, but the Commission has designated {tag.type}"
                    )
                    if not dry_run:
                        nam.type = tag.type
                if (
                    nam.genus_type_kind
                    != constants.TypeSpeciesDesignation.designated_by_the_commission
                ):
                    print(
                        f"{nam} has {nam.genus_type_kind}, but its type was set by the Commission"
                    )
                    if not dry_run:
                        nam.genus_type_kind = (
                            constants.TypeSpeciesDesignation.designated_by_the_commission
                        )
            elif isinstance(tag, TypeTag.Date):
                date = tag.date
                try:
                    date = helpers.standardize_date(date)
                except ValueError:
                    print(f"{nam} has date {tag.date}, which cannot be parsed")
                    yield nam, "unparseable date"
                if date is None:
                    continue
                tags.append(TypeTag.Date(date))
            elif isinstance(tag, TypeTag.Altitude):
                if (
                    not re.match(r"^-?\d+([\-\.]\d+)?$", tag.altitude)
                    or tag.altitude == "000"
                ):
                    print(f"{nam} has altitude {tag}, which cannot be parsed")
                    yield nam, f"bad altitude tag {tag}"
                tags.append(tag)
            elif isinstance(tag, TypeTag.LocationDetail):
                coords = helpers.extract_coordinates(tag.text)
                if coords and not any(
                    isinstance(t, TypeTag.Coordinates) for t in original_tags
                ):
                    tags.append(TypeTag.Coordinates(coords[0], coords[1]))
                    print(
                        f"{nam}: adding coordinates {tags[-1]} extracted from {tag.text!r}"
                    )
                tags.append(tag)
            elif isinstance(tag, TypeTag.Coordinates):
                try:
                    lat = helpers.standardize_coordinates(
                        tag.latitude, is_latitude=True
                    )
                except helpers.InvalidCoordinates as e:
                    print(f"{nam} has invalid latitude {tag.latitude}: {e}")
                    yield nam, f"invalid latitude {tag.latitude}"
                    lat = tag.latitude
                try:
                    longitude = helpers.standardize_coordinates(
                        tag.longitude, is_latitude=False
                    )
                except helpers.InvalidCoordinates as e:
                    print(f"{nam} has invalid longitude {tag.longitude}: {e}")
                    yield nam, f"invalid longitude {tag.longitude}"
                    longitude = tag.longitude
                tags.append(TypeTag.Coordinates(lat, longitude))
            else:
                tags.append(tag)
            # TODO: for lectotype and subsequent designations, ensure the earliest valid one is used.
        tags = sorted(set(tags))
        if not dry_run and tags != original_tags:
            print("changing tags")
            print(original_tags)
            print(tags)
            nam.type_tags = tags
    getinput.flush()
    for nam in Name.select_valid().filter(
        Name.genus_type_kind == constants.TypeSpeciesDesignation.subsequent_designation
    ):
        for tag in nam.type_tags or ():
            if isinstance(tag, TypeTag.TypeDesignation) and tag.type == nam.type:
                break
        else:
            print(f"{nam} is missing a reference for its type designation")
            yield nam, "missing type designation reference"
    for nam in Name.select_valid().filter(
        Name.species_type_kind == constants.SpeciesGroupType.lectotype
    ):
        if nam.collection and nam.collection.name in ("lost", "untraced"):
            continue
        for tag in nam.type_tags or ():
            if (
                isinstance(tag, TypeTag.LectotypeDesignation)
                and tag.lectotype == nam.type_specimen
            ):
                break
        else:
            print(f"{nam} is missing a reference for its lectotype designation")
            yield nam, "missing lectotype designation reference"
    for nam in Name.select_valid().filter(
        Name.species_type_kind == constants.SpeciesGroupType.neotype
    ):
        for tag in nam.type_tags or ():
            if (
                isinstance(tag, TypeTag.NeotypeDesignation)
                and tag.neotype == nam.type_specimen
            ):
                break
        else:
            print(f"{nam} is missing a reference for its neotype designation")
            yield nam, "missing neotype designation reference"


@generator_command
def move_to_lowest_rank(dry_run: bool = False) -> Iterable[Tuple[Name, str]]:
    for nam in Name.select_valid():
        query = Taxon.select_valid().filter(Taxon._base_name_id == nam)
        if query.count() < 2:
            continue
        if nam.group == Group.high:
            yield nam, "high-group names cannot be the base name of multiple taxa"
            continue
        lowest, *ts = sorted(query, key=lambda t: t.rank)
        last_seen = lowest
        for t in ts:
            while last_seen is not None and last_seen != t:
                last_seen = last_seen.parent
            if last_seen is None:
                yield nam, f"taxon {t} is not a parent of {lowest}"
                break
        if last_seen is None:
            continue
        if nam.taxon != lowest:
            print(f"changing taxon of {nam} to {lowest}")
            if not dry_run:
                nam.taxon = lowest
                nam.save()


AUTHOR_SYNONYMS = {
    "Afanasiev": helpers.romanize_russian("Афанасьев"),
    "Barret-Hamilton": "Barrett-Hamilton",
    "Blainville": "de Blainville",
    "Bobrinskii": helpers.romanize_russian("Бобринской"),
    "Bobrinskoi": helpers.romanize_russian("Бобринской"),
    "Boeskorov": "Boyeskorov",
    "Bogatschev": helpers.romanize_russian("Богачев"),
    "C. Hamilton Smith": "C.H. Smith",
    "C.F. Major": "Forsyth Major",
    "C. Smith": "C.H. Smith",
    "C.E.H. Smith": "C.H. Smith",
    "Chabaeva": helpers.romanize_russian("Хабаева"),
    "Chernyavskii": helpers.romanize_russian("Чернявский"),
    "Crawford Cabral": "Crawford-Cabral",
    "Czersky": helpers.romanize_russian("Черский"),
    "De Blainville": "de Blainville",
    "De Beaux": "de Beaux",
    "de Miranda Ribeiro": "Miranda-Ribeiro",
    "de Miranda-Ribeiro": "Miranda-Ribeiro",
    "De Muizon": "de Muizon",
    "de Selys-Longchamps": "de Sélys Longchamps",
    "De Winton": "de Winton",
    "du Chaillu": "Du Chaillu",
    "Degerbol": "Degerbøl",
    "DuChaillu": "Du Chaillu",
    "Dukelskaia": helpers.romanize_russian("Дукельская"),
    "Dukelski": "Dukelskiy",
    "Dukelsky": "Dukelskiy",
    "E. Geoffroy Saint-Hilaire": "É. Geoffroy Saint-Hilaire",
    "E. Geoffroy": "É. Geoffroy Saint-Hilaire",
    "Ehik": "Éhik",
    "Formosov": "Formozov",
    "Geoffroy": "Geoffroy Saint-Hilaire",
    "Gunther": "Günther",
    "H. Smith": "C.H. Smith",
    "Habaeva": helpers.romanize_russian("Хабаева"),
    "Hamilton Smith": "C.H. Smith",
    "Hamilton-Smith": "C.H. Smith",
    "I. Geoffroy": "I. Geoffroy Saint-Hilaire",
    "J. Gray": "J.E. Gray",
    "Kolossow": helpers.romanize_russian("Колосов"),
    "Kortchagin": helpers.romanize_russian("Корчагин"),
    "Kortshagin": helpers.romanize_russian("Корчагин"),
    "Kovalskaja": helpers.romanize_russian("Ковальская"),
    "Kovalskaya": helpers.romanize_russian("Ковальская"),
    "Kowalskaia": helpers.romanize_russian("Ковальская"),
    "Kowalskaja": helpers.romanize_russian("Ковальская"),
    "Krassovskii": helpers.romanize_russian("Красовский"),
    "Krassowsky": helpers.romanize_russian("Красовский"),
    "Lacepede": "Lacépède",
    "Le Soeuf": "Le Souef",
    "LeConte": "Le Conte",
    "Leconte": "Le Conte",
    "Lonnberg": "Lönnberg",
    "Major": "Forsyth Major",
    "Milne Edwards": "Milne-Edwards",
    "Miranda Ribeiro": "Miranda-Ribeiro",  # {Miranda-Ribeiro-biography.pdf}
    "Morosova-Turova": helpers.romanize_russian("Морозова-Турова"),
    "Muizon": "de Muizon",
    "Naumoff": helpers.romanize_russian("Наумов"),
    "Peron": "Péron",
    "Petenyi": "Petényi",
    "Prigogone": "Prigogine",
    "Przewalski": helpers.romanize_russian("Пржевальский"),
    "Raevski": helpers.romanize_russian("Раевский"),
    "Raevsky": helpers.romanize_russian("Раевский"),
    "Raevskyi": helpers.romanize_russian("Раевский"),
    "Rajevsky": helpers.romanize_russian("Раевский"),
    "Ruppell": "Rüppell",
    "Scalon": helpers.romanize_russian("Скалон"),
    "Selewin": helpers.romanize_russian("Селевин"),
    "Severtsow": helpers.romanize_russian("Северцов"),
    "Severtzov": helpers.romanize_russian("Северцов"),
    "Severtzow": helpers.romanize_russian("Северцов"),
    "Souef": "Le Souef",
    "St Leger": "St. Leger",
    "Tichomirov": helpers.romanize_russian("Тихомиров"),
    "Tichomirow": helpers.romanize_russian("Тихомиров"),
    "Timofeev": helpers.romanize_russian("Тимофеев"),
    "Timofeiev": helpers.romanize_russian("Тимофеев"),
    "Topal": "Topál",
    "Tzalkin": helpers.romanize_russian("Цалкин"),
    "Vasil'eva": helpers.romanize_russian("Васильева"),
    "Verestchagin": helpers.romanize_russian("Верещагин"),
    "Vereschchagin": helpers.romanize_russian("Верещагин"),
    "Vereschagin": helpers.romanize_russian("Верещагин"),
    "Van Bénéden": "Van Beneden",
    "Von Dueben": "von Dueben",
    "von Haast": "Haast",
    "Von Lehmann": "Lehmann",
    "Vorontzov": "Vorontsov",
    "Wasiljewa": helpers.romanize_russian("Васильева"),
    "Wied": "Wied-Neuwied",
    "Winton": "de Winton",
    "Worobiev": helpers.romanize_russian("Воробьев"),
    "Zalkin": helpers.romanize_russian("Цалкин"),
    "É. Geoffroy": "É. Geoffroy Saint-Hilaire",
}
AMBIGUOUS_AUTHORS = {
    "Allen",
    "Peters",
    "Thomas",
    "Geoffroy Saint-Hilaire",
    "Cuvier",
    "Howell",
    "Smith",
    "Grandidier",
    "Ameghino",
    "Major",
    "Petter",
    "Verheyen",
    "Gervais",
    "Andersen",
    "Gray",
    "Owen",
    "Martin",
    "Russell",
    "Leakey",
    "Anderson",
    "Bryant",
    "Merriam",
    "Heller",
}


def _names_with_author(author: str) -> Iterable[Name]:
    for nam in Name.select_valid().filter(Name.authority % f"*{author}*"):
        authors = nam.get_authors()
        if author in authors:
            yield nam


def _replace_author(name: Name, bad: str, good: str, dry_run: bool = True) -> None:
    authors = name.get_authors()
    orig_authors = list(authors)
    index = authors.index(bad)
    assert index != -1, f"cannot find {bad} in {name}"
    if authors.count(bad) != 1:
        print(f"there are multiple authors; please edit manually ({bad} -> {good})")
        if not dry_run:
            new_authors = getinput.get_line(prompt="> ", default=name.authority)
            print(f"change author for {name}: {name.authority} -> {new_authors}")
            name.authority = new_authors
        return
    authors[index] = good
    print(f"change author for {name}: {orig_authors} -> {authors}")
    if not dry_run:
        name.set_authors(authors)


@command
def apply_author_synonyms(dry_run: bool = False) -> None:
    for bad, good in AUTHOR_SYNONYMS.items():
        for nam in _names_with_author(bad):
            _replace_author(nam, bad, good, dry_run=dry_run)


def _get_new_author(nams: List[Name], citation: Article, author: str) -> Optional[str]:
    authors = citation.authors
    if ";" not in authors and ", " in authors:
        last, initials = authors.split(", ")
        if last == author:
            return f"{initials} {last}"
    nams[0].open_description()
    return Name.getter("authority").get_one_key("author> ")


@command
def disambiguate_authors(dry_run: bool = False) -> None:
    for author in sorted(AMBIGUOUS_AUTHORS):
        print(f"--- {author} ---")
        nams = list(_names_with_author(author))
        by_citation: Dict[Optional[Article], List[Name]] = collections.defaultdict(list)
        for nam in nams:
            by_citation[nam.original_citation].append(nam)
        for citation, nams in by_citation.items():
            if citation is None:
                print(f"skipping {len(nams)} names without citation")
                continue
            print(f"--- {citation} ---")
            print(f"author for names: {nams}")
            new_author = _get_new_author(nams, citation, author)
            if new_author:
                for nam in nams:
                    _replace_author(nam, author, new_author, dry_run=dry_run)


@generator_command
def validate_authors(dry_run: bool = False) -> Iterable[Name]:
    for nam in Name.select_valid().filter(Name.authority.contains("[")):
        print(f"{nam}: invalid authors")
        yield nam

    for nam in Name.select_valid().filter(Name.authority % "*. *"):
        authority = re.sub(r"([A-Z]\.) (?=[A-Z]\.)", r"\1", nam.authority)
        if authority != nam.authority:
            print(f"{nam}: {nam.authority} -> {authority}")
            if not dry_run:
                nam.authority = authority


@command
def initials_report() -> None:
    data: Dict[str, Dict[str, List[Name]]] = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )
    for nam in Name.select_valid().filter(Name.authority % "*.*"):
        authors = nam.get_authors()
        for author in authors:
            if ". " in author:
                initials, last = author.rsplit(". ", maxsplit=1)
                data[last][f"{initials}."].append(nam)

    for last_name, by_initial in sorted(data.items()):
        print(f"--- {last_name} ---")
        for initials, names in sorted(by_initial.items()):
            print(f"{initials} -- {len(names)}")


@command
def resolve_redirects(dry_run: bool = False) -> None:
    for nam in Name.filter(Name.type_tags != None):

        def map_fn(source: Article) -> Article:
            if source is None:
                return None
            if source.kind == constants.ArticleKind.redirect:
                print(f"{nam}: {source} -> {source.parent}")
                if not dry_run:
                    return source.parent
            return source

        nam.map_type_tags_by_type(Article, map_fn)
    for nam in Name.filter(Name.original_citation != None):
        if nam.original_citation.kind == constants.ArticleKind.redirect:
            print(f"{nam}: {nam.original_citation} -> {nam.original_citation.parent}")
            if not dry_run:
                nam.original_citation = nam.original_citation.parent
    for nam in Name.filter(Name.type_specimen_source != None):
        if nam.type_specimen_source.kind == constants.ArticleKind.redirect:
            print(
                f"{nam}: {nam.type_specimen_source} -> {nam.type_specimen_source.parent}"
            )
            if not dry_run:
                nam.type_specimen_source = nam.type_specimen_source.parent


@command
def run_maintenance(skip_slow: bool = True) -> Dict[Any, Any]:
    """Runs maintenance checks that are expected to pass for the entire database."""
    fns: List[Callable[[], Any]] = [
        lambda: set_empty_to_none(Name, "type_locality_description"),
        clean_up_verbatim,
        clean_up_type_locality_description,
        parentless_taxa,
        bad_parents,
        bad_taxa,
        bad_base_names,
        bad_occurrences,
        bad_types,
        labeled_authorless_names,
        root_name_mismatch,
        detect_complexes,
        detect_species_name_complexes,
        check_year,
        disallowed_attribute,
        autoset_original_name,
        apply_author_synonyms,
        disambiguate_authors,
        validate_authors,
        detect_corrected_original_names,
        dup_collections,
        # dup_names,
        # dup_genus,
        # dup_taxa,
        bad_stratigraphy,
        resolve_redirects,
    ]
    # these each take >60 s
    slow: List[Callable[[], Any]] = [
        correct_type_taxon,
        find_rank_mismatch,
        move_to_lowest_rank,
        check_tags,  # except for this one at 27 s
        check_type_tags,
        check_age_parents,
        name_mismatches,
    ]
    if not skip_slow:
        fns += slow
    out = {}
    timings = []
    for fn in fns:
        print(f"calling {fn}")
        with helpers.timer(str(fn)) as th:
            result = fn()
        timings.append((fn, th.time))
        if result:
            out[fn] = result

    for fn, time in sorted(timings, key=lambda pair: pair[1], reverse=True):
        print(time, fn)
    return out


@command
def names_of_authority(author: str, year: int, edit: bool = False) -> List[Name]:
    query = Name.select_valid().filter(
        Name.authority.contains(author),
        Name.year == year,
        Name.original_citation >> None,
    )

    def sort_key(nam: Name) -> int:
        if nam.page_described is None:
            return -1
        try:
            return int(nam.page_described)
        except ValueError:
            m = re.match(r"^(\d+)", nam.page_described)
            if m:
                return int(m.group(1))
            else:
                return -1

    nams = sorted(query, key=sort_key)
    print(f"{len(nams)} names")
    for nam in nams:
        nam.display()
        if edit:
            nam.fill_required_fields()
    return nams


@command
def find_multiple_repository_names(
    filter: Optional[str] = None, edit: bool = False
) -> List[Name]:
    all_nams = Name.select_valid().filter(
        Name.type_specimen.contains(", "),
        Name.collection != Collection.by_label("multiple"),
    )
    nams = []
    for nam in all_nams:
        type_specimen = re.sub(r" \([^\)]+\)", "", nam.type_specimen)
        parts = {re.split(r"[ \-]", part)[0] for part in type_specimen.split(", ")}
        if len(parts) == 1 and re.match(r"^[A-Z]+$", list(parts)[0]):
            continue  # All from same collection
        if filter is not None:
            if not nam.type_specimen.startswith(filter):
                continue
        print(nam)
        print(f" - {nam.type_specimen}")
        print(f" - {nam.collection}")
        nams.append(nam)
    if edit:
        for nam in nams:
            nam.display()
            nam.e.type_specimen
            nam.e.collection
            nam.e.type_tags
    return nams


@command
def moreau(nam: Name) -> None:
    nam.display()
    nam.e.type_locality
    nam.e.type_tags


@command
def replace_comments(substr: str, field: str = "other_comments") -> None:
    for nam in Name.filter(getattr(Name, field).contains(substr)).order_by(
        getattr(Name, field)
    ):
        nam.display()
        print(getattr(nam, field))
        nam.add_comment()
        setattr(nam, field, None)


def fgsyn(off: Optional[Name] = None) -> Name:
    """Adds a family-group synonym."""
    if off is not None:
        taxon = off.taxon
    else:
        taxon = Taxon.get_one_by("valid_name", prompt="taxon> ")
    root_name = Name.getter("original_name").get_one_key("name> ")
    source = Name.get_value_for_foreign_class("source", models.Article)
    kwargs = {}
    if off is not None:
        kwargs["type"] = off.type
    return taxon.syn_from_paper(root_name, source, original_name=root_name, **kwargs)


@command
def author_report(
    author: str, partial: bool = False, missing_attribute: Optional[str] = None
) -> None:
    if partial:
        condition = Name.authority.contains(author)
    else:
        condition = Name.authority == author
    nams = list(Name.select_valid().filter(condition, Name.original_citation == None))

    by_year = collections.defaultdict(list)
    no_year = []
    for nam in nams:
        if (
            missing_attribute is not None
            and missing_attribute not in nam.get_empty_required_fields()
        ):
            continue
        if nam.year is not None:
            by_year[nam.year].append(nam)
        else:
            no_year.append(nam)
    print(f"total names: {len(nams)}")
    if not nams:
        return
    print(f"years: {min(by_year)}–{max(by_year)}")
    for year, year_nams in sorted(by_year.items()):
        print(f"{year} ({len(year_nams)})")
        for nam in year_nams:
            print(f"    {nam}")
            if nam.verbatim_citation:
                print(f"        {nam.verbatim_citation}")
            elif nam.page_described:
                print(f"        {nam.page_described}")
    if no_year:
        print(f"no year: {no_year}")


def run_shell() -> None:
    config = Config()
    config.InteractiveShell.confirm_exit = False
    config.TerminalIPythonApp.display_banner = False
    lib_file = os.path.join(os.path.dirname(__file__), "lib.py")
    IPython.start_ipython(argv=[lib_file, "-i"], config=config, user_ns=ns)


if __name__ == "__main__":
    run_shell()
