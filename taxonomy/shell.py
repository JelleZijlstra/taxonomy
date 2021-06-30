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
from collections import defaultdict
import csv
import functools
from itertools import groupby
import logging
import os.path
from pathlib import Path
import peewee
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
    Union,
    cast,
)

import IPython
import requests
import unidecode
from traitlets.config.loader import Config

from . import getinput
from .db import constants, definition, derived_data, helpers, models
from .db.constants import (
    AgeClass,
    Group,
    NomenclatureStatus,
    Rank,
    ArticleKind,
    RequirednessLevel,
    FillDataLevel,
)
from .db.models import (
    Article,
    CitationGroup,
    CitationGroupPattern,
    CitationGroupTag,
    Collection,
    Name,
    Period,
    Person,
    NameTag,
    Taxon,
    TypeTag,
    database,
)
from .db.models.person import PersonLevel
from .db.models.taxon import DEFAULT_LEVEL

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
        "O": Name.getter("corrected_original_name"),
        "reconnect": _reconnect,
        "NameTag": models.NameTag,
        "PersonLevel": PersonLevel,
        "TypeTag": models.TypeTag,
        "Counter": collections.Counter,
        "defaultdict": defaultdict,
        "getinput": getinput,
        "models": models,
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
    def wrapper(*args: Any, **kwargs: Any) -> List[T]:
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
def make_county_regions(
    state: models.Region, name: Optional[str] = None, dry_run: bool = True
) -> None:
    if name is None:
        name = state.name
    data_path = Path(__file__).parent.parent / "data_import/data/counties.csv"
    counties = []
    with data_path.open(encoding="latin1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            county = row["GEO.display-label"]
            if county.endswith(f", {name}"):
                counties.append(county.replace(" city, Virginia", " City, Virginia"))
    print("Creating counties", counties)
    if dry_run:
        return
    for county in counties:
        try:
            models.Region.make(county, constants.RegionKind.county, state)
        except peewee.IntegrityError:
            print(f"{county} already exists")
    _more_precise_by_county(state, counties)
    more_precise(state)


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


@generator_command
def check_period_ranks() -> Iterable[models.Period]:
    for period in Period.select_valid():
        if period.system is None:
            print(f"{period} is missing a system")
            yield period
            continue
        if period.rank is None:
            print(f"{period} is missing a rank")
            yield period
            continue
        if period.rank not in constants.SYSTEM_TO_ALLOWED_RANKS[period.system]:
            print(
                f"{period} is of rank {period.rank}, which is not allowed for {period.system}"
            )
            yield period
        requires_parent = period.requires_parent()
        if period.parent is None:
            if requires_parent is RequirednessLevel.required:
                print(f"{period} must have a parent")
                yield period
        else:
            if requires_parent is RequirednessLevel.disallowed:
                print(f"{period} may not have a parent")
                yield period


@command
def infer_min_max_age(dry_run: bool = True) -> None:
    for period in Period.select_valid().filter(Period.min_age == None):
        children = list(period.children)
        if not children:
            continue
        try:
            min_age = min(child.min_age for child in children)
        except TypeError:
            pass  # one of the min_ages is None
        else:
            if min_age is not None:
                print(f"{period}: set min_age to {min_age}")
                if not dry_run:
                    period.min_age = min_age

    for period in Period.select_valid().filter(Period.max_age == None):
        children = list(period.children)
        if not children:
            continue
        try:
            max_age = max(child.max_age for child in children)
        except TypeError:
            pass  # one of the max_ages is None
        else:
            if max_age is not None:
                print(f"{period}: set max_age to {max_age}")
                if not dry_run:
                    period.max_age = max_age


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
    dry_run: bool = False,
    interactive: bool = False,
    ignore_failure: bool = False,
    aggressive: bool = False,
) -> Iterable[Name]:
    total = successful = 0
    for nam in Name.select_valid().filter(
        Name.original_name != None, Name.corrected_original_name == None
    ):
        if "corrected_original_name" not in nam.get_required_fields():
            continue
        total += 1
        inferred = nam.infer_corrected_original_name(aggressive=aggressive)
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


@generator_command
def check_root_name() -> Iterator[Tuple[Name, str]]:
    """Check that root_names are correct."""

    def make_message(nam: Name, text: str) -> Tuple[Name, str]:
        message = f"{nam}: root name {nam.root_name!r} {text}"
        print(message)
        return (nam, message)

    for nam in Name.select_valid():
        if nam.nomenclature_status.permissive_corrected_original_name():
            continue
        if nam.group in (Group.high, Group.genus, Group.family):
            if not re.match(r"^[A-Z][a-z]+$", nam.root_name):
                yield make_message(nam, "contains unexpected characters")
        elif nam.group is Group.species:
            if not re.match(r"^[a-z]+$", nam.root_name):
                yield make_message(nam, "contains unexpected characters")


@generator_command
def check_corrected_original_name() -> Iterator[Tuple[Name, str]]:
    """Check that corrected_original_names are correct."""

    def make_message(nam: Name, text: str) -> Tuple[Name, str]:
        message = (
            f"{nam}: corrected original name {nam.corrected_original_name!r} {text}"
        )
        print(message)
        return (nam, message)

    for nam in Name.select_valid().filter(Name.corrected_original_name != None):
        if nam.nomenclature_status.permissive_corrected_original_name():
            continue
        inferred = nam.infer_corrected_original_name()
        if inferred is not None and inferred != nam.corrected_original_name:
            yield make_message(
                nam,
                f"inferred name {inferred!r} does not match current name {nam.corrected_original_name!r}",
            )
        if not re.match(r"^[A-Z][a-z ]+$", nam.corrected_original_name):
            yield make_message(nam, "contains unexpected characters")
            continue
        if nam.group in (Group.high, Group.genus):
            if " " in nam.corrected_original_name:
                yield make_message(nam, "contains whitespace")
                continue
            if nam.corrected_original_name != nam.root_name:
                yield make_message(nam, f"does not match root_name {nam.root_name!r}")
                continue
        elif nam.group is Group.family:
            if (
                nam.nomenclature_status
                is NomenclatureStatus.not_based_on_a_generic_name
            ):
                possibilities = {
                    f"{nam.root_name}{suffix}" for suffix in helpers.VALID_SUFFIXES
                }
                if nam.corrected_original_name not in {nam.root_name} | possibilities:
                    yield make_message(
                        nam, f"does not match root_name {nam.root_name!r}"
                    )
                continue
            if not nam.corrected_original_name.endswith(tuple(helpers.VALID_SUFFIXES)):
                yield make_message(nam, "does not end with a valid family-group suffix")
                continue
            if nam.type is not None:
                stem = nam.type.get_stem() or nam.type.stem
                if stem is not None:
                    possibilities = {
                        f"{stem}{suffix}" for suffix in helpers.VALID_SUFFIXES
                    }
                    if stem.endswith("id"):  # allow eliding -id-
                        possibilities |= {
                            f"{stem[:-2]}{suffix}" for suffix in helpers.VALID_SUFFIXES
                        }
                    if nam.type.name_complex is not None:
                        if (
                            nam.type.name_complex.id == 95
                        ):  # ops_masculine: allow -ops- and -op-
                            possibilities |= {
                                f"{stem}s{suffix}" for suffix in helpers.VALID_SUFFIXES
                            }
                    if nam.corrected_original_name not in possibilities:
                        yield make_message(
                            nam, f"does not match expected stem {stem!r}"
                        )
                        continue
        elif nam.group is Group.species:
            parts = nam.corrected_original_name.split(" ")
            if len(parts) not in (2, 3, 4):
                yield make_message(nam, "is not a valid species or subspecies name")
                continue
            if parts[-1] != nam.root_name:
                if nam.species_name_complex is not None:
                    try:
                        forms = list(nam.species_name_complex.get_forms(nam.root_name))
                    except ValueError as e:
                        yield make_message(nam, f"has invalid name complex: {e!r}")
                        continue
                    if parts[-1] in forms:
                        continue
                yield make_message(nam, f"does not match root_name {nam.root_name!r}")
                continue


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
def detect_complexes(allow_ignoring: bool = True) -> None:
    endings = list(models.NameEnding.select())
    for name in Name.select_valid().filter(
        Name.group == Group.genus, Name.name_complex >> None
    ):
        inferred = find_ending(name, endings)
        if inferred is None:
            continue
        stem = inferred.get_stem_from_name(name.root_name)
        if allow_ignoring:
            if name.stem is not None and name.stem != stem:
                print(
                    f"ignoring {inferred} for {name} because {inferred.stem} != {stem}"
                )
                continue
            if name.name_gender is not None and name.name_gender != inferred.gender:
                print(
                    f"ignoring {inferred} for {name} because {inferred.gender} != {name.name_gender}"
                )
                continue
        print(f"Inferred stem and complex for {name}: {stem}, {inferred}")
        name.name_complex = inferred
        name.save()


@command
def detect_species_name_complexes(dry_run: bool = False) -> None:
    endings_tree: SuffixTree[models.SpeciesNameEnding] = SuffixTree()
    full_names: Dict[str, Tuple[models.SpeciesNameComplex, str]] = {}
    for ending in models.SpeciesNameEnding.select():
        for form in ending.name_complex.get_forms(ending.ending):
            if ending.full_name_only:
                full_names[form] = (ending.name_complex, str(ending))
            else:
                endings_tree.add(form, ending)
    for snc in models.SpeciesNameComplex.filter(
        models.SpeciesNameComplex.kind == constants.SpeciesNameKind.adjective
    ):
        for form in snc.get_forms(snc.stem):
            full_names[form] = (snc, "(full name)")
    success = 0
    total = 0
    for name in Name.select_valid().filter(
        Name.group == Group.species, Name.species_name_complex >> None
    ):
        total += 1
        if name.root_name in full_names:
            inferred, reason = full_names[name.root_name]
        else:
            endings = endings_tree.lookup(name.root_name)
            try:
                inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
            except ValueError:
                continue
            reason = str(endings)
        print(f"inferred complex for {name}: {inferred}, using {reason}")
        success += 1
        if not dry_run:
            name.species_name_complex = inferred
            name.save()
    print(f"{success}/{total} inferred")


class SuffixTree(Generic[T]):
    def __init__(self) -> None:
        self.children: Dict[str, SuffixTree[T]] = defaultdict(SuffixTree)
        self.values: List[T] = []

    def add(self, key: str, value: T) -> None:
        self._add(iter(reversed(key)), value)

    def count(self) -> int:
        return len(self.values) + sum(child.count() for child in self.children.values())

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
    species_name_to_names: Dict[str, List[Name]] = defaultdict(list)
    for name in Name.select_valid():
        if name.author_tags:
            for author in name.author_set():
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
    species_name_to_names: Dict[str, List[Name]] = defaultdict(list)
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
        for attr in ("original_name", "root_name", "verbatim_citation"):
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
            if interactive:
                name.display()
                if getinput.yes_no("correct? "):
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
def dup_citation_groups() -> List[Dict[str, List[CitationGroup]]]:
    cgs: Dict[str, List[CitationGroup]] = defaultdict(list)
    for cg in CitationGroup.select_valid():
        if cg.type == constants.ArticleType.REDIRECT:
            continue
        cgs[helpers.simplify_string(cg.name)].append(cg)
    return [cgs]


@_duplicate_finder
def dup_collections() -> List[Dict[str, List[Collection]]]:
    colls: Dict[str, List[Collection]] = defaultdict(list)
    for coll in Collection.select():
        colls[coll.label].append(coll)
    return [colls]


@_duplicate_finder
def dup_taxa() -> List[Dict[str, List[Taxon]]]:
    taxa: Dict[str, List[Taxon]] = defaultdict(list)
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
    names: Dict[str, List[Name]] = defaultdict(list)
    for name in Name.select_valid().filter(Name.group == Group.genus):
        if name.original_citation is not None:
            citation = name.original_citation.name
        else:
            citation = ""
        full_name = (
            f"{name.root_name} {name.taxonomic_authority()}, {name.year}, {citation}"
        )
        names[full_name].append(name)
    return [names]


@_duplicate_finder
def dup_names() -> List[
    Dict[
        Tuple[
            Optional[str], Optional[str], constants.NomenclatureStatus, Optional[str]
        ],
        List[Name],
    ]
]:
    original_year: Dict[
        Tuple[
            Optional[str], Optional[str], constants.NomenclatureStatus, Optional[str]
        ],
        List[Name],
    ] = defaultdict(list)
    for name in Name.select_valid().filter(
        Name.original_name != None, Name.year != None
    ):
        key = (
            name.original_name,
            name.year,
            name.nomenclature_status,
            name.original_citation,
        )
        original_year[key].append(name)
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
        .filter(Name.group == Group.genus, ~(Name.name_gender >> None))
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

    def by_field(
        self,
        field: str,
        min_count: int = 0,
        max_score: float = 101,
        graphical: bool = False,
    ) -> None:
        items = (
            (key, value)
            for key, value in self.data.items()
            if value["total"] > min_count
            and value.get(field, (100, None, None))[0] < max_score
        )

        def sort_key(
            pair: Tuple[Any, Dict[str, Tuple[float, int, int]]]
        ) -> Tuple[Any, ...]:
            _, data = pair
            percentage, count, required_count = data.get(field, (100, 0, 0))
            return (percentage, required_count, data["total"])

        sorted_items = sorted(items, key=sort_key)
        chart_data = []
        for taxon, data in sorted_items:
            if field in data:
                percentage, count, required_count = data[field]
            else:
                percentage, count, required_count = 100, 0, 0
            label = (
                f'{taxon} {percentage:.2f} ({count}/{required_count}) {data["total"]}'
            )
            if graphical:
                chart_data.append((label, percentage / 100))
            else:
                print(label)
        if chart_data:
            getinput.print_scores(chart_data)

    def by_num_missing(self, field: str) -> None:
        items = []
        for key, value in self.data.items():
            _, count, required_count = value.get(field, (100, 0, 0))
            num_missing = required_count - count
            if num_missing > 0:
                items.append((key, num_missing, required_count))
        for taxon, num_missing, total in sorted(
            items, key=lambda pair: (pair[1], pair[2], pair[0])
        ):
            print(f"{taxon}: {num_missing}/{total}")

    def completion_rate(self) -> None:
        fields = {field for data in self.data.values() for field in data} - {
            "total",
            "count",
            "score",
        }
        counts: Dict[str, int] = defaultdict(int)
        for data in self.data.values():
            for field in fields:
                if field not in data or data[field][0] == 100:
                    counts[field] += 1
        total = len(self.data)
        for field, count in sorted(counts.items(), key=lambda p: p[1]):
            print(f"{field}: {count * 100 / total:.2f} ({count}/{total})")

    @classmethod
    def from_taxa(
        cls,
        taxa: Iterable[Taxon],
        age: Optional[AgeClass] = None,
        graphical: bool = False,
        focus_field: Optional[str] = None,
        min_year: Optional[int] = None,
    ) -> "ScoreHolder":
        data = {}
        for taxon in taxa:
            if age is not None and taxon.age > age:
                continue
            getinput.show(f"--- {taxon} ---")
            data[taxon] = taxon.stats(
                age=age, graphical=graphical, focus_field=focus_field, min_year=min_year
            )
        return cls(data)


@command
def get_scores(
    rank: Rank,
    within_taxon: Optional[Taxon] = None,
    age: Optional[AgeClass] = None,
    graphical: bool = False,
    focus_field: Optional[str] = None,
    min_year: Optional[int] = None,
) -> ScoreHolder:
    if within_taxon is not None:
        taxa = within_taxon.children_of_rank(rank)
    else:
        taxa = Taxon.select_valid().filter(Taxon.rank == rank)
    return ScoreHolder.from_taxa(
        taxa, age=age, graphical=graphical, focus_field=focus_field, min_year=min_year
    )


@command
def get_scores_for_period(
    rank: Rank,
    period: Period,
    focus_field: Optional[str] = None,
    graphical: bool = False,
) -> ScoreHolder:
    taxa = set()
    for nam in period.all_type_localities():
        try:
            taxa.add(nam.taxon.parent_of_rank(rank))
        except ValueError:
            continue
    return ScoreHolder.from_taxa(taxa, focus_field=focus_field, graphical=graphical)


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
    attribute: str = "author_tags",
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
def labeled_authorless_names(attribute: str = "author_tags") -> List[LabeledName]:
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
    return count, [line, *["    " + line for line in lines]]


@command
def print_percentages() -> None:
    attributes = [
        "original_name",
        "original_citation",
        "page_described",
        "author_tags",
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

    counts_of_parent: Dict[int, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for name in Name.select_valid():
        try:
            parent_id = parent_of_taxon[name.taxon.id]
        except KeyError:
            print(f"failed to find taxon for {name} (id={name.id})")
            continue
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
                age != {AgeClass.removed.value} AND
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
    "name_gender": (Group.genus,),
    "name_complex": (Group.genus,),
    "species_name_complex": (Group.species,),
    "type": (Group.family, Group.genus),
    "type_locality": (Group.species,),
    "type_specimen": (Group.species,),
    "collection": (Group.species,),
    "genus_type_kind": (Group.genus,),
    "species_type_kind": (Group.species,),
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
                age != {AgeClass.removed.value} AND
                id NOT IN (
                    SELECT parent_id
                    FROM taxon
                    WHERE
                        parent_id IS NOT NULL AND
                        age != {AgeClass.removed.value}
                )
        """
    )


@generator_command
def labeled_childless_taxa() -> Iterable[LabeledName]:
    return [label_name(taxon.base_name) for taxon in childless_taxa()]


@command
def fossilize(
    *taxa: Taxon,
    to_status: AgeClass = AgeClass.fossil,
    from_status: AgeClass = AgeClass.extant,
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
def sorted_field_values(
    field: str,
    model_cls: Type[models.BaseModel] = Name,
    *,
    filters: Iterable[Any] = [],
    exclude_fn: Optional[Callable[[Any], bool]] = None,
) -> None:
    nams = model_cls.bfind(
        getattr(model_cls, field) != None, *filters, quiet=True, sort=False
    )
    key_fn = lambda nam: getattr(nam, field)
    for value, group_iter in groupby(sorted(nams, key=key_fn), key_fn):
        if exclude_fn is not None and exclude_fn(value):
            continue
        group = list(group_iter)
        if len(group) < 3:
            for nam in group:
                print(f"{value!r}: {nam}")
        else:
            print(f"{value!r}: {len(group)} objects: {group[0]}, {group[1]}, ...")


@command
def bad_page_described() -> None:
    # TODO clean this, currently matches a lot of fields
    sorted_field_values(
        "page_described",
        filters=[Name.original_citation != None],
        exclude_fn=models.name.is_valid_page_described,
    )


@command
def field_counts() -> None:
    for field in ("verbatim_citation", "verbatim_type", "stem", "name_gender"):
        print(field, Name.select_valid().filter(getattr(Name, field) != None).count())
    print("Total", Name.select_valid().count())


@command
def clean_column(
    cls: Type[models.BaseModel], column: str, dry_run: bool = True
) -> None:
    for obj in cls.select_valid().filter(getattr(cls, column) != None):
        old_value = getattr(obj, column)
        new_value = helpers.clean_string(old_value)
        if new_value != old_value:
            getinput.print_header(obj)
            getinput.print_diff(
                [old_value, repr(old_value)], [new_value, repr(new_value)]
            )
            if not dry_run:
                setattr(obj, column, new_value)
                obj.save()


@generator_command
def clean_up_gender(dry_run: bool = False) -> Iterable[Name]:
    count = 0
    for nam in Name.bfind(
        Name.name_gender != None, Name.name_complex != None, quiet=True, sort=False
    ):
        if nam.name_gender == nam.name_complex.gender:
            print(
                f"remove gender from {nam} (gender={nam.name_gender!r}, NC={nam.name_complex})"
            )
            if not dry_run:
                nam.name_gender = None
            count += 1
        else:
            print(f"{nam}: gender mismatch {nam.name_gender!r} vs. {nam.name_complex}")
            yield nam
        getinput.flush()
    print(f"{count} cleaned up")


@generator_command
def clean_up_stem(dry_run: bool = False) -> Iterable[Name]:
    count = 0
    for nam in Name.bfind(
        Name.stem != None, Name.name_complex != None, quiet=True, sort=False
    ):
        try:
            inferred = nam.name_complex.get_stem_from_name(nam.corrected_original_name)
        except ValueError as e:
            print(f"{nam}: cannot infer stem from {nam.name_complex} because of {e}")
            yield nam
            continue
        if nam.stem == inferred:
            print(f"remove stem from {nam} (stem={nam.stem!r}, NC={nam.name_complex})")
            if not dry_run:
                nam.stem = None
            count += 1
        else:
            print(
                f"{nam}: stem mismatch {nam.stem!r} vs. {inferred!r} from {nam.name_complex}"
            )
            yield nam
        getinput.flush()
    print(f"{count} cleaned up")


@command
def clean_up_verbatim(dry_run: bool = False, slow: bool = False) -> None:
    def _maybe_clean_verbatim(nam: Name) -> None:
        print(f"{nam}: {nam.type}, {nam.verbatim_type}")
        if not dry_run:
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
            nam.save()

    famgen_type_count = species_type_count = citation_count = citation_group_count = 0
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
    for nam in Name.select_valid().filter(
        Name.citation_group != None, Name.original_citation != None
    ):
        print(f"{nam}: {nam.original_citation.name}, {nam.citation_group}")
        citation_group_count += 1
        if not dry_run:
            nam.citation_group = None
    if famgen_type_count:
        print(f"Family/genera type count: {famgen_type_count}")
    if species_type_count:
        print(f"Species type count: {species_type_count}")
    if citation_count:
        print(f"Citation count: {citation_count}")
    if citation_group_count:
        print(f"Citation group count: {citation_group_count}")


@command
def set_name_complex(suffix: str) -> None:
    nams = Name.bfind(
        Name.root_name.endswith(suffix),
        Name.name_complex == None,
        group=Group.genus,
        quiet=True,
    )
    _set_name_complex_for_names(nams)


@command
def set_name_complex_in_taxon(taxon: Taxon) -> None:
    nams = [
        nam
        for nam in taxon.all_names()
        if nam.group is Group.genus
        and nam.name_complex is None
        and "name_complex" in nam.get_required_fields()
    ]
    _set_name_complex_for_names(nams)


def _set_name_complex_for_names(nams: Sequence[Name]) -> None:
    print(f"{len(nams)} total names")
    for nam in sorted(nams, key=lambda nam: "".join(reversed(nam.root_name.lower()))):
        if "name_complex" not in nam.get_required_fields():
            print(f"Skipping {nam} ({nam.nomenclature_status})")
            continue
        nam.display()
        nam.e.name_complex


@command
def set_citation_group_for_matching_citation(
    dry_run: bool = False, fix: bool = False
) -> None:
    cite_to_nams: Dict[str, List[Name]] = defaultdict(list)
    cite_to_group: Dict[str, Set[CitationGroup]] = defaultdict(set)
    count = 0
    for nam in Name.bfind(Name.verbatim_citation != None, quiet=True):
        cite_to_nams[nam.verbatim_citation].append(nam)
        if nam.citation_group is not None:
            cite_to_group[nam.verbatim_citation].add(nam.citation_group)
    for cite, groups in cite_to_group.items():
        if len(groups) == 1:
            group = next(iter(groups))
            for nam in cite_to_nams[cite]:
                if nam.citation_group is None:
                    print(f"{nam} ({cite}) -> {group}")
                    count += 1
                    if not dry_run:
                        nam.citation_group = group
        else:
            print(f"error: {cite} maps to {groups}")
            if fix:
                getinput.print_header(cite)
                for nam in cite_to_nams[cite]:
                    nam.display()
                    nam.e.citation_group
    print(f"Added {count} citation_groups")


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
    paper: Optional[models.Article] = None,
    level: FillDataLevel = DEFAULT_LEVEL,
    ask_before_opening: bool = True,
) -> None:
    if paper is None:
        paper = models.BaseModel.get_value_for_foreign_class(
            "paper", models.Article, allow_none=False
        )
    assert paper is not None, "paper needs to be specified"
    models.taxon.fill_data_from_paper(
        paper, level=level, ask_before_opening=ask_before_opening
    )


@command
def fill_data_from_author(
    author: Optional[Person] = None,
    level: FillDataLevel = DEFAULT_LEVEL,
    only_fill_cache: bool = False,
    skip_nofile: bool = True,
) -> None:
    if author is None:
        author = Person.getter(None).get_one()
    if author is None:
        return
    arts = author.get_sorted_derived_field("articles")
    models.taxon.fill_data_from_articles(
        sorted(arts, key=lambda art: art.path),
        level=level,
        only_fill_cache=only_fill_cache,
        ask_before_opening=True,
        skip_nofile=skip_nofile,
    )


@command
def fill_data_for_children(
    paper: Optional[models.Article] = None,
    level: FillDataLevel = FillDataLevel.max_level(),
    skip_nofile: bool = False,
    only_fill_cache: bool = False,
) -> None:
    if paper is None:
        paper = models.BaseModel.get_value_for_foreign_class(
            "paper", models.Article, allow_none=False
        )
    assert paper is not None, "paper needs to be specified"
    children = sorted(
        Article.select_valid().filter(Article.parent == paper),
        key=lambda child: (child.numeric_start_page(), child.name),
    )
    models.taxon.fill_data_from_articles(
        children,
        level=level,
        ask_before_opening=True,
        skip_nofile=skip_nofile,
        only_fill_cache=only_fill_cache,
    )
    models.taxon.fill_data_from_paper(
        paper, level=level, only_fill_cache=only_fill_cache
    )


@command
def fill_data_random(
    batch_size: int = 20,
    level: FillDataLevel = DEFAULT_LEVEL,
    ask_before_opening: bool = True,
) -> None:
    count = -1
    done = 0
    while True:
        for count, art in enumerate(
            Article.select_valid().order_by(peewee.fn.Random()).limit(batch_size),
            start=count + 1,
        ):
            if count > 0:
                percentage = (done / count) * 100
            else:
                percentage = 0.0
            getinput.show(f"({count}; {percentage:.03}%) {art.name}")
            result = models.taxon.fill_data_from_paper(
                art, level=level, only_fill_cache=True
            )
            try:
                models.taxon.fill_data_from_paper(
                    art, level=level, ask_before_opening=ask_before_opening
                )
            except getinput.StopException:
                continue
            if result:
                done += 1


@command
def fill_data_reverse_order(
    level: FillDataLevel = FillDataLevel.max_level(),
    ask_before_opening: bool = True,
    max_count: Optional[int] = 500,
) -> None:
    done = 0
    for i, art in enumerate(Article.select_valid().order_by(Article.id.desc())):
        if max_count is not None and i > max_count:
            return
        if i > 0:
            percentage = (done / i) * 100
        else:
            percentage = 0.0
        getinput.show(f"({i}; {percentage:.03}%) {art.name}")
        result = models.taxon.fill_data_from_paper(
            art, level=level, only_fill_cache=True
        )
        try:
            models.taxon.fill_data_from_paper(
                art, level=level, ask_before_opening=ask_before_opening
            )
        except getinput.StopException:
            continue
        if result:
            done += 1


@command
def fill_citation_group_for_type(
    article_type: constants.ArticleType, field: str, dry_run: bool = False
) -> None:
    for art in Article.bfind(
        Article.citation_group == None,
        Article.type == article_type,
        getattr(Article, field) != None,
        getattr(Article, field) != "",
        quiet=True,
    ):
        name = getattr(art, field)
        try:
            cg = CitationGroup.get(name=name)
        except CitationGroup.DoesNotExist:
            print(f"Create: {name}, type={article_type}")
            if dry_run:
                continue
            cg = CitationGroup.create(name=name, type=article_type)
        if not dry_run:
            print(f"set {art} {cg}")
            art.citation_group = cg
            art.save()


@command
def set_region_for_groups(*queries: Any) -> None:
    for cg in CitationGroup.bfind(
        CitationGroup.region == None,
        CitationGroup.type != constants.ArticleType.REDIRECT,
        *queries,
    ):
        cg.display(full=True)
        cg.e.region


@command
def fill_citation_groups(
    book: bool = False,
    interactive: bool = True,
    only_with_hints: bool = False,
    skip_inference: bool = False,
    show_hints: bool = True,
) -> None:
    book_cg = CitationGroup.get(CitationGroup.name == "book")
    if book:
        query = Name.citation_group == book_cg
    else:
        query = Name.citation_group == None
    names = Name.bfind(Name.verbatim_citation != None, query, quiet=True)
    patterns = list(CitationGroupPattern.select_valid())
    print(f"Filling citation group for {len(names)} names")

    if not book and not skip_inference:
        for nam in names:
            citation = helpers.simplify_string(nam.verbatim_citation)
            for pattern in patterns:
                if pattern.pattern in citation:
                    print("===", nam)
                    print(nam.verbatim_citation)
                    print(
                        f"Inferred group with '{pattern.pattern}': {pattern.citation_group}"
                    )
                    nam.citation_group = pattern.citation_group
                    nam.save()

    if not interactive:
        return

    for nam in sorted(
        Name.bfind(Name.verbatim_citation != None, query, quiet=True),
        key=lambda nam: (
            nam.taxonomic_authority(),
            nam.numeric_year(),
            nam.verbatim_citation or "",
        ),
    ):
        nam = nam.reload()
        if book:
            condition = nam.citation_group == book_cg
        else:
            condition = nam.citation_group is None
        if condition:
            getinput.print_header(nam)
            if show_hints:
                count = nam.possible_citation_groups()
                if count == 0 and only_with_hints:
                    continue
                print("===", nam)
            nam.display()
            nam.fill_field("citation_group")


@command
def field_by_year(field: Optional[str] = None) -> None:
    by_year_cited: Dict[str, int] = defaultdict(int)
    by_year_total: Dict[str, int] = defaultdict(int)
    if field is None:
        for nam in Name.bfind(
            Name.original_citation == None, Name.year != None, quiet=True
        ):
            by_year_total[nam.year] += 1
            if nam.verbatim_citation:
                by_year_cited[nam.year] += 1
    else:
        for nam in Name.bfind(Name.year != None, quiet=True):
            required_fields = nam.get_required_fields()
            if field not in required_fields:
                continue
            by_year_total[nam.year] += 1
            if getattr(nam, field) is not None:
                by_year_cited[nam.year] += 1
    data = []
    for year in sorted(by_year_total):
        total = by_year_total[year]
        value = by_year_cited[year] / total
        print(year, total, value)
        if len(year) == 4:
            data.append((f"{year} ({total})", value))
    getinput.print_scores(data)


@command
def type_localities_like(substring: str, full: bool = False) -> None:
    nams = Name.bfind(
        Name._raw_type_tags.contains(substring), Name.type_locality != None, quiet=True
    )
    for nam in sorted(
        nams,
        key=lambda nam: (
            str(nam.type_locality.region),
            str(nam.type_locality),
            nam.taxon.valid_name,
        ),
    ):
        print(f"{nam.type_locality}, {nam.type_locality.region}: {nam}")
        if full:
            nam.display()


def names_with_location_detail_without_type_loc(
    taxon: Optional[Taxon] = None, *, substring: Optional[str] = None
) -> Iterable[Name]:
    if taxon is None:
        nams = Name.select_valid().filter(
            Name.type_tags != None,
            Name.type_locality >> None,
            Name.group == Group.species,
        )
    else:
        nams = [
            nam
            for nam in taxon.all_names()
            if nam.type_tags is not None
            and nam.type_locality is None
            and nam.group == Group.species
        ]
    nams_with_key = []
    for nam in nams:
        tags = [tag for tag in nam.type_tags if isinstance(tag, TypeTag.LocationDetail)]
        if not tags:
            continue
        if "type_locality" not in nam.get_required_fields():
            continue
        if substring is not None:
            if not any(substring in tag.text for tag in tags):
                continue
        nams_with_key.append(([(tag.source.name, tag.text) for tag in tags], nam, tags))
    for _, nam, tags in sorted(nams_with_key):
        nam.display()
        for tag in tags:
            print(tag)
        yield nam


@command
def fill_type_locality_from_location_detail(
    taxon: Optional[Taxon] = None, substring: Optional[str] = None
) -> None:
    for nam in names_with_location_detail_without_type_loc(taxon, substring=substring):
        nam.fill_field("type_locality")


@command
def fix_general_type_localities() -> None:
    region = models.Region.getter(None).get_one()
    if region is None:
        return
    fix_general_type_localities_for_region(region)


def fix_general_type_localities_for_region(region: models.Region) -> None:
    getinput.print_header(region)
    region.display()
    for loc in region.locations:
        if not loc.has_tag(models.location.LocationTag.General):
            continue
        if loc.type_localities.count() == 0:
            continue
        models.taxon.fill_data_for_names(
            list(loc.type_localities), level=FillDataLevel.incomplete_detail
        )
        getinput.print_header(loc)
        loc.display(full=True)
        while True:
            obj = models.Name.getter("corrected_original_name").get_one(
                prompt="corrected_original_name> ",
                callbacks={
                    "d": lambda: loc.display(),
                    "f": lambda: loc.display(full=True),
                },
            )
            if obj is None:
                break
            obj.display()
            obj.edit()

        more_precise_type_localities(loc)

    for child in region.children:
        fix_general_type_localities_for_region(child)


@command
def more_precise_type_localities(
    loc: models.Location, *, substring: Optional[str] = None
) -> None:
    if substring is not None:
        substring = helpers.simplify_string(substring)
    for nam in loc.type_localities:
        if not nam.type_tags:
            continue
        if substring is not None:
            if not any(
                substring in helpers.simplify_string(tag.text)
                for tag in nam.type_tags
                if isinstance(tag, TypeTag.LocationDetail)
            ):
                continue
        getinput.print_header(nam)
        for tag in nam.type_tags:
            if isinstance(tag, TypeTag.LocationDetail):
                print(tag)
        nam.fill_field("type_locality")


@command
def more_precise_periods(
    period: models.Period,
    region: Optional[models.Region] = None,
    include_children: bool = False,
    set_stratigraphy: bool = True,
) -> None:
    for loc in sorted(
        period.all_localities(include_children=include_children),
        key=lambda loc: (loc.max_period.sort_key(), loc.region.name, loc.name),
    ):
        if region is not None and not loc.is_in_region(region):
            continue
        getinput.print_header(loc)
        loc.display(full=True)
        if loc.stratigraphic_unit == period:
            loc.e.stratigraphic_unit
        else:
            loc.e.max_period
            loc.e.min_period
            if set_stratigraphy and loc.stratigraphic_unit is None:
                loc.e.stratigraphic_unit


def _more_precise(
    region: models.Region,
    objects: Iterable[Any],
    field: str,
    filter_func: Callable[[Any], bool] = lambda _: True,
) -> None:
    for obj in objects:
        if not filter_func(obj):
            continue
        getinput.print_header(obj)
        obj.display(full=True)
        obj.fill_field(field)


def _more_precise_by_county(state: models.Region, counties: Sequence[str]) -> None:
    to_replace = f"unty, {state.name}"
    if getinput.yes_no("Run county substring search for type localities?"):
        for loc in state.get_general_localities():
            getinput.print_header(loc.name)
            for county in counties:
                more_precise_type_localities(
                    loc, substring=county.replace(to_replace, "")
                )
    if getinput.yes_no("Run county substring search for localities?"):
        for county in counties:
            _more_precise(
                state,
                state.sorted_locations(),
                "region",
                _make_loc_filterer(county.replace(to_replace, "")),
            )


def _make_loc_filterer(substring: str) -> Callable[[models.Location], bool]:
    def filterer(loc: models.Location) -> bool:
        for nam in loc.type_localities:
            for tag in nam.get_tags(nam.type_tags, TypeTag.LocationDetail):
                if substring in tag.text:
                    return True
        return False

    return filterer


def _more_precise_by_subdivision(region: models.Region) -> None:
    children = sorted(child.name for child in region.children)
    for loc in region.get_general_localities():
        getinput.print_header(loc.name)
        for child in children:
            getinput.print_header(child)
            more_precise_type_localities(loc, substring=re.sub(r" \(.*\)$", "", child))
    for child in children:
        getinput.print_header(child)
        _more_precise(
            region, region.sorted_locations(), "region", _make_loc_filterer(child)
        )


@command
def more_precise(region: models.Region) -> None:
    loc = region.get_location()
    funcs = [
        ("by subdivision", lambda: _more_precise_by_subdivision(region)),
        ("type localities", lambda: more_precise_type_localities(loc)),
        ("collections", lambda: _more_precise(region, region.collections, "location")),
        (
            "citation groups",
            lambda: _more_precise(region, region.citation_groups, "region"),
        ),
        (
            "localities",
            lambda: _more_precise(region, region.sorted_locations(), "region"),
        ),
    ]
    for label, func in funcs:
        if getinput.yes_no(f"Run {label}? "):
            func()


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
def most_common_unchecked_names(
    num_to_display: int = 10,
    max_level: Optional[PersonLevel] = PersonLevel.has_given_name,
    max_num_names: Optional[int] = None,
) -> Counter[str]:
    counter: Counter[str] = Counter()
    name_counter: Counter[str] = Counter()
    for person in Person.select_valid():
        if max_level is None or person.get_level() <= max_level:
            num_refs = sum(person.num_references().values())
            counter[person.family_name] += num_refs
            if max_num_names is not None:
                name_counter[person.family_name] += 1
    if max_num_names is not None:
        counter = Counter(
            {
                family_name: count
                for family_name, count in counter.items()
                if name_counter[family_name] <= max_num_names
            }
        )
    for value, count in counter.most_common(num_to_display):
        print(value, count)
    return counter


@command
def biggest_names(
    num_to_display: int = 10,
    max_level: Optional[PersonLevel] = PersonLevel.has_given_name,
    family_name: Optional[str] = None,
) -> Counter[Person]:
    counter: Counter[Person] = Counter()
    query = Person.select_valid()
    if family_name is not None:
        query = query.filter(Person.family_name == family_name)
    for person in query:
        if max_level is None or person.get_level() <= max_level:
            counter[person] = sum(person.num_references().values())
    for value, count in counter.most_common(num_to_display):
        print(value, count)
    return counter


@command
def rio_taxon() -> None:
    taxon = Taxon.getter(None).get_one()
    if taxon is None:
        return
    nams = taxon.all_names()
    people = [person for nam in nams for person in nam.get_authors()]
    for person in sorted(people, key=lambda p: p.sort_key()):
        if person.get_level() is PersonLevel.family_name_only:
            person.reassign_initials_only()


@command
def reassign_references(
    family_name: Optional[str] = None,
    substring: bool = True,
    max_level: Optional[PersonLevel] = PersonLevel.has_given_name,
) -> None:
    if family_name is None:
        family_name = Person.getter("family_name").get_one_key()
    if not family_name:
        return
    query = Person.select_valid()
    if substring:
        query = query.filter(Person.family_name.contains(family_name))
    else:
        query = query.filter(Person.family_name == family_name)
    persons = sorted(query, key=lambda person: person.sort_key())
    for person in persons:
        print(f"- {person!r} ({person.get_level().name})", flush=True)
    for person in persons:
        if max_level is None or person.get_level() <= max_level:
            person.maybe_reassign_references()


PersonParams = Optional[Dict[str, Optional[str]]]

_UPPER = r"[A-ZŠİŞÖČÓА-Я]"
_LOWER = r"[a-záéíãěäóèìğñüúöšýčç'а-я]{2,}"
_SIMPLE_FAMILY = rf"((Ma?c|De|La|Le|D'|d'|Du|O')?{_UPPER}{_LOWER})"
_FAMILY = (
    rf"((von |de |de la |De la |du |del |de La )?{_SIMPLE_FAMILY}(-{_SIMPLE_FAMILY})?)"
)
_SUFFIX_GROUP = r"(?P<suffix>,? (jr|Jr|Sr)\.?)?"
_INITIALS_GROUP = rf"(?P<initials>\[?((Mc|de )?{_UPPER}h?\.-?\s?)+\]?( {_FAMILY})*)"


def _initials(name: str) -> PersonParams:
    if "Expedition" in name:
        return None
    match = re.match(
        rf"^{_INITIALS_GROUP}\s*(?P<family>{_FAMILY}){_SUFFIX_GROUP}$", name
    )
    if match:
        return {
            "family_name": match.group("family"),
            "initials": re.sub(
                rf"\.(?={_FAMILY})",
                ". ",
                re.sub(r"\s", "", match.group("initials").strip("[]")),
            ),
            "suffix": _parse_suffix(match.group("suffix")),
        }
    return None


def _parse_suffix(suffix: Optional[str]) -> Optional[str]:
    if not suffix:
        return None
    suffix = suffix.strip(",. ")
    return suffix + "."


def _full_name(name: str) -> PersonParams:
    if "Expedition" in name or "Miss " in name:
        return None
    match = re.match(
        rf"^(?P<given_names>{_FAMILY}( {_UPPER}\.)*) (?P<family_name>{_FAMILY}){_SUFFIX_GROUP}$",
        name,
    )
    if match:
        return {
            "family_name": match.group("family_name"),
            "given_names": match.group("given_names"),
            "suffix": _parse_suffix(match.group("suffix")),
        }
    return None


def _last_name_only(name: str) -> PersonParams:
    match = re.match(rf"^{_FAMILY}$", name)
    if match:
        return {"family_name": name}
    return None


def _cyrillic(name: str) -> PersonParams:
    match = re.match(
        r"^(?P<family_name>[А-Я][а-я]+) (?P<initials>([А-Я]\.? ?)+)$", name
    )
    if match:
        return {
            "family_name": match.group("family_name"),
            "initials": "".join(
                c + "."
                for c in match.group("initials").replace(" ", "").replace(".", "")
            ),
        }
    return None


def _van(name: str) -> PersonParams:
    match = re.match(
        rf"^{_INITIALS_GROUP}\s+van\s+(?P<family_name>{_FAMILY}){_SUFFIX_GROUP}$", name
    )
    if match:
        return {
            "family_name": match.group("family_name"),
            "initials": re.sub(
                rf"\.(?={_FAMILY})",
                ". ",
                re.sub(r"\s", "", match.group("initials").strip("[]")),
            ),
            "suffix": _parse_suffix(match.group("suffix")),
            "tussenvoegsel": "van",
        }
    return None


MATCHERS = [_initials, _full_name, _last_name_only, _cyrillic, _van]


@command
def most_common(model_cls: Type[models.BaseModel], field: str) -> Counter[Any]:
    objects = model_cls.select_valid().filter(getattr(model_cls, field) != None)
    counter: Counter[Any] = Counter()
    for obj in objects:
        counter[getattr(obj, field)] += 1
    for value, count in counter.most_common(10):
        print(value, count)
    return counter


@command
def most_common_mapped(
    model_cls: Type[models.BaseModel],
    field: str,
    mapper: Callable[[Any], Any],
    num_to_display: int = 10,
) -> Counter[Any]:
    objects = model_cls.select_valid().filter(getattr(model_cls, field) != None)
    counter: Counter[Any] = Counter()
    for obj in objects:
        value = getattr(obj, field)
        counter[mapper(value)] += 1
    for value, count in counter.most_common(num_to_display):
        print(value, count)
    return counter


@command
def most_common_citation_groups_after(year: int) -> Dict[CitationGroup, int]:
    nams = Name.bfind(Name.citation_group != None, Name.year > year, quiet=True)
    return Counter(nam.citation_group for nam in nams)


@command
def fill_data_from_folder(
    folder: Optional[str] = None,
    level: FillDataLevel = DEFAULT_LEVEL,
    only_fill_cache: bool = False,
    ask_before_opening: bool = True,
    skip_nofile: bool = True,
) -> None:
    if folder is None:
        folder = Article.getter("path").get_one_key() or ""
    arts = Article.bfind(Article.path.startswith(folder), quiet=True)
    models.taxon.fill_data_from_articles(
        sorted(arts, key=lambda art: art.path),
        level=level,
        only_fill_cache=only_fill_cache,
        ask_before_opening=ask_before_opening,
        skip_nofile=skip_nofile,
    )


@command
def fill_data_from_citation_group(
    cg: Optional[CitationGroup] = None,
    level: FillDataLevel = DEFAULT_LEVEL,
    only_fill_cache: bool = False,
    ask_before_opening: bool = True,
    skip_nofile: bool = True,
) -> None:
    if cg is None:
        cg = CitationGroup.getter("name").get_one()
    if cg is None:
        return

    def sort_key(art: Article) -> Tuple[int, int, int]:
        year = art.numeric_year()
        try:
            volume = int(art.volume)
        except (TypeError, ValueError):
            volume = 0
        start_page = art.numeric_start_page()
        return (year, volume, start_page)

    arts = sorted(cg.get_articles(), key=sort_key)
    models.taxon.fill_data_from_articles(
        arts,
        level=level,
        only_fill_cache=only_fill_cache,
        ask_before_opening=ask_before_opening,
        skip_nofile=skip_nofile,
    )


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
        target = nam.get_tag_target(NameTag.JustifiedEmendationOf)
        if target is None:
            yield nam, "justified_emendation without a JustifiedEmendationOf tag"
            continue
        ios_target = target.get_tag_target(NameTag.IncorrectOriginalSpellingOf)
        if ios_target is None:
            yield target, "missing IncorrectOriginalSpellingOf tag"
        elif (
            target.nomenclature_status is NomenclatureStatus.incorrect_original_spelling
        ):
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
                nam.add_static_comment(constants.CommentKind.automatic_change, comment)
                nam.nomenclature_status = status  # type: ignore
                nam.save()

    names_by_tag: Dict[Type[Any], Set[Name]] = defaultdict(set)
    for nam in Name.select_valid().filter(Name.tags != None):
        try:
            tags = nam.tags
        except Exception:
            yield nam, "could not deserialize tags"
            continue
        for tag in tags:
            names_by_tag[type(tag)].add(nam)
            if isinstance(tag, NameTag.PreoccupiedBy):
                maybe_adjust_status(nam, NomenclatureStatus.preoccupied, tag)
                senior_name = tag.name
                if nam.group != senior_name.group:
                    print(
                        f"{nam} is of a different group than supposed senior name {senior_name}"
                    )
                    yield nam, "homonym of different group"
                if (
                    senior_name.nomenclature_status
                    is NomenclatureStatus.subsequent_usage
                ):
                    for senior_name_tag in senior_name.get_tags(
                        senior_name.tags, NameTag.SubsequentUsageOf
                    ):
                        senior_name = senior_name_tag.name
                if nam.effective_year() < senior_name.effective_year():
                    print(f"{nam} predates supposed senior name {senior_name}")
                    yield nam, "antedates homonym"
                # TODO apply this check to species too by handling gender endings correctly.
                if nam.group != Group.species:
                    if nam.root_name != tag.name.root_name:
                        print(
                            f"{nam} has a different root name than supposed senior name {senior_name}"
                        )
                        yield nam, "differently-named homonym"
            elif isinstance(
                tag,
                (
                    NameTag.UnjustifiedEmendationOf,
                    NameTag.IncorrectSubsequentSpellingOf,
                    NameTag.VariantOf,
                    NameTag.NomenNovumFor,
                    NameTag.JustifiedEmendationOf,
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
            elif isinstance(tag, NameTag.PartiallySuppressedBy):
                maybe_adjust_status(nam, NomenclatureStatus.partially_suppressed, tag)
            elif isinstance(tag, NameTag.FullySuppressedBy):
                maybe_adjust_status(nam, NomenclatureStatus.fully_suppressed, tag)
            elif isinstance(tag, NameTag.Conserved):
                if nam.nomenclature_status not in (
                    NomenclatureStatus.available,
                    NomenclatureStatus.as_emended,
                    NomenclatureStatus.nomen_novum,
                ):
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


def check_type_tags_for_name(
    nam: Name, dry_run: bool = False
) -> Iterable[Tuple[Name, str]]:
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
                        constants.TypeSpeciesDesignation.designated_by_the_commission  # type: ignore
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
                lat = helpers.standardize_coordinates(tag.latitude, is_latitude=True)
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
        print(f"changing tags for {nam}")
        print(original_tags)
        print(tags)
        nam.type_tags = tags  # type: ignore


@generator_command
def check_type_tags(
    dry_run: bool = False, require_type_designations: bool = False
) -> Iterable[Tuple[Name, str]]:
    for nam in Name.select_valid().filter(Name.type_tags != None):
        yield from check_type_tags_for_name(nam, dry_run)
    getinput.flush()
    if not require_type_designations:
        return
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
    "Belayeva": helpers.romanize_russian("Беляева"),
    "Beliaeva": helpers.romanize_russian("Беляева"),
    "Beliajeva": helpers.romanize_russian("Беляева"),
    "Beljaeva": helpers.romanize_russian("Беляева"),
    "Belyayeva": helpers.romanize_russian("Беляева"),
    "Belyaeva": helpers.romanize_russian("Беляева"),
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
    "Christol": "de Christol",
    "Crawford Cabral": "Crawford-Cabral",
    "Czersky": helpers.romanize_russian("Черский"),
    "De Blainville": "de Blainville",
    "De Beaux": "de Beaux",
    "De Christol": "de Christol",
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
    "Lychev": helpers.romanize_russian("Лычев"),
    "Lytschev": helpers.romanize_russian("Лычев"),
    "Lytshev": helpers.romanize_russian("Лычев"),
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
    "Serres": "de Serres",
    "Severtsow": helpers.romanize_russian("Северцов"),
    "Severtzov": helpers.romanize_russian("Северцов"),
    "Severtzow": helpers.romanize_russian("Северцов"),
    "Souef": "Le Souef",
    "St Leger": "St. Leger",
    "Teilhard": "Teilhard de Chardin",
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
    "von Huene": "Huene",
    "Von Huene": "Huene",
    "Von Meyer": "von Meyer",
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
    "Wood",
    "Wilson",
}


@command
def apply_author_synonyms(dry_run: bool = False) -> None:
    for bad, good in AUTHOR_SYNONYMS.items():
        if bad == good:
            continue
        for person in Person.select_valid().filter(Person.family_name == bad):
            if person.total_references() > 0:
                print(f"=== {person} ({bad} -> {good}) ===")
                person.display()


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


@command
def run_maintenance(skip_slow: bool = True) -> Dict[Any, Any]:
    """Runs maintenance checks that are expected to pass for the entire database."""
    fns: List[Callable[[], Any]] = [
        clean_up_verbatim,
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
        detect_corrected_original_names,
        dup_collections,
        # dup_names,
        # dup_genus,
        # dup_taxa,
        bad_stratigraphy,
        set_citation_group_for_matching_citation,
        enforce_must_have,
        fix_citation_group_redirects,
        recent_names_without_verbatim,
        enforce_must_have_series,
        check_period_ranks,
        clean_up_stem,
        clean_up_gender,
        check_corrected_original_name,
        Person.autodelete,
        Person.find_duplicates,
        Person.resolve_redirects,
        Person.lint_all,
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
        resolve_redirects,
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


def names_of_author(author: str, include_partial: bool) -> List[Name]:
    persons = Person.select_valid().filter(
        Person.family_name.contains(author)
        if include_partial
        else Person.family_name == author
    )
    return [
        nam for person in persons for nam in person.get_sorted_derived_field("names")
    ]


@command
def names_of_authority(author: str, year: int, edit: bool = False) -> List[Name]:
    nams = names_of_author(author, include_partial=False)
    nams = [nam for nam in nams if nam.year == year]

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

    nams = sorted(nams, key=sort_key)
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


def fgsyn(off: Optional[Name] = None) -> Name:
    """Adds a family-group synonym."""
    if off is not None:
        taxon = off.taxon
    else:
        taxon = Taxon.get_one_by("valid_name", prompt="taxon> ", allow_empty=False)
    root_name = Name.getter("corrected_original_name").get_one_key(
        "name> ", allow_empty=False
    )
    source = Name.get_value_for_foreign_class(
        "source", models.Article, allow_none=False
    )
    kwargs = {}
    if off is not None:
        kwargs["type"] = off.type
    return taxon.syn_from_paper(root_name, source, original_name=root_name, **kwargs)


@command
def author_report(
    author: str, partial: bool = False, missing_attribute: Optional[str] = None
) -> List[Name]:
    nams = names_of_author(author, include_partial=partial)
    if not missing_attribute:
        nams = [nam for nam in nams if nam.original_citation is None]

    by_year: Dict[str, List[Name]] = defaultdict(list)
    no_year: List[Name] = []
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
    print(f"total names: {sum(len(v) for _, v in by_year.items()) + len(no_year)}")
    if not by_year and not no_year:
        return []
    print(f"years: {min(by_year)}–{max(by_year)}")
    out: List[Name] = []
    for year, year_nams in sorted(by_year.items()):
        out += year_nams
        print(f"{year} ({len(year_nams)})")
        for nam in year_nams:
            print(f"    {nam}")
            if nam.verbatim_citation:
                print(f"        {nam.verbatim_citation}")
            elif nam.page_described:
                print(f"        {nam.page_described}")
    if no_year:
        print(f"no year: {no_year}")
        out += no_year
    return out


@generator_command
def enforce_must_have(fix: bool = True) -> Iterator[Name]:
    for cg in sorted(_must_have_citation_groups(), key=lambda cg: cg.archive or ""):
        after_tag = cg.get_tag(CitationGroupTag.MustHaveAfter)
        found_any = False
        for nam in cg.get_names():
            if nam.original_citation is not None:
                continue
            if after_tag is not None and nam.numeric_year() < int(after_tag.year):
                continue
            if not found_any:
                getinput.print_header(nam.citation_group.name)
            print(f"{nam} is in {cg}, but has no original_citation")
            nam.display()
            found_any = True
            yield nam
        if found_any:
            find_potential_citations_for_group(cg, fix=fix)


@generator_command
def enforce_must_have_series(fix: bool = True) -> Iterator[Article]:
    cgs = [
        cg
        for cg in CitationGroup.select_valid()
        if cg.get_tag(CitationGroupTag.MustHaveSeries)
    ]
    for cg in cgs:
        getinput.print_header(cg)
        for art in cg.article_set:
            if not art.series:
                art.display()
                print(f"{art} is in {cg}, but is missing a series")
                yield art
                if fix:
                    art.e.series


@generator_command
def archive_for_must_have(fix: bool = True) -> Iterator[CitationGroup]:
    for cg in _must_have_citation_groups():
        if cg.archive is None:
            getinput.print_header(cg)
            cg.display()
            cg.e.archive
            yield cg


def _must_have_citation_groups() -> List[CitationGroup]:
    return [
        cg
        for cg in CitationGroup.select_valid()
        if cg.has_tag(CitationGroupTag.MustHave)
        or cg.get_tag(CitationGroupTag.MustHaveAfter)
    ]


@command
def find_potential_citations(
    fix: bool = False, region: Optional[models.Region] = None, aggressive: bool = False
) -> int:
    if region is None:
        cgs = CitationGroup.select_valid()
    else:
        cgs = region.all_citation_groups()
    count = sum(
        find_potential_citations_for_group(cg, fix=fix, aggressive=aggressive) or 0
        for cg in cgs
        if not cg.has_tag(CitationGroupTag.IgnorePotentialCitations)
    )
    return count


def _author_names(obj: Union[Article, Name]) -> Set[str]:
    return {person.family_name for person in obj.get_authors()}


@command
def find_potential_citations_for_group(
    cg: CitationGroup, fix: bool = False, aggressive: bool = False
) -> int:
    if not cg.get_names():
        return 0
    potential_arts = Article.bfind(
        Article.kind != constants.ArticleKind.no_copy, citation_group=cg, quiet=True
    )
    if not potential_arts:
        return 0

    def is_possible_match(art: Article, nam: Name, page: int) -> bool:
        if (
            nam.year != art.year
            or art.kind is ArticleKind.no_copy
            or art.has_tag(models.article.ArticleTag.NonOriginal)
        ):
            return False
        if not art.is_page_in_range(page):
            return False
        if aggressive:
            return _author_names(nam) <= _author_names(art)
        else:
            return nam.author_set() <= art.author_set()

    count = 0
    for nam in cg.get_names():
        if nam.original_citation is not None:
            continue
        page = nam.extract_page_described()
        if not page:
            continue
        candidates = [
            art for art in potential_arts if is_possible_match(art, nam, page)
        ]
        if candidates:
            if count == 0:
                print(f"Trying {cg}...", flush=True)
            getinput.print_header(nam)
            count += 1
            nam.display()
            for candidate in candidates:
                print(repr(candidate))
            if fix:
                for candidate in candidates:
                    candidate.openf()
                    getinput.add_to_clipboard(candidate.name)
                nam.fill_required_fields()
    if count:
        print(f"{cg} had {count} potential citations", flush=True)
    return count


@generator_command
def recent_names_without_verbatim(
    threshold: int = 1990, fix: bool = False
) -> Iterator[Name]:
    return fill_verbatim_citation_for_names(Name.year >= str(threshold), fix=fix)


def fill_verbatim_citation_for_names(
    *queries: Any, fix: bool = False
) -> Iterator[Name]:
    for nam in sorted(
        Name.bfind(
            *queries, Name.original_citation == None, Name.verbatim_citation == None
        ),
        key=lambda nam: (
            -nam.numeric_year(),
            nam.taxonomic_authority(),
            nam.corrected_original_name or "",
            nam.root_name,
        ),
    ):
        nam = nam.reload()
        if "verbatim_citation" not in nam.get_empty_required_fields():
            continue
        if fix:
            getinput.print_header(nam)
            nam.possible_citation_groups()
        nam.display()
        if fix:
            nam.e.verbatim_citation
        yield nam


@command
def citation_groups_with_recent_names(threshold: int = 1923) -> None:
    for cg in CitationGroup.select_valid().filter(
        CitationGroup.type == constants.ArticleType.JOURNAL
    ):
        names = [nam for nam in cg.get_names() if nam.numeric_year() > threshold]
        if not names:
            continue
        arts = [
            art
            for art in Article.bfind(citation_group=cg, quiet=True)
            if art.numeric_year() > threshold
        ]
        if not arts:
            continue
        if not any(art.doi or art.url for art in arts):
            continue
        print(f"=== {cg} has {len(names)} names and {len(arts)} articles ===")
        for nam in sorted(names, key=lambda nam: nam.sort_key()):
            nam.display()
        getinput.flush()


@command
def fix_citation_group_redirects() -> None:
    for cg in CitationGroup.select_valid().filter(
        CitationGroup.type == constants.ArticleType.REDIRECT
    ):
        for nam in cg.get_names():
            print(f"update {nam} -> {cg.target}")
            nam.citation_group = cg.target
        for art in cg.get_articles():
            print(f"update {art} -> {cg.target}")
            art.citation_group = cg.target


@command
def find_dois() -> None:
    arts = Article.bfind(
        Article.doi != None, Article.type == constants.ArticleType.JOURNAL, quiet=True
    )
    cgs = {art.citation_group for art in arts}
    doiless = {
        art
        for cg in cgs
        if cg is not None
        for art in cg.get_articles().filter(Article.doi == None)
    }
    for art in doiless:
        art.finddoi()


@command
def reset_db() -> None:
    database = models.base.database
    database.close()
    database.connect()


@command
def print_parent() -> Optional[Taxon]:
    taxon = Taxon.getter("valid_name").get_one()
    if taxon:
        return taxon.parent
    return None


@command
def edit_names_interactive(
    art: Optional[Article] = None, field: str = "corrected_original_name"
) -> None:
    if art is None:
        art = Article.getter("name").get_one()
        if art is None:
            return
    art.display_names()
    models.taxon.edit_names_interactive(art, field=field)
    fill_data_from_paper(art)


@command
def compute_derived_fields() -> None:
    for cls in models.BaseModel.__subclasses__():
        print(f"=== Computing for {cls} ===")
        cls.compute_all_derived_fields()
    write_derived_data()


@command
def write_derived_data() -> None:
    derived_data.write_derived_data(derived_data.load_derived_data())
    derived_data.write_cached_data(derived_data.load_cached_data())


@command
def warm_all_caches() -> None:
    for model in models.BaseModel.__subclasses__():
        if hasattr(model, "label_field"):
            print(f"{model}: warming None getter")
            model.getter(None)._warm_cache()
        for name, field in model._meta.fields.items():
            if isinstance(field, peewee.CharField):
                print(f"{model}: warming {name} ({field})")
                model.getter(name)._warm_cache()
    fill_data_from_folder("", only_fill_cache=True)


@command
def show_queries(on: bool) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("peewee")
    if on:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def run_shell() -> None:
    config = Config()
    config.InteractiveShell.confirm_exit = False
    config.TerminalIPythonApp.display_banner = False
    lib_file = os.path.join(os.path.dirname(__file__), "lib.py")
    IPython.start_ipython(argv=[lib_file, "-i"], config=config, user_ns=ns)


if __name__ == "__main__":
    run_shell()
