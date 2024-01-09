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
import csv
import datetime
import functools
import gc
import logging
import operator
import os
import os.path
import re
import shutil
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping, Sequence
from itertools import groupby
from pathlib import Path
from typing import Any, Generic, NamedTuple, TypeVar, cast

import IPython
import peewee
import requests
import unidecode
from traitlets.config.loader import Config

from . import getinput
from .db import constants, definition, derived_data, export, helpers, models
from .db.constants import (
    AgeClass,
    ArticleKind,
    ArticleType,
    FillDataLevel,
    Group,
    NomenclatureStatus,
    Rank,
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
    Taxon,
    TypeTag,
    database,
)
from .db.models.base import Linter, ModelT
from .db.models.person import PersonLevel

T = TypeVar("T")

gc.disable()

COMMAND_SETS = [
    models.fill_data.CS,
    models.article.check.CS,
    export.CS,
    models.article.add_data.CS,
]


def _reconnect() -> None:
    database.close()
    database.connect()


ns = {
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
    "os": os,
}
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


def generator_command(fn: Callable[..., Iterable[T]]) -> Callable[..., list[T]]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> list[T]:
        try:
            return list(fn(*args, **kwargs))
        except getinput.StopException:
            return []

    ns[fn.__name__] = wrapper
    return wrapper


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
_MissingDataProducer = Callable[..., Iterable[tuple[Name, str]]]


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
def add_original_names() -> Iterable[tuple[Name, str]]:
    for name in (
        Name.select_valid()
        .filter(Name.original_citation != None, Name.original_name >> None)
        .order_by(Name.original_name)
    ):
        message = (
            "Name {} is missing an original name, but has original citation {{{}}}:{}"
            .format(
                name.description(), name.original_citation.name, name.page_described
            )
        )
        yield name, message


@command
@_add_missing_data
def add_page_described() -> Iterable[tuple[Name, str]]:
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
    state: models.Region, name: str | None = None, dry_run: bool = True
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
            f"Name {name} is missing type, but has original citation"
            f" {name.original_citation.name}"
        )
        models.fill_data.fill_data_from_paper(name.original_citation)


@command
def detect_corrected_original_names(aggressive: bool = False) -> None:
    query = Name.select_valid().filter(
        Name.original_name != None, Name.corrected_original_name == None
    )
    linter = functools.partial(
        models.name_lint.autoset_corrected_original_name, aggressive=aggressive
    )
    run_linter_and_fix(Name, linter, query)


@command
def detect_original_rank() -> None:
    query = Name.select_valid().filter(
        Name.corrected_original_name != None, Name.original_rank == None
    )
    run_linter_and_fix(Name, models.name_lint.autoset_original_rank, query)


@command
def detect_types(max_count: int | None = None, verbose: bool = False) -> None:
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
def detect_types_from_root_names(max_count: int | None = None) -> None:
    """Detects types for family-group names on the basis of the root_name."""

    def detect_from_root_name(name: Name, root_name: str) -> bool:
        candidates = Name.select_valid().filter(Name.group == Group.genus)
        candidates = list(filter(lambda c: c.taxon.is_child_of(name.taxon), candidates))
        if len(candidates) == 1:
            print(f"Detected type for name {name}: {candidates[0]}")
            name.type = candidates[0]
            return True
        else:
            if candidates:
                print(
                    f"found multiple candidates for {name} using root {root_name}:"
                    f" {candidates}"
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
def endswith(end: str) -> list[Name]:
    return list(
        Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name % ("%%%s" % end)
        )
    )


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
        print(f"Inferred stem and complex for {name}: {stem}, {inferred}")
        name.name_complex = inferred


@command
def detect_species_name_complexes(dry_run: bool = False) -> None:
    endings_tree: SuffixTree[models.SpeciesNameEnding] = SuffixTree()
    full_names: dict[str, tuple[models.SpeciesNameComplex, str]] = {}
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
        if not name.nomenclature_status.requires_name_complex():
            continue
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
    print(f"{success}/{total} inferred")


class SuffixTree(Generic[T]):
    def __init__(self) -> None:
        self.children: dict[str, SuffixTree[T]] = defaultdict(SuffixTree)
        self.values: list[T] = []

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
def find_patronyms(dry_run: bool = True, min_length: int = 4) -> dict[str, int]:
    """Finds names based on patronyms of authors in the database."""
    authors = set()
    species_name_to_names: dict[str, list[Name]] = defaultdict(list)
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
def find_first_declension_adjectives(dry_run: bool = True) -> dict[str, int]:
    adjectives = get_pages_in_wiki_category(
        "en.wiktionary.org", "Latin first and second declension adjectives"
    )
    species_name_to_names: dict[str, list[Name]] = defaultdict(list)
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
) -> models.NameComplex | None:
    for ending in endings:
        if name.root_name.endswith(ending.ending):
            return ending.name_complex
    return None


@command
def generate_word_list() -> set[str]:
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


def _duplicate_finder(
    fn: Callable[[], Iterable[Mapping[Any, Sequence[ModelT]]]]
) -> Callable[[], list[Sequence[ModelT]] | None]:
    @generator_command
    @functools.wraps(fn)
    def wrapper(interactive: bool = False) -> Iterable[Sequence[ModelT]]:
        for dups_dict in fn():
            for key, entries_list in dups_dict.items():
                if len(entries_list) > 1:
                    print("Duplicate:", key, len(entries_list))
                    yield entries_list
                    if interactive:
                        getinput.print_header(key)
                        for entry in entries_list:
                            print(
                                "-----------------------------"
                                f" {entry.call_sign}#{entry.id}"
                            )
                            entry.display(full=True)
                            entry.add_to_history(None)
                            entry.add_to_history(entry.label_field)
                        name_to_art = {
                            getattr(art, art.label_field): art for art in entries_list
                        }
                        options = ["f", "d", *name_to_art]

                        while True:
                            choice = getinput.get_with_completion(
                                options, history_key=key, disallow_other=True
                            )
                            if not choice:
                                break
                            if choice == "f":
                                for art in entries_list:
                                    getinput.print_header(art)
                                    art.full_data()
                            elif choice == "d":
                                for art in entries_list:
                                    print(repr(art))
                            elif choice in name_to_art:
                                name_to_art[choice].edit()

    return wrapper


@_duplicate_finder
def dup_citation_groups() -> list[dict[object, list[CitationGroup]]]:
    cgs: dict[object, list[CitationGroup]] = defaultdict(list)
    for cg in CitationGroup.select_valid():
        if cg.type == constants.ArticleType.REDIRECT:
            continue
        issns = {tag.text for tag in cg.get_tags(cg.tags, CitationGroupTag.ISSN)} | {
            tag.text for tag in cg.get_tags(cg.tags, CitationGroupTag.ISSNOnline)
        }
        key = (helpers.simplify_string(cg.name), tuple(sorted(issns)))
        cgs[key].append(cg)
    return [cgs]


@_duplicate_finder
def dup_collections() -> list[dict[str, list[Collection]]]:
    colls: dict[str, list[Collection]] = defaultdict(list)
    for coll in Collection.select():
        colls[coll.label].append(coll)
    return [colls]


@_duplicate_finder
def dup_taxa() -> list[dict[str, list[Taxon]]]:
    taxa: dict[str, list[Taxon]] = defaultdict(list)
    for txn in Taxon.select_valid():
        if txn.rank == Rank.subgenus and taxa[txn.valid_name]:
            continue
        taxa[txn.valid_name].append(txn)
    return [{
        label: [
            t
            for t in ts
            if t.base_name.nomenclature_status != NomenclatureStatus.preoccupied
        ]
        for label, ts in taxa.items()
        if len(ts) > 1
    }]


@_duplicate_finder
def dup_genus() -> list[dict[str, list[Name]]]:
    names: dict[str, list[Name]] = defaultdict(list)
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
def dup_names() -> (
    list[
        dict[
            tuple[str | None, str | None, constants.NomenclatureStatus, str | None],
            list[Name],
        ]
    ]
):
    original_year: dict[
        tuple[str | None, str | None, constants.NomenclatureStatus, str | None],
        list[Name],
    ] = defaultdict(list)
    for name in getinput.print_every_n(
        Name.select_valid().filter(Name.original_name != None, Name.year != None),
        label="names",
    ):
        key = (
            name.corrected_original_name,
            name.year,
            name.nomenclature_status,
            name.original_citation,
        )
        original_year[key].append(name)
    return [original_year]


@command
def dup_journal_articles() -> None:
    dup_articles(
        interactive=True,
        query=Article.select_valid().filter(Article.type == ArticleType.JOURNAL),
        key=lambda art: (
            art.citation_group,
            art.volume,
            art.issue,
            art.start_page,
            art.end_page,
        ),
    )


@command
def dup_articles(
    key: Callable[[Article], Hashable] = lambda art: art.doi,
    interactive: bool = False,
    query: Iterable[Article] | None = None,
) -> None:
    if query is None:
        query = Article.select_valid()
    by_key = defaultdict(list)
    for art in getinput.print_every_n(query, label="articles"):
        if art.get_redirect_target() is not None:
            continue
        if art.type is ArticleType.SUPPLEMENT and art.parent is not None:
            continue
        val = key(art)
        if val is None:
            continue
        by_key[val].append(art)
    dup_groups = {key: arts for key, arts in by_key.items() if len(arts) > 1}
    print(f"Found {len(dup_groups)} groups")
    for key_val, arts in dup_groups.items():
        getinput.print_header(key_val)
        for art in arts:
            print(repr(art))
            art.add_to_history(None)  # for merge()
        if interactive:
            name_to_art = {art.name: art for art in arts}
            options = ["o", "f", "d", *[art.name for art in arts]]

            while True:
                choice = getinput.get_with_completion(
                    options, history_key=key_val, disallow_other=True
                )
                if not choice:
                    break
                if choice == "o":
                    for art in arts:
                        art.openf()
                elif choice == "f":
                    for art in arts:
                        getinput.print_header(art)
                        art.full_data()
                elif choice == "d":
                    for art in arts:
                        print(repr(art))
                elif choice in name_to_art:
                    name_to_art[choice].edit()


class ScoreHolder:
    def __init__(self, data: dict[Taxon, dict[str, Any]]) -> None:
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
            pair: tuple[Any, dict[str, tuple[float, int, int]]]
        ) -> tuple[Any, ...]:
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
        counts: dict[str, int] = defaultdict(int)
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
        age: AgeClass | None = None,
        graphical: bool = False,
        focus_field: str | None = None,
        min_year: int | None = None,
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
    within_taxon: Taxon | None = None,
    age: AgeClass | None = None,
    graphical: bool = False,
    focus_field: str | None = None,
    min_year: int | None = None,
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
    rank: Rank, period: Period, focus_field: str | None = None, graphical: bool = False
) -> ScoreHolder:
    taxa = set()
    for nam in period.all_type_localities():
        try:
            taxa.add(nam.taxon.parent_of_rank(rank))
        except ValueError:
            continue
    return ScoreHolder.from_taxa(taxa, focus_field=focus_field, graphical=graphical)


@generator_command
def authorless_names(
    root_taxon: Taxon,
    attribute: str = "author_tags",
    predicate: Callable[[Name], bool] | None = None,
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
    order: Taxon | None
    family: Taxon | None
    is_high_quality: bool


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
    elif taxon.parent is None:
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
    return LabeledName(name, order, family, quality)


@command
def labeled_authorless_names(attribute: str = "author_tags") -> list[LabeledName]:
    nams = Name.select_valid().filter(getattr(Name, attribute) >> None)
    return [
        label_name(name) for name in nams if attribute in name.get_required_fields()
    ]


# Statistics


@command
def type_locality_tree() -> None:
    earth = models.Region.get(name="Earth")
    _, lines = _tl_count(earth)
    for line in lines:
        print(line)


def _tl_count(region: models.Region) -> tuple[int, list[str]]:
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
    parent_of_taxon: dict[int, int] = {}

    def _find_parent(taxon: Taxon) -> int:
        if taxon.id in parent_of_taxon:
            return parent_of_taxon[taxon.id]
        else:
            result: int
            if taxon.is_page_root or taxon.parent is None:
                result = taxon.id
            else:
                result = _find_parent(taxon.parent)
            # cache the parent taxon too
            parent_of_taxon[taxon.id] = result
            return result

    for taxon in Taxon.select_valid():
        _find_parent(taxon)

    print("Finished collecting parents for taxa")

    counts_of_parent: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
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
            print(f"{attribute}: {data[attribute]} ({percentage:.2f}%)")


@command
def article_stats(includefoldertree: bool = False) -> None:
    results: Counter[str] = Counter()
    nredirects = nnonfiles = 0
    for file in Article.select():
        if file.isredirect():
            nredirects += 1
        else:
            for prop in file.get_field_names():
                value = getattr(file, prop)
                if value:
                    results[prop] += 1
                if isinstance(value, dict):
                    for key, prop in value.items():
                        if prop:
                            results[key] += 1

            if file.kind != ArticleKind.electronic:
                nnonfiles += 1

    total = Article.select().count()
    print(
        f"Total number of files is {total}. Of these, {nredirects} are redirects and"
        f" {nnonfiles} are not actual files."
    )
    total -= nredirects
    for field, number in results.most_common():
        pct = number / total * 100
        print(f"{field}: {number} of {total} ({pct:.02f}%)")

    if includefoldertree:
        Article.get_foldertree().count_tree.display()


@command
def autoset_original_name() -> None:
    for nam in Name.select_valid().filter(
        Name.original_name >> None, Name.group << (Group.genus, Group.high)
    ):
        nam.original_name = nam.root_name


@generator_command
def childless_taxa() -> Iterable[Taxon]:
    return Taxon.raw(f"""
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
        """)


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
        for child in taxon.children:
            fossilize(child, to_status=to_status, from_status=from_status)


@command
def sorted_field_values(
    field: str,
    model_cls: type[models.BaseModel] = Name,
    *,
    filters: Iterable[Any] = [],
    exclude_fn: Callable[[Any], bool] | None = None,
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
    for field in ("verbatim_citation", "verbatim_type"):
        print(field, Name.select_valid().filter(getattr(Name, field) != None).count())
    print("Total", Name.select_valid().count())


@command
def clean_column(
    cls: type[models.BaseModel], column: str, dry_run: bool = True
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
    cite_to_nams: dict[str, list[Name]] = defaultdict(list)
    cite_to_group: dict[str, set[CitationGroup]] = defaultdict(set)
    count = 0
    for nam in Name.bfind(Name.verbatim_citation != None, quiet=True):
        assert nam.verbatim_citation is not None
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
    model_cls: type[models.BaseModel], field: str, dry_run: bool = False
) -> None:
    for obj in model_cls.filter(getattr(model_cls, field) == ""):
        print(f"{obj}: set {field} to None")
        if not dry_run:
            setattr(obj, field, None)


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
    show_hints: bool = False,
) -> None:
    book_cg = CitationGroup.get(CitationGroup.name == "book")
    if book:
        query = Name.citation_group == book_cg
    else:
        query = Name.citation_group == None
    names = Name.bfind(
        Name.verbatim_citation != None,
        Name.original_citation == None,
        query,
        quiet=True,
    )
    patterns = list(CitationGroupPattern.select_valid())
    print(f"Filling citation group for {len(names)} names")

    if not book and not skip_inference:
        for nam in names:
            if nam.verbatim_citation is None:
                continue
            citation = helpers.simplify_string(nam.verbatim_citation)
            for pattern in patterns:
                if pattern.pattern in citation:
                    print("===", nam)
                    print(nam.verbatim_citation)
                    print(
                        f"Inferred group with {pattern.pattern!r}:"
                        f" {pattern.citation_group}"
                    )
                    nam.citation_group = pattern.citation_group

    if not interactive:
        return

    for nam in sorted(
        Name.bfind(
            Name.verbatim_citation != None,
            Name.original_citation == None,
            query,
            quiet=True,
        ),
        key=lambda nam: (
            nam.taxonomic_authority(),
            nam.get_date_object(),
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
def field_by_year(field: str | None = None) -> None:
    by_year_cited: dict[str, int] = defaultdict(int)
    by_year_total: dict[str, int] = defaultdict(int)
    if field is None:
        for nam in Name.bfind(
            Name.original_citation == None, Name.year != None, quiet=True
        ):
            assert nam.year is not None
            by_year_total[nam.year] += 1
            if nam.verbatim_citation:
                by_year_cited[nam.year] += 1
    else:
        for nam in Name.bfind(Name.year != None, quiet=True):
            assert nam.year is not None
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
        Name.type_tags.contains(substring), Name.type_locality != None, quiet=True
    )
    for nam in sorted(
        nams,
        key=lambda nam: (
            str(nam.type_locality.region),
            str(nam.type_locality),
            nam.taxon.valid_name,
        ),
    ):
        assert nam.type_locality is not None
        print(f"{nam.type_locality}, {nam.type_locality.region}: {nam}")
        if full:
            nam.display()


def names_with_location_detail_without_type_loc(
    taxon: Taxon | None = None, *, substring: str | None = None
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
        tags = list(nam.get_tags(nam.type_tags, TypeTag.LocationDetail))
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
    taxon: Taxon | None = None, substring: str | None = None
) -> None:
    for nam in names_with_location_detail_without_type_loc(taxon, substring=substring):
        nam.fill_field("type_locality")


def names_with_type_detail_without_type(taxon: Taxon | None = None) -> Iterable[Name]:
    if taxon is None:
        nams = Name.select_valid().filter(
            Name.type_tags != None, Name.type >> None, Name.group == Group.genus
        )
    else:
        nams = [
            nam
            for nam in taxon.all_names()
            if nam.type_tags is not None
            and nam.type is None
            and nam.group == Group.genus
        ]
    nams_with_key = []
    for nam in nams:
        tags = list(nam.get_tags(nam.type_tags, TypeTag.TypeSpeciesDetail))
        if not tags:
            continue
        if "type" not in nam.get_required_fields():
            continue
        nams_with_key.append(([(tag.source.name, tag.text) for tag in tags], nam, tags))
    for _, nam, tags in sorted(nams_with_key):
        nam.display()
        for tag in tags:
            print(tag)
        yield nam


@command
def fill_type_from_type_detail(taxon: Taxon | None = None) -> None:
    for nam in names_with_type_detail_without_type(taxon):
        nam.fill_field("type")


@command
def fix_general_type_localities() -> None:
    region = models.Region.getter(None).get_one()
    if region is None:
        return
    fix_general_type_localities_for_region(region)


def fix_general_type_localities_for_location(loc: models.Location) -> None:
    if not loc.should_be_specified():
        return
    if loc.type_localities.count() == 0:
        return
    models.fill_data.fill_data_for_names(
        list(loc.type_localities), level=FillDataLevel.incomplete_detail
    )
    getinput.print_header(loc)
    loc.display(full=True)
    while True:
        obj = models.Name.getter("corrected_original_name").get_one(
            prompt="corrected_original_name> ",
            callbacks={"d": lambda: loc.display(), "f": lambda: loc.display(full=True)},
        )
        if obj is None:
            break
        obj.display()
        obj.edit()

    more_precise_type_localities(loc)


def fix_general_type_localities_for_region(region: models.Region) -> None:
    getinput.print_header(region)
    region.display()
    for loc in region.locations:
        fix_general_type_localities_for_location(loc)

    for child in region.children:
        fix_general_type_localities_for_region(child)


@command
def biggest_general_type_localities() -> None:
    counts: Counter[models.Location] = Counter()
    for loc in getinput.print_every_n(
        models.Location.select_valid(), n=100, label="localities"
    ):
        if not loc.should_be_specified():
            continue
        counts[loc] = loc.type_localities.count()
    for loc, count in counts.most_common(100):
        print(count, loc)


@command
def more_precise_type_localities(
    loc: models.Location, *, substring: str | None = None
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
    region: models.Region | None = None,
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
    max_level: PersonLevel | None = PersonLevel.has_given_name,
    max_num_names: int | None = None,
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
        counter = Counter({
            family_name: count
            for family_name, count in counter.items()
            if name_counter[family_name] <= max_num_names
        })
    for value, count in counter.most_common(num_to_display):
        print(value, count)
    return counter


@command
def most_common_initials() -> Counter[Person]:
    counter: Counter[Person] = Counter()
    for pers in Person.select_valid():
        if pers.get_level() is not PersonLevel.initials_only:
            continue
        arts = pers.get_raw_derived_field("articles")
        if arts is None:
            continue
        counter[pers] = len(arts)
    for pers, val in counter.most_common(10):
        print(val, pers)
    return counter


@command
def biggest_names(
    num_to_display: int = 10,
    max_level: PersonLevel | None = PersonLevel.has_given_name,
    family_name: str | None = None,
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
    family_name: str | None = None,
    substring: bool = True,
    max_level: PersonLevel | None = PersonLevel.has_given_name,
    min_level: PersonLevel | None = None,
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
        if max_level is not None and person.get_level() > max_level:
            continue
        if min_level is not None and person.get_level() < min_level:
            continue
        person.maybe_reassign_references()


@command
def doubled_authors(autofix: bool = False) -> list[Name]:
    nams = Name.select_valid().filter(Name.author_tags != None)
    bad_nams = []
    for nam in nams:
        tags = nam.get_raw_tags_field("author_tags")
        if len(tags) != len({t[1] for t in tags}):
            print(nam, nam.author_tags)
            bad_nams.append(nam)
            if autofix:
                nam.display()
                for i, tag in enumerate(nam.author_tags):
                    print(f"{i}: {tag}")
                nam.e.author_tags
    return bad_nams


@command
def reassign_authors(
    taxon: Taxon | None = None, skip_family: bool = False, skip_initials: bool = False
) -> None:
    if taxon is None:
        taxon = Taxon.getter(None).get_one()
    if taxon is None:
        return
    if not skip_family:
        print("v-ing...")
        nams = [nam for nam in taxon.all_names() if nam.verbatim_citation is not None]
        authors = {author for nam in nams for author in nam.get_authors()}
        authors = {
            author
            for author in authors
            if author.get_level() == PersonLevel.family_name_only
        }
        print(f"Found {len(authors)} authors")
        for author in sorted(authors, key=lambda a: a.sort_key()):
            print(author)
            author.reassign_names_with_verbatim(filter_for_name=True)
    if not skip_initials:
        print("rio-ing...")
        nams = [nam for nam in taxon.all_names() if nam.original_citation is not None]
        authors = {author for nam in nams for author in nam.get_authors()}
        authors = {
            author
            for author in authors
            if author.get_level() == PersonLevel.initials_only
        }
        print(f"Found {len(authors)} authors")
        for author in sorted(authors, key=lambda a: a.sort_key()):
            print(author)
            author.reassign_initials_only()
    print("checking authors...")
    for nam in taxon.all_names():
        nam.check_authors()


@command
def most_common(model_cls: type[models.BaseModel], field: str) -> Counter[Any]:
    objects = model_cls.select_valid().filter(getattr(model_cls, field) != None)
    counter: Counter[Any] = Counter()
    for obj in objects:
        counter[getattr(obj, field)] += 1
    for value, count in counter.most_common(10):
        print(value, count)
    return counter


@command
def most_common_mapped(
    model_cls: type[models.BaseModel],
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
def most_common_citation_groups_after(year: int) -> dict[CitationGroup, int]:
    nams = Name.bfind(Name.citation_group != None, Name.year > year, quiet=True)
    return Counter(nam.citation_group for nam in nams)


@generator_command
def check_expected_base_name() -> Iterable[Taxon]:
    """Finds cases where a Taxon's base name is not the oldest available name."""
    for txn in Taxon.select_valid().filter(Taxon.rank <= Rank.superfamily):
        if not txn.check_expected_base_name():
            yield txn


@command
def fix_justified_emendations() -> None:
    query = Name.select_valid().filter(
        Name.nomenclature_status
        << (
            NomenclatureStatus.as_emended,
            NomenclatureStatus.justified_emendation,
            NomenclatureStatus.incorrect_original_spelling,
        )
    )
    run_linter_and_fix(Name, models.name_lint.check_justified_emendations, query)


@command
def run_linter_and_fix(
    model_cls: type[ModelT],
    linter: Linter[ModelT] | None = None,
    query: Iterable[ModelT] | None = None,
    interactive: bool = True,
) -> None:
    """Helper for running a lint on a subset of objects and fixing the issues."""
    bad = model_cls.lint_all(linter, query=query, interactive=interactive)
    print(f"Found {len(bad)} issues")
    if not bad:
        return
    for obj, messages in getinput.print_every_n(bad, label="issues", n=5):
        obj = obj.reload()
        getinput.print_header(obj)
        obj.display()
        for message in messages:
            print(message)
        while not obj.is_lint_clean(extra_linter=linter):
            try:
                obj.edit()
            except getinput.StopException:
                return
            obj = obj.reload()


@generator_command
def move_to_lowest_rank(dry_run: bool = False) -> Iterable[tuple[Name, str]]:
    for nam in getinput.print_every_n(Name.select_valid(), label="names"):
        query = Taxon.select_valid().filter(Taxon.base_name == nam)
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


@command
def resolve_redirects(dry_run: bool = False) -> None:
    for model_cls in models.BaseModel.__subclasses__():
        for obj in getinput.print_every_n(
            model_cls.select(), label=f"{model_cls.__name__}s"
        ):
            for _ in obj.check_all_fields(autofix=not dry_run):
                pass


@command
def run_maintenance(skip_slow: bool = True) -> dict[Any, Any]:
    """Runs maintenance checks that are expected to pass for the entire database."""
    fns: list[Callable[[], Any]] = [
        labeled_authorless_names,
        detect_complexes,
        detect_species_name_complexes,
        autoset_original_name,
        dup_collections,
        dup_citation_groups,
        # dup_names,
        # dup_genus,
        # dup_taxa,
        dup_articles,
        set_citation_group_for_matching_citation,
        enforce_must_have,
        fix_citation_group_redirects,
        recent_names_without_verbatim,
        Person.autodelete,
        Person.find_duplicates,
        Person.resolve_redirects,
    ]
    # these each take >60 s
    slow: list[Callable[[], Any]] = [
        move_to_lowest_rank,
        *[cls.lint_all for cls in models.BaseModel.__subclasses__()],
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


def names_of_author(author: str, include_partial: bool) -> list[Name]:
    persons = Person.select_valid().filter(
        Person.family_name.contains(author)
        if include_partial
        else Person.family_name == author
    )
    return [
        nam for person in persons for nam in person.get_sorted_derived_field("names")
    ]


@command
def names_of_authority(author: str, year: int, edit: bool = False) -> list[Name]:
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
    filter: str | None = None, edit: bool = False
) -> list[Name]:
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


def fgsyn(off: Name | None = None) -> Name | None:
    """Adds a family-group synonym."""
    if off is not None:
        taxon = off.taxon
    else:
        taxon = Taxon.get_one_by("valid_name", prompt="taxon> ")
        if taxon is None:
            return None
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
    author: str | None = None,
    partial: bool = False,
    missing_attribute: str | None = None,
) -> list[Name]:
    if author is None:
        author = Person.getter("family_name").get_one_key(prompt="name> ")
    if author is None:
        return []
    nams = names_of_author(author, include_partial=partial)
    if not missing_attribute:
        nams = [nam for nam in nams if nam.original_citation is None]

    by_year: dict[str, list[Name]] = defaultdict(list)
    no_year: list[Name] = []
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
    print(f"years: {min(by_year, default=None)}–{max(by_year, default=None)}")
    out: list[Name] = []
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
                assert nam.citation_group is not None
                getinput.print_header(nam.citation_group.name)
            print(f"{nam} is in {cg}, but has no original_citation")
            nam.display()
            found_any = True
            yield nam
        if found_any:
            find_potential_citations_for_group(cg, fix=fix)


@generator_command
def archive_for_must_have(fix: bool = True) -> Iterator[CitationGroup]:
    for cg in _must_have_citation_groups():
        if cg.archive is None:
            getinput.print_header(cg)
            cg.display()
            cg.e.archive
            yield cg


def _must_have_citation_groups() -> list[CitationGroup]:
    return [
        cg
        for cg in CitationGroup.select_valid()
        if cg.has_tag(CitationGroupTag.MustHave)
        or cg.get_tag(CitationGroupTag.MustHaveAfter)
    ]


@command
def find_potential_citations(
    fix: bool = False, region: models.Region | None = None, aggressive: bool = False
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


def _author_names(obj: Article | Name) -> set[str]:
    return {helpers.simplify_string(person.family_name) for person in obj.get_authors()}


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
        if nam.numeric_year() != art.numeric_year() or art.is_non_original():
            return False
        if not art.is_page_in_range(page):
            return False
        if aggressive:
            return _author_names(nam) <= _author_names(art)
        else:
            return nam.author_set() <= art.author_set()

    count = 0
    for nam in cg.get_names():
        nam = nam.reload()
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
def print_parent() -> Taxon | None:
    taxon = Taxon.getter("valid_name").get_one()
    if taxon:
        return taxon.parent
    return None


@command
def occ(
    t: Taxon | None = None,
    loc: models.Location | None = None,
    source: Article | None = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> models.Occurrence | None:
    if t is None:
        t = Taxon.getter(None).get_one("taxon> ")
    if t is None:
        return None
    if loc is None:
        loc = models.Location.getter(None).get_one("location> ")
    if loc is None:
        return None
    if source is None:
        source = Article.getter(None).get_one("source> ")
    if source is None:
        return None
    try:
        o = t.at(loc)
    except models.Occurrence.DoesNotExist:
        o = t.add_occurrence(loc, source, **kwargs)
        print("ADDED: %s" % o)
    else:
        print("EXISTING: %s" % o)
        if replace_source and o.source != source:
            o.source = source
            o.s(**kwargs)
            print("Replaced source: %s" % o)
    return o


@command
def mocc(
    t: Taxon | None = None,
    source: Article | None = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    if t is None:
        t = Taxon.getter(None).get_one("taxon> ")
    if t is None:
        return None
    if source is None:
        source = Article.getter(None).get_one("source> ")
    if source is None:
        return None
    while True:
        loc = models.Location.getter(None).get_one("location> ")
        if loc is None:
            break
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


@command
def multi_taxon(
    loc: models.Location | None = None,
    source: Article | None = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    if loc is None:
        loc = models.Location.getter(None).get_one("location> ")
    if loc is None:
        return None
    if source is None:
        source = Article.getter(None).get_one("source> ")
    if source is None:
        return None
    while True:
        t = Taxon.getter(None).get_one("taxon> ")
        if t is None:
            break
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


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
    keys = set(derived_data.load_derived_data())
    for model in models.BaseModel.__subclasses__():
        if hasattr(model, "label_field"):
            print(f"{model}: warming None getter")
            model.getter(None).rewarm_cache()
        for name, field in model._meta.fields.items():
            getter = model.getter(name)
            if isinstance(field, peewee.CharField) or getter._cache_key() in keys:
                print(f"{model}: warming {name} ({field})")
                getter.rewarm_cache()
    write_derived_data()


@command
def show_queries(on: bool) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("peewee")
    if on:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


@command
def rename_specimen_photos(dry_run: bool = True) -> None:
    base_path = models.base.settings.photos_path
    for dirname, _, paths in os.walk(base_path):
        for filename in paths:
            path = Path(dirname) / filename
            if path.suffix != ".jpg":
                continue
            match = re.match(r"^(JSZ \d+)( .*)?", path.parts[-2])
            if match is None:
                continue
            specimen_number = match.group(1)
            if filename.startswith(specimen_number):
                continue
            new_path = path.parent / f"{specimen_number} {path.name}"
            print(f"rename {path} -> {new_path}")
            if not dry_run:
                shutil.move(path, new_path)


def _sort_key(volume_or_issue: str | None) -> tuple[object, ...]:
    if volume_or_issue is None:
        return (float("inf"), "")
    try:
        return (int(volume_or_issue), "")
    except ValueError:
        return (float("inf"), volume_or_issue)


@command
def cg_recent_report(
    cg: CitationGroup | None = None, min_year: int | None = None
) -> None:
    if cg is None:
        cg = CitationGroup.getter(None).get_one("citation group> ")
    if min_year is None:
        min_year = datetime.date.today().year - 3
    query = Article.select_valid().filter(Article.citation_group == cg)
    # {volume: {issue: [articles]}}
    arts: dict[str, dict[str, list[Article]]] = defaultdict(lambda: defaultdict(list))
    for art in query:
        if art.kind is ArticleKind.alternative_version:
            continue
        if art.numeric_year() >= min_year:
            arts[art.volume][art.issue].append(art)
    getinput.print_header(f"{cg} ({min_year}–present)")
    for volume in sorted(arts, key=_sort_key):
        print(f"=== Volume {volume}")
        volume_data = arts[volume]
        for issue in sorted(volume_data, key=_sort_key):
            print(f"    === Issue {issue}")
            for art in sorted(
                volume_data[issue], key=lambda art: art.numeric_start_page()
            ):
                print(f"         {art!r}")


@command
def most_common_authors_without_verbatim_citation(
    print_n: int = 20,
) -> dict[Person, int]:
    data: Counter[Person] = Counter()
    for nam in getinput.print_every_n(
        Name.select_valid().filter(
            Name.verbatim_citation == None, Name.original_citation == None
        ),
        label="names",
    ):
        for author in nam.get_authors():
            data[author] += 1
    for author, count in data.most_common(print_n):
        print(count, author)
    return data


@command
def find_patronym_clusters() -> None:
    clusters = {}
    suffixes = ("i", "ae", "orum", "arum")
    rgx = re.compile(rf"{'|'.join(suffixes)}$")
    nams = Name.select_valid().filter(
        Name.group == Group.species,
        functools.reduce(
            operator.or_, [Name.root_name.endswith(suffix) for suffix in suffixes]
        ),
    )
    for nam in getinput.print_every_n(nams, label="names", n=100):
        root_name = rgx.sub("", nam.root_name)
        key = (nam.taxon, root_name)
        possibilities = [root_name + suffix for suffix in suffixes]
        cluster = []
        unique_names = set()
        for synonym in Name.add_validity_check(nam.taxon.names):
            if synonym.root_name in possibilities:
                cluster.append(synonym)
                unique_names.add(synonym.root_name)
        if len(unique_names) > 1:
            clusters[key] = cluster
    for key, cluster in clusters.items():
        getinput.print_header(key)
        for nam in cluster:
            nam.display(full=False)


@command
def rename_type_specimens() -> None:
    collection = Collection.getter(None).get_one("collection> ")
    if collection is None:
        return
    age = getinput.get_enum_member(constants.AgeClass, prompt="age> ", allow_empty=True)
    parent_taxon = Taxon.getter(None).get_one("taxon> ")
    include_regex = getinput.get_line("include regex> ", allow_none=True)
    to_replace = getinput.get_line("replace> ", allow_none=False)
    replace_with = getinput.get_line("replace with> ", allow_none=False)
    dry_run = getinput.yes_no("dry run? ")
    replacements = 0
    for nam in collection.type_specimens.filter(Name.type_specimen != None):
        if age is not None and nam.taxon.age is not age:
            continue
        if parent_taxon is not None and not nam.taxon.is_child_of(parent_taxon):
            continue
        if include_regex and not re.fullmatch(include_regex, nam.type_specimen):
            continue
        new_type_specimen = nam.type_specimen.replace(to_replace, replace_with)
        if nam.type_specimen == new_type_specimen:
            continue
        print(f"{nam.type_specimen!r} -> {new_type_specimen!r} ({nam})")
        replacements += 1
        if not dry_run:
            old_type_specimen = nam.type_specimen
            nam.type_specimen = new_type_specimen

            def mapper(
                tag: TypeTag,
                old_type_specimen: str = old_type_specimen,
                new_type_specimen: str = new_type_specimen,
            ) -> TypeTag | None:
                if (
                    isinstance(tag, TypeTag.TypeSpecimenLinkFor)
                    and tag.specimen == old_type_specimen
                ):
                    return TypeTag.TypeSpecimenLinkFor(tag.url, new_type_specimen)
                return tag

            nam.map_type_tags(mapper)
    print(f"{replacements} replacements made")


@command
def generate_summary_paragraph() -> str:
    name_count = Name.select_valid().count()
    mammalia = Taxon.getter("valid_name")("Mammalia")
    assert mammalia is not None
    mammal_count = len(mammalia.all_names())
    location_count = models.Location.select_valid().count()
    region_count = models.Region.select_valid().count()
    period_count = Period.select_valid().count()
    su_count = models.StratigraphicUnit.select_valid().count()
    art_count = Article.select_valid().count()
    tl_count = Name.select_valid().filter(Name.type_locality != None).count()
    spec_count = Name.select_valid().filter(Name.type_specimen != None).count()
    template = f"""- {name_count} [names](/docs/name), of which {mammal_count} are [mammals](/t/Mammalia)
- {location_count} [locations](/docs/location) grouped into {region_count} [regions](/docs/region), {period_count}
  [periods](/docs/period), and {su_count} [stratigraphic units](/docs/stratigraphic-unit)
- {art_count} [citations](/docs/article)
- Type localities for {tl_count} names
- Type specimens for {spec_count} names
"""
    print(template)
    return template


@command
def check_wikipedia_links(path: str) -> None:
    url = f"https://en.wikipedia.org/w/index.php?title={path}&action=raw"
    data = requests.get(url).text
    for match in re.finditer(r"''\[\[([A-Za-z ]+)\]\]''", data):
        name = match.group(1)
        nams = list(Name.select_valid().filter(Name.corrected_original_name == name))
        if not nams:
            print(name)


def find_names_with_organ(organ: constants.SpecimenOrgan) -> Iterable[Name]:
    return Name.select_valid().filter(
        Name.type_tags.contains(f"[{TypeTag.Organ._tag}, {organ.value},")
    )


@command
def edit_organ(organ: constants.SpecimenOrgan | None = None) -> None:
    if organ is None:
        organ = getinput.get_enum_member(constants.SpecimenOrgan, "organ> ")
        if organ is None:
            return
    substring = getinput.get_line("substring> ")
    check_lint = getinput.yes_no("only edit if there are lint issues? ")
    for nam in getinput.print_every_n(
        find_names_with_organ(organ), n=100, label="names"
    ):
        relevant_tags = [
            t
            for t in nam.type_tags
            if isinstance(t, TypeTag.Organ) and t.organ is organ and t.detail
        ]
        if substring:
            relevant_tags = [
                t
                for t in relevant_tags
                if t.detail is not None and substring in t.detail
            ]
        if not relevant_tags:
            continue
        if check_lint and nam.is_lint_clean():
            continue
        getinput.print_header(nam)
        dirty = False
        for tag in relevant_tags:
            print(tag)
            for issue in models.name_lint.check_organ_tag(tag):
                print(issue)
                dirty = True
        if check_lint and not dirty:
            print(f"Ignoring as {organ.name} tags are clean")
            continue
        nam.format(quiet=True)
        if getinput.yes_no("edit?"):
            original_tags = set(nam.type_tags)
            nam.edit()
            new_tags = [tag for tag in nam.type_tags if tag not in relevant_tags]
            if any(tag not in original_tags for tag in new_tags):
                nam.type_tags = new_tags  # type: ignore[assignment]


@command
def organ_report(focus_organ: constants.SpecimenOrgan | None = None) -> None:
    organ_to_count: Counter[constants.SpecimenOrgan] = Counter()
    organ_to_text_to_count: dict[constants.SpecimenOrgan, Counter[str]] = defaultdict(
        Counter
    )
    if focus_organ is None:
        query = Name.select_valid().filter(
            Name.type_tags.contains(f"[{TypeTag.Organ._tag},")
        )
    else:
        query = find_names_with_organ(focus_organ)
    for nam in getinput.print_every_n(query, label="names"):
        for tag in nam.type_tags:
            if isinstance(tag, TypeTag.Organ):
                organ_to_count[tag.organ] += 1
                if not tag.detail:
                    continue
                detail = re.sub(r"\([^\)]+\)", "", tag.detail).split(",")
                for piece in detail:
                    organ_to_text_to_count[tag.organ][piece.strip()] += 1

    getinput.print_header("Overall counts")
    for organ, count in organ_to_count.most_common():
        print(f"{count} {organ.name}")
    for organ, _ in organ_to_count.most_common():
        getinput.print_header(organ.name)
        text_to_count = organ_to_text_to_count[organ]
        for text, count in text_to_count.most_common():
            print(count, text)


@command
def lint_recent(limit: int = 1000) -> None:
    for cls in models.BaseModel.__subclasses__():
        getinput.print_header(cls)
        run_linter_and_fix(
            cls, query=cls.select_valid().order_by(cls.id.desc()).limit(limit)
        )


@command
def try_extract_page_described(dry_run: bool = True, verbose: bool = False) -> None:
    count = 0
    for nam in getinput.print_every_n(
        Name.select_valid().filter(
            Name.verbatim_citation != None, Name.page_described == None
        ),
        label="names",
        n=100,
    ):
        cite = nam.verbatim_citation
        if cite is None:
            continue
        cite = re.sub(r"\[[^\[\]]+\]", " ", cite).strip().rstrip(".")
        cite = re.sub(
            r", (\d{1,2} )?([A-Z][a-z][a-z][a-z]?\.?\s)?1[789]\d{2}$", "", cite
        ).strip()
        if match := re.search(r"(?:\bp\.|\bS\.|:)\s*(\d{1,4})$", cite):
            page = match.group(1)
            print(f"{nam}: infer page {page!r} from {nam.verbatim_citation!r}")
            count += 1
            if not dry_run:
                nam.page_described = page
        elif verbose:
            print(nam.verbatim_citation, repr(cite))
    print(f"extracted {count} page_described")


def run_shell() -> None:
    # GC does bad things on my current setup for some reason
    gc.disable()
    for cs in COMMAND_SETS:
        for cmd in cs.commands:
            command(cmd)
    config = Config()
    config.InteractiveShell.confirm_exit = False
    config.TerminalIPythonApp.display_banner = False
    lib_file = os.path.join(os.path.dirname(__file__), "lib.py")
    IPython.start_ipython(argv=[lib_file, "-i"], config=config, user_ns=ns)


if __name__ == "__main__":
    run_shell()
