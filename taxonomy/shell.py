"""Shell commands, functions that can be called directly from the shell.

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
import importlib
import operator
import os
import pprint
import re
import shutil
import sqlite3
import subprocess
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import groupby, pairwise
from pathlib import Path
from typing import Any, NamedTuple, TypeVar, cast

import clirm
import httpx
import IPython
import unidecode
from traitlets.config.loader import Config

from taxonomy import config
from taxonomy.adt import ADT
from taxonomy.apis import bhl
from taxonomy.config import get_options
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)

from . import getinput, urlparse
from .command_set import CommandSet
from .db import constants, definition, derived_data, export, helpers, models
from .db.constants import (
    NEED_TEXTUAL_RANK,
    AgeClass,
    ArticleKind,
    ArticleType,
    Group,
    NameDataLevel,
    NamingConvention,
    NomenclatureStatus,
    OriginalCitationDataLevel,
    PersonType,
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
)
from .db.models.base import LintConfig, Linter, ModelT
from .db.models.person import PersonLevel

T = TypeVar("T")

gc.disable()

_CS = CommandSet("shell", "Miscellaneous commands")

COMMAND_SETS = [
    models.fill_data.CS,
    models.article.check.CS,
    export.CS,
    models.article.add_data.CS,
    models.classification_entry.ce.CS,
    _CS,
]

command = _CS.register


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
    "NameTag": models.NameTag,
    "PersonLevel": PersonLevel,
    "TypeTag": models.TypeTag,
    "Counter": collections.Counter,
    "defaultdict": defaultdict,
    "getinput": getinput,
    "models": models,
    "os": os,
    "gc": gc,
    "importlib": importlib,
    "pp": pprint.pp,
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


def _register_command(fn: CallableT) -> CallableT:
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
        return Taxon.select_valid().filter(Taxon.valid_name == name).get()
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
        .filter(Name.original_citation != None, Name.original_name == None)
        .order_by(Name.original_name)
    ):
        message = (
            f"Name {name.description()} is missing an original name, but has original"
            f" citation {{{name.original_citation.name}}}:{name.page_described}"
        )
        yield name, message


@command
@_add_missing_data
def add_page_described() -> Iterable[tuple[Name, str]]:
    for name in (
        Name.select_valid()
        .filter(
            Name.original_citation != None,
            Name.page_described == None,
            Name.year != "in press",
        )
        .order_by(Name.original_citation, Name.original_name)
    ):
        if name.year in ("2015", "2016"):
            continue  # recent JVP papers don't have page numbers
        message = (
            f"Name {name.description()} is missing page described, but has original"
            f" citation {{{name.original_citation.name}}}"
        )
        yield name, message


@command
def make_county_regions(
    state: models.Region, name: str | None = None, *, dry_run: bool = True
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
        except sqlite3.IntegrityError:
            print(f"{county} already exists")
    _more_precise_by_county(state, counties)
    more_precise(state)


@command
def infer_min_max_age(*, dry_run: bool = True) -> None:
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
            Name.type == None,
            Name.year > "1930",
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
def endswith(end: str) -> list[Name]:
    return list(
        Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name % f"%{end}"
        )
    )


@command
def find_first_declension_adjectives(*, dry_run: bool = True) -> dict[str, int]:
    adjectives = get_pages_in_wiki_category(
        "en.wiktionary.org", "Latin first and second declension adjectives"
    )
    species_name_to_names: dict[str, list[Name]] = defaultdict(list)
    for name in Name.select_valid().filter(
        Name.group == Group.species, Name.species_name_complex == None
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
        run_linter_and_fix(
            Name,
            query=Name.select_valid().filter(
                Name.species_name_complex == None, Name.group == Group.species
            ),
            linter=models.name.lint.infer_species_name_complex,
        )
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
        json = httpx.get(url, params=params).json()
        for entry in json["query"]["categorymembers"]:
            if entry["ns"] == 0:
                yield entry["title"]
        if "continue" in json:
            cmcontinue = json["continue"]["cmcontinue"]
        else:
            break


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
        elif original_root_name != nam.root_name:
            print(f"{nam}: {original_root_name} does not match {nam.root_name}")
            yield nam


def _duplicate_finder(
    fn: Callable[[], Iterable[Mapping[Any, Sequence[ModelT]]]],
) -> Callable[[], list[Sequence[ModelT]] | None]:
    @generator_command
    @functools.wraps(fn)
    def wrapper(*, interactive: bool = False) -> Iterable[Sequence[ModelT]]:
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
    for coll in Collection.select_valid():
        colls[coll.label].append(coll)
    return [colls]


@_duplicate_finder
def dup_taxa() -> list[dict[str, list[Taxon]]]:
    taxa: dict[str, list[Taxon]] = defaultdict(list)
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
    *,
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
        *,
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
            pair: tuple[Any, dict[str, tuple[float, int, int]]],
        ) -> tuple[Any, ...]:
            _, data = pair
            percentage, count, required_count = data.get(field, (100, 0, 0))
            return (percentage, required_count, data["total"])

        sorted_items = sorted(items, key=sort_key)
        chart_data = []
        for taxon, data in sorted_items:
            percentage, count, required_count = data.get(field, (100, 0, 0))
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
            for field_name in fields:
                if field_name not in data or data[field_name][0] == 100:
                    counts[field_name] += 1
        total = len(self.data)
        for field_name, count in sorted(counts.items(), key=lambda p: p[1]):
            print(f"{field_name}: {count * 100 / total:.2f} ({count}/{total})")

    @classmethod
    def from_taxa(
        cls,
        taxa: Iterable[Taxon],
        *,
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
    *,
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
    rank: Rank,
    period: Period,
    *,
    focus_field: str | None = None,
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
    nams = Name.select_valid().filter(getattr(Name, attribute) == None)
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
        # static analysis: ignore[incompatible_argument]
        (Taxon.select_valid().filter(Taxon.id == parent_id).get(), data)
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
def article_stats(*, includefoldertree: bool = False) -> None:
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
                    for key, inner_prop in value.items():
                        if inner_prop:
                            results[key] += 1

            if file.kind != ArticleKind.electronic:
                nnonfiles += 1

    total = Article.select().count()
    print(
        f"Total number of files is {total}. Of these, {nredirects} are redirects and"
        f" {nnonfiles} are not actual files."
    )
    total -= nredirects
    for field_name, number in results.most_common():
        pct = number / total * 100
        print(f"{field_name}: {number} of {total} ({pct:.02f}%)")

    if includefoldertree:
        Article.get_foldertree().count_tree.display()


@generator_command
def childless_taxa() -> Iterable[Taxon]:
    taxa = list(Taxon.select_valid())
    all_parents = {taxon.parent.id for taxon in taxa if taxon.parent is not None}
    return [
        taxon
        for taxon in taxa
        if taxon.id not in all_parents and taxon.rank > Rank.species
    ]


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
        taxon.age = to_status
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
def field_counts() -> None:
    for field_name in ("verbatim_citation",):
        print(
            field_name,
            Name.select_valid().filter(getattr(Name, field_name) != None).count(),
        )
    print("Total", Name.select_valid().count())


@command
def clean_column(
    cls: type[models.BaseModel], column: str, *, dry_run: bool = True
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
        nam.fill_field("name_complex")


@command
def set_citation_group_for_matching_citation(
    *, dry_run: bool = False, fix: bool = False
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
                    nam.fill_field("citation_group")
    print(f"Added {count} citation_groups")


@command
def set_empty_to_none(
    model_cls: type[models.BaseModel], field: str, *, dry_run: bool = False
) -> None:
    for obj in model_cls.filter(getattr(model_cls, field) == ""):
        print(f"{obj}: set {field} to None")
        if not dry_run:
            setattr(obj, field, None)


@command
def fill_citation_group_for_type(
    article_type: constants.ArticleType, field: str, *, dry_run: bool = False
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
        except clirm.DoesNotExist:
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
        cg.fill_field("region")


@command
def fill_citation_groups(
    *,
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
        nam.load()
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
def type_localities_like(substring: str, *, full: bool = False) -> None:
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
            Name.type_locality == None,
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
            Name.type_tags != None, Name.type == None, Name.group == Group.genus
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
    models.fill_data.fill_data_for_names(list(loc.type_localities))
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
    *,
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
            loc.fill_field("stratigraphic_unit")
        else:
            loc.fill_field("max_period")
            loc.fill_field("min_period")
            if set_stratigraphy and loc.stratigraphic_unit is None:
                loc.fill_field("stratigraphic_unit")


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
    *,
    substring: bool = True,
    max_level: PersonLevel | None = PersonLevel.has_given_name,
    min_level: PersonLevel | None = None,
    auto: bool = False,
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
        person.maybe_reassign_references(auto=auto)


@command
def reassign_references_auto(*, substring: bool = False) -> None:
    reassign_references(auto=True, substring=substring)


@command
def reassign_references_for_convention(
    convention: NamingConvention | None = None,
) -> None:
    if convention is None:
        convention = getinput.get_enum_member(NamingConvention, "convention> ")
    if convention is None:
        return
    family_names = {
        person.family_name
        for person in Person.select_valid().filter(
            Person.naming_convention == convention
        )
    }
    for family_name in sorted(family_names):
        reassign_references(family_name, auto=True, substring=False)


@command
def doubled_authors(*, autofix: bool = False) -> list[Name]:
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
                nam.fill_field("author_tags")
    return bad_nams


@command
def reassign_authors(
    taxon: Taxon | None = None,
    *,
    skip_family: bool = False,
    skip_initials: bool = False,
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
        Name.nomenclature_status.is_in(
            (
                NomenclatureStatus.as_emended,
                NomenclatureStatus.justified_emendation,
                NomenclatureStatus.incorrect_original_spelling,
            )
        )
    )
    run_linter_and_fix(Name, models.name.lint.check_justified_emendations, query)


@command
def run_linter_and_fix(
    model_cls: type[ModelT],
    linter: Linter[ModelT] | None = None,
    query: Iterable[ModelT] | None = None,
    *,
    interactive: bool = False,
    verbose: bool = False,
    manual_mode: bool = False,
    enable_all: bool = False,
) -> None:
    """Helper for running a lint on a subset of objects and fixing the issues."""
    bad = model_cls.lint_all(
        linter,
        query=query,
        interactive=interactive,
        verbose=verbose,
        manual_mode=manual_mode,
        enable_all=enable_all,
    )
    print(f"Found {len(bad)} issues")
    if not bad:
        return
    cfg = LintConfig(
        autofix=True, interactive=True, verbose=verbose, manual_mode=manual_mode
    )
    for obj, messages in getinput.print_every_n(bad, label="issues", n=5):
        obj.load()
        getinput.print_header(obj)
        obj.display()
        for message in messages:
            print(message)
        while not obj.is_lint_clean(extra_linter=linter, cfg=cfg):
            try:
                obj.edit()
            except getinput.StopException:
                return
            obj.load()


@command
def resolve_redirects(*, dry_run: bool = False) -> None:
    cfg = LintConfig(autofix=not dry_run)
    for model_cls in models.BaseModel.__subclasses__():
        for obj in getinput.print_every_n(
            model_cls.select(), label=f"{model_cls.__name__}s"
        ):
            for _ in obj.check_all_fields(cfg):
                pass


@command
def run_maintenance(*, skip_slow: bool = True) -> dict[Any, Any]:
    """Runs maintenance checks that are expected to pass for the entire database."""
    fns: list[Callable[[], Any]] = [
        labeled_authorless_names,
        dup_collections,
        dup_citation_groups,
        # dup_names,
        # dup_genus,
        # dup_taxa,
        dup_articles,
        set_citation_group_for_matching_citation,
        enforce_must_have,
        Person.autodelete,
        Person.find_duplicates,
        Person.resolve_redirects,
    ]
    # these each take >60 s
    slow: list[Callable[[], Any]] = [
        cls.lint_all for cls in models.BaseModel.__subclasses__()
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


def names_of_author(author: str, *, include_partial: bool) -> list[Name]:
    persons = Person.select_valid().filter(
        Person.family_name.contains(author)
        if include_partial
        else Person.family_name == author
    )
    return [
        nam for person in persons for nam in person.get_sorted_derived_field("names")
    ]


@command
def names_of_authority(author: str, year: int, *, edit: bool = False) -> list[Name]:
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
def author_report(
    author: str | None = None,
    *,
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
def enforce_must_have(*, fix: bool = True) -> Iterator[Name]:
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


def _must_have_citation_groups() -> list[CitationGroup]:
    return [
        cg
        for cg in CitationGroup.select_valid()
        if cg.has_tag(CitationGroupTag.MustHave)
        or cg.get_tag(CitationGroupTag.MustHaveAfter)
    ]


@command
def find_potential_citations(
    *, fix: bool = True, region: models.Region | None = None, aggressive: bool = True
) -> int:
    if region is None:
        cgs = CitationGroup.select_valid()
    else:
        cgs = region.all_citation_groups()
    return sum(
        find_potential_citations_for_group(cg, fix=fix, aggressive=aggressive) or 0
        for cg in cgs
        if not cg.has_tag(CitationGroupTag.IgnorePotentialCitations)
    )


def _author_names(obj: Article | Name) -> set[str]:
    return {helpers.simplify_string(person.family_name) for person in obj.get_authors()}


@command
def find_potential_citations_for_group(
    cg: CitationGroup | None = None, *, fix: bool = True, aggressive: bool = True
) -> int:
    if cg is None:
        cg = CitationGroup.getter(None).get_one()
    if cg is None:
        return 0
    if not cg.get_names():
        return 0
    potential_arts = Article.bfind(
        Article.kind != constants.ArticleKind.no_copy, citation_group=cg, quiet=True
    )
    if not potential_arts:
        return 0

    def is_possible_match(art: Article, nam: Name, page: int) -> bool:
        if nam.numeric_year() != art.numeric_year() or art.lacks_full_text():
            return False
        if not art.is_page_in_range(page):
            return False
        if aggressive:
            condition = _author_names(nam) <= _author_names(art)
        else:
            condition = nam.author_set() <= art.author_set()
        if not condition:
            return False
        for tag in nam.type_tags:
            if (
                isinstance(tag, TypeTag.IgnorePotentialCitationFrom)
                and tag.article == art
            ):
                return False
        return True

    count = 0
    for nam in cg.get_names():
        nam.load()
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
                    candidate.add_to_history()
                nam.edit()
                if nam.original_citation is not None:
                    nam.edit_until_clean()
                elif getinput.yes_no("Add IgnorePotentialCitationFrom?"):
                    for candidate in candidates:
                        nam.add_type_tag(
                            TypeTag.IgnorePotentialCitationFrom(
                                article=candidate, comment=""
                            )
                        )
    if count:
        print(f"{cg} had {count} potential citations", flush=True)
    return count


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
    *,
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
    except clirm.DoesNotExist:
        maybe_o = t.add_occurrence(loc, source, **kwargs)
        assert maybe_o is not None, "must be non-None if loc is given"
        o = maybe_o
        print(f"ADDED: {o}")
    else:
        print(f"EXISTING: {o}")
        if replace_source and o.source != source:
            o.source = source
            o.s(**kwargs)
            print(f"Replaced source: {o}")
    return o


@command
def mocc(
    t: Taxon | None = None,
    source: Article | None = None,
    *,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    if t is None:
        t = Taxon.getter(None).get_one("taxon> ")
    if t is None:
        return
    if source is None:
        source = Article.getter(None).get_one("source> ")
    if source is None:
        return
    while True:
        loc = models.Location.getter(None).get_one("location> ")
        if loc is None:
            break
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


@command
def multi_taxon(
    loc: models.Location | None = None,
    source: Article | None = None,
    *,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    if loc is None:
        loc = models.Location.getter(None).get_one("location> ")
    if loc is None:
        return
    if source is None:
        source = Article.getter(None).get_one("source> ")
    if source is None:
        return
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
    _save_all_caches(warm=False)


@command
def warm_all_caches() -> None:
    _save_all_caches(warm=True)


def _save_all_caches(*, warm: bool = True) -> None:
    for model in models.BaseModel.__subclasses__():
        if hasattr(model, "label_field"):
            getter = model.getter(None)
            if warm:
                print(f"{model}: warming None getter")
                getter.rewarm_cache()
            getter.save_cache()
        for name, field_obj in model.clirm_fields.items():
            getter = model.getter(name)
            if field_obj.name in model.fields_without_completers:
                continue
            if field_obj.type_object is str:
                if warm:
                    print(f"{model}: warming {name} ({field_obj})")
                    getter.rewarm_cache()
                getter.save_cache()


@command
def rename_specimen_photos(*, dry_run: bool = True) -> None:
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
        min_year = datetime.datetime.now(tz=datetime.UTC).year - 3
    query = Article.select_valid().filter(Article.citation_group == cg)
    # the format is {volume: {issue: [articles]}}
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
    collection.rename_type_specimens(full=True)


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
    data = httpx.get(url).text
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
            for issue in models.name.lint.check_organ_tag(tag):
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
            cls,
            # static analysis: ignore[undefined_attribute]
            query=cls.select_valid().order_by(cls.id.desc()).limit(limit),
        )


def maybe_rename_paper(art: Article) -> str | None:
    if art.name.startswith(("Theria ", "Placentalia ")):
        return re.sub(r"^(Theria|Placentalia) ", "Mammalia ", art.name)
    if art.name.endswith(("-review.pdf", "-revision.pdf", "-taxonomy.pdf")):
        authors = art.get_authors()
        if len(authors) == 1:
            citation = authors[0].family_name
        elif len(authors) == 2:
            citation = f"{authors[0].family_name} & {authors[1].family_name}"
        else:
            citation = f"{authors[0].family_name} et al."
        citation = unidecode.unidecode(citation)
        replacement = f" ({citation} {art.numeric_year()}).pdf"
        return re.sub(r"-(review|revision|taxonomy)\.pdf$", replacement, art.name)
    return None


@command
def rename_papers(query: Iterable[Article] | None = None) -> None:
    if query is None:
        query = Article.select_valid()
    for art in query:
        new_name = maybe_rename_paper(art)
        if new_name is None:
            continue
        getinput.print_header(art)
        art.display()
        if Article.select().filter(Article.name == new_name).count() > 0:
            print("new name already exists")
            continue
        if getinput.yes_no(f"rename to {new_name!r}? "):
            art.move(new_name)
        else:
            art.edit()


def is_more_less_specific_sequence(persons: Iterable[Person]) -> bool:
    persons = sorted(persons, key=lambda pers: pers.get_full_name())
    return all(right.is_more_specific_than(left) for left, right in pairwise(persons))


def sum_year_ranges(persons: Iterable[Person]) -> tuple[int, int] | None:
    min_year: int | None = None
    max_year: int | None = None
    for person in persons:
        year_range = person.get_active_year_range()
        if year_range is None:
            continue
        new_min, new_max = year_range
        if min_year is None or new_min < min_year:
            min_year = new_min
        if max_year is None or new_max > max_year:
            max_year = new_max
    if min_year is None or max_year is None:
        return None
    return min_year, max_year


@command
def find_potential_person_clusters(*, interactive: bool = True) -> None:
    for person in Person.select_valid().filter(
        Person.given_names == None, Person.initials == None
    ):
        all_persons = list(
            Person.select_valid().filter(
                Person.family_name == person.family_name,
                Person.type.is_not_in(
                    (PersonType.soft_redirect, PersonType.hard_redirect)
                ),
            )
        )
        if len(all_persons) <= 1:
            continue
        if not is_more_less_specific_sequence(all_persons):
            continue
        total_range = sum_year_ranges(all_persons)
        if total_range is None:
            continue
        min_year, max_year = total_range
        # probably not the same person
        if max_year - min_year > 70:
            continue
        getinput.print_header(person.family_name)
        for related_person in all_persons:
            related_person.display(full=True)
        if interactive:
            reassign_references(person.family_name, substring=False)


@command
def fix_lints(
    bad: Mapping[Any, list[tuple[Any, list[str]]]], cls: type[models.BaseModel]
) -> None:
    """Use this after running bad = run_maintenance()."""
    for obj, lints in bad[cls.lint_all]:
        obj.load()
        getinput.print_header(obj)
        obj.display()
        for lint in lints:
            print(lint)
        obj.edit_until_clean()


@command
def run_command_shell() -> None:
    """Runs a shell for running commands."""
    cb_map = models.base.get_static_callbacks()
    while True:
        try:
            cmd = getinput.get_with_completion(
                ["q"], "> ", callbacks=cb_map, history_key=run_command_shell
            )
        except getinput.StopException:
            break
        if cmd == "q":
            break


def lint_collections() -> None:
    cfg = models.base.LintConfig()
    total = models.Collection.select_valid().count()
    print(f"{total} total")
    for coll in getinput.print_every_n(
        # static analysis: ignore[undefined_attribute]
        models.Collection.select_valid().order_by(models.Collection.id.desc()),  # type: ignore[attr-defined]
        n=5,
        label=f"of {total} collections",
    ):
        types = list(coll.type_specimens)
        issues = {
            nam: lints
            for nam in types
            if (lints := list(models.name.lint.check_type_specimen(nam, cfg)))
        }
        if not issues:
            print(f"{coll} ({len(types)} types) is clean")
            continue
        getinput.print_header(coll)
        coll.display(full=True)
        for nam, lints in issues.items():
            nam.display(full=False)
            print("      ", nam.type_specimen)
            for lint in lints:
                print("      ", lint)
        print(f"{len(types)} names, {len(issues)} with issues")
        coll.edit()
        run_linter_and_fix(
            Name, query=coll.type_specimens, linter=models.name.lint.check_type_specimen
        )


@command
def find_valid_names_with_invalid_bases() -> None:
    for txn in Taxon.select_valid().filter(
        Taxon.age.is_in((AgeClass.extant, AgeClass.recently_extinct)),
        Taxon.rank == Rank.species,
    ):
        if (
            txn.base_name.nomenclature_status
            not in (
                NomenclatureStatus.available,
                NomenclatureStatus.nomen_novum,
                NomenclatureStatus.as_emended,
                NomenclatureStatus.informal,
            )
            and txn.base_name.status is constants.Status.valid
        ):
            txn.display()


@command
def download_bhl_parts(
    nams: Iterable[Name] | None = None, *, dry_run: bool = False
) -> None:
    options = get_options()
    if nams is None:
        nams = Name.with_type_tag(TypeTag.AuthorityPageLink).filter(
            Name.original_citation == None
        )
    for nam in nams:
        nam.load()
        if nam.original_citation is not None:
            continue
        for tag in nam.type_tags:
            if not isinstance(tag, TypeTag.AuthorityPageLink):
                continue
            parsed = urlparse.parse_url(tag.url)
            if not isinstance(parsed, urlparse.BhlPage):
                continue
            for part_id in bhl.get_possible_parts_from_page(int(parsed.page_id)):
                url = f"https://www.biodiversitylibrary.org/partpdf/{part_id}"
                message = f"download {part_id} for {nam}"
                if dry_run:
                    print(message)
                    continue
                if not getinput.yes_no(f"{message}?"):
                    return
                print("Downloading:")
                # Line by itself for easier copy-pasting
                print(url)
                response = httpx.get(url)
                path = options.new_path / f"{part_id}.pdf"
                path.write_bytes(response.content)
                print("Adding part for name", nam)
                models.article.check.check_new()


@command
def download_bhl_items(
    nams: Iterable[Name] | None = None, *, dry_run: bool = False
) -> None:
    options = get_options()
    if nams is None:
        nams = Name.with_type_tag(TypeTag.AuthorityPageLink).filter(
            Name.original_citation == None
        )
    for nam in nams:
        nam.load()
        if nam.original_citation is not None:
            continue
        for tag in nam.type_tags:
            if not isinstance(tag, TypeTag.AuthorityPageLink):
                continue
            item_id = bhl.get_bhl_item_from_url(tag.url)
            if item_id is None:
                continue
            url = f"https://www.biodiversitylibrary.org/itempdf/{item_id}"
            message = f"download {item_id} for {nam} ({nam.verbatim_citation})"
            if dry_run:
                print(message)
                continue
            if not getinput.yes_no(f"{message}?"):
                return
            print("Downloading:")
            # Line by itself for easier copy-pasting
            print(url)
            response = httpx.get(url, follow_redirects=True)
            path = options.burst_path / f"{item_id}.pdf"
            path.write_bytes(response.content)
            subprocess.check_call(["open", path])
            if getinput.yes_no("catalog as one item? "):
                shutil.move(path, options.new_path)
            print("Adding item for name", nam)
            models.article.check.check_new()


@command
def fill_bhl_names(nams: Iterable[Name] | None = None) -> None:
    if nams is None:
        nams = Name.with_type_tag(TypeTag.AuthorityPageLink).filter(
            Name.original_citation == None
        )
    for nam in nams:
        nam.load()
        if nam.original_citation is not None:
            continue
        tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
        if not tags:
            continue
        getinput.print_header(nam)
        if nam.citation_group is not None:
            year = nam.numeric_year()
            nam.citation_group.for_years(year - 1, year + 2, include_articles=True)
        for tag in tags:
            subprocess.check_call(["open", tag.url])
        nam.edit()


@command
def print_data_level_report() -> None:
    """Output as of March 2024:

    OCDL | NDL         missing_crucial_fields  missing_required_fields  missing_details_tags  missing_derived_tags  nothing_needed
    no_citation        10281                   17504                    62                    67                    470
    no_data            0                       0                        932                   785                   21602
    some_data          1                       673                      3103                  343                   304
    all_required_data  38                      9065                     0                     3504                  33073

    """
    counts: Counter[tuple[OriginalCitationDataLevel, NameDataLevel]] = Counter()
    for nam in Name.select_valid():
        ocdl, _ = nam.original_citation_data_level()
        ndl, _ = nam.name_data_level()
        counts[(ocdl, ndl)] += 1
    rows = [
        ["OCDL | NDL", *[level.name for level in NameDataLevel]],
        *[
            [ocdl.name, *[str(counts[(ocdl, ndl)]) for ndl in NameDataLevel]]
            for ocdl in OriginalCitationDataLevel
        ],
    ]
    getinput.print_table(rows)


@command
def edit_names_at_level(query: Iterable[Name] | None = None) -> None:
    ocdl = getinput.get_enum_member(
        OriginalCitationDataLevel, "original citation data level> "
    )
    if ocdl is None:
        return
    ndl = getinput.get_enum_member(NameDataLevel, "name data level> ")
    if ndl is None:
        return
    if query is None:
        query = Name.select_valid()
    for nam in query:
        name_ocdl, _ = nam.original_citation_data_level()
        if name_ocdl is not ocdl:
            continue
        name_ndl, _ = nam.name_data_level()
        if name_ndl is not ndl:
            continue
        getinput.print_header(nam)
        nam.display()
        nam.edit()


@command
def confirm_zmmu_types() -> None:
    coll = Collection.getter("label")("ZMMU")
    assert coll is not None
    for nam in coll.type_specimens:
        if (
            nam.corrected_original_name is not None
            and nam.corrected_original_name < "Sciurus"
        ):
            continue
        if not nam.type_specimen:
            continue
        if nam.has_type_tag(models.name.TypeTag.TypeSpecimenLinkFor):
            continue
        nam.load()
        for spec in models.name.type_specimen.parse_type_specimen(nam.type_specimen):
            if not isinstance(spec, models.name.type_specimen.Specimen):
                continue
            if not isinstance(spec.base, models.name.type_specimen.SimpleSpecimen):
                continue
            m = re.fullmatch(r"ZMMU (S-\d+)", spec.base.text)
            assert m, spec
            number = m.group(1)
            url = f"https://zmmu.msu.ru/dbs/list_record.php?id={number}"
            getinput.print_header(nam)
            nam.display()
            print(url)
            subprocess.check_call(["open", url])
            if not getinput.yes_no("confirm? "):
                continue
            nam.add_type_tag(
                models.name.TypeTag.TypeSpecimenLinkFor(url, spec.stringify())
            )
            models.article.check.check_new()


@command
def add_bhl_pages_by_cg() -> None:
    for cg in CitationGroup.select_valid():
        if not cg.get_bhl_title_ids():
            continue
        getinput.print_header(cg)
        cg.interactively_add_bhl_urls()


@command
def add_coordinates(names: Iterable[Name]) -> None:
    nams = [
        nam
        for nam in names
        if "type_locality" in nam.get_required_fields()
        and not nam.has_type_tag(TypeTag.Coordinates)
    ]
    print(f"{len(nams)} names without coordinates")
    for nam in nams:
        getinput.print_header(nam)
        nam.display()
        for tag in nam.type_tags:
            if isinstance(tag, TypeTag.LocationDetail):
                print(tag.text)
        nam.edit()
        nam.edit_until_clean()


@command
def set_network_available() -> None:
    available = getinput.yes_no("Is the network available? ")
    config.set_network_available(value=available)


@command
def textual_rank_report() -> None:
    for rank in sorted(NEED_TEXTUAL_RANK):
        getinput.print_header(f"{rank!r} (CE)")
        ces = list(
            ClassificationEntry.select_valid().filter(ClassificationEntry.rank == rank)
        )
        print(f"Total entries: {len(ces)}")
        by_text: Counter[str] = Counter()
        for ce in ces:
            for tag in ce.get_tags(ce.tags, ClassificationEntryTag.TextualRank):
                by_text[tag.text.casefold()] += 1
        for text, count in by_text.most_common():
            print(f"{count} {text}")

        getinput.print_header(f"{rank!r} (name)")
        nams = list(Name.select_valid().filter(Name.original_rank == rank))
        print(f"Total names: {len(nams)}")
        by_text = Counter()
        for nam in nams:
            for tag in nam.get_tags(nam.type_tags, TypeTag.TextualOriginalRank):
                by_text[tag.text.casefold()] += 1
        for text, count in by_text.most_common():
            print(f"{count} {text}")


@command
def add_ces_for_new_genera(up_to: int) -> None:
    for nam in (
        Name.select_valid()
        .filter(
            Name.original_rank == Rank.genus,
            Name.nomenclature_status == NomenclatureStatus.available,
            Name.year < up_to,
            Name.original_citation != None,
        )
        .order_by(Name.year.desc())
    ):
        art = nam.original_citation
        if art is None:
            continue
        if nam.taxon.age is not AgeClass.extant:
            continue
        nams = list(art.get_new_names())
        if any(other_nam.original_parent == nam for other_nam in nams):
            continue
        getinput.print_header(nam)
        art.display_names()
        print(f"{art} is the original citation of {nam}")
        art.edit()


@command
def add_ces_for_parent_species(up_to: int) -> None:
    for nam in (
        Name.select_valid()
        .filter(
            Name.original_rank.is_in((Rank.subspecies, Rank.variety)),
            Name.nomenclature_status == NomenclatureStatus.available,
            Name.year < up_to,
            Name.original_citation != None,
        )
        .order_by(Name.year.desc())
    ):
        art = nam.original_citation
        if art is None:
            continue
        if nam.taxon.age is not AgeClass.extant:
            continue
        if (
            nam.corrected_original_name is None
            or nam.corrected_original_name.count(" ") != 2
        ):
            continue
        gen, sp, ssp = nam.corrected_original_name.split(" ")
        species_name = f"{gen} {sp}"
        existing = (
            Name.select_valid()
            .filter(
                Name.corrected_original_name == species_name,
                Name.original_rank == Rank.species,
            )
            .count()
        )
        if existing > 0:
            continue
        getinput.print_header(nam)
        art.display_names()
        print(f"{art} is the original citation of {nam}")
        art.edit()
        art.lint_object_list(art.new_names)
        art.lint_object_list(art.classification_entries)


@command
def missing_valid_species(
    rank: Rank = Rank.species, taxon: Taxon | None = None
) -> list[Name]:
    if taxon is None:
        taxon = Taxon.getter(None).get_one()
        if taxon is None:
            return []
    relevant_nams = [
        t.base_name
        for t in taxon.children_of_rank(rank)
        if t.age in (AgeClass.extant, AgeClass.recently_extinct)
        and t.base_name.status is constants.Status.valid
    ]
    return _get_missing_names(relevant_nams, rank)


def _get_missing_names(nams: list[Name], rank: Rank) -> list[Name]:
    missing_nams = [
        nam
        for nam in nams
        if nam.original_citation is None
        or nam.original_citation.kind is ArticleKind.no_copy
    ]
    if nams:
        print(
            f"Missing {len(missing_nams)}/{len(nams)} ({len(missing_nams) / len(nams):%}) valid {rank.name}"
        )
    return sorted(missing_nams, key=lambda nam: nam.numeric_year())


@command
def missing_valid_species_of_article(
    art: Article | None = None, rank: Rank = Rank.species
) -> list[Name]:
    if art is None:
        art = Article.getter(None).get_one("article> ")
    if art is None:
        return []
    nams = species_of_article(art, rank)
    return _get_missing_names(nams, rank)


def species_of_article(art: Article, rank: Rank = Rank.species) -> list[Name]:
    ces = _species_ces_of_article(art, rank)
    return [
        ce.mapped_name.resolve_variant() for ce in ces if ce.mapped_name is not None
    ]


def _species_ces_of_article(art: Article, rank: Rank) -> Iterable[ClassificationEntry]:
    yield from ClassificationEntry.select_valid().filter(
        ClassificationEntry.article == art, ClassificationEntry.rank == rank
    )
    for child in art.get_children():
        yield from _species_ces_of_article(child, rank)


@command
def fill_in_type_localities() -> None:
    for species in Taxon.select_valid().filter(
        Taxon.age.is_in((AgeClass.extant, AgeClass.recently_extinct)),
        Taxon.rank == Rank.species,
    ):
        if species.get_derived_field("class_").valid_name != "Mammalia":
            continue
        nam = species.base_name
        if nam.original_citation is None:
            continue
        if any(
            (
                isinstance(tag, TypeTag.LocationDetail)
                and tag.source == nam.original_citation
            )
            or tag is TypeTag.NoLocation
            for tag in nam.type_tags
        ):
            continue
        if "type_locality" not in nam.get_required_fields():
            continue
        getinput.print_header(species)
        art = nam.original_citation
        art.display_classification_entries()
        art.display_names()
        print("Missing TL for", nam)
        art.edit()


@dataclass
class _TagCount:
    total: int = 0
    unique_objs: int = 0
    field_to_presence: Counter[str] = field(default_factory=Counter)
    field_to_counts: dict[str, Counter[object]] = field(
        default_factory=lambda: defaultdict(Counter)
    )


def model_selector() -> type[models.BaseModel] | None:
    classes = {cls.__name__: cls for cls in models.BaseModel.__subclasses__()}
    choice = getinput.get_with_completion(classes, "Model> ", disallow_other=True)
    if choice is None:
        return None
    return classes[choice]


@command
def tag_counter() -> None:
    model = model_selector()
    if model is None:
        return
    possible_fields = {
        name: field
        for name, field in model.clirm_fields.items()
        if isinstance(field, models.base.ADTField)
    }
    if len(possible_fields) == 0:
        print(f"{model} has no ADT fields")
        return
    elif len(possible_fields) == 1:
        field_name = next(iter(possible_fields))
    else:
        field_name = getinput.get_with_completion(
            possible_fields, "Field> ", disallow_other=True
        )
    if field_name is None:
        return

    counts: dict[type[ADT], _TagCount] = defaultdict(_TagCount)
    for obj in model.select_valid():
        seen_for_nam = set()
        for tag in getattr(obj, field_name):
            if tag._has_args:
                tag_type = type(tag)
            else:
                tag_type = tag
            cnt = counts[tag_type]
            cnt.total += 1
            if tag_type not in seen_for_nam:
                cnt.unique_objs += 1
                seen_for_nam.add(tag_type)
            if tag._has_args:
                for field in tag_type.__annotations__:
                    value = getattr(tag, field)
                    if value is not None:
                        cnt.field_to_presence[field] += 1
                        cnt.field_to_counts[field][value] += 1

    for tag_type, cnt in sorted(
        counts.items(), key=lambda item: item[1].total, reverse=True
    ):
        print(f"{tag_type}: {cnt.total} total, {cnt.unique_objs} unique")
        for field, presence in sorted(
            cnt.field_to_presence.items(), key=lambda item: item[1], reverse=True
        ):
            percent = f"{presence / cnt.total:.2%}"
            print(f"  {field}: {presence} ({percent})")
            for value, count in sorted(
                cnt.field_to_counts[field].items(),
                key=lambda item: item[1],
                reverse=True,
            )[:5]:
                percent = f"{count / presence:.2%}"
                print(f"    {value}: {count} ({percent})")


def run_shell() -> None:
    # GC does bad things on my current setup for some reason
    gc.disable()
    for cs in COMMAND_SETS:
        for cmd in cs.commands:
            _register_command(cmd)
    config = Config()
    config.InteractiveShell.confirm_exit = False
    config.TerminalIPythonApp.display_banner = False
    lib_file = Path(__file__).parent / "lib.py"
    IPython.start_ipython(argv=[str(lib_file), "-i"], config=config, user_ns=ns)


if __name__ == "__main__":
    run_shell()
