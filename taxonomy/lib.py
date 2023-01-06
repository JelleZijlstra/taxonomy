"""

File that gets loaded on startup of the taxonomy shell.

Contents overlap with shell.py, which defines "commands".

"""
from collections import defaultdict
from functools import partial
import os
import re
from taxonomy.db.models.name import TypeTag
from typing import Any, Container, Dict, Iterable, List, Optional, Tuple, Type, Union

import peewee

from taxonomy import getinput
from taxonomy.db.constants import (
    AgeClass,
    FillDataLevel,
    Group,
    Rank,
    RegionKind,
    Status,
)
from taxonomy.db.helpers import root_name_of_name
from taxonomy.db.models import (
    Article,
    BaseModel,
    Book,
    CitationGroup,
    Collection,
    Location,
    Name,
    Occurrence,
    Period,
    Person,
    Region,
    Taxon,
)
from taxonomy.db.models.person import AuthorTag


def biggest_citation_groups_no_region(
    limit: int = 50,
) -> List[Tuple[CitationGroup, int]]:
    query = (
        CitationGroup.select(
            CitationGroup, peewee.fn.Count(CitationGroup.id).alias("num_names")
        )
        .filter(CitationGroup.region == None)
        .join(Name, peewee.JOIN_LEFT_OUTER)
        .group_by(CitationGroup.id)
        .order_by(peewee.fn.Count(CitationGroup.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_names) for t in query]))


def biggest_citation_groups(limit: int = 50) -> List[Tuple[CitationGroup, int]]:
    query = (
        CitationGroup.select(
            CitationGroup, peewee.fn.Count(CitationGroup.id).alias("num_names")
        )
        .join(Name, peewee.JOIN_LEFT_OUTER)
        .group_by(CitationGroup.id)
        .order_by(peewee.fn.Count(CitationGroup.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_names) for t in query]))


def biggest_localities(limit: int = 50) -> List[Tuple[Location, int]]:
    query = (
        Location.select(
            Location, peewee.fn.Count(Occurrence.id).alias("num_occurrences")
        )
        .join(Occurrence, peewee.JOIN_LEFT_OUTER)
        .group_by(Location.id)
        .order_by(peewee.fn.Count(Occurrence.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_occurrences) for t in query]))


def most_type_localities(limit: int = 50) -> List[Tuple[Location, int]]:
    query = (
        Location.select(Location, peewee.fn.Count(Name.id).alias("num_occurrences"))
        .join(Name, peewee.JOIN_LEFT_OUTER)
        .group_by(Location.id)
        .order_by(peewee.fn.Count(Name.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_occurrences) for t in query]))


def biggest_ranges(limit: int = 50) -> List[Tuple[Taxon, int]]:
    query = (
        Taxon.select(Taxon, peewee.fn.Count(Occurrence.id).alias("num_occurrences"))
        .join(Occurrence, peewee.JOIN_LEFT_OUTER)
        .group_by(Taxon.id)
        .order_by(peewee.fn.Count(Occurrence.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_occurrences) for t in query]))


def most_type_specimens(limit: int = 50) -> List[Tuple[Collection, int]]:
    query = (
        Collection.select(Collection, peewee.fn.Count(Name.id).alias("num_types"))
        .join(Name, peewee.JOIN_LEFT_OUTER)
        .group_by(Collection.id)
        .order_by(peewee.fn.Count(Name.id).desc())
        .limit(limit)
    )
    return list(reversed([(t, t.num_types) for t in query]))


def unrecorded_taxa(root: Taxon) -> None:
    def has_occurrence(taxon: Taxon) -> bool:
        return taxon.occurrences.count() > 0

    if root.age is AgeClass.fossil:
        return

    if root.rank is Rank.species:
        if not has_occurrence(root) and not any(
            has_occurrence(child) for child in root.children
        ):
            print(root)
    else:
        for taxon in root.children:
            unrecorded_taxa(taxon)


def move_localities(period: Period) -> None:
    for location in Location.filter(
        Location.max_period == period, Location.min_period == period
    ):
        location.min_period = location.max_period = None
        location.stratigraphic_unit = period
        location.save()


def move_to_stratigraphy(loc: Location, period: Period) -> None:
    loc.stratigraphic_unit = period
    loc.min_period = loc.max_period = None
    loc.save()


def count_field(model: Type[BaseModel], field: str) -> List[Tuple[Any, int]]:
    field_obj = getattr(model, field)
    return [
        (getattr(t, field), t.num)
        for t in model.select(field_obj, peewee.fn.Count().alias("num"))
        .group_by(field_obj)
        .order_by(peewee.fn.Count().desc())
    ]


def locless_names(
    genus: Taxon,
    attribute: str = "type_locality",
    age: Optional[AgeClass] = AgeClass.removed,
    min_year: Optional[int] = None,
    exclude: Container["Taxon"] = frozenset(),
) -> List[Name]:
    if age is AgeClass.removed:
        age = genus.age
    nams = list(
        genus.names_missing_field(
            attribute, age=age, min_year=min_year, exclude=exclude
        )
    )
    for nam in nams:
        nam.display()
    return nams


def names_with_attribute(
    txn: Taxon,
    attribute: str,
    age: Optional[AgeClass] = None,
    exclude: Container["Taxon"] = frozenset(),
) -> List[Name]:
    nams = [
        name
        for name in txn.all_names(age=age, exclude=exclude)
        if getattr(name, attribute) is not None
    ]
    for nam in nams:
        nam.display()
    return nams


def f(
    nams: Union[Name, Taxon, List[Name], List[Taxon]],
    skip_fields: Container[str] = frozenset(),
    always_edit: bool = False,
) -> None:
    if isinstance(nams, list):
        nam_or_taxon = nams[0]
    else:
        nam_or_taxon = nams
    if isinstance(nam_or_taxon, Taxon):
        nam = nam_or_taxon.base_name
    else:
        nam = nam_or_taxon
    nam.display()
    edited_any = nam.fill_required_fields(skip_fields=skip_fields)
    if always_edit and not edited_any:
        nam.e.type_tags


g = partial(
    f,
    skip_fields={"original_citation", "type_specimen", "collection", "genus_type_kind"},
)


def h(
    author: str, year: int, page: Optional[int] = None, uncited_only: bool = False
) -> Tuple[List[Article], List[Name]]:
    authors = Person.select_valid().filter(Person.family_name == author)
    nams = []
    arts = []
    for aut in authors:
        for art in aut.get_sorted_derived_field("articles"):
            if art.numeric_year() == year:
                arts.append(art)
        for nam in aut.get_sorted_derived_field("names"):
            if nam.numeric_year() == year:
                nams.append(nam)
    getinput.print_header(f"Articles by {author} ({year})")
    for art in arts:
        if page is not None and not art.is_page_in_range(page):
            continue
        print(repr(art))
    getinput.print_header(f"Names by {author} ({year})")
    for nam in nams:
        if page is not None and (
            nam.page_described is None or str(page) not in nam.page_described
        ):
            continue
        if uncited_only and nam.original_citation is not None:
            continue
        nam.display(full=False)
        indent = " " * 8
        if nam.verbatim_citation:
            print(f"{indent}{nam.verbatim_citation}")
        if nam.citation_group:
            print(f"{indent}{nam.citation_group}")
    return arts, nams


def set_page(nams: Iterable[Name]) -> None:
    for nam in nams:
        if nam.verbatim_citation is not None and nam.page_described is None:
            nam.display()
            print(nam.verbatim_citation)
            nam.e.page_described


class _NamesGetter:
    def __init__(self, group: Group) -> None:
        self._cache: Optional[Dict[str, List[Name]]] = None
        self._group = group

    def __getattr__(self, attr: str) -> List[Name]:
        return list(
            Name.filter(
                Name.group == self._group,
                Name.status != Status.removed,
                Name.root_name == attr,
            )
        )

    def __dir__(self) -> Iterable[str]:
        self._fill_cache()
        assert self._cache is not None
        yield from self._cache.keys()
        yield from super().__dir__()

    def _fill_cache(self) -> None:
        if self._cache is not None:
            return
        self._cache = defaultdict(list)
        for nam in Name.filter(
            Name.group == self._group, Name.status != Status.removed
        ):
            self._cache[nam.root_name].append(nam)

    def clear_cache(self) -> None:
        self._cache = None


ns = _NamesGetter(Group.species)
gs = _NamesGetter(Group.genus)


def edit_at_level(level: FillDataLevel = FillDataLevel.incomplete_derived_tags) -> None:
    txn = Taxon.getter(None).get_one()
    if txn is None:
        return
    edit_names(txn.all_names(), level)


def edit_by_author(
    level: FillDataLevel = FillDataLevel.incomplete_derived_tags,
) -> None:
    txn = Person.getter(None).get_one()
    if txn is None:
        return
    nams = txn.get_sorted_derived_field("names")
    edit_names(nams, level)


def edit_names(
    nam_iter: Iterable[Name],
    level: FillDataLevel = FillDataLevel.incomplete_derived_tags,
) -> None:
    nams = sorted(
        nam_iter,
        key=lambda nam: (nam.numeric_year(), nam.numeric_page_described()),
    )
    print(f"{len(nams)} total names")
    for i, nam in enumerate(nams):
        if i % 100 == 0:
            percentage = i / len(nams) * 100
            getinput.print_header(f"{i}/{len(nams)} ({percentage:.2f}%) done")
        while True:
            name_level, _ = nam.fill_data_level()
            if name_level != level:
                break
            nam.display()
            nam.edit()


def make_genus() -> Taxon | None:
    parent = Taxon.getter(None).get_one("parent> ")
    if parent is None:
        return None
    name = Taxon.getter("valid_name").get_one_key("name> ")
    if name is None:
        return None
    try:
        existing = Taxon.select_valid().filter(Taxon.valid_name == name).get()
        print(f"{existing} already exists")
        return None
    except Taxon.DoesNotExist:
        pass
    authors = []
    while True:
        author = Person.getter("family_name").get_one_key("author> ")
        if author is None:
            break
        authors.append(author)
    year = Name.getter("year").get_one_key("year> ")
    if year is None:
        return None
    people = [Person.get_or_create_unchecked(name) for name in authors]
    tags = [AuthorTag.Author(person=person) for person in people]
    new_taxon = parent.add_static(Rank.genus, name, year, author_tags=tags)
    new_taxon.display()
    return new_taxon


def make_species() -> Taxon | None:
    parent = Taxon.getter(None).get_one("parent> ")
    if parent is None or parent.rank is not Rank.genus:
        return None
    original_name = Name.getter("original_name").get_one_key("name> ")
    if original_name is None:
        return None
    root_name = root_name_of_name(original_name, Rank.species)
    name = f"{parent.valid_name} {root_name}"
    try:
        existing = Taxon.select_valid().filter(Taxon.valid_name == name).get()
        print(f"{existing} already exists")
        return None
    except Taxon.DoesNotExist:
        pass
    authors = []
    while True:
        author = Person.getter("family_name").get_one_key("author> ")
        if author is None:
            break
        authors.append(author)
    year = Name.getter("year").get_one_key("year> ")
    if year is None:
        return None
    clean_year = re.sub(r" .*", "", year)
    page = Name.getter("page_described").get_one_key("page> ")
    location = getinput.get_line("location> ")
    if location:
        tag = TypeTag.LocationDetail(
            location, Article.get(name="Geoplaninae (Ogren & Kawakatsu 1990).pdf")
        )
        type_tags = [tag]
    else:
        type_tags = []
    people = [Person.get_or_create_unchecked(name) for name in authors]
    key = (
        tuple(name.lower().replace("von ", "").replace("du ", "") for name in authors),
        year,
    )
    verbatim_citation = refs.get(key)  # type: ignore
    if verbatim_citation is not None:
        print(f"found cite: {verbatim_citation}")
    tags = [AuthorTag.Author(person=person) for person in people]
    new_taxon = parent.add_static(
        Rank.species,
        name,
        clean_year,
        author_tags=tags,
        page_described=page,
        root_name=root_name,
        original_name=original_name,
        verbatim_citation=verbatim_citation,
        type_tags=type_tags,
    )
    new_taxon.display()
    return new_taxon


def print_prefix(prefix: str) -> None:
    books = Book.select_valid().filter(Book.dewey.startswith(prefix))
    for book in sorted(
        books,
        key=lambda book: (
            book.dewey,
            tuple(
                author.get_description(family_first=True)
                for author in book.get_authors()
            ),
        ),
    ):
        print(book.dewey, repr(book))


def clean_regions(kind: RegionKind, print_only: bool = False) -> None:
    regions = Region.bfind(kind=kind, quiet=True)
    print(f"{len(regions)} total")
    by_parent: dict[Region, list[Region]] = defaultdict(list)
    for region in regions:
        by_parent[region.parent].append(region)
    for region, children in by_parent.items():
        print(f"== {region} ==")
        print(", ".join(child.name for child in children))
        if print_only:
            continue
        new_kind = getinput.get_enum_member(RegionKind, "new kind> ")
        if new_kind:
            for child in children:
                print(child)
                child.kind = new_kind  # type: ignore
            print("Perform fixup")
            Region.getter(None).get_and_edit()
        else:
            if getinput.yes_no("Edit manually?"):
                for child in children:
                    print(child)
                    child.e.kind
