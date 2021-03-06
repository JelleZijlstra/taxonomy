"""

File that gets loaded on startup of the taxonomy shell.

Contents overlap with shell.py, which defines "commands".

"""
from collections import defaultdict
from functools import partial
from typing import Any, Container, Dict, Iterable, List, Optional, Tuple, Type, Union

import peewee

from taxonomy import getinput
from taxonomy.db.constants import AgeClass, Group, Rank, Status
from taxonomy.db.models import (
    Article,
    BaseModel,
    CitationGroup,
    Collection,
    Location,
    Name,
    Occurrence,
    Period,
    Person,
    Taxon,
)


def occ(
    t: Taxon,
    loc: Location,
    source: Optional[Article] = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> Occurrence:
    if source is None:
        source = s  # type: ignore  # noqa
    try:
        o = t.at(loc)
    except Occurrence.DoesNotExist:
        o = t.add_occurrence(loc, source, **kwargs)
        print("ADDED: %s" % o)
    else:
        print("EXISTING: %s" % o)
        if replace_source and o.source != source:
            o.source = source
            o.s(**kwargs)
            o.save()
            print("Replaced source: %s" % o)
    return o


def occur(
    t: Taxon,
    locs: Iterable[Location],
    source: Optional[Article] = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


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


def mocc(
    t: Taxon,
    locs: Iterable[Location],
    source: Optional[Article] = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


def multi_taxon(
    ts: Iterable[Taxon],
    loc: Location,
    source: Optional[Article] = None,
    replace_source: bool = False,
    **kwargs: Any,
) -> None:
    for t in ts:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


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


def h(author: str, year: int) -> Tuple[List[Article], List[Name]]:
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
        print(repr(art))
    getinput.print_header(f"Names by {author} ({year})")
    for nam in nams:
        nam.display(full=False)
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
