import peewee
from typing import Any, Iterable, List, Optional, Tuple

from taxonomy.db.constants import Age, Rank
from taxonomy.db.models import Collection, Location, Name, Occurrence, Period, Taxon


def occ(t: Taxon, loc: Location, source: Optional[str] = None, replace_source: bool = False, **kwargs: Any) -> Occurrence:
    if source is None:
        source = s  # type: ignore  # noqa
    try:
        o = t.at(loc)
    except Occurrence.DoesNotExist:
        o = t.add_occurrence(loc, source, **kwargs)
        print('ADDED: %s' % o)
    else:
        print('EXISTING: %s' % o)
        if replace_source and o.source != source:
            o.source = source
            o.s(**kwargs)
            o.save()
            print('Replaced source: %s' % o)
    return o


def occur(t: Taxon, locs: Iterable[Location], source: Optional[str] = None, replace_source: bool = False, **kwargs: Any) -> None:
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


def biggest_localities(limit: int = 50) -> List[Tuple[Location, int]]:
    query = Location \
        .select(Location, peewee.fn.Count(Occurrence.id).alias('num_occurrences')) \
        .join(Occurrence, peewee.JOIN_LEFT_OUTER) \
        .group_by(Location.id) \
        .order_by(peewee.fn.Count(Occurrence.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_occurrences) for t in query]))


def most_type_localities(limit: int = 50) -> List[Tuple[Location, int]]:
    query = Location \
        .select(Location, peewee.fn.Count(Name.id).alias('num_occurrences')) \
        .join(Name, peewee.JOIN_LEFT_OUTER) \
        .group_by(Location.id) \
        .order_by(peewee.fn.Count(Name.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_occurrences) for t in query]))


def biggest_ranges(limit: int = 50) -> List[Tuple[Taxon, int]]:
    query = Taxon \
        .select(Taxon, peewee.fn.Count(Occurrence.id).alias('num_occurrences')) \
        .join(Occurrence, peewee.JOIN_LEFT_OUTER) \
        .group_by(Taxon.id) \
        .order_by(peewee.fn.Count(Occurrence.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_occurrences) for t in query]))


def most_type_specimens(limit: int = 50) -> List[Tuple[Collection, int]]:
    query = Collection \
        .select(Collection, peewee.fn.Count(Name.id).alias('num_types')) \
        .join(Name, peewee.JOIN_LEFT_OUTER) \
        .group_by(Collection.id) \
        .order_by(peewee.fn.Count(Name.id).desc()) \
        .limit(limit)
    return list(reversed([(t, t.num_types) for t in query]))


def mocc(t: Taxon, locs: Iterable[Location], source: Optional[str] = None, replace_source: bool = False, **kwargs: Any) -> None:
    for loc in locs:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


def multi_taxon(ts: Iterable[Taxon], loc: Location, source: Optional[str] = None, replace_source: bool = False, **kwargs: Any) -> None:
    for t in ts:
        occ(t, loc, source=source, replace_source=replace_source, **kwargs)


def unrecorded_taxa(root: Taxon) -> None:
    def has_occurrence(taxon: Taxon) -> bool:
        return taxon.occurrences.count() > 0

    if root.age == Age.fossil:
        return

    if root.rank == Rank.species:
        if not has_occurrence(root) and not any(has_occurrence(child) for child in root.children):
            print(root)
    else:
        for taxon in root.children:
            unrecorded_taxa(taxon)


def move_localities(period: Period) -> None:
    for location in Location.filter(Location.max_period == period, Location.min_period == period):
        location.min_period = location.max_period = None
        location.stratigraphic_unit = period
        location.save()


def move_to_stratigraphy(loc: Location, period: Period) -> None:
    loc.stratigraphic_unit = period
    loc.min_period = loc.max_period = None
    loc.save()


def h(author: str, year: str, uncited: bool = False) -> List[Name]:
    query = Name.filter(Name.authority % author, Name.year == year)
    if uncited:
        query = query.filter(Name.original_citation >> None)
    return list(query)
