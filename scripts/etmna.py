"""Script for associating data from the book
Evolution of Tertiary Mammals in North America (ETMNA).

"""

from collections.abc import Iterable

from taxonomy import getinput
from taxonomy.db.models import Location, Region
from taxonomy.db.models.location import LocationTag


def all_locations(region: Region) -> Iterable[Location]:
    yield from region.locations
    for child in region.children:
        yield from all_locations(child)


def filter_locations(locations: Iterable[Location]) -> Iterable[Location]:
    for location in locations:
        if location.deleted:
            continue
        if location.has_tag(LocationTag.General):
            continue
        if location.has_tag(LocationTag.ETMNA):
            continue
        (max_age, _, _, _), _, _ = location.sort_key()
        if max_age < -67_000_000:
            continue  # too old
        elif max_age > -2_000_000:
            continue  # too young
        yield location


def etmna_location(location: Location) -> None:
    getinput.print_header(location)
    location.display(full=True)
    etmna = getinput.get_line("etmna> ")
    if etmna:
        location.add_tag(LocationTag.ETMNA(etmna))


def run_for_region(region: Region) -> None:
    locs = filter_locations(all_locations(region))
    locs = sorted(locs, key=lambda loc: loc.sort_key())
    for loc in locs:
        etmna_location(loc)


def run_all(start_at: Region | None = None) -> None:
    regions = [
        Region.get(name="Canada"),
        Region.get(name="United States"),
        Region.get(name="Mexico"),
    ]
    for region in regions:
        for child in region.children:
            if start_at is not None:
                if child == start_at:
                    start_at = None
                else:
                    continue
            run_for_region(child)
