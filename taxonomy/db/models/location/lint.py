"""Lint steps for Locations."""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable
from typing import Any

from taxonomy.apis import nominatim
from taxonomy.db import coordinate_lint, helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.region import Region

from .model import Location, LocationStatus, LocationTag

_GEOCODABLE_OSM_CATEGORIES = {"boundary", "natural", "place", "waterway"}
_OSM_CATEGORY_PRIORITY = {"place": 0, "natural": 1, "waterway": 2, "boundary": 3}
_ADDRESS_REGION_KINDS = {
    RegionKind.country,
    RegionKind.subnational,
    RegionKind.county,
    RegionKind.state,
    RegionKind.province,
    RegionKind.department,
    RegionKind.region,
    RegionKind.canton,
    RegionKind.prefecture,
    RegionKind.territory,
}
_SEARCH_REGION_KINDS = _ADDRESS_REGION_KINDS | {RegionKind.other, RegionKind.island}


def remove_unused_ignores(location: Location, unused: Collection[str]) -> None:
    new_tags = []
    for tag in location.tags:
        if isinstance(tag, LocationTag.IgnoreLintLocation) and tag.label in unused:
            print(f"{location}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    location.tags = new_tags  # type: ignore[assignment]


def get_ignores(location: Location) -> Iterable[IgnoreLint]:
    return location.get_tags(location.tags, LocationTag.IgnoreLintLocation)


def add_ignore(location: Location, label: str, comment: str) -> None:
    location.add_tag(LocationTag.IgnoreLintLocation(label, comment=comment))


LINT = Lint(Location, get_ignores, remove_unused_ignores, add_ignore)


@LINT.add("alias_target")
def check_alias_target(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.deleted is LocationStatus.alias and not location.parent:
        yield "alias location has no parent"


@LINT.add("alias_references")
def check_alias_references(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.deleted is LocationStatus.alias and not location.is_empty():
        yield "alias location has references"


@LINT.add("period")
def check_period(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.is_invalid():
        return
    if location.min_period is None and location.max_period is not None:
        yield "missing min_period"
    if location.max_period is None and location.min_period is not None:
        yield "missing max_period"


@LINT.add("coordinates")
def check_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.is_invalid():
        return
    if location.latitude is None and location.longitude is None:
        return
    if location.latitude is None:
        yield "missing latitude"
        return
    if location.longitude is None:
        yield "missing longitude"
        return
    if location.is_general():
        yield "general location should not have coordinates"
        return
    try:
        latitude, _ = coordinate_lint.standardize_coordinate(
            location.latitude, is_latitude=True
        )
        longitude, _ = coordinate_lint.standardize_coordinate(
            location.longitude, is_latitude=False
        )
    except helpers.InvalidCoordinates:
        yield f"invalid coordinates {location.latitude}, {location.longitude}"
        return
    if (latitude, longitude) != (location.latitude, location.longitude):
        message = (
            f"coordinates should be {latitude}, {longitude}, not "
            f"{location.latitude}, {location.longitude}"
        )
        if cfg.autofix and not LINT.is_ignoring_lint(location, "coordinates"):
            print(f"{location}: {message}")
            location.latitude = latitude
            location.longitude = longitude
        else:
            yield message
    point = coordinate_lint.make_point(location.latitude, location.longitude)
    assert point is not None
    yield from coordinate_lint.check_point_in_region(point, location.region)


def _should_infer_coordinates(location: Location) -> bool:
    if location.is_invalid() or location.is_general():
        return False
    return not (
        location.stratigraphic_unit is not None
        and re.fullmatch(rf"{location.stratigraphic_unit.name} \(.*\)", location.name)
    )


def _get_linked_coordinate_candidates(
    location: Location,
) -> list[tuple[str, str, Any, str]]:
    from taxonomy.db.models.name import TypeTag
    from taxonomy.db.models.occurrence_record import OccurrenceRecordTag

    candidates: list[tuple[str, str, Any, str]] = []
    for name in location.type_localities:
        for tag in name.get_tags(name.type_tags, TypeTag.Coordinates):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                latitude, longitude, point = parsed
                candidates.append((latitude, longitude, point, f"Name {name}"))
    for record in location.occurrence_records:
        for tag in record.get_tags(record.tags, OccurrenceRecordTag.Coordinates):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                latitude, longitude, point = parsed
                candidates.append(
                    (latitude, longitude, point, f"OccurrenceRecord {record}")
                )
    return candidates


@LINT.add("linked_coordinates")
def check_linked_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.latitude is not None or location.longitude is not None:
        return
    if not _should_infer_coordinates(location):
        return
    candidates = _get_linked_coordinate_candidates(location)
    if not candidates:
        return

    latitude, longitude, point, source = candidates[0]
    for _, _, other_point, other_source in candidates[1:]:
        distance = coordinate_lint.distance_km(point, other_point)
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"cannot infer coordinates because {point} (from {source}) and "
                f"{other_point} (from {other_source}) differ by {distance:.1f} km"
            )
            return

    message = f"coordinates should be {latitude}, {longitude}, inferred from {source}"
    if cfg.autofix and not LINT.is_ignoring_lint(location, "linked_coordinates"):
        print(f"{location}: {message}")
        location.latitude = latitude
        location.longitude = longitude
    else:
        yield message


@LINT.add("nominatim_coordinates", requires_network=True)
def check_nominatim_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.latitude is not None or location.longitude is not None:
        return
    if not _should_infer_coordinates(location):
        return
    if _get_linked_coordinate_candidates(location):
        return

    candidates = _get_nominatim_coordinate_candidates(location)
    if not candidates:
        return

    result, (latitude, longitude, point) = candidates[0]
    has_conflict = any(
        coordinate_lint.distance_km(point, other_point)
        > coordinate_lint.COORDINATE_TOLERANCE_KM
        for _, (_, _, other_point) in candidates[1:]
    )
    if has_conflict:
        matches = "".join(
            f"- {candidate.display_name!r} "
            f"({candidate.category}/{candidate.feature_type}, "
            f"{candidate_latitude}, {candidate_longitude})\n"
            for candidate, (candidate_latitude, candidate_longitude, _) in candidates
        )
        yield (
            f"Nominatim returned {len(candidates)} conflicting exact matches:\n{matches}"
        )
        return

    region_issues = list(coordinate_lint.check_point_in_region(point, location.region))
    if region_issues:
        yield (
            f"Nominatim result {result.display_name!r} failed the region check: "
            f"{'; '.join(region_issues)}"
        )
        return

    message = (
        f"coordinates should be {latitude}, {longitude}, inferred from "
        f"OpenStreetMap Nominatim {result.category}/{result.feature_type} result "
        f"{result.display_name!r}"
    )
    if cfg.autofix and not LINT.is_ignoring_lint(location, "nominatim_coordinates"):
        print(f"{location}: {message}")
        location.latitude = latitude
        location.longitude = longitude
    else:
        yield message


@LINT.add("nominatim_coordinate_consistency", requires_network=True)
def check_nominatim_coordinate_consistency(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if location.latitude is None or location.longitude is None:
        return
    if not _should_infer_coordinates(location):
        return
    parsed = coordinate_lint.standardize_coordinate_pair(
        location.latitude, location.longitude
    )
    if parsed is None:
        return
    _, _, location_point = parsed

    candidates = _get_nominatim_coordinate_candidates(location)
    if not candidates:
        return
    candidates_with_distances = [
        (result, candidate, coordinate_lint.distance_km(location_point, candidate[2]))
        for result, candidate in candidates
    ]
    if any(
        distance <= coordinate_lint.COORDINATE_TOLERANCE_KM
        for _, _, distance in candidates_with_distances
    ):
        return

    matches = "".join(
        f"- {result.display_name!r} "
        f"({result.category}/{result.feature_type}, {latitude}, {longitude}; "
        f"{distance:.1f} km away)\n"
        for result, (latitude, longitude, _), distance in candidates_with_distances
    )
    yield (
        f"coordinates {location.latitude}, {location.longitude} are more than "
        f"{coordinate_lint.COORDINATE_TOLERANCE_KM} km from all "
        f"{len(candidates)} exact Nominatim matches:\n{matches}"
    )


def _get_nominatim_coordinate_candidates(
    location: Location,
) -> list[tuple[nominatim.SearchResult, tuple[str, str, Any]]]:
    query = get_nominatim_query(location)
    results = nominatim.search(query)
    candidates = []
    for result in results:
        if not is_sane_nominatim_result(location, result):
            continue
        parsed = coordinate_lint.standardize_coordinate_pair(
            result.latitude, result.longitude
        )
        if parsed is not None:
            candidates.append((result, parsed))
    candidates.sort(key=lambda candidate: _OSM_CATEGORY_PRIORITY[candidate[0].category])
    return candidates


def get_nominatim_query(location: Location) -> str:
    components = [location.name]
    seen = {helpers.simplify_string(location.name, clean_words=False)}
    for region in (location.region, *location.region.all_parents()):
        if region.kind not in _SEARCH_REGION_KINDS:
            continue
        name = _get_unqualified_region_name(region)
        simplified = helpers.simplify_string(name, clean_words=False)
        if simplified not in seen:
            components.append(name)
            seen.add(simplified)
    return ", ".join(components)


def is_sane_nominatim_result(
    location: Location, result: nominatim.SearchResult
) -> bool:
    if result.category not in _GEOCODABLE_OSM_CATEGORIES:
        return False
    if helpers.simplify_string(
        result.name, clean_words=False
    ) != helpers.simplify_string(location.name, clean_words=False):
        return False

    address_names = {
        helpers.simplify_string(value, clean_words=False)
        for key, value in result.address.items()
        if key not in {"country_code", "postcode"} and not key.startswith("ISO3166")
    }
    checked_region = False
    for region in (location.region, *location.region.all_parents()):
        if region.kind not in _ADDRESS_REGION_KINDS:
            continue
        checked_region = True
        expected_names = {
            helpers.simplify_string(name, clean_words=False)
            for name in _get_region_name_aliases(region)
        }
        if address_names.isdisjoint(expected_names):
            return False
    return checked_region


def _get_unqualified_region_name(region: Region) -> str:
    if region.parent is not None:
        parent_suffix = f", {region.parent.name}"
        if region.name.endswith(parent_suffix):
            return region.name.removesuffix(parent_suffix)
    return region.name


def _get_region_name_aliases(region: Region) -> set[str]:
    names = {region.name, _get_unqualified_region_name(region)}
    if region.kind is RegionKind.country:
        names.add(nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name))
    return names
