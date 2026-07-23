"""Lint steps for Locations."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from functools import cache
from typing import Any

from taxonomy.apis import nominatim
from taxonomy.db import coordinate_lint, helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.period import Period
from taxonomy.db.models.region import Region

from .model import Location, LocationTag

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
_LOCALITY_FEATURE_PREFIXES = {
    "city": "city",
    "cidade": "city",
    "ciudad": "city",
    "ville": "city",
    "bahia": "bay",
    "baie": "bay",
    "bay": "bay",
    "bucht": "bay",
    "cabo": "cape",
    "cap": "cape",
    "cape": "cape",
    "catarata": "falls",
    "cave": "cave",
    "caverna": "cave",
    "cerro": "hill",
    "colline": "hill",
    "creek": "creek",
    "cueva": "cave",
    "fall": "falls",
    "falls": "falls",
    "forest": "forest",
    "foret": "forest",
    "fort": "fort",
    "grotte": "cave",
    "harbor": "harbor",
    "halbinsel": "peninsula",
    "hill": "hill",
    "hohle": "cave",
    "island": "island",
    "isla": "island",
    "isle": "island",
    "ile": "island",
    "ilha": "island",
    "insel": "island",
    "isola": "island",
    "pulau": "island",
    "lac": "lake",
    "lago": "lake",
    "lagoon": "lagoon",
    "laguna": "lagoon",
    "lagune": "lagoon",
    "lake": "lake",
    "llano": "plain",
    "llanos": "plain",
    "mata": "forest",
    "see": "lake",
    "mont": "mountain",
    "monte": "mountain",
    "mount": "mountain",
    "mountain": "mountain",
    "mountains": "mountain",
    "peninsula": "peninsula",
    "plain": "plain",
    "plains": "plain",
    "port": "port",
    "porto": "port",
    "puerto": "port",
    "rio": "river",
    "river": "river",
    "rivier": "river",
    "riviere": "river",
    "spring": "spring",
    "tal": "valley",
    "valle": "valley",
    "vallee": "valley",
    "valley": "valley",
    "wald": "forest",
    "bosque": "forest",
}
_LOCALITY_FEATURE_SUFFIXES = {
    "bay": "bay",
    "cape": "cape",
    "city": "city",
    "cave": "cave",
    "creek": "creek",
    "fall": "falls",
    "falls": "falls",
    "forest": "forest",
    "fort": "fort",
    "harbor": "harbor",
    "hill": "hill",
    "island": "island",
    "isle": "island",
    "lake": "lake",
    "lagoon": "lagoon",
    "mountain": "mountain",
    "mountains": "mountain",
    "peninsula": "peninsula",
    "plain": "plain",
    "port": "port",
    "river": "river",
    "spring": "spring",
    "valley": "valley",
}
_LOCALITY_FEATURE_CONNECTORS = {
    "de",
    "del",
    "el",
    "la",
    "las",
    "le",
    "les",
    "los",
    "of",
    "the",
}
_LOCALITY_LEADING_ARTICLES = {"el", "la", "las", "le", "les", "los", "the"}
_FEATURES_REQUIRING_EXPLICIT_WORD = {
    "bay",
    "cape",
    "cave",
    "creek",
    "falls",
    "forest",
    "fort",
    "harbor",
    "hill",
    "lake",
    "lagoon",
    "mountain",
    "peninsula",
    "plain",
    "port",
    "river",
    "spring",
    "valley",
}
_LOCALITY_WORD_NORMALIZATION = {
    "ave": "avenue",
    "e": "east",
    "eastern": "east",
    "este": "east",
    "ft": "fort",
    "harbour": "harbor",
    "hwy": "highway",
    "jct": "junction",
    "mt": "mount",
    "mtn": "mountain",
    "mts": "mountains",
    "n": "north",
    "nord": "north",
    "norte": "north",
    "northern": "north",
    "ost": "east",
    "oeste": "west",
    "pt": "point",
    "rd": "road",
    "rte": "route",
    "s": "south",
    "sainte": "saint",
    "southern": "south",
    "st": "saint",
    "ste": "saint",
    "stn": "station",
    "springs": "spring",
    "sud": "south",
    "sur": "south",
    "w": "west",
    "western": "west",
}
_LOCALITY_EDGE_ABBREVIATIONS = {
    "cr": "creek",
    "hbr": "harbor",
    "lk": "lake",
    "r": "river",
    "riv": "river",
    "spg": "spring",
    "spgs": "spring",
    "spr": "spring",
}
_DIRECTION_WORDS = {"east", "north", "south", "west"}
_MILES_TO_KILOMETRES = 1.609344
_DIRECTION_TO_BEARING = {
    "n": 0.0,
    "nne": 22.5,
    "ne": 45.0,
    "ene": 67.5,
    "e": 90.0,
    "ese": 112.5,
    "se": 135.0,
    "sse": 157.5,
    "s": 180.0,
    "ssw": 202.5,
    "sw": 225.0,
    "wsw": 247.5,
    "w": 270.0,
    "wnw": 292.5,
    "nw": 315.0,
    "nnw": 337.5,
    "north": 0.0,
    "northeast": 45.0,
    "east": 90.0,
    "southeast": 135.0,
    "south": 180.0,
    "southwest": 225.0,
    "west": 270.0,
    "northwest": 315.0,
}
_DIRECTION_TO_ABBREVIATION = {
    "north": "N",
    "northeast": "NE",
    "east": "E",
    "southeast": "SE",
    "south": "S",
    "southwest": "SW",
    "west": "W",
    "northwest": "NW",
}
_DIRECTION_PATTERN = "|".join(sorted(_DIRECTION_TO_BEARING, key=len, reverse=True))
_DISTANCE_PATTERN = r"(?:\d+\s+\d+/\d+|\d+/\d+|\d+(?:\.\d+)?)"
_LOCALITY_OFFSET_COMPONENT = re.compile(
    rf"^\s*(?P<distance>{_DISTANCE_PATTERN})\s*"
    rf"(?P<unit>km|kilomet(?:er|re)s?|mi|miles?)\.?\s+"
    rf"(?P<direction>{_DIRECTION_PATTERN})\b\.?(?:\s+of\b)?\s*",
    re.IGNORECASE,
)
_OFFSET_SEPARATOR = re.compile(r"^(?:,\s*|and\s+)(?=\d)", re.IGNORECASE)
_TRAILING_DISAMBIGUATOR = re.compile(r"\s+\((?P<content>[^()]+)\)\s*$")


@dataclass(frozen=True, slots=True)
class LocalityOffset:
    distance_km: float
    bearing_degrees: float
    description: str


@dataclass(frozen=True, slots=True)
class NominatimSearchPlan:
    locality_name: str
    offsets: tuple[LocalityOffset, ...] = ()
    disambiguator_suffix: str = ""
    coordinates_can_be_inferred: bool = True

    @property
    def offset_description(self) -> str | None:
        if not self.offsets:
            return None
        return " then ".join(offset.description for offset in self.offsets)

    @property
    def standardized_name(self) -> str:
        if not self.offsets:
            return f"{self.locality_name}{self.disambiguator_suffix}"
        offset_text = " ".join(offset.description for offset in self.offsets)
        return f"{offset_text} {self.locality_name}{self.disambiguator_suffix}"


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


def _locality_similarity_key(name: str) -> tuple[str, str | None]:
    name = re.sub(r"\s*\([^()]*\)", "", name)
    words = _normalized_locality_words(name)
    feature: str | None = None
    if words and words[0] in _LOCALITY_FEATURE_PREFIXES:
        feature = _LOCALITY_FEATURE_PREFIXES[words.pop(0)]
        while words and words[0] in _LOCALITY_FEATURE_CONNECTORS:
            words.pop(0)
    elif words and words[-1] in _LOCALITY_FEATURE_SUFFIXES:
        feature = _LOCALITY_FEATURE_SUFFIXES[words.pop()]
        while words and words[-1] in _LOCALITY_FEATURE_CONNECTORS:
            words.pop()
    return "".join(words), feature


def _normalized_locality_words(name: str) -> list[str]:
    words = helpers.simplify_string(
        name, clean_words=True, keep_whitespace=True
    ).split()
    words = [_LOCALITY_WORD_NORMALIZATION.get(word, word) for word in words]
    if words and words[0] in _LOCALITY_LEADING_ARTICLES:
        words.pop(0)
    if words:
        words[0] = _LOCALITY_EDGE_ABBREVIATIONS.get(words[0], words[0])
        words[-1] = _LOCALITY_EDGE_ABBREVIATIONS.get(words[-1], words[-1])
    return words


def _has_different_parenthetical_qualifiers(left: str, right: str) -> bool:
    left_qualifiers = tuple(
        helpers.simplify_string(item, clean_words=True)
        for item in re.findall(r"\(([^()]*)\)", left)
    )
    right_qualifiers = tuple(
        helpers.simplify_string(item, clean_words=True)
        for item in re.findall(r"\(([^()]*)\)", right)
    )
    return bool(
        left_qualifiers and right_qualifiers and left_qualifiers != right_qualifiers
    )


def _has_different_directions(left: str, right: str) -> bool:
    left_directions = set(_normalized_locality_words(left)) & _DIRECTION_WORDS
    right_directions = set(_normalized_locality_words(right)) & _DIRECTION_WORDS
    return left_directions != right_directions and bool(
        left_directions or right_directions
    )


def _has_distinguishing_short_designator(left: str, right: str) -> bool:
    left_words = _normalized_locality_words(left)
    right_words = _normalized_locality_words(right)
    common_length = 0
    for left_word, right_word in zip(left_words, right_words, strict=False):
        if left_word != right_word:
            break
        common_length += 1
    if common_length == 0:
        return False
    left_remainder = left_words[common_length:]
    right_remainder = right_words[common_length:]
    if not left_remainder and not right_remainder:
        return False
    return all(len(word) <= 4 for word in (*left_remainder, *right_remainder))


def _edit_distance_at_most(left: str, right: str, maximum: int) -> bool:
    if abs(len(left) - len(right)) > maximum:
        return False
    previous = list(range(len(right) + 1))
    for left_index, left_character in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_character in enumerate(right, start=1):
            current.append(
                min(
                    current[-1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (left_character != right_character),
                )
            )
        if min(current) > maximum:
            return False
        previous = current
    return previous[-1] <= maximum


def are_likely_synonymous_names(left: str, right: str) -> bool:
    if _has_different_parenthetical_qualifiers(left, right):
        return False
    left_base, left_feature = _locality_similarity_key(left)
    right_base, right_feature = _locality_similarity_key(right)
    if not left_base or not right_base:
        return False
    if left_feature is not None and right_feature is not None:
        if left_feature != right_feature:
            return False
    elif (
        left_feature in _FEATURES_REQUIRING_EXPLICIT_WORD
        or right_feature in _FEATURES_REQUIRING_EXPLICIT_WORD
    ):
        return False
    if left_base == right_base:
        return True
    if _has_different_directions(left, right):
        return False
    if _has_distinguishing_short_designator(left, right):
        return False
    if any(character.isdigit() for character in left_base + right_base):
        return False
    shorter_length = min(len(left_base), len(right_base))
    if shorter_length < 5:
        return False
    if left_base[0] != right_base[0] and {left_base[0], right_base[0]} != {"i", "y"}:
        return False
    maximum_distance = 2 if max(len(left_base), len(right_base)) >= 10 else 1
    return _edit_distance_at_most(left_base, right_base, maximum_distance)


def _find_component(parents: dict[int, int], location_id: int) -> int:
    while parents[location_id] != location_id:
        parents[location_id] = parents[parents[location_id]]
        location_id = parents[location_id]
    return location_id


def _union_components(parents: dict[int, int], left_id: int, right_id: int) -> None:
    left_root = _find_component(parents, left_id)
    right_root = _find_component(parents, right_id)
    if left_root != right_root:
        parents[right_root] = left_root


def _build_likely_synonym_map(
    locations: Iterable[Location],
) -> dict[int, tuple[Location, tuple[Location, ...]]]:
    by_region: dict[int, list[Location]] = defaultdict(list)
    for location in locations:
        if not location.is_general():
            by_region[location.region.id].append(location)

    output: dict[int, tuple[Location, tuple[Location, ...]]] = {}
    for region_locations in by_region.values():
        parents = {location.id: location.id for location in region_locations}

        for index, left in enumerate(region_locations):
            for right in region_locations[index + 1 :]:
                if (
                    left.min_period != right.min_period
                    or left.max_period != right.max_period
                    or (
                        left.stratigraphic_unit != right.stratigraphic_unit
                        and (
                            left.stratigraphic_unit is not None
                            or right.stratigraphic_unit is not None
                        )
                    )
                ):
                    continue
                if are_likely_synonymous_names(left.name, right.name):
                    _union_components(parents, left.id, right.id)

        components: dict[int, list[Location]] = defaultdict(list)
        for location in region_locations:
            components[_find_component(parents, location.id)].append(location)
        for component in components.values():
            if len(component) < 2:
                continue
            group = tuple(sorted(component, key=lambda location: location.id))
            keeper = group[0]
            for location in group[1:]:
                output[location.id] = keeper, group
    return output


@cache
def _get_likely_synonym_map() -> dict[int, tuple[Location, tuple[Location, ...]]]:
    return _build_likely_synonym_map(Location.select_valid())


@LINT.add("likely_synonymous", clear_caches=_get_likely_synonym_map.cache_clear)
def check_likely_synonymous(location: Location, cfg: LintConfig) -> Iterable[str]:
    match = _get_likely_synonym_map().get(location.id)
    if match is None:
        return
    keeper, group = match
    group_refreshed = [item.reload() for item in group]
    group_refreshed = [item for item in group_refreshed if not item.is_invalid()]
    if len(group_refreshed) < 2:
        return
    group_text = ", ".join(f"{item.id}: {item.name!r}" for item in group_refreshed)
    yield (
        f"likely synonymous with lower-ID Location {keeper.id}: {keeper.name!r} "
        f"in Region {location.region.name!r}; group: {group_text}"
    )


@LINT.add("offset_name")
def check_offset_name(location: Location, cfg: LintConfig) -> Iterable[str]:
    search_plan = get_nominatim_search_plan(location)
    standardized_name = search_plan.standardized_name
    if not search_plan.offsets or location.name == standardized_name:
        return
    message = f"distance-offset name should be {standardized_name!r}"
    if cfg.autofix and not LINT.is_ignoring_lint(location, "offset_name"):
        if _is_location_name_taken(standardized_name):
            yield f"{message}; cannot autofix because that name is already in use"
        else:
            print(f"{location}: {message}")
            location.name = standardized_name
    else:
        yield message


def _is_location_name_taken(name: str) -> bool:
    return Location.select().filter(Location.name == name).count() > 0


@cache
def _get_periods_by_name() -> dict[str, tuple[Period, ...]]:
    periods_by_name: defaultdict[str, list[Period]] = defaultdict(list)
    for period in Period.select_valid():
        periods_by_name[period.name].append(period)
    return {name: tuple(periods) for name, periods in periods_by_name.items()}


def _split_trailing_parenthetical(name: str) -> tuple[str, str] | None:
    if not name.endswith(")"):
        return None
    depth = 0
    for index in range(len(name) - 1, -1, -1):
        character = name[index]
        if character == ")":
            depth += 1
        elif character == "(":
            depth -= 1
            if depth == 0:
                if index == 0 or not name[index - 1].isspace():
                    return None
                return name[:index].rstrip(), name[index + 1 : -1]
    return None


def _get_trailing_disambiguators(name: str) -> tuple[str, ...]:
    disambiguators = []
    while split := _split_trailing_parenthetical(name):
        name, disambiguator = split
        disambiguators.append(disambiguator)
    disambiguators.reverse()
    return tuple(disambiguators)


def _get_qualified_name_variants(name: str) -> set[str]:
    variants = {name}
    if split := _split_trailing_parenthetical(name):
        base_name, qualifier = split
        variants.add(base_name)
        variants.add(f"{base_name}, {qualifier}")
    elif ", " in name:
        base_name, qualifier = name.split(", ", maxsplit=1)
        variants.add(base_name)
        variants.add(f"{base_name} ({qualifier})")
    return variants


def _is_period_disambiguator(location: Location, disambiguator: str) -> bool:
    for assigned_period in (location.min_period, location.max_period):
        period = assigned_period
        while period is not None:
            if period.name == disambiguator:
                return True
            period = period.parent

    oldest_age = (
        location.max_age
        if location.max_age is not None
        else (
            location.max_period.get_max_age()
            if location.max_period is not None
            else None
        )
    )
    youngest_age = (
        location.min_age
        if location.min_age is not None
        else (
            location.min_period.get_min_age()
            if location.min_period is not None
            else None
        )
    )
    if oldest_age is None or youngest_age is None:
        return False
    for period in _get_periods_by_name().get(disambiguator, ()):
        period_oldest_age = period.get_max_age()
        period_youngest_age = period.get_min_age()
        if (
            period_oldest_age is not None
            and period_youngest_age is not None
            and period_oldest_age >= oldest_age
            and period_youngest_age <= youngest_age
        ):
            return True
    return False


def _is_stratigraphic_unit_disambiguator(
    location: Location, disambiguator: str
) -> bool:
    unit = location.stratigraphic_unit
    while unit is not None:
        if disambiguator in _get_qualified_name_variants(unit.name):
            return True
        unit = unit.parent
    return False


def _is_sane_disambiguator(location: Location, disambiguator: str) -> bool:
    if any(
        disambiguator in _get_qualified_name_variants(region.name)
        for region in (location.region, *location.region.all_parents())
    ):
        return True
    if _is_period_disambiguator(location, disambiguator):
        return True
    return _is_stratigraphic_unit_disambiguator(location, disambiguator)


@LINT.add("disambiguator", clear_caches=_get_periods_by_name.cache_clear)
def check_disambiguator(location: Location, cfg: LintConfig) -> Iterable[str]:
    for disambiguator in _get_trailing_disambiguators(location.name):
        if not _is_sane_disambiguator(location, disambiguator):
            yield (
                f"disambiguator {disambiguator!r} is not an enclosing Region, "
                "an assigned Period, or an assigned StratigraphicUnit"
            )


@LINT.add("period")
def check_period(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.min_period is None and location.max_period is not None:
        yield "missing min_period"
    if location.max_period is None and location.min_period is not None:
        yield "missing max_period"


@LINT.add("coordinates")
def check_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
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
    parsed = coordinate_lint.standardize_coordinate_pair(
        location.latitude, location.longitude
    )
    if parsed is None:
        yield f"invalid coordinates {location.latitude}, {location.longitude}"
        return
    latitude, longitude, extent = parsed
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
    yield from coordinate_lint.check_extent_in_region(extent, location.region)


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
                latitude, longitude, extent = parsed
                candidates.append((latitude, longitude, extent, f"Name {name}"))
    for record in location.occurrence_records:
        for tag in record.get_tags(record.tags, OccurrenceRecordTag.Coordinates):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                latitude, longitude, extent = parsed
                candidates.append(
                    (latitude, longitude, extent, f"OccurrenceRecord {record}")
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

    latitude, longitude, extent, source = candidates[0]
    combined_extent = extent
    for _, _, other_extent, other_source in candidates[1:]:
        distance = coordinate_lint.extent_distance_km(extent, other_extent)
        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
            yield (
                f"cannot infer coordinates because {extent} (from {source}) and "
                f"{other_extent} (from {other_source}) differ by {distance:.1f} km"
            )
            return
        combined_extent = combined_extent.union(other_extent)

    latitude = combined_extent.latitude.standardized_text
    longitude = combined_extent.longitude.standardized_text

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

    result, (latitude, longitude, extent) = candidates[0]
    has_conflict = any(
        coordinate_lint.extent_distance_km(extent, other_extent)
        > coordinate_lint.COORDINATE_TOLERANCE_KM
        for _, (_, _, other_extent) in candidates[1:]
    )
    preferred_over_boundaries = False
    if has_conflict:
        preferred = _get_place_candidate_among_administrative_boundaries(candidates)
        if preferred is None:
            matches = "".join(
                f"- {candidate.display_name!r} "
                f"({candidate.category}/{candidate.feature_type}, "
                f"{candidate_latitude}, {candidate_longitude})\n"
                for candidate, (
                    candidate_latitude,
                    candidate_longitude,
                    _,
                ) in candidates
            )
            yield (
                f"Nominatim returned {len(candidates)} conflicting exact matches:\n"
                f"{matches}"
            )
            return
        result, (latitude, longitude, extent) = preferred
        preferred_over_boundaries = True

    region_issues = list(
        coordinate_lint.check_extent_in_region(extent, location.region)
    )
    if region_issues:
        yield (
            f"Nominatim result {result.display_name!r} failed the region check: "
            f"{'; '.join(region_issues)}"
        )
        return

    search_plan = get_nominatim_search_plan(location)
    if search_plan.offset_description is None:
        message = (
            f"coordinates should be {latitude}, {longitude}, inferred from "
            f"OpenStreetMap Nominatim {result.category}/{result.feature_type} result "
            f"{result.display_name!r}"
        )
    else:
        message = (
            f"coordinates should be {latitude}, {longitude}, inferred by applying "
            f"{search_plan.offset_description} to OpenStreetMap Nominatim "
            f"{result.category}/{result.feature_type} result {result.display_name!r}"
        )
    if preferred_over_boundaries:
        message += " (preferred over boundary/administrative matches)"
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
    _, _, location_extent = parsed

    candidates = _get_nominatim_coordinate_candidates(location)
    if not candidates:
        return
    candidates_with_distances = [
        (
            result,
            candidate,
            coordinate_lint.extent_distance_km(location_extent, candidate[2]),
        )
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
    search_plan = get_nominatim_search_plan(location)
    if not search_plan.coordinates_can_be_inferred:
        return []
    query = get_nominatim_query(location)
    results = nominatim.search(query)
    candidates = []
    for result in results:
        if not is_sane_nominatim_result(
            location, result, expected_name=search_plan.locality_name
        ):
            continue
        parsed = get_nominatim_result_coordinates(result, offsets=search_plan.offsets)
        if parsed is not None:
            candidates.append((result, parsed))
    candidates.sort(key=lambda candidate: _OSM_CATEGORY_PRIORITY[candidate[0].category])
    return candidates


def _get_place_candidate_among_administrative_boundaries(
    candidates: list[tuple[nominatim.SearchResult, tuple[str, str, Any]]],
) -> tuple[nominatim.SearchResult, tuple[str, str, Any]] | None:
    place_candidates = [
        candidate for candidate in candidates if candidate[0].category == "place"
    ]
    if len(place_candidates) != 1 or len(candidates) < 2:
        return None
    if any(
        result.category != "place"
        and not (
            result.category == "boundary" and result.feature_type == "administrative"
        )
        for result, _ in candidates
    ):
        return None
    return place_candidates[0]


def get_nominatim_search_plan(location: Location) -> NominatimSearchPlan:
    name = location.name
    disambiguators: list[str] = []
    offsets: list[LocalityOffset] = []
    has_parenthetical_offsets = False
    while match := _TRAILING_DISAMBIGUATOR.search(name):
        parenthetical_offsets, remainder = _parse_locality_offsets(
            match.group("content")
        )
        if parenthetical_offsets and not remainder:
            offsets.extend(parenthetical_offsets)
            has_parenthetical_offsets = True
        else:
            disambiguators.insert(0, match.group(0).strip())
        name = name[: match.start()]
    disambiguator_suffix = f" {' '.join(disambiguators)}" if disambiguators else ""

    leading_offsets, locality_name = _parse_locality_offsets(name)
    offsets.extend(leading_offsets)
    if offsets and locality_name:
        offsets.sort(key=_locality_offset_sort_key)
        return NominatimSearchPlan(
            locality_name,
            tuple(offsets),
            disambiguator_suffix,
            coordinates_can_be_inferred=not has_parenthetical_offsets,
        )
    return NominatimSearchPlan(name, disambiguator_suffix=disambiguator_suffix)


def _parse_locality_offsets(text: str) -> tuple[list[LocalityOffset], str]:
    remaining = text
    offsets = []
    while match := _LOCALITY_OFFSET_COMPONENT.match(remaining):
        offset = _make_locality_offset(match)
        if offset is None:
            break
        offsets.append(offset)
        remaining = remaining[match.end() :]
        remaining = _OFFSET_SEPARATOR.sub("", remaining)
    return offsets, remaining.strip()


def _make_locality_offset(match: re.Match[str]) -> LocalityOffset | None:
    distance_text = " ".join(match.group("distance").split())
    distance = _parse_offset_distance(distance_text)
    if distance is None:
        return None
    unit = match.group("unit").lower()
    direction = match.group("direction").lower()
    if unit.startswith("mi"):
        distance_km = distance * _MILES_TO_KILOMETRES
        display_unit = "mi"
    else:
        distance_km = distance
        display_unit = "km"
    return LocalityOffset(
        distance_km=distance_km,
        bearing_degrees=_DIRECTION_TO_BEARING[direction],
        description=(
            f"{distance_text} {display_unit} "
            f"{_DIRECTION_TO_ABBREVIATION.get(direction, direction.upper())}"
        ),
    )


def _parse_offset_distance(text: str) -> float | None:
    if "/" not in text:
        return float(text)
    parts = text.split()
    whole = int(parts[0]) if len(parts) == 2 else 0
    fraction = parts[-1]
    numerator_text, denominator_text = fraction.split("/", maxsplit=1)
    denominator = int(denominator_text)
    if denominator == 0:
        return None
    return whole + int(numerator_text) / denominator


def _locality_offset_sort_key(offset: LocalityOffset) -> tuple[int, float]:
    if offset.bearing_degrees in {0, 180}:
        axis_order = 0
    elif offset.bearing_degrees in {90, 270}:
        axis_order = 1
    else:
        axis_order = 2
    return axis_order, offset.bearing_degrees


def get_nominatim_result_coordinates(
    result: nominatim.SearchResult, *, offsets: tuple[LocalityOffset, ...] = ()
) -> tuple[str, str, Any] | None:
    parsed = coordinate_lint.standardize_coordinate_pair(
        result.latitude, result.longitude
    )
    if parsed is None or not offsets:
        return parsed
    point = parsed[2].point
    if point is None:
        return None
    for offset in offsets:
        point = coordinate_lint.move_point(
            point, offset.distance_km, offset.bearing_degrees
        )
    latitude = _format_inferred_decimal_coordinate(point.latitude)
    longitude = _format_inferred_decimal_coordinate(point.longitude)
    return coordinate_lint.standardize_coordinate_pair(latitude, longitude)


def _format_inferred_decimal_coordinate(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def get_nominatim_query(location: Location) -> str:
    locality_name = get_nominatim_search_plan(location).locality_name
    components = [locality_name]
    seen = {helpers.simplify_string(locality_name, clean_words=False)}
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
    location: Location,
    result: nominatim.SearchResult,
    *,
    expected_name: str | None = None,
) -> bool:
    if result.category not in _GEOCODABLE_OSM_CATEGORIES:
        return False
    if expected_name is None:
        expected_name = get_nominatim_search_plan(location).locality_name
    if helpers.simplify_string(
        result.name, clean_words=False
    ) != helpers.simplify_string(expected_name, clean_words=False):
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


@LINT.add("should_have_disambiguator")
def check_should_have_disambiguator(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if "(" in location.name or location.name == location.region.name:
        return

    similar = list(
        Location.select_valid().filter(
            Location.id != location.id, Location.name.startswith(f"{location.name} (")
        )
    )
    if not similar:
        return
    yield (
        f"location {location.name!r} should have a disambiguator "
        f"(e.g., '({location.region.name})') because of collision with {len(similar)} other location(s): "
        f"{', '.join(repr(loc.name) for loc in similar)}"
    )
