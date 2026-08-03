"""Lint steps for Locations."""

import re
from collections import defaultdict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from functools import cache
from typing import Any

import httpx

from taxonomy import adt, coordinates
from taxonomy.apis import geonames, nominatim, plss
from taxonomy.db import coordinate_lint, helpers, models
from taxonomy.db.constants import RegionKind, SpeciesGroupType
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.period import Period
from taxonomy.db.models.region import Region, RegionTag

from .age import is_recent_location
from .model import Location, LocationTag, is_coordinate_provenance_tag
from .name import ParsedLocationName, split_trailing_parenthetical

_GEOCODABLE_OSM_CATEGORIES = {"boundary", "natural", "place", "water", "waterway"}
_OSM_CATEGORY_PRIORITY = {
    "place": 0,
    "natural": 1,
    "water": 1,
    "waterway": 2,
    "boundary": 3,
}
_POSSIBLE_GENERAL_NOMINATIM_FEATURES = {
    "boundary": frozenset({"protected_area"}),
    "natural": frozenset(
        {
            "bay",
            "fell",
            "glacier",
            "grassland",
            "heath",
            "mountain_range",
            "peninsula",
            "plateau",
            "reef",
            "ridge",
            "scrub",
            "strait",
            "valley",
            "water",
            "wetland",
            "wood",
        }
    ),
    "place": frozenset({"archipelago", "atoll", "island", "islet", "region"}),
    "water": frozenset({"lake"}),
    "waterway": frozenset({"flowline", "river", "stream", "tidal_channel"}),
}
_POSSIBLE_GENERAL_ADMINISTRATIVE_FEATURES = frozenset(
    {"administrative", "historic:administrative", "political"}
)
_POSSIBLE_GENERAL_GEOGRAPHIC_NAME_WORDS = frozenset(
    {
        "archipelago",
        "atoll",
        "basin",
        "bay",
        "coast",
        "desert",
        "forest",
        "gulf",
        "highlands",
        "island",
        "islands",
        "lake",
        "lowlands",
        "mountains",
        "peninsula",
        "plain",
        "plains",
        "plateau",
        "range",
        "ridge",
        "river",
        "stream",
        "valley",
        "watershed",
    }
)
_POSSIBLE_GENERAL_ADMINISTRATIVE_NAME_WORDS = frozenset(
    {
        "canton",
        "county",
        "department",
        "district",
        "municipality",
        "oblast",
        "parish",
        "prefecture",
        "province",
        "region",
        "state",
        "territory",
    }
)
_NOMINATIM_SETTLEMENT_FEATURES = frozenset(
    {
        "city",
        "hamlet",
        "locality",
        "neighbourhood",
        "quarter",
        "suburb",
        "town",
        "village",
    }
)
_POSSIBLE_GENERAL_MINIMUM_RADIUS_KM = 10
_GEONAMES_COUNTRY_FEATURE_CODES = {"PCLI", "PCLD", "PCLF", "PCLIX", "PCLS", "TERR"}
_GEONAMES_ADMIN_FEATURE_CODES = {"ADM1", "ADM2", "ADM3", "ADM4"}
_GEONAMES_SEARCH_LIMIT = 100
# GeoNames stores a representative point even for linear and areal features. Only
# feature types whose point has a reasonably unambiguous locality meaning may set
# or challenge a Location point. Other matches remain visible as coordinate
# evidence. See https://download.geonames.org/export/dump/featureCodes_en.txt.
_GEONAMES_POINT_FEATURE_CLASSES = frozenset({"P", "S"})
_GEONAMES_POINT_FEATURE_CODES = frozenset(
    {
        # Hydrographic points.
        ("H", "CNFL"),
        ("H", "FLLS"),
        ("H", "GYSR"),
        ("H", "SPNG"),
        ("H", "SPNS"),
        ("H", "SPNT"),
        ("H", "STMB"),
        ("H", "STMM"),
        ("H", "WADB"),
        ("H", "WADJ"),
        ("H", "WADM"),
        ("H", "WHRL"),
        ("H", "WLL"),
        ("H", "WLLQ"),
        ("H", "WTRH"),
        # Road and railroad junctions or bends.
        ("R", "RDB"),
        ("R", "RDJCT"),
        ("R", "RJCT"),
        # Landform points with a reasonably well-defined center, summit, or tip.
        ("T", "BLHL"),
        ("T", "BUTE"),
        ("T", "CAPE"),
        ("T", "CONE"),
        ("T", "CRTR"),
        ("T", "FORD"),
        ("T", "GAP"),
        ("T", "HDLD"),
        ("T", "HLL"),
        ("T", "HMCK"),
        ("T", "MESA"),
        ("T", "MND"),
        ("T", "MT"),
        ("T", "NTK"),
        ("T", "PASS"),
        ("T", "PK"),
        ("T", "PROM"),
        ("T", "PT"),
        ("T", "RK"),
        ("T", "SINK"),
        ("T", "VLC"),
    }
)
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
_SUBNATIONAL_REGION_KINDS = _ADDRESS_REGION_KINDS - {RegionKind.country}
_REVERSE_REGION_PROBE_DISTANCE_KM = 2
_REGION_KIND_DESIGNATORS = {
    RegionKind.canton: "Canton",
    RegionKind.county: "County",
    RegionKind.department: "Department",
    RegionKind.prefecture: "Prefecture",
    RegionKind.province: "Province",
    RegionKind.region: "Region",
    RegionKind.state: "State",
    RegionKind.territory: "Territory",
}
_ADMINISTRATIVE_DESIGNATORS = frozenset(_REGION_KIND_DESIGNATORS.values())
_OSM_QUERY_OMITTED_DESIGNATORS = {"Department"}
# Context-specific equivalents observed in current English-language Nominatim
# results. The key is (taxonomy Region name, country), and the first value is
# the preferred spelling for forward queries. Keep only names for the same
# administrative entity here; geographic mismatches must remain lint issues.
_OSM_REGION_NAME_TRANSLATIONS = {
    ("Alpes-Maritimes", "France"): ("Maritime Alps",),
    ("Basque Country", "Spain"): ("Autonomous Community of the Basque Country",),
    ("Bougainville Region", "Papua New Guinea"): ("Autonomous Region of Bougainville",),
    ("Castilla-La Mancha", "Spain"): ("Castile-La Mancha",),
    ("Distrito Federal (Brazil)", "Brazil"): ("Federal District",),
    ("Distrito Federal (Mexico)", "Mexico"): ("Mexico City",),
    ("Graubünden", "Switzerland"): ("Grisons",),
    ("Haute-Corse", "France"): ("Upper Corsica",),
    ("Haute-Savoie", "France"): ("Upper Savoy",),
    ("La Guaira", "Venezuela"): ("Vargas State",),
    ("Madrid", "Spain"): ("Community of Madrid", "Autonomous Community of Madrid"),
    ("North Aegean", "Greece"): ("Northern Aegean",),
    ("North Ossetia", "Russia"): ("Republic of North Ossetia – Alania",),
    ("Orissa", "India"): ("Odisha",),
    ("Tibet", "China"): ("Xizang",),
}
_REVERSE_ADMINISTRATIVE_ADDRESS_KEYS = (
    "state",
    "province",
    "region",
    "territory",
    "state_district",
    "county",
)
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
_SIGNED_DECIMAL_COORDINATE_PAIR = re.compile(
    r"^\s*(?P<latitude>[+-]?\d+(?:\.\d+)?)\s*[,;/]\s*"
    r"(?P<longitude>[+-]?\d+(?:\.\d+)?)\s*$"
)
_DIRECTIONAL_COORDINATE_CHARACTERS = r"""0-9.\s°*◦'`ʹ’‘′ ́"”"""
_DIRECTIONAL_COORDINATE_PAIR = re.compile(
    rf"^\s*(?P<latitude>[{_DIRECTIONAL_COORDINATE_CHARACTERS}]+[NS])"
    rf"\s*[,;/]?\s+"
    rf"(?P<longitude>[{_DIRECTIONAL_COORDINATE_CHARACTERS}]+[EW])\s*$",
    re.IGNORECASE,
)
_DIRECTIONAL_COORDINATE_LIKE = re.compile(
    rf"^\s*[{_DIRECTIONAL_COORDINATE_CHARACTERS}]+[NSEW]"
    rf"\s*[,;/]?\s+"
    rf"[{_DIRECTIONAL_COORDINATE_CHARACTERS}]+[NSEW]\s*$",
    re.IGNORECASE,
)
_EDITORIAL_LOCATION_EQUIVALENCE = re.compile(
    r"\[(?P<bracketed>[^\[\]]+)\](?![\[(])|\(\s*=\s*(?P<parenthetical>[^()]+?)\s*\)"
)
_ALWAYS_ALLOWED_DISAMBIGUATORS = {"island", "region", "historical region"}


@dataclass(frozen=True, slots=True)
class LocalityOffset:
    distance_km: float
    bearing_degrees: float
    description: str


@dataclass(frozen=True, slots=True)
class CoordinateModifierPlan:
    prefix: ParsedLocationName
    raw_coordinates: str
    parsed_coordinates: tuple[str, str, coordinate_lint.CoordinateExtent] | None = None

    @property
    def standardized_name(self) -> str | None:
        if self.parsed_coordinates is None:
            return None
        latitude, longitude, _ = self.parsed_coordinates
        return ParsedLocationName(
            self.prefix.base_name, self.prefix.disambiguator, f"{latitude} {longitude}"
        ).render()


@dataclass(frozen=True, slots=True)
class NominatimSearchPlan:
    locality_name: str
    offsets: tuple[LocalityOffset, ...] = ()
    disambiguator: str | None = None
    modifier: str | None = None
    coordinates_can_be_inferred: bool = True

    @property
    def offset_description(self) -> str | None:
        if not self.offsets:
            return None
        return " then ".join(offset.description for offset in self.offsets)

    @property
    def standardized_name(self) -> str:
        return ParsedLocationName(
            self.locality_name, self.disambiguator, self.modifier
        ).render()


@dataclass(frozen=True, slots=True)
class GeoNamesCoordinateCandidate:
    match: geonames.GeoNamesMatch
    latitude: str
    longitude: str
    extent: coordinate_lint.CoordinateExtent
    region_issues: tuple[str, ...]

    @property
    def is_accepted(self) -> bool:
        return not self.region_issues


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


@dataclass(frozen=True, slots=True)
class _LinkedPLSSEvidence:
    description: plss.PLSSDescription
    source: str
    has_alternative_section: bool = False


def _get_applicable_location_detail_tags(name: Any) -> tuple[Any, ...]:
    """Return LocationDetails that describe the Name's current type locality.

    For a neotypified Name, unstructured locality accounts predating the neotype may
    describe only the original type locality.  Restrict inference to LocationDetails
    from a source that actually designates the neotype.  A source-less structured
    Coordinates tag remains usable because it represents the Name's current type
    locality directly.
    """
    details = tuple(name.get_tags(name.type_tags, models.name.TypeTag.LocationDetail))
    if name.species_type_kind is not SpeciesGroupType.neotype:
        return details
    designation_source_ids = {
        tag.optional_source.id
        for tag in name.get_tags(name.type_tags, models.name.TypeTag.NeotypeDesignation)
        if tag.valid and tag.optional_source is not None
    }
    return tuple(tag for tag in details if tag.source.id in designation_source_ids)


def _get_applicable_coordinate_text_tags(name: Any) -> tuple[Any, ...]:
    """Return Name text tags that may document its current type locality."""
    details = (
        *_get_applicable_location_detail_tags(name),
        *name.get_tags(name.type_tags, models.name.TypeTag.SpecimenDetail),
    )
    if name.species_type_kind is not SpeciesGroupType.neotype:
        return details
    designation_source_ids = {
        tag.optional_source.id
        for tag in name.get_tags(name.type_tags, models.name.TypeTag.NeotypeDesignation)
        if tag.valid and tag.optional_source is not None
    }
    return tuple(
        tag
        for tag in details
        if tag.source is not None and tag.source.id in designation_source_ids
    )


def _extract_name_text_tag_coordinate_pairs(
    name: models.Name, tag: Any
) -> tuple[tuple[str, str], ...]:
    """Extract usable type-locality coordinates from a Name text tag.

    SpecimenDetail tags often contain a catalog dump for the holotype followed by
    paratypes or other referred specimens from different localities.  A lone pair
    can document the type locality, but multiple distinct pairs cannot safely be
    assigned to the Name's Location without parsing the specimen roles as well.
    LocationDetail tags are locality evidence throughout, so retain all pairs from
    them and let the compatibility lint report genuine conflicts.
    """
    if name.has_lint_ignore("location_detail_coordinates"):
        # A reviewed Name-level ignore records that coordinate-looking text in
        # its locality evidence is inapplicable, erroneous, or too uncertain to
        # drive the selected type locality. Location inference and provenance
        # must honor that decision as well as the Name lint itself.
        return ()
    if (
        isinstance(tag, models.name.TypeTag.SpecimenDetail)
        and name.species_type_kind is not SpeciesGroupType.neotype
        and re.search(r"\bpropos\w*\s+to\s+designat", tag.text, re.IGNORECASE)
    ):
        # A proposed neotype does not replace the name-bearing type until the
        # designation is accepted and represented on the Name.  In particular,
        # its specimen locality must not overwrite the current holotype locality.
        return ()
    pairs = tuple(dict.fromkeys(helpers.extract_coordinate_pairs(tag.text)))
    if isinstance(tag, models.name.TypeTag.SpecimenDetail) and len(pairs) != 1:
        return ()
    return pairs


def _region_of_kind(region: Region, kind: RegionKind) -> Region | None:
    if region.kind is kind:
        return region
    return next(
        (parent for parent in region.all_parents() if parent.kind is kind), None
    )


def _get_us_plss_context(location: Location) -> tuple[str, str, str | None] | None:
    state = _region_of_kind(location.region, RegionKind.state)
    country = _region_of_kind(location.region, RegionKind.country)
    if state is None or state.name not in plss.US_STATE_CODES:
        return None
    if country is not None and country.name != "United States":
        return None
    state_code, state_fips = plss.US_STATE_CODES[state.name]
    county = _region_of_kind(location.region, RegionKind.county)
    county_name = county.name if county is not None else None
    return state_code, state_fips, county_name


def _get_linked_plss_evidence(location: Location) -> list[_LinkedPLSSEvidence]:
    evidence: list[_LinkedPLSSEvidence] = []
    own_texts = (
        (location.name, "Location name"),
        (location.location_detail, "Location location_detail"),
    )
    for text, source in own_texts:
        if text:
            evidence.extend(
                _LinkedPLSSEvidence(
                    extracted.description,
                    f"{source} {text!r}",
                    extracted.has_alternative_section,
                )
                for extracted in plss.extract_plss(text)
            )
    for name in location.type_localities:
        for tag in _get_applicable_location_detail_tags(name):
            evidence.extend(
                _LinkedPLSSEvidence(
                    extracted.description,
                    f"Name {name.id} LocationDetail {tag.text!r}",
                    extracted.has_alternative_section,
                )
                for extracted in plss.extract_plss(tag.text)
            )
    occurrence_text_tags = (
        models.occurrence_record.OccurrenceRecordTag.LocationHint,
        models.occurrence_record.OccurrenceRecordTag.SpecimenDetail,
        models.occurrence_record.OccurrenceRecordTag.CommentFromSource,
    )
    for record in location.occurrence_records:
        for tag_type in occurrence_text_tags:
            for tag in record.get_tags(record.tags, tag_type):
                if isinstance(
                    tag, models.occurrence_record.OccurrenceRecordTag.LocationHint
                ):
                    text = tag.name
                else:
                    text = tag.text
                if text:
                    evidence.extend(
                        _LinkedPLSSEvidence(
                            extracted.description,
                            f"OccurrenceRecord {record.id} {type(tag).__name__} {text!r}",
                            extracted.has_alternative_section,
                        )
                        for extracted in plss.extract_plss(text)
                    )
    return evidence


def _get_location_extent(location: Location) -> coordinate_lint.CoordinateExtent | None:
    if location.latitude is None or location.longitude is None:
        return None
    return coordinate_lint.make_extent(location.latitude, location.longitude)


def _replace_plss_tag(
    location: Location, old_tag: LocationTag.PLSS, new_tag: LocationTag.PLSS  # type: ignore[name-defined]
) -> None:
    """Replace a PLSS tag and keep PLSS coordinate provenance attached to it."""
    new_tags = []
    for tag in location.tags or ():
        if tag == old_tag:
            tag = new_tag
        elif (
            isinstance(tag, LocationTag.CoordinatesFromPLSS)
            and tag.plss_id == old_tag.plss_id
            and old_tag.plss_id != new_tag.plss_id
        ):
            tag = LocationTag.CoordinatesFromPLSS(new_tag.plss_id)
        if tag not in new_tags:
            new_tags.append(tag)
    location.tags = tuple(new_tags)  # type: ignore[assignment]


@LINT.add("plss_tag")
def check_plss_tag(location: Location, cfg: LintConfig) -> Iterable[str]:
    tags = list(location.get_tags(location.tags, LocationTag.PLSS))
    if len(tags) > 1:
        yield f"has multiple PLSS tags: {tags}"
    for tag in tags:
        if not plss.is_valid_plss_id(tag.plss_id):
            yield f"PLSS identifier {tag.plss_id!r} is not a valid CadNSDI PLSSID"
        extracted = plss.extract_plss(tag.text)
        description = plss.parse_canonical(tag.text)
        if description is None:
            if len(extracted) == 1:
                description = extracted[0].description
                canonical_text = description.canonical_text
                message = (
                    f"PLSS text {tag.text!r} is not canonical; use "
                    f"{canonical_text!r}"
                )
                if cfg.autofix and not LINT.is_ignoring_lint(location, "plss_tag"):
                    print(f"{location}: {message}")
                    new_tag = adt.replace(tag, text=canonical_text)
                    _replace_plss_tag(location, tag, new_tag)
                    tag = new_tag
                else:
                    yield message
            else:
                yield f"PLSS text {tag.text!r} does not contain one complete description"
                continue
        if description.meridian is None:
            yield f"PLSS text {tag.text!r} must include the resolved principal meridian"
    if tags and _get_us_plss_context(location) is None:
        yield "has a PLSS tag but is not assigned within a recognized U.S. state"


@LINT.add("general_plss")
def check_general_plss(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.is_general() and location.has_tag(LocationTag.PLSS):
        yield (
            "general location should not have a precise PLSS tag; move the tag to "
            "an exact Location or remove the General status"
        )


def _format_plss_bounds(geometries: Iterable[dict[str, Any]]) -> str | None:
    bounds = plss.geometry_bounds(geometries)
    if bounds is None:
        return None
    south, north, west, east = bounds
    south_text, _ = coordinate_lint.standardize_coordinate(
        f"{south:.6f}", is_latitude=True
    )
    north_text, _ = coordinate_lint.standardize_coordinate(
        f"{north:.6f}", is_latitude=True
    )
    west_text, _ = coordinate_lint.standardize_coordinate(
        f"{west:.6f}", is_latitude=False
    )
    east_text, _ = coordinate_lint.standardize_coordinate(
        f"{east:.6f}", is_latitude=False
    )
    latitude = south_text if south_text == north_text else f"{south_text}-{north_text}"
    longitude = west_text if west_text == east_text else f"{west_text}-{east_text}"
    return f"{latitude}, {longitude}"


def _resolve_plss_evidence(
    description: plss.PLSSDescription,
    *,
    state_code: str,
    state_fips: str,
    county_name: str | None,
    extent: coordinate_lint.CoordinateExtent | None,
) -> plss.TownshipResolution:
    return plss.resolve_township(
        description,
        state_code=state_code,
        state_fips=state_fips,
        county_name=county_name,
        point=extent.point if extent is not None else None,
        extent=extent,
    )


def _common_plss_description(
    descriptions: Collection[plss.PLSSDescription],
) -> plss.PLSSDescription | None:
    """Return the most specific PLSS description supported by every item.

    Distinct sections or nonoverlapping aliquots are not distinct township/range
    assignments.  A Location that genuinely covers both can still retain the
    common section or township.  Different township/range pairs remain
    incompatible and return ``None``.
    """
    if not descriptions:
        return None
    first = next(iter(descriptions))
    if any(
        description.township_key != first.township_key for description in descriptions
    ):
        return None
    meridians = {description.meridian for description in descriptions}
    if len(meridians) != 1:
        return None

    sections = {description.section for description in descriptions}
    if len(sections) != 1 or None in sections:
        return plss.PLSSDescription(
            first.township,
            first.township_fraction,
            first.township_direction,
            first.range,
            first.range_fraction,
            first.range_direction,
            meridian=first.meridian,
        )

    section = next(iter(sections))
    assert section is not None
    most_specific = max(descriptions, key=lambda description: description.specificity)
    if all(
        description.is_compatible_with(most_specific) for description in descriptions
    ):
        return most_specific
    return plss.PLSSDescription(
        first.township,
        first.township_fraction,
        first.township_direction,
        first.range,
        first.range_fraction,
        first.range_direction,
        section=section,
        meridian=first.meridian,
    )


@LINT.add("plss", requires_network=True)
def check_plss(location: Location, cfg: LintConfig) -> Iterable[str]:
    tags = list(location.get_tags(location.tags, LocationTag.PLSS))
    evidence = [] if tags else _get_linked_plss_evidence(location)
    if not tags and not evidence:
        return
    context = _get_us_plss_context(location)
    if context is None:
        return
    if tags and location.is_general():
        # The local general_plss lint reports the modeling problem. Avoid deriving
        # further exact metadata for a Location that is intentionally broad.
        return
    state_code, state_fips, county_name = context
    extent = _get_location_extent(location)

    if len(tags) == 1:
        tag = tags[0]
        parsed_tag = plss.parse_canonical(tag.text)
        if parsed_tag is not None:
            try:
                resolution = _resolve_plss_evidence(
                    parsed_tag,
                    state_code=state_code,
                    state_fips=state_fips,
                    county_name=county_name,
                    extent=extent,
                )
            except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
                yield f"could not check PLSS tag against external data: {exc}"
                return
            if not resolution.is_resolved:
                yield f"PLSS tag {tag.text!r} could not be resolved: {resolution.problem}"
            else:
                assert resolution.township is not None
                tag_township = resolution.township
                resolved_text = parsed_tag.with_meridian(
                    tag_township.meridian
                ).canonical_text
                replacement = adt.replace(
                    tag, text=resolved_text, plss_id=tag_township.plss_id
                )
                if tag.text != resolved_text:
                    message = (
                        f"PLSS tag uses meridian text {parsed_tag.meridian!r}; use the "
                        f"BLM name in {resolved_text!r}"
                    )
                    if not cfg.autofix or LINT.is_ignoring_lint(location, "plss"):
                        yield message
                    else:
                        print(f"{location}: {message}")
                if tag.plss_id != tag_township.plss_id:
                    message = (
                        f"PLSS tag identifier is {tag.plss_id!r}, but {tag.text!r} "
                        f"resolves to {tag_township.plss_id!r}"
                    )
                    if not cfg.autofix or LINT.is_ignoring_lint(location, "plss"):
                        yield message
                    else:
                        print(f"{location}: {message}")
                if (
                    replacement != tag
                    and cfg.autofix
                    and not LINT.is_ignoring_lint(location, "plss")
                ):
                    _replace_plss_tag(location, tag, replacement)
                    tag = replacement
                try:
                    resolved_geometry = plss.get_description_geometry(
                        tag_township, parsed_tag
                    )
                except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
                    yield f"could not retrieve PLSS polygon: {exc}"
                else:
                    if not resolved_geometry.geometries:
                        if cfg.verbose:
                            yield (
                                f"BLM has no {resolved_geometry.level} polygon for "
                                f"{tag.text!r}"
                            )
                    elif extent is not None:
                        distance = plss.geometry_extent_distance_km(
                            extent, resolved_geometry.geometries
                        )
                        if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
                            yield (
                                f"coordinates are {distance:.1f} km from the "
                                f"{resolved_geometry.level} polygon for {tag.text!r}"
                            )
                    elif (
                        bounds_text := _format_plss_bounds(resolved_geometry.geometries)
                    ) is not None:
                        message = (
                            f"coordinate bounds could be {bounds_text}, inferred from "
                            f"the {resolved_geometry.level} polygon for {tag.text!r}"
                        )
                        if (
                            cfg.autofix
                            and location.latitude is None
                            and location.longitude is None
                            and not LINT.is_ignoring_lint(location, "plss")
                        ):
                            print(f"{location}: {message}")
                            latitude, longitude = bounds_text.split(", ", maxsplit=1)
                            location.latitude = latitude
                            location.longitude = longitude
                            _add_coordinate_provenance(
                                location,
                                [LocationTag.CoordinatesFromPLSS(tag_township.plss_id)],
                            )
                        else:
                            yield message

    if tags:
        # A stored PLSS tag is the reviewed Location-level interpretation. Validate
        # that tag and its coordinates above, but leave conflicts in linked Name
        # evidence to the Name lint so a source error does not make the Location itself
        # perpetually unclean.
        return

    resolved_evidence: list[
        tuple[_LinkedPLSSEvidence, plss.Township, plss.PLSSDescription]
    ] = []
    for item in evidence:
        description = item.description
        if item.has_alternative_section:
            # A source such as "sections 22 and 27, T23N R9W" does not support
            # either individual section, but it still unambiguously supports the
            # enclosing township.  Retaining that common structured marker is both
            # more informative and less noisy than treating the section list as an
            # irresolvable conflict.
            description = plss.PLSSDescription(
                description.township,
                description.township_fraction,
                description.township_direction,
                description.range,
                description.range_fraction,
                description.range_direction,
                meridian=description.meridian,
            )
        try:
            resolution = _resolve_plss_evidence(
                description,
                state_code=state_code,
                state_fips=state_fips,
                county_name=county_name,
                extent=extent,
            )
        except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
            yield f"could not resolve PLSS data from {item.source}: {exc}"
            continue
        if not resolution.is_resolved:
            yield f"PLSS data from {item.source} could not be resolved: {resolution.problem}"
            continue
        assert resolution.township is not None
        description = description.with_meridian(resolution.township.meridian)
        if extent is not None:
            try:
                resolved_geometry = plss.get_description_geometry(
                    resolution.township, description
                )
            except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
                yield f"could not retrieve PLSS polygon for {item.source}: {exc}"
                continue
            if not resolved_geometry.geometries:
                if cfg.verbose:
                    yield (
                        f"BLM has no {resolved_geometry.level} polygon for PLSS data "
                        f"from {item.source}"
                    )
                continue
            distance = plss.geometry_extent_distance_km(
                extent, resolved_geometry.geometries
            )
            if distance > coordinate_lint.COORDINATE_TOLERANCE_KM:
                yield (
                    f"coordinates are {distance:.1f} km from the "
                    f"{resolved_geometry.level} polygon for PLSS data from {item.source}"
                )
                continue
        resolved_evidence.append((item, resolution.township, description))

    if not resolved_evidence:
        return

    first_item, first_township, first_description = resolved_evidence[0]
    township_conflict = next(
        (
            (item, township, description)
            for item, township, description in resolved_evidence[1:]
            if township.plss_id != first_township.plss_id
        ),
        None,
    )
    if township_conflict is not None:
        item, township, description = township_conflict
        yield (
            "Location evidence resolves to distinct township/range data: "
            f"{first_description.canonical_text!r} ({first_township.plss_id}, from "
            f"{first_item.source}) and {description.canonical_text!r} "
            f"({township.plss_id}, from {item.source})"
        )
        return

    common_description = _common_plss_description(
        [description for _, _, description in resolved_evidence]
    )
    assert common_description is not None
    most_specific_township = first_township
    most_specific = common_description
    expected = LocationTag.PLSS(
        most_specific.canonical_text, most_specific_township.plss_id
    )
    if location.is_general():
        yield (
            f"general location has precise PLSS evidence {expected!r}, inferred "
            f"from {first_item.source}; make a separate exact Location if the "
            "evidence is accepted"
        )
        return
    message = f"add {expected!r}, inferred from {first_item.source}"
    if cfg.autofix and not LINT.is_ignoring_lint(location, "plss"):
        print(f"{location}: {message}")
        location.add_tag(expected)
        if location.latitude is None and location.longitude is None:
            try:
                resolved_geometry = plss.get_description_geometry(
                    most_specific_township, most_specific
                )
            except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
                yield f"could not retrieve PLSS polygon: {exc}"
            else:
                bounds_text = _format_plss_bounds(resolved_geometry.geometries)
                if bounds_text is not None:
                    coordinate_message = (
                        f"coordinate bounds could be {bounds_text}, inferred from "
                        f"the {resolved_geometry.level} polygon for {expected.text!r}"
                    )
                    print(f"{location}: {coordinate_message}")
                    latitude, longitude = bounds_text.split(", ", maxsplit=1)
                    location.latitude = latitude
                    location.longitude = longitude
                    _add_coordinate_provenance(
                        location,
                        [
                            LocationTag.CoordinatesFromPLSS(
                                most_specific_township.plss_id
                            )
                        ],
                    )
                elif cfg.verbose:
                    yield (
                        f"BLM has no {resolved_geometry.level} polygon for "
                        f"{expected.text!r}"
                    )
    else:
        yield message


@LINT.add("fully_divided_region")
def check_fully_divided_region(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.is_general() or location.has_tag(LocationTag.Unplaced):
        return
    region = location.region
    if not region.has_children() or region.has_tag(RegionTag.IncompletelyDivided):
        return
    yield (
        f"is directly assigned to fully divided Region {region.name!r}; move it to "
        "a child Region or add the General or Unplaced tag"
    )


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


def _locations_have_matching_context(left: Location, right: Location) -> bool:
    return (
        left.region.id == right.region.id
        and left.min_period == right.min_period
        and left.max_period == right.max_period
        and left.stratigraphic_unit == right.stratigraphic_unit
    )


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
                if not _locations_have_matching_context(left, right):
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


def _iter_location_equivalences(text: str) -> Iterable[tuple[str, bool, int]]:
    for match in _EDITORIAL_LOCATION_EQUIVALENCE.finditer(text):
        parenthetical = match.group("parenthetical")
        if parenthetical is not None:
            content = parenthetical.strip()
            explicitly_marked = True
        else:
            content = match.group("bracketed").strip()
            explicitly_marked = content.startswith("=")
        equivalent = re.sub(r"^=\s*", "", content).strip()
        if equivalent:
            yield equivalent, explicitly_marked, match.start()


def extract_bracketed_location_equivalences(text: str) -> tuple[str, ...]:
    """Extract bracketed and explicitly parenthesized locality equivalents."""
    return tuple(equivalent for equivalent, _, _ in _iter_location_equivalences(text))


def _bracket_follows_location_name(
    text: str, start: int, location: Location, *, explicitly_marked: bool
) -> bool:
    prefix = text[:start].rstrip()
    if not explicitly_marked and prefix.endswith((",", ";", ":")):
        return False
    base_name = ParsedLocationName.parse(location.name).base_name
    normalized_name = helpers.simplify_string(base_name, clean_words=False)
    normalized_prefix = helpers.simplify_string(prefix, clean_words=False)
    return bool(normalized_name) and normalized_prefix.endswith(normalized_name)


def _equivalence_matches_location(
    current: Location, equivalent: str, candidate: Location, *, explicitly_marked: bool
) -> bool:
    if are_likely_synonymous_names(equivalent, candidate.name):
        return True
    if not explicitly_marked:
        return False

    # An explicit "[= ...]" is stronger evidence than an unmarked editorial
    # bracket, so permit one additional edit without weakening the general
    # likely-synonym comparison. This covers chains such as Tuare/Toware.
    equivalent_base, equivalent_feature = _locality_similarity_key(equivalent)
    candidate_base, candidate_feature = _locality_similarity_key(candidate.name)
    if not equivalent_base or not candidate_base:
        return False
    if equivalent_feature != candidate_feature and (
        equivalent_feature is not None or candidate_feature is not None
    ):
        return False
    if min(len(equivalent_base), len(candidate_base)) < 5:
        return False
    if any(character.isdigit() for character in equivalent_base + candidate_base):
        return False
    if equivalent_base[0] != candidate_base[0]:
        return False
    return are_likely_synonymous_names(
        current.name, candidate.name
    ) and _edit_distance_at_most(equivalent_base, candidate_base, 2)


def _build_locations_by_region(
    locations: Iterable[Location],
) -> dict[int, tuple[Location, ...]]:
    by_region: dict[int, list[Location]] = defaultdict(list)
    for location in locations:
        if not location.is_general():
            by_region[location.region.id].append(location)
    return {
        region_id: tuple(region_locations)
        for region_id, region_locations in by_region.items()
    }


@cache
def _get_locations_by_region() -> dict[int, tuple[Location, ...]]:
    return _build_locations_by_region(Location.select_valid())


@LINT.add(
    "explicit_location_equivalence", clear_caches=_get_locations_by_region.cache_clear
)
def check_explicit_location_equivalence(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if location.is_general():
        return
    candidates = _get_locations_by_region().get(location.region.id, ())
    reported: set[tuple[str, int]] = set()
    for name in location.type_localities:
        for tag in name.get_tags(name.type_tags, models.name.TypeTag.LocationDetail):
            for equivalent, explicitly_marked, start in _iter_location_equivalences(
                tag.text
            ):
                if not _bracket_follows_location_name(
                    tag.text, start, location, explicitly_marked=explicitly_marked
                ):
                    continue
                for candidate in candidates:
                    if candidate.id == location.id:
                        continue
                    if not _locations_have_matching_context(location, candidate):
                        continue
                    if not _equivalence_matches_location(
                        location,
                        equivalent,
                        candidate,
                        explicitly_marked=explicitly_marked,
                    ):
                        continue

                    # The cached Region map is only a candidate index. Confirm that
                    # the record remains valid and still matches before reporting it.
                    candidate = candidate.reload()
                    if candidate.is_invalid() or candidate.id == location.id:
                        continue
                    if not _locations_have_matching_context(location, candidate):
                        continue
                    if not _equivalence_matches_location(
                        location,
                        equivalent,
                        candidate,
                        explicitly_marked=explicitly_marked,
                    ):
                        continue
                    key = (helpers.simplify_string(equivalent), candidate.id)
                    if key in reported:
                        continue
                    reported.add(key)
                    yield (
                        f"LocationDetail on Name {name.id} contains bracketed "
                        f"equivalent {equivalent!r}, matching valid Location "
                        f"{candidate.id}: {candidate.name!r} in Region "
                        f"{location.region.name!r}: {tag.text!r}"
                    )


def _coordinate_collision_key(
    location: Location,
) -> tuple[float, float, float, float] | None:
    if (
        not is_recent_location(location)
        or location.latitude is None
        or location.longitude is None
    ):
        return None
    parsed = coordinate_lint.standardize_coordinate_pair(
        location.latitude, location.longitude
    )
    if parsed is None:
        return None
    extent = parsed[2]
    return (
        extent.latitude.minimum,
        extent.latitude.maximum,
        extent.longitude.minimum,
        extent.longitude.maximum,
    )


def _build_coordinate_collision_map(
    locations: Iterable[Location],
) -> dict[int, tuple[Location, ...]]:
    by_coordinates: defaultdict[tuple[float, float, float, float], list[Location]] = (
        defaultdict(list)
    )
    for location in locations:
        key = _coordinate_collision_key(location)
        if key is not None:
            by_coordinates[key].append(location)

    output: dict[int, tuple[Location, ...]] = {}
    for group in by_coordinates.values():
        if len(group) < 2:
            continue
        sorted_group = tuple(sorted(group, key=lambda item: item.id))
        for location in sorted_group:
            output[location.id] = sorted_group
    return output


@cache
def _get_coordinate_collision_map() -> dict[int, tuple[Location, ...]]:
    return _build_coordinate_collision_map(Location.select_valid())


def _coordinate_collision_plss_key(location: Location) -> plss.PLSSDescription | None:
    """Return the one PLSS description that deliberately supplied the extent."""
    tags = tuple(location.tags or ())
    provenance = tuple(
        tag for tag in tags if isinstance(tag, LocationTag.CoordinatesFromPLSS)
    )
    if len(provenance) != 1:
        return None
    matching = tuple(
        tag
        for tag in tags
        if isinstance(tag, LocationTag.PLSS)
        if tag.plss_id == provenance[0].plss_id
    )
    if len(matching) != 1:
        return None
    return plss.parse_canonical(matching[0].text)


@LINT.add(
    "coordinate_collision", clear_caches=_get_coordinate_collision_map.cache_clear
)
def check_coordinate_collision(location: Location, cfg: LintConfig) -> Iterable[str]:
    group = _get_coordinate_collision_map().get(location.id)
    if group is None:
        return
    # The cached map is only a candidate index. Coordinates may have changed
    # since it was built, so reload every candidate and verify the collision
    # against current database values before reporting it.
    location = location.reload()
    if location.is_invalid():
        return
    coordinate_key = _coordinate_collision_key(location)
    if coordinate_key is None:
        return
    plss_key = _coordinate_collision_plss_key(location)
    others = [
        refreshed
        for item in group
        if item.id != location.id
        if not (refreshed := item.reload()).is_invalid()
        if _coordinate_collision_key(refreshed) == coordinate_key
        # Multiple source localities in the same PLSS section or aliquot are
        # expected to share the polygon used as their coordinate extent.  That is
        # not an independently coincident point and therefore is not a collision.
        if plss_key is None or _coordinate_collision_plss_key(refreshed) != plss_key
    ]
    if not others:
        return
    coordinates = f"{location.latitude}, {location.longitude}"
    other_text = ", ".join(
        f"{item.id}: {item.name!r} ({item.region.name})" for item in others
    )
    other_regions = {item.region.id for item in others}
    if location.region.id not in other_regions or len(other_regions) > 1:
        qualification = " in different Regions"
    else:
        qualification = ""
    yield (
        f"exact coordinates {coordinates} are shared with Location(s){qualification}: "
        f"{other_text}"
    )


def _is_location_name_taken(name: str) -> bool:
    return Location.select().filter(Location.name == name).count() > 0


def _maybe_autofix_name(
    location: Location,
    cfg: LintConfig,
    *,
    lint_label: str,
    proposed_name: str,
    message: str,
) -> str | None:
    if not cfg.autofix or LINT.is_ignoring_lint(location, lint_label):
        return message
    if _is_location_name_taken(proposed_name):
        return f"{message}; cannot autofix because that name is already in use"
    print(f"{location}: {message}")
    location.name = proposed_name
    return None


@LINT.add("location_name")
def check_location_name(location: Location, cfg: LintConfig) -> Iterable[str]:
    parsed_name = ParsedLocationName.parse(location.name)
    if split_trailing_parenthetical(parsed_name.base_name) is not None and (
        parsed_name.disambiguator is None
        or not _looks_like_coordinate_pair(parsed_name.disambiguator)
    ):
        yield "location name may contain only one parenthetical disambiguator"
        return
    proposed_name = parsed_name.render()
    if location.name == proposed_name:
        return
    yield f"location name should be {proposed_name!r}"


@LINT.add("offset_name")
def check_offset_name(location: Location, cfg: LintConfig) -> Iterable[str]:
    search_plan = get_nominatim_search_plan(location)
    proposed_name = search_plan.standardized_name
    if not search_plan.offsets or location.name == proposed_name:
        return
    message = f"distance-offset name should be {proposed_name!r}"
    if issue := _maybe_autofix_name(
        location,
        cfg,
        lint_label="offset_name",
        proposed_name=proposed_name,
        message=message,
    ):
        yield issue


def _looks_like_coordinate_pair(text: str) -> bool:
    if _SIGNED_DECIMAL_COORDINATE_PAIR.fullmatch(text) is not None:
        return True
    return _DIRECTIONAL_COORDINATE_LIKE.fullmatch(text) is not None


def _parse_coordinate_pair(
    text: str,
) -> tuple[str, str, coordinate_lint.CoordinateExtent] | None:
    match = _SIGNED_DECIMAL_COORDINATE_PAIR.fullmatch(text)
    if match is None:
        match = _DIRECTIONAL_COORDINATE_PAIR.fullmatch(text)
    if match is None:
        return None
    return coordinate_lint.standardize_coordinate_pair(
        match.group("latitude").upper(), match.group("longitude").upper()
    )


def _get_coordinate_modifier_plan(name: str) -> CoordinateModifierPlan | None:
    parsed_name = ParsedLocationName.parse(name)
    if parsed_name.modifier is not None and _looks_like_coordinate_pair(
        parsed_name.modifier
    ):
        raw_coordinates = parsed_name.modifier
        prefix = ParsedLocationName(parsed_name.base_name, parsed_name.disambiguator)
    elif (
        parsed_name.modifier is None
        and parsed_name.disambiguator is not None
        and _looks_like_coordinate_pair(parsed_name.disambiguator)
    ):
        raw_coordinates = parsed_name.disambiguator
        prefix = ParsedLocationName.parse(parsed_name.base_name)
    else:
        return None
    return CoordinateModifierPlan(
        prefix, raw_coordinates, _parse_coordinate_pair(raw_coordinates)
    )


def _coordinate_extents_are_equal(
    first: coordinate_lint.CoordinateExtent, second: coordinate_lint.CoordinateExtent
) -> bool:
    return (
        first.latitude.minimum == second.latitude.minimum
        and first.latitude.maximum == second.latitude.maximum
        and first.longitude.minimum == second.longitude.minimum
        and first.longitude.maximum == second.longitude.maximum
    )


@LINT.add("coordinate_modifier")
def check_coordinate_modifier(location: Location, cfg: LintConfig) -> Iterable[str]:
    plan = _get_coordinate_modifier_plan(location.name)
    if plan is None:
        return
    if plan.parsed_coordinates is None:
        yield f"invalid coordinate modifier {plan.raw_coordinates!r}"
        return

    latitude, longitude, extent = plan.parsed_coordinates
    proposed_name = plan.standardized_name
    assert proposed_name is not None
    if location.name != proposed_name:
        message = f"coordinate-offset name should be {proposed_name!r}"
        if issue := _maybe_autofix_name(
            location,
            cfg,
            lint_label="coordinate_modifier",
            proposed_name=proposed_name,
            message=message,
        ):
            yield issue

    for issue in coordinate_lint.check_extent_in_region(extent, location.region):
        yield f"coordinate modifier {latitude} {longitude}: {issue}"

    if location.latitude is None or location.longitude is None:
        yield (
            f"coordinate modifier is {latitude} {longitude}, but Location coordinates "
            "are missing or incomplete"
        )
        return
    location_coordinates = coordinate_lint.standardize_coordinate_pair(
        location.latitude, location.longitude
    )
    if location_coordinates is None:
        yield (
            f"coordinate modifier is {latitude} {longitude}, but Location coordinates "
            f"{location.latitude}, {location.longitude} are invalid"
        )
        return
    location_latitude, location_longitude, location_extent = location_coordinates
    if _coordinate_extents_are_equal(extent, location_extent):
        return
    distance = coordinate_lint.extent_distance_km(extent, location_extent)
    yield (
        f"coordinate modifier {latitude} {longitude} does not match Location "
        f"coordinates {location_latitude}, {location_longitude} "
        f"({distance:.1f} km apart)"
    )


@cache
def _get_periods_by_name() -> dict[str, tuple[Period, ...]]:
    periods_by_name: defaultdict[str, list[Period]] = defaultdict(list)
    for period in Period.select_valid():
        periods_by_name[period.name].append(period)
    return {name: tuple(periods) for name, periods in periods_by_name.items()}


def _get_qualified_name_variants(name: str) -> set[str]:
    variants = {name}
    if split := split_trailing_parenthetical(name):
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


def _get_nearby_regions(location: Location) -> tuple[Region, ...]:
    parsed_name = ParsedLocationName.parse(location.name)
    if parsed_name.modifier is None:
        return ()
    return tuple(
        tag.region for tag in location.get_tags(location.tags, LocationTag.NearbyRegion)
    )


def _has_primary_region_base_name(
    location: Location, parsed_name: ParsedLocationName
) -> bool:
    return parsed_name.base_name == location.region.name or any(
        parsed_name.base_name == region.name for region in _get_nearby_regions(location)
    )


def _is_sane_disambiguator(location: Location, disambiguator: str) -> bool:
    if disambiguator.casefold() in _ALWAYS_ALLOWED_DISAMBIGUATORS:
        return True
    regions = (
        location.region,
        *location.region.all_parents(),
        *(
            candidate
            for nearby_region in _get_nearby_regions(location)
            for candidate in (nearby_region, *nearby_region.all_parents())
        ),
    )
    if any(
        disambiguator in _get_qualified_name_variants(region.name) for region in regions
    ):
        return True
    if _is_period_disambiguator(location, disambiguator):
        return True
    return _is_stratigraphic_unit_disambiguator(location, disambiguator)


@LINT.add("disambiguator", clear_caches=_get_periods_by_name.cache_clear)
def check_disambiguator(location: Location, cfg: LintConfig) -> Iterable[str]:
    parsed_name = ParsedLocationName.parse(location.name)
    disambiguator = parsed_name.disambiguator
    if disambiguator is None or _is_sane_disambiguator(location, disambiguator):
        return
    offsets, remainder = _parse_locality_offsets(disambiguator)
    if offsets and not remainder:
        # The offset_name lint owns this legacy parenthetical form.
        return
    if _looks_like_coordinate_pair(disambiguator):
        # The coordinate_modifier lint owns this legacy parenthetical form.
        return

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
    if location.is_general() and extent.point is not None:
        yield "general location should use a coordinate range, not point coordinates"
        return
    if location.has_tag(LocationTag.Unplaced) and extent.point is not None:
        yield "unplaced location should use a coordinate range, not point coordinates"
        return
    is_reviewed_unplaced = location.has_tag(LocationTag.Unplaced)
    yield from coordinate_lint.check_extent_in_region(
        extent,
        location.region,
        require_full_containment=not location.is_general() and not is_reviewed_unplaced,
    )
    if (
        extent.point is not None
        or location.is_general()
        or is_reviewed_unplaced
        or not is_recent_location(location)
    ):
        return
    radius_km = coordinate_lint.extent_radius_km(extent)
    if radius_km <= _POSSIBLE_GENERAL_MINIMUM_RADIUS_KM:
        return
    message = (
        f"non-General location has a coordinate range with {radius_km:.1f} km "
        "radius; review for the General tag or replace it with narrower "
        "evidence-backed coordinates"
    )
    precise_sources = [
        f"{latitude}, {longitude} from {source}"
        for latitude, longitude, candidate_extent, source in (
            _get_linked_coordinate_candidates(location)
        )
        if candidate_extent.point is not None
        and coordinate_lint.extent_distance_km(extent, candidate_extent) == 0
    ]
    if precise_sources:
        message += "; the range contains more precise linked evidence: " + "; ".join(
            precise_sources
        )
    yield message


def _should_infer_coordinates(location: Location) -> bool:
    if (
        location.is_invalid()
        or location.is_general()
        or location.has_tag(LocationTag.Unplaced)
    ):
        return False
    return not (
        location.stratigraphic_unit is not None
        and re.fullmatch(rf"{location.stratigraphic_unit.name} \(.*\)", location.name)
    )


@dataclass(frozen=True, slots=True)
class _LinkedCoordinateEvidence:
    latitude: str
    longitude: str
    extent: coordinate_lint.CoordinateExtent
    source: str
    provenance: Any


def _get_linked_coordinate_evidence(
    location: Location,
) -> list[_LinkedCoordinateEvidence]:
    evidence: list[_LinkedCoordinateEvidence] = []
    seen_name_extents: set[tuple[int, str, str]] = set()
    for name in location.type_localities:
        for tag in name.get_tags(name.type_tags, models.name.TypeTag.Coordinates):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                latitude, longitude, extent = parsed
                seen_name_extents.add((name.id, latitude, longitude))
                evidence.append(
                    _LinkedCoordinateEvidence(
                        latitude,
                        longitude,
                        extent,
                        f"Name {name}",
                        LocationTag.CoordinatesFromName(name),
                    )
                )
        for tag in _get_applicable_coordinate_text_tags(name):
            for extracted in _extract_name_text_tag_coordinate_pairs(name, tag):
                parsed = coordinate_lint.standardize_coordinate_pair(*extracted)
                if parsed is None:
                    continue
                latitude, longitude, extent = parsed
                key = (name.id, latitude, longitude)
                if key in seen_name_extents:
                    continue
                seen_name_extents.add(key)
                evidence.append(
                    _LinkedCoordinateEvidence(
                        latitude,
                        longitude,
                        extent,
                        f"Name {name} {type(tag).__name__} {tag.text!r}",
                        LocationTag.CoordinatesFromName(name),
                    )
                )
    for record in location.occurrence_records:
        for tag in record.get_tags(
            record.tags, models.occurrence_record.OccurrenceRecordTag.Coordinates
        ):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                latitude, longitude, extent = parsed
                evidence.append(
                    _LinkedCoordinateEvidence(
                        latitude,
                        longitude,
                        extent,
                        f"OccurrenceRecord {record}",
                        LocationTag.CoordinatesFromOccurrenceRecord(record.id),
                    )
                )
    return evidence


def _get_linked_coordinate_candidates(
    location: Location,
) -> list[tuple[str, str, Any, str]]:
    return [
        (item.latitude, item.longitude, item.extent, item.source)
        for item in _get_linked_coordinate_evidence(location)
    ]


def _add_coordinate_provenance(location: Location, tags: Iterable[Any]) -> None:
    existing = tuple(location.tags or ())
    for tag in dict.fromkeys(tags):
        if tag not in existing:
            location.add_tag(tag)
            existing = (*existing, tag)


def _replace_location_tag(location: Location, old_tag: Any, new_tag: Any) -> None:
    location.tags = tuple(  # type: ignore[assignment]
        dict.fromkeys(new_tag if tag == old_tag else tag for tag in location.tags or ())
    )


def _nominatim_provenance_tag(
    result: nominatim.SearchResult, *, use_bounding_box: bool
) -> Any | None:
    if result.osm_type is None or result.osm_id is None:
        return None
    return LocationTag.CoordinatesFromNominatim(
        result.osm_type, result.osm_id, result.category, use_bounding_box
    )


@LINT.add("linked_coordinates")
def check_linked_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.latitude is not None or location.longitude is not None:
        return
    if not _should_infer_coordinates(location):
        return
    evidence = _get_linked_coordinate_evidence(location)
    if not evidence:
        return

    first = evidence[0]
    latitude, longitude, extent, source = (
        first.latitude,
        first.longitude,
        first.extent,
        first.source,
    )
    combined_extent = extent
    for item in evidence[1:]:
        other_extent, other_source = item.extent, item.source
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
        _add_coordinate_provenance(location, (item.provenance for item in evidence))
    else:
        yield message


@cache
def _get_geonames_country_code(country_name: str) -> str | None:
    query_names = (
        country_name,
        nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(country_name, country_name),
    )
    try:
        for query_name in dict.fromkeys(query_names):
            matches = geonames.search_name(
                query_name,
                feature_classes=("A",),
                feature_codes=_GEONAMES_COUNTRY_FEATURE_CODES,
                limit=10,
            )
            codes: set[str] = {
                match.record.country_code
                for match in matches
                if match.record.country_code
            }
            if len(codes) == 1:
                return codes.pop()
            if len(codes) > 1:
                return None
    except RuntimeError:
        # GeoNames is an optional local data source. An unset or missing database
        # should not make every Location lint fail.
        return None
    return None


def _get_geonames_coordinate_matches(
    location: Location,
) -> list[GeoNamesCoordinateCandidate]:
    search_plan = get_nominatim_search_plan(location)
    if not search_plan.coordinates_can_be_inferred:
        return []
    country_name = _get_region_country_name(location.region)
    if country_name is None:
        return []
    country_code = _get_geonames_country_code(country_name)
    if country_code is None:
        return []
    try:
        matches = geonames.search_name(
            search_plan.locality_name,
            country_code=country_code,
            limit=_GEONAMES_SEARCH_LIMIT,
        )
    except RuntimeError:
        return []

    expected_region = next(
        (
            region
            for region in (location.region, *location.region.all_parents())
            if region.kind in _SUBNATIONAL_REGION_KINDS
        ),
        None,
    )
    expected_region_codes = (
        frozenset()
        if expected_region is None
        else _get_geonames_region_code_prefixes_for_region(
            country_code, expected_region
        )
    )
    candidates = []
    for match in matches:
        parsed = _get_coordinates_with_offsets(
            str(match.record.latitude),
            str(match.record.longitude),
            offsets=search_plan.offsets,
        )
        if parsed is None:
            continue
        latitude, longitude, extent = parsed
        region_issues = _get_geonames_region_issues(
            location, extent, match.record, expected_region, expected_region_codes
        )
        candidates.append(
            GeoNamesCoordinateCandidate(
                match, latitude, longitude, extent, region_issues
            )
        )
    return candidates


def _get_record_admin_code_prefixes(
    record: geonames.GeoNamesRecord,
) -> frozenset[tuple[str, ...]]:
    codes = (
        record.admin1_code,
        record.admin2_code,
        record.admin3_code,
        record.admin4_code,
    )
    return frozenset(
        (f"ADM{level}", *codes[:level])
        for level, code in enumerate(codes, start=1)
        if code and all(codes[:level])
    )


@cache
def _get_geonames_region_code_prefixes(
    country_code: str,
    region_names: tuple[str, ...],
    parent_code_prefixes: tuple[tuple[str, ...], ...] = (),
) -> frozenset[tuple[str, ...]]:
    # Prefer a qualified alias (for example "Marin County" or "La Paz
    # Department") when GeoNames recognizes one. Bare names are more likely to
    # collide with a lower-level administrative unit elsewhere in the country.
    for region_name in sorted(set(region_names), key=lambda name: (-len(name), name)):
        try:
            matches = geonames.search_name(
                region_name,
                country_code=country_code,
                feature_classes=("A",),
                feature_codes=_GEONAMES_ADMIN_FEATURE_CODES,
                limit=_GEONAMES_SEARCH_LIMIT,
            )
        except RuntimeError:
            return frozenset()
        prefixes = frozenset(
            prefix
            for match in matches
            for prefix in _get_record_admin_code_prefixes(match.record)
            if prefix[0] == match.record.feature_code
            and (
                not parent_code_prefixes
                or any(
                    len(prefix) > len(parent_prefix)
                    and prefix[1 : len(parent_prefix)] == parent_prefix[1:]
                    for parent_prefix in parent_code_prefixes
                )
            )
        )
        if prefixes:
            return prefixes
    return frozenset()


def _get_geonames_region_code_prefixes_for_region(
    country_code: str, region: Region
) -> frozenset[tuple[str, ...]]:
    regions = [
        candidate
        for candidate in reversed((region, *region.all_parents()))
        if candidate.kind in _SUBNATIONAL_REGION_KINDS
    ]
    parent_code_prefixes: tuple[tuple[str, ...], ...] = ()
    for candidate in regions:
        prefixes = _get_geonames_region_code_prefixes(
            country_code,
            tuple(sorted(_get_region_name_aliases(candidate))),
            parent_code_prefixes,
        )
        if not prefixes:
            return frozenset()
        parent_code_prefixes = tuple(sorted(prefixes))
    return frozenset(parent_code_prefixes)


def _get_geonames_region_issues(
    location: Location,
    extent: coordinate_lint.CoordinateExtent,
    record: geonames.GeoNamesRecord,
    expected_region: Region | None,
    expected_region_codes: frozenset[tuple[str, ...]],
) -> tuple[str, ...]:
    if expected_region is None:
        # The GeoNames country code was already matched before this point.
        return ()
    if not expected_region_codes:
        country = next(
            (
                region
                for region in (location.region, *location.region.all_parents())
                if region.kind is RegionKind.country
            ),
            None,
        )
        detailed_region_and_path = (
            None
            if country is None
            else coordinate_lint._get_detailed_region_path(location.region, country)
        )
        if detailed_region_and_path is None or detailed_region_and_path[0] == country:
            return (
                f"cannot validate assigned Region {expected_region.name!r}: no "
                "matching GeoNames administrative unit or detailed region polygon",
            )
        return tuple(coordinate_lint.check_extent_in_region(extent, location.region))

    record_codes = _get_record_admin_code_prefixes(record)
    if not record_codes.isdisjoint(expected_region_codes):
        return ()
    actual_codes = ", ".join(".".join(prefix) for prefix in sorted(record_codes))
    expected_codes = ", ".join(
        ".".join(prefix) for prefix in sorted(expected_region_codes)
    )
    return (
        f"GeoNames administrative codes {actual_codes or '(none)'} do not match "
        f"assigned Region {expected_region.name!r} ({expected_codes})",
    )


def _clear_geonames_lint_caches() -> None:
    _get_geonames_country_code.cache_clear()
    _get_geonames_region_code_prefixes.cache_clear()


def _get_accepted_geonames_coordinate_candidates(
    location: Location,
) -> list[GeoNamesCoordinateCandidate]:
    return [
        candidate
        for candidate in _get_geonames_coordinate_matches(location)
        if candidate.is_accepted
    ]


def _is_geonames_point_candidate(candidate: GeoNamesCoordinateCandidate) -> bool:
    record = candidate.match.record
    return (
        record.feature_class in _GEONAMES_POINT_FEATURE_CLASSES
        or (record.feature_class, record.feature_code) in _GEONAMES_POINT_FEATURE_CODES
    )


def _get_usable_geonames_coordinate_candidates(
    location: Location,
) -> list[GeoNamesCoordinateCandidate]:
    return [
        candidate
        for candidate in _get_accepted_geonames_coordinate_candidates(location)
        if _is_geonames_point_candidate(candidate)
    ]


def _extents_form_single_coordinate_cluster(
    extents: Collection[coordinate_lint.CoordinateExtent],
) -> bool:
    extents = tuple(extents)
    return all(
        coordinate_lint.extent_distance_km(extent, other_extent)
        <= coordinate_lint.COORDINATE_TOLERANCE_KM
        for index, extent in enumerate(extents)
        for other_extent in extents[index + 1 :]
    )


def _describe_geonames_match(match: geonames.GeoNamesMatch) -> str:
    record = match.record
    matched_as = (
        ""
        if match.match_kind == "name"
        else f", matched {match.match_kind.replace('_', ' ')} {match.matched_name!r}"
    )
    return (
        f"GeoNames {record.feature_class}/{record.feature_code} "
        f"{record.name!r} (ID {record.geoname_id}{matched_as})"
    )


@LINT.add(
    "geonames_coordinates",
    requires_network=True,
    clear_caches=_clear_geonames_lint_caches,
)
def check_geonames_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.latitude is not None or location.longitude is not None:
        return
    if location.has_tag(LocationTag.PLSS):
        # PLSS describes the collecting site; wait for its polygon instead of
        # falling back to the centroid of a similarly named gazetteer feature.
        return
    if not _should_infer_coordinates(location):
        return
    if _get_linked_coordinate_candidates(location):
        return

    geonames_candidates = _get_usable_geonames_coordinate_candidates(location)
    if not geonames_candidates:
        return
    if not _extents_form_single_coordinate_cluster(
        [candidate.extent for candidate in geonames_candidates]
    ):
        matches = "".join(
            f"- {_describe_geonames_match(candidate.match)}, "
            f"{candidate.latitude}, {candidate.longitude}\n"
            for candidate in geonames_candidates
        )
        yield (
            f"GeoNames returned {len(geonames_candidates)} conflicting usable "
            f"exact matches in the assigned Region:\n{matches}"
        )
        return

    nominatim_candidates = _get_nominatim_coordinate_candidates(location)
    if nominatim_candidates:
        if not _extents_form_single_coordinate_cluster(
            [parsed[2] for _, parsed in nominatim_candidates]
        ):
            matches = "".join(
                f"- {candidate.display_name!r} "
                f"({candidate.category}/{candidate.feature_type}, "
                f"{latitude}, {longitude})\n"
                for candidate, (latitude, longitude, _) in nominatim_candidates
            )
            yield (
                f"GeoNames resolves to one coordinate cluster, but Nominatim "
                f"returned {len(nominatim_candidates)} conflicting exact matches; "
                f"coordinates cannot be inferred:\n{matches}"
            )
            return
        combined_extents = [
            *(candidate.extent for candidate in geonames_candidates),
            *(parsed[2] for _, parsed in nominatim_candidates),
        ]
        if not _extents_form_single_coordinate_cluster(combined_extents):
            geonames_match = geonames_candidates[0]
            nominatim_result, (nominatim_latitude, nominatim_longitude, _) = (
                nominatim_candidates[0]
            )
            yield (
                f"GeoNames and Nominatim resolve to incompatible coordinate "
                f"clusters: {_describe_geonames_match(geonames_match.match)} at "
                f"{geonames_match.latitude}, {geonames_match.longitude}; "
                f"Nominatim {nominatim_result.category}/"
                f"{nominatim_result.feature_type} result "
                f"{nominatim_result.display_name!r} at {nominatim_latitude}, "
                f"{nominatim_longitude}"
            )
            return
        result, (latitude, longitude, extent) = nominatim_candidates[0]
        region_issues = list(
            coordinate_lint.check_extent_in_region(extent, location.region)
        )
        if region_issues:
            yield (
                f"Nominatim result {result.display_name!r} failed the region check: "
                f"{'; '.join(region_issues)}"
            )
            return
        message = (
            f"coordinates should be {latitude}, {longitude}, inferred from "
            f"OpenStreetMap Nominatim {result.category}/{result.feature_type} "
            f"result {result.display_name!r}, confirmed by "
            f"{len(geonames_candidates)} compatible GeoNames exact "
            f"match{'es' if len(geonames_candidates) != 1 else ''}"
        )
        nominatim_tag = _nominatim_provenance_tag(result, use_bounding_box=False)
        provenance_tags = (
            [nominatim_tag]
            if nominatim_tag is not None
            else [
                LocationTag.CoordinatesFromGeoNames(
                    geonames_candidates[0].match.record.geoname_id
                )
            ]
        )
    else:
        reference = geonames_candidates[0]
        latitude = reference.latitude
        longitude = reference.longitude
        message = (
            f"coordinates should be {latitude}, {longitude}, inferred from "
            f"{_describe_geonames_match(reference.match)}; Nominatim returned "
            "no usable exact match"
        )
        provenance_tags = [
            LocationTag.CoordinatesFromGeoNames(reference.match.record.geoname_id)
        ]
    if cfg.autofix and not LINT.is_ignoring_lint(location, "geonames_coordinates"):
        print(f"{location}: {message}")
        location.latitude = latitude
        location.longitude = longitude
        _add_coordinate_provenance(location, provenance_tags)
    else:
        yield message


@LINT.add("geonames_coordinate_consistency")
def check_geonames_coordinate_consistency(
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

    candidates = _get_usable_geonames_coordinate_candidates(location)
    if not candidates:
        return
    candidates_with_distances = [
        (
            candidate,
            coordinate_lint.extent_distance_km(location_extent, candidate.extent),
        )
        for candidate in candidates
    ]
    masked_candidates = [
        candidate
        for candidate, distance in candidates_with_distances
        if location_extent.point is None and distance == 0
    ]
    if len(masked_candidates) > 1 and not _extents_form_single_coordinate_cluster(
        [candidate.extent for candidate in masked_candidates]
    ):
        matches = "".join(
            f"- {_describe_geonames_match(candidate.match)}, "
            f"{candidate.latitude}, {candidate.longitude}\n"
            for candidate in masked_candidates
        )
        yield (
            f"coordinate range {location.latitude}, {location.longitude} "
            f"encompasses {len(masked_candidates)} conflicting point-like "
            f"GeoNames exact matches in the assigned Region and may mask "
            f"conflated homonyms:\n{matches}"
        )
        return
    if any(
        distance <= coordinate_lint.COORDINATE_TOLERANCE_KM
        for _, distance in candidates_with_distances
    ):
        return

    matches = "".join(
        f"- {_describe_geonames_match(candidate.match)}, "
        f"{candidate.latitude}, {candidate.longitude}; {distance:.1f} km away\n"
        for candidate, distance in candidates_with_distances
    )
    yield (
        f"coordinates {location.latitude}, {location.longitude} are more than "
        f"{coordinate_lint.COORDINATE_TOLERANCE_KM} km from all "
        f"{len(candidates)} exact GeoNames matches in the assigned Region:\n{matches}"
    )


@LINT.add("nominatim_coordinates", requires_network=True)
def check_nominatim_coordinates(location: Location, cfg: LintConfig) -> Iterable[str]:
    if location.latitude is not None or location.longitude is not None:
        return
    if location.has_tag(LocationTag.PLSS):
        return
    if not _should_infer_coordinates(location):
        return
    if _get_linked_coordinate_candidates(location):
        return
    # GeoNames inference is the cross-source coordinator whenever GeoNames has
    # an accepted point-like candidate. Do not let lint order choose a source.
    if _get_usable_geonames_coordinate_candidates(location):
        return

    candidates = _get_nominatim_coordinate_candidates(location)
    if not candidates:
        return

    result, (latitude, longitude, extent) = candidates[0]
    has_conflict = not _extents_form_single_coordinate_cluster(
        [parsed[2] for _, parsed in candidates]
    )
    if has_conflict:
        matches = "".join(
            f"- {candidate.display_name!r} "
            f"({candidate.category}/{candidate.feature_type}, "
            f"{candidate_latitude}, {candidate_longitude})\n"
            for candidate, (candidate_latitude, candidate_longitude, _) in candidates
        )
        yield (
            f"Nominatim returned {len(candidates)} conflicting exact matches:\n"
            f"{matches}"
        )
        return

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
    provenance = _nominatim_provenance_tag(result, use_bounding_box=False)
    if provenance is None:
        yield (
            f"{message}; cannot infer automatically because the Nominatim result "
            "has no stable OpenStreetMap identifier"
        )
        return
    if cfg.autofix and not LINT.is_ignoring_lint(location, "nominatim_coordinates"):
        print(f"{location}: {message}")
        location.latitude = latitude
        location.longitude = longitude
        _add_coordinate_provenance(location, [provenance])
    else:
        yield message


@LINT.add("nominatim_general_coordinates", requires_network=True)
def check_nominatim_general_coordinates(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if (
        not location.is_general()
        or not is_recent_location(location)
        or location.name == location.region.name
    ):
        return
    has_existing_point = False
    if location.latitude is not None or location.longitude is not None:
        if location.latitude is None or location.longitude is None:
            return
        existing_extent = coordinate_lint.make_extent(
            location.latitude, location.longitude
        )
        if existing_extent is None or existing_extent.point is None:
            return
        has_existing_point = True

    all_candidates = _get_nominatim_bounding_box_candidates(location)
    possible_general_candidates = _get_possible_general_nominatim_candidates(
        location, bounding_box_candidates=all_candidates
    )
    candidates = [
        (result, (latitude, longitude, extent))
        for result, latitude, longitude, extent, _ in possible_general_candidates
    ]
    if not candidates:
        return

    result, (latitude, longitude, extent) = candidates[0]
    has_conflict = any(
        not _coordinate_extents_are_equal(extent, other_extent)
        for _, (_, _, other_extent) in all_candidates
    )
    preferred_over_boundaries = False
    preferred_over_contained_matches = False
    if has_conflict:
        encompassing = _get_encompassing_general_candidate(
            possible_general_candidates, all_candidates
        )
        if encompassing is not None:
            result, latitude, longitude, extent, _ = encompassing
            preferred_over_contained_matches = True
            preferred = None
        else:
            preferred = _get_place_candidate_among_administrative_boundaries(
                all_candidates
            )
            if preferred is not None and not any(
                preferred[0] is candidate[0]
                and _coordinate_extents_are_equal(preferred[1][2], candidate[3])
                for candidate in possible_general_candidates
            ):
                preferred = None
        if preferred is None:
            if not preferred_over_contained_matches:
                matches = "".join(
                    f"- {candidate.display_name!r} "
                    f"({candidate.osm_type} {candidate.category}/"
                    f"{candidate.feature_type}, {candidate_latitude}, "
                    f"{candidate_longitude})\n"
                    for candidate, (
                        candidate_latitude,
                        candidate_longitude,
                        _,
                    ) in all_candidates
                )
                yield (
                    f"Nominatim returned {len(all_candidates)} conflicting "
                    f"exact-match bounding boxes:\n{matches}"
                )
                return
        else:
            result, (latitude, longitude, extent) = preferred
            preferred_over_boundaries = True

    region_issues = list(
        coordinate_lint.check_extent_in_region(extent, location.region)
    )
    if region_issues:
        yield (
            f"Nominatim bounding box for {result.display_name!r} failed the "
            f"region check: {'; '.join(region_issues)}"
        )
        return

    message = (
        f"coordinate range should be {latitude}, {longitude}, inferred from "
        f"OpenStreetMap Nominatim bounding box for {result.osm_type} "
        f"{result.category}/{result.feature_type} result {result.display_name!r}"
    )
    if preferred_over_boundaries:
        message += " (preferred over boundary/administrative matches)"
    elif preferred_over_contained_matches:
        message += " (preferred because it contains all other exact-match bounds)"
    provenance = _nominatim_provenance_tag(result, use_bounding_box=True)
    if provenance is None:
        yield (
            f"{message}; cannot infer automatically because the Nominatim result "
            "has no stable OpenStreetMap identifier"
        )
        return
    if has_existing_point:
        yield (
            f"{message}; replace point coordinates {location.latitude}, "
            f"{location.longitude} manually"
        )
        return
    if cfg.autofix and not LINT.is_ignoring_lint(
        location, "nominatim_general_coordinates"
    ):
        print(f"{location}: {message}")
        location.latitude = latitude
        location.longitude = longitude
        _add_coordinate_provenance(location, [provenance])
    else:
        yield message


def _get_nominatim_bounding_box_radius_km(
    result: nominatim.SearchResult,
) -> float | None:
    if result.bounding_box is None:
        return None
    try:
        south, north, west, east = map(float, result.bounding_box)
    except ValueError:
        return None
    center = coordinates.Point((west + east) / 2, (south + north) / 2)
    return max(
        coordinate_lint.distance_km(center, coordinates.Point(longitude, latitude))
        for longitude in (west, east)
        for latitude in (south, north)
    )


def _coordinate_extent_contains(
    outer: coordinate_lint.CoordinateExtent, inner: coordinate_lint.CoordinateExtent
) -> bool:
    return (
        outer.latitude.minimum <= inner.latitude.minimum
        and outer.latitude.maximum >= inner.latitude.maximum
        and outer.longitude.minimum <= inner.longitude.minimum
        and outer.longitude.maximum >= inner.longitude.maximum
    )


def _get_encompassing_general_candidate(
    general_candidates: list[tuple[nominatim.SearchResult, str, str, Any, float]],
    all_candidates: list[tuple[nominatim.SearchResult, tuple[str, str, Any]]],
) -> tuple[nominatim.SearchResult, str, str, Any, float] | None:
    encompassing = [
        candidate
        for candidate in general_candidates
        if all(
            _coordinate_extent_contains(candidate[3], other_extent)
            for _, (_, _, other_extent) in all_candidates
        )
    ]
    if not encompassing:
        return None
    return min(encompassing, key=lambda candidate: candidate[4])


def _get_possible_general_nominatim_candidates(
    location: Location,
    *,
    bounding_box_candidates: (
        list[tuple[nominatim.SearchResult, tuple[str, str, Any]]] | None
    ) = None,
) -> list[tuple[nominatim.SearchResult, str, str, Any, float]]:
    search_plan = get_nominatim_search_plan(location)
    if (
        not search_plan.coordinates_can_be_inferred
        or search_plan.offsets
        or search_plan.modifier is not None
    ):
        return []
    name_words = set(re.findall(r"[a-z]+", search_plan.locality_name.casefold()))
    has_geographic_name = not name_words.isdisjoint(
        _POSSIBLE_GENERAL_GEOGRAPHIC_NAME_WORDS
    )
    has_administrative_name = not name_words.isdisjoint(
        _POSSIBLE_GENERAL_ADMINISTRATIVE_NAME_WORDS
    )
    location_extent = (
        coordinate_lint.make_extent(location.latitude, location.longitude)
        if location.latitude is not None and location.longitude is not None
        else None
    )
    has_settlement_match: bool | None = None
    candidates = []
    seen = set()
    if bounding_box_candidates is None:
        bounding_box_candidates = _get_nominatim_bounding_box_candidates(location)
    for result, (latitude, longitude, extent) in bounding_box_candidates:
        radius_km = _get_nominatim_bounding_box_radius_km(result)
        if radius_km is None:
            continue
        if (
            location_extent is not None
            and coordinate_lint.extent_distance_km(location_extent, extent)
            > coordinate_lint.COORDINATE_TOLERANCE_KM
        ):
            continue
        accepted_features = _POSSIBLE_GENERAL_NOMINATIM_FEATURES.get(
            result.category, frozenset()
        )
        is_broad_feature = (
            result.feature_type in accepted_features
            and radius_km > _POSSIBLE_GENERAL_MINIMUM_RADIUS_KM
        )
        is_named_administrative_feature = (
            result.category == "boundary"
            and result.feature_type in _POSSIBLE_GENERAL_ADMINISTRATIVE_FEATURES
            and radius_km > _POSSIBLE_GENERAL_MINIMUM_RADIUS_KM
            and (has_geographic_name or has_administrative_name)
        )
        if (
            is_named_administrative_feature
            and has_administrative_name
            and not has_geographic_name
        ):
            if has_settlement_match is None:
                has_settlement_match = any(
                    candidate.category == "place"
                    and candidate.feature_type in _NOMINATIM_SETTLEMENT_FEATURES
                    for candidate, _ in _get_nominatim_coordinate_candidates(location)
                )
            if has_settlement_match:
                is_named_administrative_feature = False
        if not is_broad_feature and not is_named_administrative_feature:
            continue
        key = (
            result.category,
            result.feature_type,
            result.osm_type,
            result.bounding_box,
        )
        if key in seen:
            continue
        seen.add(key)
        candidates.append((result, latitude, longitude, extent, radius_km))
    candidates.sort(key=lambda candidate: -candidate[4])
    return candidates


def _get_maximum_linked_coordinate_spread(
    location: Location,
) -> tuple[float, str, str] | None:
    linked = _get_linked_coordinate_candidates(location)
    maximum: tuple[float, str, str] | None = None
    for index, (_, _, extent, source) in enumerate(linked):
        for _, _, other_extent, other_source in linked[index + 1 :]:
            distance = coordinate_lint.extent_distance_km(extent, other_extent)
            if maximum is None or distance > maximum[0]:
                maximum = distance, source, other_source
    return maximum


@LINT.add("possible_general_location", requires_network=True)
def check_possible_general_location(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if location.is_general() or not is_recent_location(location):
        return
    candidates = _get_possible_general_nominatim_candidates(location)
    if not candidates:
        return

    matches = "".join(
        f"- {result.osm_type} {result.category}/{result.feature_type} "
        f"{result.display_name!r}: {radius_km:.1f} km bounding-box radius, "
        f"{latitude}, {longitude}\n"
        for result, latitude, longitude, _, radius_km in candidates
    )
    message = (
        "may represent a broad feature or political area and should be reviewed "
        f"for the General tag; exact Nominatim matches:\n{matches}"
    )
    spread = _get_maximum_linked_coordinate_spread(location)
    if spread is not None and spread[0] > coordinate_lint.COORDINATE_TOLERANCE_KM:
        distance, source, other_source = spread
        message += (
            f"Linked coordinate evidence also spans {distance:.1f} km between "
            f"{source} and {other_source}."
        )
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
    # Nominatim commonly returns multiple nodes or ways for different parts of
    # one river. Exclude linear waterways (and administrative boundaries) from
    # the homonym-union check while retaining them for the ordinary distance
    # consistency check below.
    masked_candidates = [
        (result, candidate)
        for result, candidate, distance in candidates_with_distances
        if location_extent.point is None
        and distance == 0
        and result.category not in {"boundary", "waterway"}
    ]
    if len(masked_candidates) > 1 and not _extents_form_single_coordinate_cluster(
        [candidate[2] for _, candidate in masked_candidates]
    ):
        matches = "".join(
            f"- {result.display_name!r} "
            f"({result.category}/{result.feature_type}, {latitude}, {longitude})\n"
            for result, (latitude, longitude, _) in masked_candidates
        )
        yield (
            f"coordinate range {location.latitude}, {location.longitude} "
            f"encompasses {len(masked_candidates)} conflicting non-linear "
            f"Nominatim exact matches and may mask conflated homonyms:\n{matches}"
        )
        return
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


def _get_reverse_region(location: Location) -> tuple[Region, int] | None:
    for region in (location.region, *location.region.all_parents()):
        if region.kind in _SUBNATIONAL_REGION_KINDS:
            # Nominatim's zoom 5 returns only the broad "state" level. This is
            # insufficient for subdivisions such as French departments and
            # Spanish provinces, while zoom 8 retains the broader ancestors in
            # the address alongside the more specific subdivision.
            return region, 8
    return None


def _get_nominatim_address_name_aliases(value: str) -> set[str]:
    # OSM commonly stores bilingual administrative names in forms such as
    # "Alacant / Alicante". It also renders equivalent designators in several
    # forms, such as "Community of Madrid", "Bolivar State", and
    # "Autonomous Region of Bougainville". Match their meaningful name parts
    # while retaining the full value.
    names = {value, *(part.strip() for part in re.split(r"\s*/\s*", value))}
    output = set(names)
    prefix_designators = {
        *_ADMINISTRATIVE_DESIGNATORS,
        "Autonomous Community",
        "Autonomous Region",
        "Community",
        "Republic",
    }
    suffix_designators = {*_ADMINISTRATIVE_DESIGNATORS, "Republic"}
    for name in names:
        for designator in prefix_designators:
            prefix = f"{designator} of "
            if name.casefold().startswith(prefix.casefold()):
                stripped = name[len(prefix) :]
                if stripped.casefold().startswith("the "):
                    stripped = stripped[4:]
                output.add(stripped)
        for designator in suffix_designators:
            suffix = f" {designator}"
            if name.casefold().endswith(suffix.casefold()):
                output.add(name[: -len(suffix)])
    return output


def _normalize_nominatim_region_name(value: str) -> str:
    # Hyphens and spaces vary freely in administrative names (for example,
    # Khyber-Pakhtunkhwa and Khyber Pakhtunkhwa).
    return helpers.simplify_string(value, clean_words=False).replace("-", "")


def _reverse_result_matches_region(
    result: nominatim.ReverseResult, region: Region
) -> bool:
    address_names = {
        _normalize_nominatim_region_name(name)
        for key in _REVERSE_ADMINISTRATIVE_ADDRESS_KEYS
        if (value := result.address.get(key)) is not None
        for name in _get_nominatim_address_name_aliases(value)
    }
    expected_names = {
        _normalize_nominatim_region_name(name)
        for name in _get_region_name_aliases(region)
    }
    return not address_names.isdisjoint(expected_names)


def _get_reverse_region_value(result: nominatim.ReverseResult) -> str | None:
    return next(
        (
            result.address[key]
            for key in _REVERSE_ADMINISTRATIVE_ADDRESS_KEYS
            if key in result.address
        ),
        None,
    )


def _reverse_result_matches_country(
    result: nominatim.ReverseResult, region: Region
) -> bool:
    country = next(
        (
            candidate
            for candidate in (region, *region.all_parents())
            if candidate.kind is RegionKind.country
        ),
        None,
    )
    osm_country = result.address.get("country")
    if country is None or osm_country is None:
        return False
    normalized_osm_country = _normalize_nominatim_region_name(osm_country)
    return normalized_osm_country in {
        _normalize_nominatim_region_name(name)
        for name in _get_region_name_aliases(country)
    }


@LINT.add("nominatim_region_consistency", requires_network=True)
def check_nominatim_region_consistency(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    if (
        location.latitude is None
        or location.longitude is None
        or location.is_general()
        or not is_recent_location(location)
    ):
        return
    extent = coordinate_lint.make_extent(location.latitude, location.longitude)
    if extent is None or extent.point is None:
        return
    expected = _get_reverse_region(location)
    if expected is None:
        return
    expected_region, zoom = expected

    center_result = nominatim.reverse(extent.point, zoom=zoom)
    if center_result is None or not _reverse_result_matches_country(
        center_result, expected_region
    ):
        return
    if _reverse_result_matches_region(center_result, expected_region):
        return
    center_value = _get_reverse_region_value(center_result)
    if center_value is None:
        return
    normalized_center_value = helpers.simplify_string(center_value, clean_words=False)

    # Nominatim returns the address of the nearest suitable mapped object rather
    # than polygon containment. Require the same mismatch in four nearby probes
    # so that coordinates near administrative boundaries do not produce a warning.
    for bearing in (0, 90, 180, 270):
        probe = coordinate_lint.move_point(
            extent.point, _REVERSE_REGION_PROBE_DISTANCE_KM, bearing
        )
        result = nominatim.reverse(probe, zoom=zoom)
        if result is None or not _reverse_result_matches_country(
            result, expected_region
        ):
            return
        if _reverse_result_matches_region(result, expected_region):
            return
        value = _get_reverse_region_value(result)
        if (
            value is None
            or helpers.simplify_string(value, clean_words=False)
            != normalized_center_value
        ):
            return

    yield (
        f"coordinates {location.latitude}, {location.longitude} consistently "
        f"reverse-geocode to {center_value!r}, not assigned Region "
        f"{expected_region.name!r} (nearest address "
        f"{center_result.display_name!r})"
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


def _get_nominatim_bounding_box_candidates(
    location: Location,
) -> list[tuple[nominatim.SearchResult, tuple[str, str, Any]]]:
    search_plan = get_nominatim_search_plan(location)
    if not search_plan.coordinates_can_be_inferred:
        return []
    results = nominatim.search(get_nominatim_query(location))
    candidates = []
    for result in results:
        if result.osm_type not in {"way", "relation"}:
            continue
        if not is_sane_nominatim_result(
            location, result, expected_name=search_plan.locality_name
        ):
            continue
        parsed = get_nominatim_result_bounding_box(result)
        if parsed is not None:
            candidates.append((result, parsed))
    candidates.sort(key=lambda candidate: _OSM_CATEGORY_PRIORITY[candidate[0].category])
    return candidates


def get_nominatim_result_bounding_box(
    result: nominatim.SearchResult,
) -> tuple[str, str, coordinate_lint.CoordinateExtent] | None:
    if result.bounding_box is None:
        return None
    try:
        south, north, west, east = map(float, result.bounding_box)
    except ValueError:
        return None
    if not (-90 <= south <= north <= 90 and -180 <= west <= east <= 180):
        return None
    if (south, west) == (north, east):
        return None
    if north - south >= 180 or east - west >= 180:
        return None
    return coordinate_lint.standardize_coordinate_pair(
        f"{result.bounding_box[0]} to {result.bounding_box[1]}",
        f"{result.bounding_box[2]} to {result.bounding_box[3]}",
    )


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
    coordinate_plan = _get_coordinate_modifier_plan(location.name)
    if coordinate_plan is not None:
        if coordinate_plan.parsed_coordinates is None:
            modifier = coordinate_plan.raw_coordinates
        else:
            latitude, longitude, _ = coordinate_plan.parsed_coordinates
            modifier = f"{latitude} {longitude}"
        return NominatimSearchPlan(
            coordinate_plan.prefix.base_name,
            disambiguator=coordinate_plan.prefix.disambiguator,
            modifier=modifier,
            coordinates_can_be_inferred=False,
        )

    parsed_name = ParsedLocationName.parse(location.name)

    if parsed_name.modifier is not None:
        offsets, remainder = _parse_locality_offsets(parsed_name.modifier)
        if offsets and not remainder:
            offsets.sort(key=_locality_offset_sort_key)
            return NominatimSearchPlan(
                parsed_name.base_name,
                tuple(offsets),
                parsed_name.disambiguator,
                _render_offset_modifier(offsets),
            )
        return NominatimSearchPlan(
            parsed_name.base_name,
            disambiguator=parsed_name.disambiguator,
            modifier=parsed_name.modifier,
            coordinates_can_be_inferred=False,
        )

    if parsed_name.disambiguator is not None:
        offsets, remainder = _parse_locality_offsets(parsed_name.disambiguator)
        if offsets and not remainder:
            offsets.sort(key=_locality_offset_sort_key)
            return NominatimSearchPlan(
                parsed_name.base_name,
                tuple(offsets),
                modifier=_render_offset_modifier(offsets),
                coordinates_can_be_inferred=False,
            )

    offsets, locality_name = _parse_locality_offsets(parsed_name.base_name)
    if offsets and locality_name:
        offsets.sort(key=_locality_offset_sort_key)
        return NominatimSearchPlan(
            locality_name,
            tuple(offsets),
            parsed_name.disambiguator,
            _render_offset_modifier(offsets),
        )
    return NominatimSearchPlan(
        parsed_name.base_name, disambiguator=parsed_name.disambiguator
    )


def _render_offset_modifier(offsets: Iterable[LocalityOffset]) -> str:
    return " ".join(offset.description for offset in offsets)


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
    return _get_coordinates_with_offsets(
        result.latitude, result.longitude, offsets=offsets
    )


def _get_coordinates_with_offsets(
    latitude: str, longitude: str, *, offsets: tuple[LocalityOffset, ...] = ()
) -> tuple[str, str, coordinate_lint.CoordinateExtent] | None:
    parsed = coordinate_lint.standardize_coordinate_pair(latitude, longitude)
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
        name = _get_nominatim_region_query_name(region)
        simplified = helpers.simplify_string(name, clean_words=False)
        if simplified not in seen:
            components.append(name)
            seen.add(simplified)
    return ", ".join(components)


@dataclass(frozen=True, slots=True)
class NominatimResultAssessment:
    issues: tuple[str, ...]
    notes: tuple[str, ...]

    @property
    def is_accepted(self) -> bool:
        return not self.issues


def _is_likely_nominatim_spelling_variant(left: str, right: str) -> bool:
    normalized_left = helpers.simplify_string(left, clean_words=False)
    normalized_right = helpers.simplify_string(right, clean_words=False)
    if (
        normalized_left == normalized_right
        or min(len(normalized_left), len(normalized_right)) < 5
        or normalized_left[0] != normalized_right[0]
        or any(character.isdigit() for character in normalized_left + normalized_right)
    ):
        return False
    return _edit_distance_at_most(normalized_left, normalized_right, 1)


def assess_nominatim_result(
    location: Location,
    result: nominatim.SearchResult,
    *,
    expected_name: str | None = None,
) -> NominatimResultAssessment:
    issues = []
    notes = []
    if result.category not in _GEOCODABLE_OSM_CATEGORIES:
        issues.append(f"unsupported OSM category {result.category!r}")
    if expected_name is None:
        expected_name = get_nominatim_search_plan(location).locality_name
    result_name = helpers.simplify_string(result.name, clean_words=False)
    normalized_expected_name = helpers.simplify_string(expected_name, clean_words=False)
    if result_name != normalized_expected_name:
        if _is_likely_nominatim_spelling_variant(result.name, expected_name):
            notes.append(
                f"locality spelling differs but is a likely variant: expected "
                f"{expected_name!r}, got {result.name!r}"
            )
        else:
            issues.append(
                f"locality name mismatch: expected {expected_name!r}, got "
                f"{result.name!r}"
            )

    address_names = {
        _normalize_nominatim_region_name(name)
        for key, value in result.address.items()
        if key not in {"country_code", "postcode"} and not key.startswith("ISO3166")
        for name in _get_nominatim_address_name_aliases(value)
    }
    checked_region = False
    for region in (location.region, *location.region.all_parents()):
        if region.kind not in _ADDRESS_REGION_KINDS:
            continue
        checked_region = True
        expected_names = {
            _normalize_nominatim_region_name(name)
            for name in _get_region_name_aliases(region)
        }
        if address_names.isdisjoint(expected_names):
            issues.append(
                f"assigned Region {region.name!r} does not match any address "
                "component"
            )
    if not checked_region:
        issues.append("no supported country or administrative Region to validate")
    return NominatimResultAssessment(tuple(issues), tuple(notes))


def is_sane_nominatim_result(
    location: Location,
    result: nominatim.SearchResult,
    *,
    expected_name: str | None = None,
) -> bool:
    return assess_nominatim_result(
        location, result, expected_name=expected_name
    ).is_accepted


def _get_unqualified_region_name(region: Region) -> str:
    if split := split_trailing_parenthetical(region.name):
        base_name, qualifier = split
        if qualifier in {parent.name for parent in region.all_parents()}:
            return base_name
    if region.parent is not None:
        parent_suffix = f", {region.parent.name}"
        if region.name.endswith(parent_suffix):
            return region.name.removesuffix(parent_suffix)
    return region.name


def _get_region_country_name(region: Region) -> str | None:
    return next(
        (
            candidate.name
            for candidate in (region, *region.all_parents())
            if candidate.kind is RegionKind.country
        ),
        None,
    )


def _get_osm_region_name_translations(region: Region) -> tuple[str, ...]:
    country_name = _get_region_country_name(region)
    if country_name is None:
        return ()
    return _OSM_REGION_NAME_TRANSLATIONS.get((region.name, country_name), ())


def _get_undesignated_region_name(region: Region, name: str) -> str | None:
    matching_designators = (
        _ADMINISTRATIVE_DESIGNATORS
        if region.kind is RegionKind.subnational
        else frozenset(
            designator
            for kind, designator in _REGION_KIND_DESIGNATORS.items()
            if region.kind is kind
        )
    )
    found_designator = next(
        (
            designator
            for designator in _ADMINISTRATIVE_DESIGNATORS
            if name.casefold().endswith(f" {designator.casefold()}")
        ),
        None,
    )
    if found_designator not in matching_designators:
        return None
    return name[: -len(found_designator) - 1]


def _get_nominatim_region_query_name(region: Region) -> str:
    translations = _get_osm_region_name_translations(region)
    if translations:
        return translations[0]
    name = _get_unqualified_region_name(region)
    undesignated_name = _get_undesignated_region_name(region, name)
    if undesignated_name is not None and any(
        name.casefold().endswith(f" {designator.casefold()}")
        for designator in _OSM_QUERY_OMITTED_DESIGNATORS
    ):
        return undesignated_name
    return name


def _get_region_name_aliases(region: Region) -> set[str]:
    names = _get_qualified_name_variants(region.name)
    names.add(_get_unqualified_region_name(region))
    names.update(_get_osm_region_name_translations(region))
    shortest_name = min(names, key=lambda name: (len(name), name))
    undesignated_name = _get_undesignated_region_name(region, shortest_name)
    if undesignated_name is not None:
        found_designator = shortest_name[len(undesignated_name) + 1 :]
        names.add(undesignated_name)
        names.add(f"{found_designator} of {undesignated_name}")
    else:
        matching_designators = (
            _ADMINISTRATIVE_DESIGNATORS
            if region.kind is RegionKind.subnational
            else frozenset(
                designator
                for kind, designator in _REGION_KIND_DESIGNATORS.items()
                if region.kind is kind
            )
        )
    if undesignated_name is None and len(matching_designators) == 1:
        designator = next(iter(matching_designators))
        names.add(f"{shortest_name} {designator}")
    if region.kind is RegionKind.country:
        names.add(nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name))
    return names


def _get_plss_provenance_extent(
    location: Location, provenance: Any
) -> tuple[coordinate_lint.CoordinateExtent | None, str | None]:
    tags = [
        tag
        for tag in location.get_tags(location.tags, LocationTag.PLSS)
        if tag.plss_id == provenance.plss_id
    ]
    if not tags:
        return None, f"no PLSS tag has identifier {provenance.plss_id!r}"
    if len(tags) > 1:
        return None, f"multiple PLSS tags have identifier {provenance.plss_id!r}"
    description = plss.parse_canonical(tags[0].text)
    context = _get_us_plss_context(location)
    if description is None or context is None:
        return None, f"PLSS tag {tags[0]!r} cannot be resolved"
    state_code, state_fips, county_name = context
    try:
        resolution = _resolve_plss_evidence(
            description,
            state_code=state_code,
            state_fips=state_fips,
            county_name=county_name,
            extent=None,
        )
        if not resolution.is_resolved:
            return (
                None,
                f"PLSS tag {tags[0]!r} cannot be resolved: {resolution.problem}",
            )
        assert resolution.township is not None
        geometry = plss.get_description_geometry(resolution.township, description)
    except (httpx.HTTPError, plss.PLSSUnavailableError) as exc:
        return None, f"could not retrieve PLSS provenance: {exc}"
    bounds = _format_plss_bounds(geometry.geometries)
    if bounds is None:
        return None, f"BLM has no {geometry.level} polygon for {tags[0].text!r}"
    latitude, longitude = bounds.split(", ", maxsplit=1)
    return coordinate_lint.make_extent(latitude, longitude), None


def _get_coordinate_provenance_extents(
    location: Location, provenance: Any
) -> tuple[list[coordinate_lint.CoordinateExtent], str | None]:
    if isinstance(provenance, LocationTag.CoordinatesManual):
        return [], None
    if provenance is LocationTag.CoordinatesFromLocationName:
        plan = _get_coordinate_modifier_plan(location.name)
        if plan is None or plan.parsed_coordinates is None:
            return [], "Location name no longer contains valid coordinates"
        return [plan.parsed_coordinates[2]], None
    if isinstance(provenance, LocationTag.CoordinatesFromName):
        name = next(
            (
                name
                for name in location.type_localities
                if name.id == provenance.name.id
            ),
            None,
        )
        if name is None:
            return (
                [],
                f"Name {provenance.name.id} is no longer linked to the Location",
            )
        if provenance.text is not None:
            if not provenance.text.strip():
                return [], "quoted coordinate evidence is empty"
            matching_details = [
                tag
                for tag in _get_applicable_location_detail_tags(name)
                if provenance.text in tag.text
            ]
            if not matching_details:
                return (
                    [],
                    f"quoted coordinate evidence {provenance.text!r} is not present "
                    "in an applicable LocationDetail tag",
                )
            # The quote was selected explicitly, so it is safe to accept a small
            # normalization that would be too permissive in arbitrary prose. In
            # particular, older sources often put a dash directly between the
            # latitude direction and the longitude. If the ordinary parser can
            # then read the quote, retain its numeric extent so a later Location
            # coordinate edit is still cross-checked. Otherwise the exact-quote
            # assertion itself is the reviewed evidence.
            parseable_text = re.sub(
                r"(?<=[NS])\s*[-\N{EN DASH}\N{EM DASH}]\s*(?=\d)",
                ", ",
                provenance.text,
                flags=re.IGNORECASE,
            )
            parseable_text = re.sub(
                r"(?<![\d°])(?P<degrees>\d{1,3})\.(?P<minutes>\d{2})"
                r"(?=\s*[NSEW]\b)",
                r"\g<degrees>°\g<minutes>'",
                parseable_text,
                flags=re.IGNORECASE,
            )
            parseable_text = re.sub(
                r"\b(?P<direction>north|south|east|west)\b",
                lambda match: match.group("direction")[0].upper(),
                parseable_text,
                flags=re.IGNORECASE,
            )
            parseable_text = re.sub(
                r"südl\.?\s+Breite", "S", parseable_text, flags=re.IGNORECASE
            )
            parseable_text = re.sub(r"\bI(?=\d+°)", "1", parseable_text)
            extents = []
            for latitude, longitude in helpers.extract_coordinate_pairs(parseable_text):
                parsed = coordinate_lint.standardize_coordinate_pair(
                    latitude, longitude
                )
                if parsed is not None:
                    extents.append(parsed[2])
            if extents:
                return extents, None
            location_extent = coordinate_lint.make_extent(
                location.latitude, location.longitude
            )
            if location_extent is None:
                return [], "Location no longer has valid coordinates"
            return [location_extent], None
        extents = []
        for tag in name.get_tags(name.type_tags, models.name.TypeTag.Coordinates):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                extents.append(parsed[2])
        if extents:
            return extents, None
        for tag in _get_applicable_coordinate_text_tags(name):
            for extracted in _extract_name_text_tag_coordinate_pairs(name, tag):
                parsed = coordinate_lint.standardize_coordinate_pair(*extracted)
                if parsed is not None:
                    extents.append(parsed[2])
        if not extents:
            return ([], f"Name {provenance.name.id} has no valid coordinate evidence")
        return extents, None
    if isinstance(provenance, LocationTag.CoordinatesFromOccurrenceRecord):
        record = next(
            (
                record
                for record in location.occurrence_records
                if record.id == provenance.occurrence_record_id
            ),
            None,
        )
        if record is None:
            return (
                [],
                f"OccurrenceRecord {provenance.occurrence_record_id} is no longer "
                "linked to the Location",
            )
        extents = []
        for tag in record.get_tags(
            record.tags, models.occurrence_record.OccurrenceRecordTag.Coordinates
        ):
            parsed = coordinate_lint.standardize_coordinate_pair(
                tag.latitude, tag.longitude
            )
            if parsed is not None:
                extents.append(parsed[2])
        if extents:
            return extents, None
        for tag in record.get_tags(
            record.tags,
            models.occurrence_record.OccurrenceRecordTag.VerbatimCoordinates,
        ):
            verbatim = models.occurrence_record.lint.parse_verbatim_coordinates(
                tag.text
            )
            if verbatim is not None:
                standardized = coordinate_lint.standardize_coordinate_pair(*verbatim)
                if standardized is not None:
                    extents.append(standardized[2])
        if not extents:
            return (
                [],
                f"OccurrenceRecord {provenance.occurrence_record_id} has no valid "
                "coordinate evidence",
            )
        return extents, None
    if isinstance(provenance, LocationTag.CoordinatesFromGeoNames):
        try:
            record = geonames.get_by_id(provenance.geoname_id)
        except RuntimeError as exc:
            return [], f"could not read GeoNames provenance: {exc}"
        if record is None:
            return [], f"GeoNames has no record {provenance.geoname_id}"
        search_plan = get_nominatim_search_plan(location)
        parsed = _get_coordinates_with_offsets(
            str(record.latitude), str(record.longitude), offsets=search_plan.offsets
        )
        if parsed is None:
            return (
                [],
                f"GeoNames record {provenance.geoname_id} has invalid coordinates",
            )
        return [parsed[2]], None
    if isinstance(provenance, LocationTag.CoordinatesFromNominatim):
        _, extents, issue = _get_nominatim_provenance_extents(location, provenance)
        return extents, issue
    if isinstance(provenance, LocationTag.CoordinatesFromPLSS):
        extent, issue = _get_plss_provenance_extent(location, provenance)
        return ([] if extent is None else [extent]), issue
    return [], f"unrecognized coordinate provenance tag {provenance!r}"


def _get_nominatim_provenance_extents(
    location: Location, provenance: Any
) -> tuple[
    nominatim.SearchResult | None, list[coordinate_lint.CoordinateExtent], str | None
]:
    try:
        result = nominatim.lookup(provenance.osm_type, provenance.osm_id)
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        return None, [], f"could not retrieve Nominatim provenance: {exc}"
    if result is None:
        return (
            None,
            [],
            f"Nominatim cannot find {provenance.osm_type} {provenance.osm_id}",
        )
    if result.category != provenance.category:
        return (
            result,
            [],
            f"Nominatim object category changed from {provenance.category!r} "
            f"to {result.category!r}",
        )
    if provenance.use_bounding_box:
        parsed = get_nominatim_result_bounding_box(result)
    else:
        parsed = get_nominatim_result_coordinates(
            result, offsets=get_nominatim_search_plan(location).offsets
        )
    if parsed is None:
        kind = "bounding box" if provenance.use_bounding_box else "coordinates"
        return result, [], f"Nominatim object has no usable {kind}"
    return result, [parsed[2]], None


def _extent_exactly_matches_location(
    location_extent: coordinate_lint.CoordinateExtent,
    evidence_extent: coordinate_lint.CoordinateExtent,
) -> bool:
    return _coordinate_extents_are_equal(location_extent, evidence_extent)


def _union_coordinate_extents(
    extents: Iterable[coordinate_lint.CoordinateExtent],
) -> coordinate_lint.CoordinateExtent | None:
    iterator = iter(extents)
    try:
        combined = next(iterator)
    except StopIteration:
        return None
    for extent in iterator:
        combined = combined.union(extent)
    return combined


def _provenance_extents_match(
    location_extent: coordinate_lint.CoordinateExtent,
    evidence_extent_groups: Iterable[list[coordinate_lint.CoordinateExtent]],
) -> bool:
    """Whether provenance supports the stored extent exactly.

    Multiple extents exposed by one provenance tag may be alternative statements in
    the same Name or occurrence record. An exact match to any one alternative from
    every cited provenance object is sufficient. The union remains accepted because
    several cited provenance tags may jointly define a stored range.
    """
    groups = [group for group in evidence_extent_groups if group]
    if not groups:
        return True
    if all(
        any(
            _extent_exactly_matches_location(location_extent, extent)
            for extent in group
        )
        for group in groups
    ):
        return True
    combined_extent = _union_coordinate_extents(
        extent for group in groups for extent in group
    )
    return combined_extent is not None and _extent_exactly_matches_location(
        location_extent, combined_extent
    )


def _get_backfill_coordinate_provenance(
    location: Location, location_extent: coordinate_lint.CoordinateExtent
) -> list[Any]:
    coordinate_plan = _get_coordinate_modifier_plan(location.name)
    if (
        coordinate_plan is not None
        and coordinate_plan.parsed_coordinates is not None
        and _coordinate_extents_are_equal(
            location_extent, coordinate_plan.parsed_coordinates[2]
        )
    ):
        return [LocationTag.CoordinatesFromLocationName]

    linked_evidence = _get_linked_coordinate_evidence(location)
    matching_linked_provenance = list(
        dict.fromkeys(
            evidence.provenance
            for evidence in linked_evidence
            if _extent_exactly_matches_location(location_extent, evidence.extent)
        )
    )
    if matching_linked_provenance:
        return matching_linked_provenance
    linked_extent = _union_coordinate_extents(
        evidence.extent for evidence in linked_evidence
    )
    linked_is_compatible = not linked_evidence or all(
        coordinate_lint.extent_distance_km(linked_evidence[0].extent, evidence.extent)
        <= coordinate_lint.COORDINATE_TOLERANCE_KM
        for evidence in linked_evidence[1:]
    )
    if (
        linked_is_compatible
        and linked_extent is not None
        and _extent_exactly_matches_location(location_extent, linked_extent)
    ):
        return list(dict.fromkeys(evidence.provenance for evidence in linked_evidence))

    for tag in location.get_tags(location.tags, LocationTag.PLSS):
        provenance = LocationTag.CoordinatesFromPLSS(tag.plss_id)
        plss_extent, _ = _get_plss_provenance_extent(location, provenance)
        if plss_extent is not None and _coordinate_extents_are_equal(
            location_extent, plss_extent
        ):
            return [provenance]

    if location.is_general():
        matching_bounds = [
            result
            for result, (_, _, extent) in _get_nominatim_bounding_box_candidates(
                location
            )
            if _coordinate_extents_are_equal(location_extent, extent)
            and _nominatim_provenance_tag(result, use_bounding_box=True) is not None
        ]
        if matching_bounds:
            provenance = _nominatim_provenance_tag(
                matching_bounds[0], use_bounding_box=True
            )
            assert provenance is not None
            return [provenance]
        return []

    if not _should_infer_coordinates(location):
        return []
    geonames_candidates = _get_usable_geonames_coordinate_candidates(location)
    nominatim_candidates = _get_nominatim_coordinate_candidates(location)
    matching_nominatim = [
        result
        for result, (_, _, extent) in nominatim_candidates
        if _extent_exactly_matches_location(location_extent, extent)
        and _nominatim_provenance_tag(result, use_bounding_box=False) is not None
    ]
    if geonames_candidates and matching_nominatim:
        provenance = _nominatim_provenance_tag(
            matching_nominatim[0], use_bounding_box=False
        )
        assert provenance is not None
        return [provenance]
    matching_geonames = [
        candidate
        for candidate in geonames_candidates
        if _extent_exactly_matches_location(location_extent, candidate.extent)
    ]
    if matching_geonames:
        return [
            LocationTag.CoordinatesFromGeoNames(
                matching_geonames[0].match.record.geoname_id
            )
        ]
    if matching_nominatim:
        provenance = _nominatim_provenance_tag(
            matching_nominatim[0], use_bounding_box=False
        )
        assert provenance is not None
        return [provenance]

    return []


@LINT.add("coordinate_provenance", requires_network=True)
def check_coordinate_provenance(location: Location, cfg: LintConfig) -> Iterable[str]:
    provenance_tags = [
        tag for tag in location.tags or () if is_coordinate_provenance_tag(tag)
    ]
    if location.latitude is None or location.longitude is None:
        if provenance_tags:
            yield f"has coordinate provenance tags but incomplete coordinates: {provenance_tags}"
        return
    location_extent = coordinate_lint.make_extent(location.latitude, location.longitude)
    if location_extent is None:
        return

    if not provenance_tags:
        inferred = _get_backfill_coordinate_provenance(location, location_extent)
        if inferred:
            message = f"add coordinate provenance tags {inferred!r}"
            if cfg.autofix and not LINT.is_ignoring_lint(
                location, "coordinate_provenance"
            ):
                print(f"{location}: {message}")
                _add_coordinate_provenance(location, inferred)
            else:
                yield message
        else:
            yield (
                "coordinates are not supported by a coordinate provenance tag; "
                "add reviewed CoordinatesManual provenance or correct the coordinates"
            )
        return

    evidence_extent_groups: list[list[coordinate_lint.CoordinateExtent]] = []
    for index, provenance in enumerate(provenance_tags):
        if isinstance(provenance, LocationTag.CoordinatesFromNominatim):
            current_result, extents, issue = _get_nominatim_provenance_extents(
                location, provenance
            )
            if (
                current_result is not None
                and current_result.category != provenance.category
            ):
                replacement = LocationTag.CoordinatesFromNominatim(
                    provenance.osm_type,
                    provenance.osm_id,
                    current_result.category,
                    provenance.use_bounding_box,
                )
                if provenance.use_bounding_box:
                    parsed = get_nominatim_result_bounding_box(current_result)
                else:
                    parsed = get_nominatim_result_coordinates(
                        current_result,
                        offsets=get_nominatim_search_plan(location).offsets,
                    )
                message = (
                    f"replace stale Nominatim provenance {provenance!r} with "
                    f"{replacement!r}; stable OSM object "
                    f"{provenance.osm_type} {provenance.osm_id} now has category "
                    f"{current_result.category!r}"
                )
                if cfg.autofix and not LINT.is_ignoring_lint(
                    location, "coordinate_provenance"
                ):
                    print(f"{location}: {message}")
                    _replace_location_tag(location, provenance, replacement)
                else:
                    yield message
                provenance = replacement
                provenance_tags[index] = replacement
                if parsed is None:
                    kind = (
                        "bounding box" if provenance.use_bounding_box else "coordinates"
                    )
                    extents = []
                    issue = f"Nominatim object has no usable {kind}"
                else:
                    extents = [parsed[2]]
                    issue = None
        else:
            extents, issue = _get_coordinate_provenance_extents(location, provenance)
        if issue is not None:
            yield f"{provenance!r}: {issue}"
            continue
        if isinstance(provenance, LocationTag.CoordinatesManual):
            continue
        if extents:
            evidence_extent_groups.append(extents)

    if not _provenance_extents_match(location_extent, evidence_extent_groups):
        combined_extent = _union_coordinate_extents(
            extent for group in evidence_extent_groups for extent in group
        )
        assert combined_extent is not None
        yield (
            f"coordinates {location.latitude}, {location.longitude} do not exactly "
            "match the numeric extent derived from coordinate provenance tags "
            f"(expected {combined_extent.latitude.standardized_text}, "
            f"{combined_extent.longitude.standardized_text})"
        )


@cache
def _get_base_name_to_locations() -> dict[str, tuple[Location, ...]]:
    locations_by_base_name: defaultdict[str, list[Location]] = defaultdict(list)
    for location in Location.select_valid():
        parsed_name = ParsedLocationName.parse(location.name)
        locations_by_base_name[parsed_name.base_name].append(location)
    return {
        base_name: tuple(locations)
        for base_name, locations in locations_by_base_name.items()
    }


def _regions_are_related(first: Region, second: Region) -> bool:
    return first in (second, *second.all_parents()) or second in (
        first,
        *first.all_parents(),
    )


def _locations_share_base_region(first: Location, second: Location) -> bool:
    if first.region == second.region:
        return True
    return any(
        _regions_are_related(nearby_region, second.region)
        for nearby_region in _get_nearby_regions(first)
    ) or any(
        _regions_are_related(nearby_region, first.region)
        for nearby_region in _get_nearby_regions(second)
    )


@LINT.add(
    "should_have_disambiguator", clear_caches=_get_base_name_to_locations.cache_clear
)
def check_should_have_disambiguator(
    location: Location, cfg: LintConfig
) -> Iterable[str]:
    parsed_name = ParsedLocationName.parse(location.name)
    if parsed_name.disambiguator is not None or _has_primary_region_base_name(
        location, parsed_name
    ):
        return

    similar = [
        other
        for other in _get_base_name_to_locations().get(parsed_name.base_name, ())
        if other.id != location.id
        and (
            ParsedLocationName.parse(other.name).disambiguator is not None
            or not _locations_share_base_region(location, other)
        )
    ]
    if not similar:
        return
    proposed_name = ParsedLocationName(
        parsed_name.base_name, location.region.name, parsed_name.modifier
    ).render()
    message = (
        f"location {location.name!r} should have a disambiguator "
        f"and be named {proposed_name!r} because of collision with "
        f"{len(similar)} other location(s): "
        f"{', '.join(repr(loc.name) for loc in similar)}"
    )
    yield message
