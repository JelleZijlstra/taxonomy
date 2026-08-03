"""Parse and resolve U.S. Public Land Survey System descriptions.

Parsing is local and deliberately accepts common source variants. Resolution uses the
BLM national CadNSDI service. County geometry used for disambiguation comes from the
2020 Census TIGERweb service. Raw successful responses are persisted in ``url_cache``.
"""

import itertools
import json
import math
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from typing import Any, Self

import httpx

from taxonomy import coordinates
from taxonomy.db import coordinate_lint
from taxonomy.db.url_cache import CacheDomain, cached

from .util import RateLimiter

BLM_BASE_URL = (
    "https://gis.blm.gov/arcgis/rest/services/Cadastral/"
    "BLM_Natl_PLSS_CadNSDI/MapServer"
)
CENSUS_COUNTY_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/State_County/MapServer/55"
)
USER_AGENT = "taxonomy (https://github.com/JelleZijlstra/taxonomy)"
REQUEST_TIMEOUT = 30
_RATE_LIMITER = RateLimiter(min_interval=0.1)

_DIRECTION_PATTERN = r"(?:North|South|East|West|N|S|E|W)"
_FRACTION_PATTERN = r"(?:[¼½¾]|\.(?:25|5|50|75)|[13]\s*/\s*4|1\s*/\s*2)?"
_TOWNSHIP_RANGE_RE = re.compile(
    rf"""
    (?<![A-Za-z0-9])
    (?:Township|T)\.?\s*
    (?P<township>\d{{1,3}})\s*(?P<township_fraction>{_FRACTION_PATTERN})\s*
    (?P<township_direction>{_DIRECTION_PATTERN})\.?
    \s*[,;:]?\s*
    (?:Range|R)\.?\s*
    (?P<range>\d{{1,3}})\s*(?P<range_fraction>{_FRACTION_PATTERN})\s*
    (?P<range_direction>{_DIRECTION_PATTERN})\.?
    """,
    re.IGNORECASE | re.VERBOSE,
)
_SECTION_RE = re.compile(
    r"\b(?P<section_label>Sections?|Sects?|Secs?)\.*\s*"
    r"(?P<section>3[0-6]|[12]\d|[1-9])\b",
    re.IGNORECASE,
)
_SECTION_ALTERNATIVE_RE = re.compile(
    r"\s*(?:(?:or|and|to)\s+|[-–—]\s*)\d{1,3}\b", re.IGNORECASE
)
_PLURAL_SECTION_ALTERNATIVE_RE = re.compile(r"\s*,\s*\d{1,3}\b")
_SINGULAR_SECTION_LIST_RE = re.compile(
    r"\s*,\s*\d{1,2}(?:\s*,\s*\d{1,2})*\s*,?\s*(?:and|or)\s+\d{1,2}\b", re.IGNORECASE
)
_ALIQUOT_PART_RE = re.compile(
    r"(?:(?P<quarter>[NS]\.?\s*[EW]\.?)\s*(?:¼|1\s*/\s*4)"
    r"|(?P<half>[NSEW])\.?\s*(?:½|1\s*/\s*2))",
    re.IGNORECASE,
)
_LOT_RE = re.compile(
    r"\b(?:Government\s+|Gov\.?\s+)?Lot\s+(?P<lot>[A-Za-z0-9-]+)\b", re.IGNORECASE
)
_MERIDIAN_RE = re.compile(r",\s*(?P<meridian>[^,;]{1,60}?\bMeridian)\b", re.IGNORECASE)
# CadNSDI township identifiers concatenate the two-letter state abbreviation,
# two-character principal-meridian code, three-digit township number, township
# fraction and direction, three-digit range number, range fraction and direction,
# and the township duplicate code. The BLM field permits 16 characters, but its
# published form and returned identifiers are 15 characters (for example,
# ``OR330250S0140W0``).
_PLSSID_RE = re.compile(
    r"^[A-Z]{2}[A-Z0-9]{2}\d{3}[A-Z0-9][NSEW]\d{3}[A-Z0-9][NSEW][A-Z0-9]$"
)

_FRACTION_TO_BLM: dict[str, frozenset[str | None]] = {
    "": frozenset({None, "", "0"}),
    "¼": frozenset({"1"}),
    "½": frozenset({"2"}),
    "¾": frozenset({"3"}),
}
_FRACTION_FROM_DECIMAL = {
    "": "",
    ".25": "¼",
    "1/4": "¼",
    ".5": "½",
    ".50": "½",
    "1/2": "½",
    ".75": "¾",
    "3/4": "¾",
    "¼": "¼",
    "½": "½",
    "¾": "¾",
}

# The national service occasionally returns the principal-meridian code in its
# nominal PRINMER text field. These display names follow the BLM LR2000 Meridian
# Codes table (https://reports.blm.gov/docs/lr2000/CodeMeridian.pdf), adjusted to
# the names used elsewhere by the national CadNSDI service.
_MERIDIAN_NAME_BY_CODE = {
    "01": "1st Principal Meridian",
    "02": "2nd Principal Meridian",
    "03": "3rd Principal Meridian",
    "04": "4th Principal Meridian",
    "05": "5th Meridian",
    "06": "6th Meridian",
    "07": "Black Hills Meridian",
    "08": "Boise Meridian",
    "09": "Chickasaw Meridian",
    "10": "Choctaw Meridian",
    "11": "Cimarron Meridian",
    "14": "Gila-Salt River Meridian",
    "15": "Humboldt Meridian",
    "16": "Huntsville Meridian",
    "17": "Indian Meridian",
    "18": "Louisiana Meridian",
    "19": "Michigan Meridian",
    "20": "Montana Meridian",
    "21": "Mount Diablo Meridian",
    "23": "New Mexico Meridian",
    "24": "St. Helena Meridian",
    "25": "St. Stephens Meridian",
    "26": "Salt Lake Meridian",
    "27": "San Bernardino Meridian",
    "29": "Tallahassee Meridian",
    "30": "Uintah Special Meridian",
    "31": "Ute Meridian",
    "32": "Washington Meridian",
    "33": "Willamette Meridian",
    "34": "Wind River Meridian",
}


class PLSSUnavailableError(RuntimeError):
    """Raised when an external PLSS dependency returns unusable data."""


@dataclass(frozen=True, slots=True)
class PLSSDescription:
    township: int
    township_fraction: str
    township_direction: str
    range: int
    range_fraction: str
    range_direction: str
    section: int | None = None
    aliquot: tuple[str, ...] = ()
    lot: str | None = None
    meridian: str | None = None

    def __post_init__(self) -> None:
        if self.township <= 0 or self.range <= 0:
            raise ValueError("township and range numbers must be positive")
        if self.township_fraction not in _FRACTION_TO_BLM:
            raise ValueError(
                f"unsupported township fraction {self.township_fraction!r}"
            )
        if self.range_fraction not in _FRACTION_TO_BLM:
            raise ValueError(f"unsupported range fraction {self.range_fraction!r}")
        if self.township_direction not in "NSEW":
            raise ValueError(f"invalid township direction {self.township_direction!r}")
        if self.range_direction not in "NSEW":
            raise ValueError(f"invalid range direction {self.range_direction!r}")
        if self.section is not None and not 1 <= self.section <= 36:
            raise ValueError("section number must be between 1 and 36")
        if any(
            part not in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}
            for part in self.aliquot
        ):
            raise ValueError(f"invalid aliquot description {self.aliquot!r}")
        if self.aliquot and self.lot is not None:
            raise ValueError("a description cannot contain both an aliquot and a lot")
        if (self.aliquot or self.lot is not None) and self.section is None:
            raise ValueError("section is required for an aliquot or lot")

    @property
    def township_text(self) -> str:
        return f"T{self.township}{self.township_fraction}{self.township_direction}"

    @property
    def range_text(self) -> str:
        return f"R{self.range}{self.range_fraction}{self.range_direction}"

    @property
    def canonical_text(self) -> str:
        text = f"{self.township_text} {self.range_text}"
        if self.section is not None:
            text += f" Sec. {self.section}"
        if self.aliquot:
            text += " " + "".join(
                f"{part}{'¼' if len(part) == 2 else '½'}" for part in self.aliquot
            )
        elif self.lot is not None:
            text += f" Lot {self.lot}"
        if self.meridian is not None:
            text += f", {self.meridian}"
        return text

    @property
    def township_key(self) -> tuple[int, str, str, int, str, str]:
        return (
            self.township,
            self.township_fraction,
            self.township_direction,
            self.range,
            self.range_fraction,
            self.range_direction,
        )

    @property
    def specificity(self) -> int:
        if self.lot is not None:
            return 3
        if self.aliquot:
            return 1 + len(self.aliquot)
        if self.section is not None:
            return 1
        return 0

    def with_meridian(self, meridian: str) -> Self:
        return replace(self, meridian=meridian)

    def is_compatible_with(self, other: Self) -> bool:
        if self.township_key != other.township_key:
            return False
        if (
            self.meridian is not None
            and other.meridian is not None
            and _normalize_meridian(self.meridian)
            != _normalize_meridian(other.meridian)
        ):
            return False
        if (
            self.section is not None
            and other.section is not None
            and self.section != other.section
        ):
            return False
        if self.lot is not None and other.lot is not None and self.lot != other.lot:
            return False
        if self.aliquot and other.aliquot:
            if not _aliquot_bounds_overlap(self.aliquot, other.aliquot):
                return False
        if (self.lot is not None and other.aliquot) or (
            other.lot is not None and self.aliquot
        ):
            return False
        return True


def _aliquot_bounds(aliquot: tuple[str, ...]) -> tuple[float, float, float, float]:
    """Return unit-square bounds for an aliquot description."""
    west, east, south, north = 0.0, 1.0, 0.0, 1.0
    # Legal descriptions are written innermost first: NW¼NE¼ means the
    # northwest quarter of the northeast quarter, so apply the outermost part first.
    for part in reversed(aliquot):
        horizontal_midpoint = (west + east) / 2
        vertical_midpoint = (south + north) / 2
        if "W" in part:
            east = horizontal_midpoint
        elif "E" in part:
            west = horizontal_midpoint
        if "S" in part:
            north = vertical_midpoint
        elif "N" in part:
            south = vertical_midpoint
    return west, east, south, north


def _aliquot_bounds_overlap(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    first_west, first_east, first_south, first_north = _aliquot_bounds(first)
    second_west, second_east, second_south, second_north = _aliquot_bounds(second)
    return max(first_west, second_west) < min(first_east, second_east) and max(
        first_south, second_south
    ) < min(first_north, second_north)


@dataclass(frozen=True, slots=True)
class ExtractedPLSS:
    description: PLSSDescription
    start: int
    end: int
    has_alternative_section: bool = False


@dataclass(frozen=True, slots=True)
class Township:
    state: str
    meridian: str
    meridian_code: str
    plss_id: str
    description: PLSSDescription
    geometries: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class TownshipResolution:
    township: Township | None
    candidates: tuple[Township, ...]
    problem: str | None = None

    @property
    def is_resolved(self) -> bool:
        return self.township is not None


@dataclass(frozen=True, slots=True)
class ResolvedGeometry:
    level: str
    geometries: tuple[dict[str, Any], ...]


def _normalize_fraction(text: str) -> str:
    key = re.sub(r"\s+", "", text)
    try:
        return _FRACTION_FROM_DECIMAL[key]
    except KeyError:
        raise ValueError(f"unsupported PLSS fraction {text!r}") from None


def _normalize_direction(text: str) -> str:
    return text.strip()[0].upper()


def _extract_aliquot(text: str) -> tuple[tuple[str, ...], int]:
    matches = list(_ALIQUOT_PART_RE.finditer(text))
    return (
        tuple(
            (
                re.sub(r"[^NSEW]", "", match.group("quarter").upper())
                if match.group("quarter") is not None
                else match.group("half").upper()
            )
            for match in matches
        ),
        matches[-1].end() if matches else 0,
    )


def _has_section_alternative(match: re.Match[str], following_text: str) -> bool:
    if _SECTION_ALTERNATIVE_RE.match(following_text):
        return True
    if _SINGULAR_SECTION_LIST_RE.match(following_text):
        return True
    label = match.group("section_label").rstrip(".").casefold()
    return label.endswith("s") and bool(
        _PLURAL_SECTION_ALTERNATIVE_RE.match(following_text)
    )


def _parse_optional_components(
    text: str, match: re.Match[str]
) -> tuple[int | None, tuple[str, ...], str | None, str | None, int, bool]:
    # Section normally follows township/range. Accept it immediately before the
    # township as well, but do not cross a semicolon or sentence boundary.
    suffix = text[match.end() : match.end() + 140]
    section_match: re.Match[str] | None = None
    section_is_prefix = False
    prefix_start = max(0, match.start() - 60)
    prefix = text[prefix_start : match.start()]
    prefix_matches = list(_SECTION_RE.finditer(prefix))
    # Prefer a safe prefix over a later section.  In lists such as
    # ``Sec. 36, T33N R56W; Sec. 2, T32N R56W``, the following section belongs to
    # the next township rather than the current one.
    if prefix_matches:
        candidate = prefix_matches[-1]
        intervening = prefix[candidate.end() :]
        if re.fullmatch(r"[\s,.:()\[\]–—-]*", intervening) or _has_section_alternative(
            candidate, intervening
        ):
            section_match = candidate
            section_is_prefix = True
    if section_match is None:
        suffix_candidate = _SECTION_RE.search(suffix)
        if (
            suffix_candidate is not None
            and ";" not in suffix[: suffix_candidate.start()]
        ):
            section_match = suffix_candidate

    section: int | None = None
    aliquot: tuple[str, ...] = ()
    lot: str | None = None
    has_alternative_section = False
    end = match.end()
    if section_match is not None:
        section = int(section_match.group("section"))
        if not section_is_prefix:
            has_alternative_section = _has_section_alternative(
                section_match, suffix[section_match.end() :]
            )
            end = match.end() + section_match.end()
            before_section = suffix[
                max(0, section_match.start() - 70) : section_match.start()
            ]
            component_text = suffix[section_match.end() : section_match.end() + 80]
        else:
            has_alternative_section = _has_section_alternative(
                section_match, prefix[section_match.end() :]
            )
            before_start = max(0, section_match.start() - 70)
            previous_sections = list(
                _SECTION_RE.finditer(prefix[before_start : section_match.start()])
            )
            if previous_sections:
                before_start += previous_sections[-1].end()
            before_section = prefix[before_start : section_match.start()]
            component_text = text[match.end() : match.end() + 80]

        aliquot, aliquot_end = _extract_aliquot(before_section)
        aliquot_is_before = bool(aliquot)
        if not aliquot:
            aliquot, aliquot_end = _extract_aliquot(component_text)
        lot_match = _LOT_RE.search(before_section) or _LOT_RE.search(component_text)
        if aliquot and not aliquot_is_before and not section_is_prefix:
            end += aliquot_end
        elif lot_match is not None and not aliquot:
            lot = lot_match.group("lot")
            if not section_is_prefix and lot_match.string is component_text:
                end += lot_match.end()

    meridian_match = _MERIDIAN_RE.search(text[match.end() : match.end() + 180])
    meridian = None
    if meridian_match is not None:
        meridian = re.sub(r"\s+", " ", meridian_match.group("meridian")).strip()
        end = max(end, match.end() + meridian_match.end())
    return section, aliquot, lot, meridian, end, has_alternative_section


def extract_plss(text: str) -> list[ExtractedPLSS]:
    """Extract common township/range descriptions from arbitrary source text."""
    extracted: list[ExtractedPLSS] = []
    for match in _TOWNSHIP_RANGE_RE.finditer(text):
        section, aliquot, lot, meridian, end, has_alternative_section = (
            _parse_optional_components(text, match)
        )
        try:
            description = PLSSDescription(
                township=int(match.group("township")),
                township_fraction=_normalize_fraction(match.group("township_fraction")),
                township_direction=_normalize_direction(
                    match.group("township_direction")
                ),
                range=int(match.group("range")),
                range_fraction=_normalize_fraction(match.group("range_fraction")),
                range_direction=_normalize_direction(match.group("range_direction")),
                section=section,
                aliquot=aliquot,
                lot=lot,
                meridian=meridian,
            )
        except ValueError:
            continue
        extracted.append(
            ExtractedPLSS(description, match.start(), end, has_alternative_section)
        )
    return extracted


def parse_canonical(text: str) -> PLSSDescription | None:
    extracted = extract_plss(text)
    if len(extracted) != 1 or extracted[0].has_alternative_section:
        return None
    description = extracted[0].description
    if description.canonical_text != text:
        return None
    return description


def is_valid_plss_id(plss_id: str) -> bool:
    return bool(_PLSSID_RE.fullmatch(plss_id))


@cached(CacheDomain.plss)
def _get_api_data(url: str) -> str:
    _RATE_LIMITER.wait()
    response = httpx.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise PLSSUnavailableError(f"non-JSON response from {url}") from exc
    if not isinstance(data, dict) or "error" in data:
        raise PLSSUnavailableError(f"unusable response from {url}: {data!r}")
    return response.text


def _query_url(base_url: str, params: dict[str, str]) -> str:
    return str(
        httpx.URL(f"{base_url}/query").copy_with(params=httpx.QueryParams(params))
    )


def _get_features(base_url: str, params: dict[str, str]) -> list[dict[str, Any]]:
    data = json.loads(_get_api_data(_query_url(base_url, params)))
    features = data.get("features")
    if not isinstance(features, list):
        raise PLSSUnavailableError(f"response contains no feature list: {data!r}")
    if data.get("exceededTransferLimit"):
        raise PLSSUnavailableError("PLSS query exceeded the service transfer limit")
    return [feature for feature in features if isinstance(feature, dict)]


def _sql_string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _blm_fraction_matches(source_fraction: str, value: object) -> bool:
    if value is None:
        return None in _FRACTION_TO_BLM[source_fraction]
    if isinstance(value, str):
        return value in _FRACTION_TO_BLM[source_fraction]
    return False


def _normalize_api_meridian_name(value: object, code: str) -> str:
    name = str(value or "").strip()
    if not name or name == code:
        return _MERIDIAN_NAME_BY_CODE.get(code, name)
    return name


def get_townships(
    description: PLSSDescription, state_code: str
) -> tuple[Township, ...]:
    """Return all BLM township polygons matching the state and township/range."""
    state_code = state_code.upper()
    where = " AND ".join(
        (
            f"STATEABBR={_sql_string(state_code)}",
            f"TWNSHPNO={_sql_string(f'{description.township:03d}')}",
            f"TWNSHPDIR={_sql_string(description.township_direction)}",
            f"RANGENO={_sql_string(f'{description.range:03d}')}",
            f"RANGEDIR={_sql_string(description.range_direction)}",
        )
    )
    features = _get_features(
        f"{BLM_BASE_URL}/1",
        {
            "where": where,
            "outFields": (
                "STATEABBR,PRINMERCD,PRINMER,TWNSHPNO,TWNSHPFRAC,TWNSHPDIR,"
                "RANGENO,RANGEFRAC,RANGEDIR,PLSSID"
            ),
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        },
    )
    by_id: dict[str, list[dict[str, Any]]] = {}
    properties_by_id: dict[str, dict[str, Any]] = {}
    for feature in features:
        properties = feature.get("properties")
        geometry = feature.get("geometry")
        if not isinstance(properties, dict) or not isinstance(geometry, dict):
            continue
        if not _blm_fraction_matches(
            description.township_fraction, properties.get("TWNSHPFRAC")
        ):
            continue
        if not _blm_fraction_matches(
            description.range_fraction, properties.get("RANGEFRAC")
        ):
            continue
        plss_id = properties.get("PLSSID")
        if not isinstance(plss_id, str):
            continue
        by_id.setdefault(plss_id, []).append(geometry)
        properties_by_id[plss_id] = properties

    townships = []
    for plss_id, geometries in by_id.items():
        properties = properties_by_id[plss_id]
        meridian_code = str(properties.get("PRINMERCD") or "").strip()
        meridian = _normalize_api_meridian_name(
            properties.get("PRINMER"), meridian_code
        )
        if description.meridian is not None and _normalize_meridian(
            description.meridian
        ) != _normalize_meridian(meridian):
            continue
        townships.append(
            Township(
                state=state_code,
                meridian=meridian,
                meridian_code=meridian_code,
                plss_id=plss_id,
                description=description.with_meridian(meridian),
                geometries=tuple(geometries),
            )
        )
    return tuple(sorted(townships, key=lambda township: township.plss_id))


def _normalize_meridian(text: str) -> str:
    text = text.casefold()
    text = re.sub(r"\bmt\.?\s*d\.?\b", "mount diablo", text)
    text = re.sub(r"\bmt\.?\b", "mount", text)
    for word, replacement in {
        "first": "1st",
        "second": "2nd",
        "third": "3rd",
        "fourth": "4th",
        "fifth": "5th",
        "sixth": "6th",
    }.items():
        text = re.sub(rf"\b{word}\b", replacement, text)
    text = re.sub(
        r"\b(?:principal|principle|meridian|base\s*line|baseline|base|and|of)\b",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return re.sub(r"[^a-z0-9]", "", text.casefold())


def _get_county_geometries(
    state_fips: str, county_name: str
) -> tuple[dict[str, Any], ...]:
    county_name = county_name.split(",", 1)[0].strip()
    basename = re.sub(
        r"\s+(?:County|Parish|Borough|Census Area|Municipality|City and Borough)$",
        "",
        county_name,
        flags=re.IGNORECASE,
    )
    where = (
        f"STATE={_sql_string(state_fips)} AND "
        f"(NAME={_sql_string(county_name)} OR BASENAME={_sql_string(basename)})"
    )
    features = _get_features(
        CENSUS_COUNTY_URL,
        {
            "where": where,
            "outFields": "GEOID,NAME,BASENAME",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        },
    )
    return tuple(
        geometry
        for feature in features
        if isinstance((geometry := feature.get("geometry")), dict)
    )


def resolve_township(
    description: PLSSDescription,
    *,
    state_code: str,
    state_fips: str | None = None,
    county_name: str | None = None,
    point: coordinates.Point | None = None,
    extent: coordinate_lint.CoordinateExtent | None = None,
) -> TownshipResolution:
    candidates = get_townships(description, state_code)
    if not candidates:
        return TownshipResolution(None, (), "no matching BLM township")

    if county_name is not None and state_fips is not None:
        county_geometries = _get_county_geometries(state_fips, county_name)
        if not county_geometries:
            return TownshipResolution(
                None, candidates, f"Census returned no geometry for {county_name}"
            )
        county_candidates = tuple(
            township
            for township in candidates
            if geometries_intersect(township.geometries, county_geometries)
        )
        if not county_candidates:
            return TownshipResolution(
                None, candidates, f"no matching township intersects {county_name}"
            )
        candidates = county_candidates

    if point is not None and len(candidates) > 1:
        coordinate_candidates = tuple(
            township
            for township in candidates
            if geometry_distance_km(point, township.geometries)
            <= coordinate_lint.COORDINATE_TOLERANCE_KM
        )
        if coordinate_candidates:
            candidates = coordinate_candidates
    elif extent is not None and len(candidates) > 1:
        coordinate_candidates = tuple(
            township
            for township in candidates
            if geometry_extent_distance_km(extent, township.geometries)
            <= coordinate_lint.COORDINATE_TOLERANCE_KM
        )
        if coordinate_candidates:
            candidates = coordinate_candidates

    if description.section is not None and len(candidates) > 1:
        # Duplicate township records (most often the CadNSDI A/B duplicate codes)
        # do not necessarily contain the same first-division coverage. A cited
        # section can therefore disambiguate records that county and coordinates
        # cannot. An empty result is not negative evidence because BLM coverage is
        # incomplete; narrow the candidates only when at least one record contains
        # the requested section.
        section_candidates = tuple(
            township
            for township in candidates
            if get_description_geometry(township, description).geometries
        )
        if section_candidates:
            candidates = section_candidates

    if len(candidates) == 1:
        return TownshipResolution(candidates[0], candidates)
    candidate_text = ", ".join(
        f"{candidate.plss_id} ({candidate.meridian})" for candidate in candidates
    )
    return TownshipResolution(None, candidates, f"ambiguous among {candidate_text}")


def get_description_geometry(
    township: Township, description: PLSSDescription
) -> ResolvedGeometry:
    if description.section is None:
        return ResolvedGeometry("township", township.geometries)

    section_values = {str(description.section), f"{description.section:02d}"}
    if len(section_values) == 1:
        section_where = f"FRSTDIVNO={_sql_string(section_values.pop())}"
    else:
        section_where = (
            "FRSTDIVNO IN ("
            + ",".join(_sql_string(value) for value in sorted(section_values))
            + ")"
        )
    where_parts = [f"PLSSID={_sql_string(township.plss_id)}", section_where]
    layer = 2
    level = "section"
    out_fields = "PLSSID,FRSTDIVID,FRSTDIVNO"
    if description.lot is not None:
        layer = 3
        level = "lot"
        out_fields = "PLSSID,FRSTDIVID,FRSTDIVNO,SECDIVID,QSEC,QQSEC,GOVLOT"
        where_parts.append(f"GOVLOT={_sql_string(description.lot)}")
    elif (
        description.aliquot
        and len(description.aliquot) <= 2
        and all(len(part) == 2 for part in description.aliquot)
    ):
        layer = 3
        level = (
            "quarter-quarter section"
            if len(description.aliquot) == 2
            else "quarter section"
        )
        out_fields = "PLSSID,FRSTDIVID,FRSTDIVNO,SECDIVID,QSEC,QQSEC,GOVLOT"
        field = "QQSEC" if len(description.aliquot) == 2 else "QSEC"
        where_parts.append(f"{field}={_sql_string(''.join(description.aliquot))}")

    features = _get_features(
        f"{BLM_BASE_URL}/{layer}",
        {
            "where": " AND ".join(where_parts),
            "outFields": out_fields,
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        },
    )
    geometries = tuple(
        geometry
        for feature in features
        if isinstance((geometry := feature.get("geometry")), dict)
    )
    if not geometries and (description.aliquot or description.lot is not None):
        # Atomic/intersected coverage is incomplete. Retain the section-level
        # cross-check instead of treating a missing aliquot polygon as evidence that
        # the source description is invalid.
        return get_description_geometry(
            township, replace(description, aliquot=(), lot=None)
        )
    return ResolvedGeometry(level, geometries)


def _iter_polygons(geometry: dict[str, Any]) -> Iterator[list[list[list[float]]]]:
    coordinates_ = geometry.get("coordinates")
    if not isinstance(coordinates_, list):
        return
    match geometry.get("type"):
        case "Polygon":
            yield coordinates_
        case "MultiPolygon":
            yield from coordinates_


def _points(ring: list[list[float]]) -> list[coordinates.Point]:
    return [
        coordinates.Point(float(pair[0]), float(pair[1]))
        for pair in ring
        if isinstance(pair, list) and len(pair) >= 2
    ]


def _point_in_ring(point: coordinates.Point, ring: list[list[float]]) -> bool:
    points = _points(ring)
    inside = False
    if len(points) < 3:
        return False
    previous = points[-1]
    for current in points:
        if (current.latitude > point.latitude) != (previous.latitude > point.latitude):
            longitude = (previous.longitude - current.longitude) * (
                point.latitude - current.latitude
            ) / (previous.latitude - current.latitude) + current.longitude
            if point.longitude < longitude:
                inside = not inside
        previous = current
    return inside


def point_in_geometry(point: coordinates.Point, geometry: dict[str, Any]) -> bool:
    for polygon in _iter_polygons(geometry):
        if not polygon or not _point_in_ring(point, polygon[0]):
            continue
        if any(_point_in_ring(point, hole) for hole in polygon[1:]):
            continue
        return True
    return False


def _orientation(
    first: coordinates.Point, second: coordinates.Point, third: coordinates.Point
) -> float:
    return (second.longitude - first.longitude) * (third.latitude - first.latitude) - (
        second.latitude - first.latitude
    ) * (third.longitude - first.longitude)


def _segments_intersect(
    first_start: coordinates.Point,
    first_end: coordinates.Point,
    second_start: coordinates.Point,
    second_end: coordinates.Point,
) -> bool:
    return (
        _orientation(first_start, first_end, second_start)
        * _orientation(first_start, first_end, second_end)
        <= 0
        and _orientation(second_start, second_end, first_start)
        * _orientation(second_start, second_end, first_end)
        <= 0
        and max(
            min(first_start.longitude, first_end.longitude),
            min(second_start.longitude, second_end.longitude),
        )
        <= min(
            max(first_start.longitude, first_end.longitude),
            max(second_start.longitude, second_end.longitude),
        )
        and max(
            min(first_start.latitude, first_end.latitude),
            min(second_start.latitude, second_end.latitude),
        )
        <= min(
            max(first_start.latitude, first_end.latitude),
            max(second_start.latitude, second_end.latitude),
        )
    )


def _ring_edges(
    ring: list[list[float]],
) -> Iterator[tuple[coordinates.Point, coordinates.Point]]:
    points = _points(ring)
    if len(points) < 2:
        return
    yield from itertools.pairwise(points)
    if points[0] != points[-1]:
        yield points[-1], points[0]


def _polygon_outer_rings(geometry: dict[str, Any]) -> Iterator[list[list[float]]]:
    for polygon in _iter_polygons(geometry):
        if polygon:
            yield polygon[0]


def _geometries_intersect(first: dict[str, Any], second: dict[str, Any]) -> bool:
    for first_ring in _polygon_outer_rings(first):
        first_points = _points(first_ring)
        if any(point_in_geometry(point, second) for point in first_points):
            return True
        first_edges = tuple(_ring_edges(first_ring))
        for second_ring in _polygon_outer_rings(second):
            second_points = _points(second_ring)
            if any(point_in_geometry(point, first) for point in second_points):
                return True
            if any(
                _segments_intersect(*first_edge, *second_edge)
                for first_edge in first_edges
                for second_edge in _ring_edges(second_ring)
            ):
                return True
    return False


def geometries_intersect(
    first: Iterable[dict[str, Any]], second: Iterable[dict[str, Any]]
) -> bool:
    return any(
        _geometries_intersect(first_geometry, second_geometry)
        for first_geometry in first
        for second_geometry in second
    )


def _point_to_segment_distance_km(
    point: coordinates.Point, start: coordinates.Point, end: coordinates.Point
) -> float:
    latitude_scale = 111.195
    longitude_scale = latitude_scale * math.cos(math.radians(point.latitude))
    px = point.longitude * longitude_scale
    py = point.latitude * latitude_scale
    sx = start.longitude * longitude_scale
    sy = start.latitude * latitude_scale
    ex = end.longitude * longitude_scale
    ey = end.latitude * latitude_scale
    dx = ex - sx
    dy = ey - sy
    if dx == 0 and dy == 0:
        return math.hypot(px - sx, py - sy)
    fraction = ((px - sx) * dx + (py - sy) * dy) / (dx * dx + dy * dy)
    fraction = max(0.0, min(1.0, fraction))
    return math.hypot(px - (sx + fraction * dx), py - (sy + fraction * dy))


def geometry_distance_km(
    point: coordinates.Point, geometries: Iterable[dict[str, Any]]
) -> float:
    geometry_list = tuple(geometries)
    if any(point_in_geometry(point, geometry) for geometry in geometry_list):
        return 0.0
    distances = [
        _point_to_segment_distance_km(point, start, end)
        for geometry in geometry_list
        for ring in _polygon_outer_rings(geometry)
        for start, end in _ring_edges(ring)
    ]
    return min(distances, default=math.inf)


def _extent_geometry(extent: coordinate_lint.CoordinateExtent) -> dict[str, Any]:
    west = extent.longitude.minimum
    east = extent.longitude.maximum
    south = extent.latitude.minimum
    north = extent.latitude.maximum
    return {
        "type": "Polygon",
        "coordinates": [
            [[west, south], [east, south], [east, north], [west, north], [west, south]]
        ],
    }


def geometry_extent_distance_km(
    extent: coordinate_lint.CoordinateExtent, geometries: Iterable[dict[str, Any]]
) -> float:
    geometry_list = tuple(geometries)
    if geometries_intersect((_extent_geometry(extent),), geometry_list):
        return 0.0
    # This is conservative for a rectangular coordinate range: subtracting its
    # center-to-corner radius cannot overstate the separation from the PLSS polygon.
    return max(
        0.0,
        geometry_distance_km(extent.center, geometry_list)
        - coordinate_lint.extent_radius_km(extent),
    )


def geometry_bounds(
    geometries: Iterable[dict[str, Any]],
) -> tuple[float, float, float, float] | None:
    points = [
        point
        for geometry in geometries
        for ring in _polygon_outer_rings(geometry)
        for point in _points(ring)
    ]
    if not points:
        return None
    return (
        min(point.latitude for point in points),
        max(point.latitude for point in points),
        min(point.longitude for point in points),
        max(point.longitude for point in points),
    )


US_STATE_CODES: dict[str, tuple[str, str]] = {
    "Alabama": ("AL", "01"),
    "Alaska": ("AK", "02"),
    "Arizona": ("AZ", "04"),
    "Arkansas": ("AR", "05"),
    "California": ("CA", "06"),
    "Colorado": ("CO", "08"),
    "Connecticut": ("CT", "09"),
    "Delaware": ("DE", "10"),
    "District of Columbia": ("DC", "11"),
    "Florida": ("FL", "12"),
    "Georgia": ("GA", "13"),
    "Hawaii": ("HI", "15"),
    "Idaho": ("ID", "16"),
    "Illinois": ("IL", "17"),
    "Indiana": ("IN", "18"),
    "Iowa": ("IA", "19"),
    "Kansas": ("KS", "20"),
    "Kentucky": ("KY", "21"),
    "Louisiana": ("LA", "22"),
    "Maine": ("ME", "23"),
    "Maryland": ("MD", "24"),
    "Massachusetts": ("MA", "25"),
    "Michigan": ("MI", "26"),
    "Minnesota": ("MN", "27"),
    "Mississippi": ("MS", "28"),
    "Missouri": ("MO", "29"),
    "Montana": ("MT", "30"),
    "Nebraska": ("NE", "31"),
    "Nevada": ("NV", "32"),
    "New Hampshire": ("NH", "33"),
    "New Jersey": ("NJ", "34"),
    "New Mexico": ("NM", "35"),
    "New York": ("NY", "36"),
    "North Carolina": ("NC", "37"),
    "North Dakota": ("ND", "38"),
    "Ohio": ("OH", "39"),
    "Oklahoma": ("OK", "40"),
    "Oregon": ("OR", "41"),
    "Pennsylvania": ("PA", "42"),
    "Rhode Island": ("RI", "44"),
    "South Carolina": ("SC", "45"),
    "South Dakota": ("SD", "46"),
    "Tennessee": ("TN", "47"),
    "Texas": ("TX", "48"),
    "Utah": ("UT", "49"),
    "Vermont": ("VT", "50"),
    "Virginia": ("VA", "51"),
    "Washington": ("WA", "53"),
    "West Virginia": ("WV", "54"),
    "Wisconsin": ("WI", "55"),
    "Wyoming": ("WY", "56"),
}
