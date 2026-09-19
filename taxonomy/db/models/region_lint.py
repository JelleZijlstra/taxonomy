"""Lint steps for Regions."""

import functools
import re
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from taxonomy import coordinates
from taxonomy.apis import nominatim, openstreetmap
from taxonomy.db import coordinate_lint, helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import IgnoreLint, Lint, add_tag_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.region import Region, RegionTag

_ADMINISTRATIVE_REGION_KINDS = frozenset(
    {
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
)

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

# Nominatim's English display names sometimes differ materially from the names
# used by Hesperomys. Keep these aliases explicit: fuzzy name matching is too
# permissive for homonymous administrative divisions.
_OSM_NAME_ALIASES = {
    "Adygea": {"Republic of Adygea"},
    "Alpes-Maritimes": {"Maritime Alps"},
    "Arica and Parinacota Region": {"Arica y Parinacota Region"},
    "Ascension": {"Ascension Island"},
    "Asunción": {"Distrito Capital"},
    "Aysén Region": {"Aysen del General Carlos Ibanez del Campo Region"},
    "Bangka-Belitung": {"Bangka-Belitung Islands"},
    "Basel-Stadt": {"Basel-City"},
    "Basque Country": {"Autonomous Community of the Basque Country"},
    "Bougainville Region": {"Autonomous Region of Bougainville"},
    "Castellón": {"Castelló / Castellón"},
    "Castilla-La Mancha": {"Castile-La Mancha"},
    "Chimbu Province": {"Simbu"},
    "Chukotka": {"Chukotka Autonomous Okrug"},
    "Clipperton": {"Clipperton Island"},
    "Cocos Islands": {"Cocos (Keeling) Islands"},
    "Córdoba (Spain)": {"Córdoba"},
    "Corse-du-Sud": {"South Corsica"},
    "Crete": {"Region of Crete"},
    "Distrito Federal (Brazil)": {"Federal District"},
    "Distrito Federal (Mexico)": {"Mexico City"},
    "De Soto Parish, Louisiana": {"DeSoto Parish"},
    "Emberá-Wounaan Comarca": {"Emberá-Wounaan"},
    "Friesland": {"Frisia"},
    "Graubünden": {"Grisons"},
    "Guna Yala Comarca": {"Guna Yala"},
    "Haute-Corse": {"Upper Corsica"},
    "Haute-Savoie": {"Upper Savoy"},
    "Ionian Islands": {"Ioanian Islands"},
    "Jakarta": {"Special Capital Region of Jakarta"},
    "Kaliningrad Oblast": {"Kaliningrad"},
    "Kalmykia": {"Republic of Kalmykia"},
    "Kemerovo Oblast": {"Kemerovo Oblast–Kuzbass"},
    "Khyber-Pakhtunkhwa": {"Khyber Pakhtunkhwa"},
    "Khanty–Mansi Autonomous Okrug": {"Khanty-Mansiysk Autonomous Okrug – Ugra"},
    "La Rioja (Spain)": {"Rioja"},
    "La Guaira": {"Vargas State"},
    "Luzern": {"Lucerne"},
    "Mari El": {"Mari El Republic"},
    "Madrid": {"Community of Madrid", "Autonomous Community of Madrid"},
    "Magallanes Region": {"Magallanes and Chilean Antarctica Region"},
    "Mordovia": {"Republic of Mordovia"},
    "Naso Tjër Di Comarca": {"Naso Tjër Di"},
    "Ngäbe-Buglé Comarca": {"Ngäbe-Buglé"},
    "North Aegean": {"Northern Aegean"},
    "North Caribbean Coast Autonomous Region": {"North Caribbean Coast"},
    "North Ossetia": {"Republic of North Ossetia – Alania"},
    "Orissa": {"Odisha"},
    "Palestine": {"Palestinian Territories"},
    "Sakha": {"Sakha Republic"},
    "San Andrés y Providencia": {
        "Archipelago of San Andrés, Providencia and Santa Catalina"
    },
    "Callao Province": {"Callao"},
    "Savoie": {"Savoy"},
    "South Caribbean Coast Autonomous Region": {"South Caribbean Coast"},
    "Tibet": {"Xizang"},
    "Trentino-Alto Adige": {"Trentino – Alto Adige/Südtirol"},
    "Tuva": {"Tuva Republic"},
    "Valais": {"Valais/Wallis"},
    "Washington County, Rhode Island": {"South County"},
}


def get_ignores(region: Region) -> Iterable[IgnoreLint]:
    return region.get_tags(region.tags, RegionTag.IgnoreLint)


def add_ignore(region: Region, label: str, comment: str) -> None:
    region.add_tag(RegionTag.IgnoreLint(label, comment=comment))


LINT = Lint(Region, get_ignores, add_ignore)


def _normalize_name(name: str) -> str:
    name = re.sub(r"\bSt\.?\b", "Saint", name, flags=re.IGNORECASE)
    return helpers.simplify_string(name, clean_words=False).replace("-", "")


def _unqualified_name(region: Region) -> str:
    name = region.name
    if region.parent is not None:
        parent_names = {region.parent.name, _unqualified_name(region.parent)}
        for parent_name in sorted(parent_names, key=len, reverse=True):
            parent_suffix = f", {parent_name}"
            if name.endswith(parent_suffix):
                name = name.removesuffix(parent_suffix)
            parenthetical_suffix = f" ({parent_name})"
            if name.endswith(parenthetical_suffix):
                name = name.removesuffix(parenthetical_suffix)
    return name


def _is_english_county(region: Region) -> bool:
    return region.kind is RegionKind.county and any(
        parent.kind is RegionKind.subnational and parent.name == "England"
        for parent in region.all_parents()
    )


@functools.cache
def _get_redirect_alias_names_by_target_id() -> dict[int, frozenset[str]]:
    aliases: defaultdict[int, set[str]] = defaultdict(set)
    for redirect in Region.select():
        if redirect.kind is not RegionKind.redirect or redirect.parent is None:
            continue
        aliases[redirect.parent.id].add(redirect.name)
    return {target_id: frozenset(names) for target_id, names in aliases.items()}


def _get_redirect_name_aliases(region: Region) -> set[str]:
    """Return persisted redirect names, including context-free name variants."""
    region_id = getattr(region, "id", None)
    if not isinstance(region_id, int):
        return set()
    redirect_names = _get_redirect_alias_names_by_target_id().get(region_id, ())
    aliases = set(redirect_names)
    qualifiers = {
        name
        for parent in region.all_parents()
        for name in {parent.name, _unqualified_name(parent)}
    }
    for redirect_name in redirect_names:
        for qualifier in qualifiers:
            aliases.add(redirect_name.removesuffix(f", {qualifier}"))
            aliases.add(redirect_name.removesuffix(f" ({qualifier})"))
    return aliases


def get_region_name_aliases(region: Region) -> set[str]:
    """Return exact aliases accepted for a Region across OSM integrations."""
    name = _unqualified_name(region)
    aliases = {region.name, name}
    designator = _REGION_KIND_DESIGNATORS.get(region.kind)
    if designator is not None:
        suffix = f" {designator}"
        if name.casefold().endswith(suffix.casefold()):
            bare_name = name[: -len(suffix)]
            aliases.update({bare_name, f"{designator} of {bare_name}"})
        else:
            aliases.add(f"{name} {designator}")
    if region.kind is RegionKind.county:
        for suffix in (" City", " city"):
            if name.endswith(suffix):
                bare_name = name[: -len(suffix)]
                aliases.update({bare_name, f"City of {bare_name}"})
        if name.endswith(" County"):
            bare_name = name.removesuffix(" County")
            aliases.add(f"City and County of {bare_name}")
        if _is_english_county(region):
            # OSM normally uses the ordinary county name, but occasionally adds
            # this qualifier to distinguish a lieutenancy boundary from a
            # narrower local-government boundary (currently Shropshire).
            aliases.add(f"{name} (Ceremonial)")
    if region.kind is RegionKind.country:
        aliases.add(nominatim.HESP_REGION_TO_OSM_NAME.get(region.name, region.name))
    aliases.update(get_openstreetmap_name_aliases(region))
    return aliases


def get_openstreetmap_name_aliases(region: Region) -> set[str]:
    """Return persisted and transitional same-entity OSM spelling aliases."""
    return {
        *_get_redirect_name_aliases(region),
        *_OSM_NAME_ALIASES.get(region.name, ()),
    }


# Backward compatibility for one-off audit scripts; new code should use the
# public helper above so Region and Location lints share the same registry.
_region_name_aliases = get_region_name_aliases


def _address_name_aliases(region: Region) -> set[str]:
    aliases = get_region_name_aliases(region)
    if region.kind is RegionKind.country:
        aliases.add(nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name))
    return aliases


def _query_name(region: Region, *, canonical: bool = False) -> str:
    if region.kind is RegionKind.country:
        return nominatim.HESP_REGION_TO_OSM_NAME.get(region.name, region.name)
    if canonical:
        return _unqualified_name(region)
    aliases = get_region_name_aliases(region)
    return min(aliases, key=lambda name: (len(name), name))


def _get_nominatim_query(
    region: Region, *, canonical: bool, local_name: str | None = None
) -> str:
    components = [
        (
            local_name
            if local_name is not None
            else _query_name(region, canonical=canonical)
        )
    ]
    seen = {_normalize_name(components[0])}
    for parent in _administrative_parents_for_matching(region):
        name = _query_name(parent, canonical=canonical)
        normalized = _normalize_name(name)
        if normalized not in seen:
            components.append(name)
            seen.add(normalized)
    return ", ".join(components)


def get_nominatim_query(region: Region) -> str:
    return _get_nominatim_query(region, canonical=False)


def get_nominatim_queries(region: Region) -> tuple[str, ...]:
    primary = get_nominatim_query(region)
    canonical = _get_nominatim_query(region, canonical=True)
    aliases = get_region_name_aliases(region)
    if region.name != _unqualified_name(region):
        aliases.discard(region.name)
    alias_queries = (
        _get_nominatim_query(region, canonical=True, local_name=alias)
        for alias in sorted(aliases, key=lambda name: (len(name), name))
    )
    return tuple(dict.fromkeys((primary, canonical, *alias_queries)))


def _get_structured_nominatim_query(region: Region) -> dict[str, str] | None:
    if region.kind is RegionKind.country:
        return None
    country = next(
        (
            parent
            for parent in region.all_parents()
            if parent.kind is RegionKind.country
        ),
        None,
    )
    if country is None:
        return None
    field = "county" if region.kind is RegionKind.county else "state"
    query = {
        field: _unqualified_name(region),
        "country": nominatim.HESP_REGION_TO_OSM_NAME.get(country.name, country.name),
    }
    if (
        region.kind is RegionKind.county
        and region.parent is not None
        and region.parent.kind
        in {RegionKind.state, RegionKind.province, RegionKind.subnational}
    ):
        query["state"] = _unqualified_name(region.parent)
    return query


def _expected_address_types(region: Region) -> frozenset[str]:
    if region.parent is not None and region.parent.kind is RegionKind.country:
        if region.parent.name == "Chile" and region.kind is RegionKind.region:
            # Chilean regions are states in both Nominatim schemas. Accepting
            # cities here selects the communes named Aysén and O'Higgins when
            # lookup omits admin_level.
            return frozenset({"region", "state"})
        if region.parent.name == "Panama" and region.kind is RegionKind.province:
            # Bocas del Toro names both a province and a smaller district.
            return frozenset({"province", "state"})
    if region.kind is RegionKind.country:
        # Overseas territories represented as country-like Regions in this
        # database may be lower-level administrative objects in OSM. Nominatim
        # also derives address types from linked place features, so island
        # countries and disputed boundaries need not be exposed as ``country``.
        return frozenset(
            {
                "administrative",
                "archipelago",
                "city",
                "country",
                "county",
                "disputed",
                "district",
                "locality",
                "municipality",
                "region",
                "state",
            }
        )
    if region.kind is RegionKind.county:
        address_types = {"city", "county", "district"}
        if _is_english_county(region):
            # Most separate lieutenancy boundaries are exposed as
            # ``ceremonial``. Coterminous boundaries may instead be indexed as
            # an administrative county or, for Greater Manchester, a state
            # district.
            address_types.update({"ceremonial", "state_district"})
        return frozenset(address_types)
    if region.kind is RegionKind.prefecture:
        # Nominatim classifies Hokkaido as a region but the other Japanese
        # prefecture-level boundaries as provinces or states. All represent
        # admin_level=4.
        return frozenset({"province", "region", "state"})
    if region.kind is RegionKind.state:
        return frozenset({"city", "state"})
    if region.kind is RegionKind.province:
        return frozenset(
            {"city", "county", "district", "province", "state", "state_district"}
        )
    if region.kind is RegionKind.department:
        return frozenset({"city", "county", "district", "state", "state_district"})
    if region.kind is RegionKind.region:
        return frozenset(
            {"archipelago", "city", "county", "district", "region", "state"}
        )
    if region.kind is RegionKind.subnational:
        return frozenset({"city", "county", "district", "region", "state"})
    if region.kind is RegionKind.territory:
        return frozenset({"city", "county", "district", "state", "territory"})
    return frozenset({"city", "county", "district", "state"})


def _has_acceptable_address_type(
    region: Region, result: nominatim.SearchResult
) -> bool:
    if result.address_type in _preferred_address_types(region):
        return True
    if region.kind not in {RegionKind.county, RegionKind.prefecture}:
        return False
    country = next(
        (
            parent
            for parent in region.all_parents()
            if parent.kind is RegionKind.country
        ),
        None,
    )
    if country is None:
        return False
    if region.kind is RegionKind.prefecture:
        # Tokyo's prefectural boundary is indexed from its linked city. Keep
        # accepting only the prefectural level, not a same-named municipality.
        return (
            country.name == "Japan"
            and result.address_type == "city"
            and result.extra.get("admin_level") == "4"
        )
    if country.name != "United States":
        return False
    # Consolidated city-counties and county-equivalent independent cities are
    # frequently indexed from their linked place as a city, town, or suburb.
    # Accept those labels only for an admin_level=6 relation, which continues
    # to reject an ordinary same-named municipality (normally admin_level=8).
    return result.address_type in {
        "city",
        "city_district",
        "county",
        "suburb",
        "town",
    } and (result.extra.get("admin_level") == "6")


_EXPECTED_ADMIN_LEVELS = {
    RegionKind.canton: frozenset({"4"}),
    RegionKind.county: frozenset({"5", "6"}),
    RegionKind.department: frozenset({"4", "5", "6"}),
    RegionKind.prefecture: frozenset({"3", "4"}),
    RegionKind.province: frozenset({"4", "6"}),
    RegionKind.region: frozenset({"4", "5"}),
    RegionKind.state: frozenset({"4"}),
    RegionKind.subnational: frozenset({"3", "4", "6", "7"}),
    RegionKind.territory: frozenset({"4"}),
}


def _has_acceptable_admin_level(region: Region, result: nominatim.SearchResult) -> bool:
    expected = _EXPECTED_ADMIN_LEVELS.get(region.kind)
    if expected is None:
        return True
    actual = result.extra.get("admin_level")
    # Nominatim may omit admin_level from an otherwise valid lookup result.
    # Historic and ceremonial boundaries normally omit it because they are not
    # part of the current administrative hierarchy.
    if actual is None:
        return True
    return actual in expected


def _is_supported_administrative_boundary(
    region: Region, *, category: str, feature_type: str
) -> bool:
    if category != "boundary":
        return False
    if feature_type == "administrative":
        return True
    if region.kind is RegionKind.country and feature_type == "disputed":
        return True
    # Connecticut's abolished counties, and some comparable county-level
    # divisions, remain useful geographic Regions even though OSM correctly
    # represents their boundaries as historical rather than current. English
    # lieutenancy areas are likewise explicit non-administrative boundaries.
    return region.kind is RegionKind.county and feature_type in {
        "ceremonial",
        "historic",
    }


def _get_openstreetmap_tag(region: Region) -> Any | None:
    return next(
        (tag for tag in region.tags if isinstance(tag, RegionTag.OpenStreetMap)), None
    )


def _administrative_parents_for_matching(region: Region) -> Iterable[Region]:
    if region.kind is RegionKind.country:
        # The Region tree embeds some country-like territories under a sovereign
        # state and Taiwan under China. Those database choices should not make
        # the territory's own OSM identity fail validation or inference.
        return
    for parent in region.all_parents():
        if (
            parent.kind is RegionKind.country
            or _get_openstreetmap_tag(parent) is not None
        ):
            yield parent


def _address_names(result: nominatim.GeocodeResult) -> set[str]:
    return {
        _normalize_name(value)
        for value in (*result.address.values(), *result.administrative.values())
    }


def _is_matching_result(region: Region, result: nominatim.GeocodeResult) -> bool:
    if (
        result.osm_type is None
        or result.osm_id is None
        or not _is_supported_administrative_boundary(
            region, category=result.category, feature_type=result.feature_type
        )
        or result.address_type not in _preferred_address_types(region)
    ):
        return False
    admin_level = _result_admin_level(result)
    if (
        admin_level is not None
        and region.kind in {RegionKind.state, RegionKind.prefecture}
        and admin_level > 4
    ):
        return False
    expected_names = {_normalize_name(name) for name in get_region_name_aliases(region)}
    if _normalize_name(result.name) not in expected_names:
        return False
    address_names = _address_names(result)
    return all(
        not address_names.isdisjoint(
            {_normalize_name(name) for name in _address_name_aliases(parent)}
        )
        for parent in _administrative_parents_for_matching(region)
    )


def _result_admin_level(result: nominatim.GeocodeResult) -> int | None:
    result_name = _normalize_name(result.name)
    levels = []
    for key, value in result.administrative.items():
        if not key.startswith("level") or _normalize_name(value) != result_name:
            continue
        try:
            levels.append(int(key.removeprefix("level")))
        except ValueError:
            continue
    return max(levels, default=None)


def _select_inference_candidate(
    region: Region,
    candidates: dict[tuple[str | None, int | None, str], nominatim.GeocodeResult],
) -> tuple[tuple[str | None, int | None, str], nominatim.GeocodeResult] | None:
    preferred = [
        (key, result)
        for key, result in candidates.items()
        if result.address_type in _preferred_address_types(region)
    ]
    if len(preferred) == 1:
        return preferred[0]
    if preferred:
        candidates = dict(preferred)
    if len(candidates) == 1:
        return next(iter(candidates.items()))
    canonical_name = _normalize_name(_unqualified_name(region))
    exact_canonical = [
        (key, result)
        for key, result in candidates.items()
        if _normalize_name(result.name) == canonical_name
    ]
    if len(exact_canonical) == 1:
        return exact_canonical[0]
    if exact_canonical:
        candidates = dict(exact_canonical)
    candidates_with_levels = [
        (key, result, level)
        for key, result in candidates.items()
        if (level := _result_admin_level(result)) is not None
    ]
    if not candidates_with_levels:
        return None
    minimum_level = min(level for _, _, level in candidates_with_levels)
    shallowest = [
        (key, result)
        for key, result, level in candidates_with_levels
        if level == minimum_level
    ]
    return shallowest[0] if len(shallowest) == 1 else None


def _preferred_address_types(region: Region) -> frozenset[str]:
    if region.kind is not RegionKind.county:
        return _expected_address_types(region)
    name = _unqualified_name(region)
    if name.endswith((" City", " city")):
        return frozenset({"city"})
    country = next(
        (
            parent
            for parent in region.all_parents()
            if parent.kind is RegionKind.country
        ),
        None,
    )
    if country is not None and country.name == "United States":
        # Nominatim's geocodejson search currently exposes OSM
        # ``boundary=historic`` county relations as ``locality``, while jsonv2
        # lookup reports ``historic``. Current counties use ``county``. All
        # three are preferable to a same-named city or town.
        return frozenset({"county", "historic", "locality"})
    return _expected_address_types(region)


def _is_preferred_inference_result(
    region: Region, result: nominatim.GeocodeResult
) -> bool:
    return result.address_type in _preferred_address_types(region)


@functools.cache
def _get_openstreetmap_tag_owners() -> dict[tuple[str, int], tuple[Region, ...]]:
    owners: defaultdict[tuple[str, int], list[Region]] = defaultdict(list)
    for region in Region.select_valid():
        for tag in region.tags:
            if isinstance(tag, RegionTag.OpenStreetMap):
                owners[(tag.osm_type, tag.osm_id)].append(region)
    return {key: tuple(regions) for key, regions in owners.items()}


@functools.cache
def _get_openstreetmap_lookup_results() -> (
    dict[tuple[str, int], nominatim.SearchResult]
):
    return nominatim.lookup_many(_get_openstreetmap_tag_owners())


def _names_overlap(first: Iterable[str], second: Iterable[str]) -> bool:
    first_normalized = {_normalize_name(name) for name in first}
    second_normalized = {_normalize_name(name) for name in second}
    return not first_normalized.isdisjoint(second_normalized)


def _get_iso_alpha2(result: nominatim.SearchResult) -> str | None:
    for key in ("ISO3166-1:alpha2", "country_code_iso3166_1_alpha_2"):
        value = result.extra.get(key)
        if value is not None and len(value) == 2:
            return value.upper()
    return None


def _get_iso_subdivision_codes(result: nominatim.SearchResult) -> set[str]:
    return {
        value.upper()
        for mapping in (result.address, result.names, result.extra)
        for key, value in mapping.items()
        if key.startswith("ISO3166-2")
    }


def _result_matches_parent_iso(
    parent: Region,
    result: nominatim.SearchResult,
    lookup_results: dict[tuple[str, int], nominatim.SearchResult],
) -> bool:
    tag = _get_openstreetmap_tag(parent)
    if tag is None:
        return False
    parent_result = lookup_results.get((tag.osm_type, tag.osm_id))
    if parent_result is None or (alpha2 := _get_iso_alpha2(parent_result)) is None:
        return False
    return any(
        code.startswith(f"{alpha2}-") for code in _get_iso_subdivision_codes(result)
    )


def _validate_openstreetmap_result(
    region: Region,
    tag: Any,
    result: nominatim.SearchResult,
    lookup_results: dict[tuple[str, int], nominatim.SearchResult],
) -> Iterable[str]:
    if result.category != tag.category:
        yield (
            f"stored OpenStreetMap category {tag.category!r} differs from current "
            f"category {result.category!r} for {tag.osm_type} {tag.osm_id}"
        )
    is_valid_administrative_feature = _is_supported_administrative_boundary(
        region, category=result.category, feature_type=result.feature_type
    )
    if (
        region.kind in _ADMINISTRATIVE_REGION_KINDS
        and not is_valid_administrative_feature
    ):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} is "
            f"{result.category}/{result.feature_type}, not an administrative boundary"
        )
    if result.address_type is not None and not _has_acceptable_address_type(
        region, result
    ):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} has address type "
            f"{result.address_type!r}, expected one of "
            f"{sorted(_preferred_address_types(region))!r}"
        )
    if not _has_acceptable_admin_level(region, result):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} has admin_level "
            f"{result.extra.get('admin_level')!r}, expected one of "
            f"{sorted(_EXPECTED_ADMIN_LEVELS[region.kind])!r} for "
            f"RegionKind.{region.kind.name}"
        )
    if not _names_overlap(
        get_region_name_aliases(region), nominatim.get_name_variants(result)
    ):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} is named {result.name!r}, "
            f"which does not match Region aliases {sorted(get_region_name_aliases(region))!r}"
        )
    address_names = {
        value
        for key, value in result.address.items()
        if key not in {"country_code", "postcode"}
    }
    for parent in _administrative_parents_for_matching(region):
        if not _names_overlap(
            _address_name_aliases(parent), address_names
        ) and not _result_matches_parent_iso(parent, result, lookup_results):
            yield (
                f"OpenStreetMap {tag.osm_type} {tag.osm_id} address "
                f"{result.display_name!r} does not include linked parent "
                f"Region {parent.name!r}"
            )


def _clear_openstreetmap_validation_caches() -> None:
    _get_redirect_alias_names_by_target_id.cache_clear()
    _get_openstreetmap_tag_owners.cache_clear()
    _get_openstreetmap_lookup_results.cache_clear()


def _stored_reference_is_searchable(region: Region, tag: Any) -> bool:
    """Fall back to search when Nominatim's lookup index temporarily omits an object."""
    for query in get_nominatim_queries(region):
        results = nominatim.search_geocodejson(query, limit=10)
        if any(
            result.osm_type == tag.osm_type
            and result.osm_id == tag.osm_id
            and result.category == tag.category
            and _is_matching_result(region, result)
            for result in results
        ):
            return True
    structured_query = _get_structured_nominatim_query(region)
    if structured_query is None:
        return False
    return any(
        result.osm_type == tag.osm_type
        and result.osm_id == tag.osm_id
        and result.category == tag.category
        and _is_matching_result(region, result)
        for result in nominatim.search_geocodejson_structured(
            **structured_query, limit=10
        )
    )


def _openstreetmap_element_names(element: openstreetmap.Element) -> set[str]:
    names: set[str] = set()
    for key, value in element.tags.items():
        if key in {
            "name",
            "official_name",
            "alt_name",
            "short_name",
            "loc_name",
        } or key.startswith("name:"):
            names.update(name.strip() for name in value.split(";") if name.strip())
    return names


def _validate_direct_openstreetmap_element(
    region: Region, tag: Any, element: openstreetmap.Element
) -> Iterable[str]:
    """Validate an extant OSM object that Nominatim has not indexed."""
    feature_type = element.tags.get(tag.category)
    if feature_type is None:
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} does not have stored "
            f"category tag {tag.category!r}"
        )
    elif (
        region.kind in _ADMINISTRATIVE_REGION_KINDS
        and not _is_supported_administrative_boundary(
            region, category=tag.category, feature_type=feature_type
        )
    ):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} is "
            f"{tag.category}/{feature_type}, not an administrative boundary"
        )
    names = _openstreetmap_element_names(element)
    if not _names_overlap(get_region_name_aliases(region), names):
        yield (
            f"OpenStreetMap {tag.osm_type} {tag.osm_id} has names {sorted(names)!r}, "
            f"which do not match Region aliases "
            f"{sorted(get_region_name_aliases(region))!r}"
        )


@LINT.add(
    "validate_openstreetmap",
    uses_optional_network=True,
    skip_virtual=True,
    clear_caches=_clear_openstreetmap_validation_caches,
)
def validate_openstreetmap(region: Region, cfg: LintConfig) -> Iterable[LintResult]:
    tags = [tag for tag in region.tags if isinstance(tag, RegionTag.OpenStreetMap)]
    if not tags:
        return
    if len(tags) > 1:
        yield f"has {len(tags)} OpenStreetMap tags; expected at most one"
    owners_by_identity = _get_openstreetmap_tag_owners()
    valid_tags = []
    for tag in tags:
        if tag.osm_type not in {"node", "way", "relation"}:
            yield f"has unsupported OpenStreetMap object type {tag.osm_type!r}"
            continue
        if tag.osm_id <= 0:
            yield f"has nonpositive OpenStreetMap object identifier {tag.osm_id}"
            continue
        if not tag.category:
            yield "has an empty OpenStreetMap category"
            continue
        valid_tags.append(tag)
        owners = owners_by_identity.get((tag.osm_type, tag.osm_id), ())
        if len(owners) > 1:
            yield (
                f"OpenStreetMap {tag.osm_type} {tag.osm_id} is also linked to "
                f"{[(owner.id, owner.name) for owner in owners if owner != region]!r}"
            )
    if not cfg.is_resource_available(LintResource.NETWORK):
        return
    lookup_results = _get_openstreetmap_lookup_results()
    for tag in valid_tags:
        result = lookup_results.get((tag.osm_type, tag.osm_id))
        if result is None:
            if _stored_reference_is_searchable(region, tag):
                continue
            element = openstreetmap.lookup_element(tag.osm_type, tag.osm_id)
            if element is None:
                yield (
                    f"OpenStreetMap {tag.osm_type} {tag.osm_id} is not returned by "
                    "Nominatim and does not exist in the OpenStreetMap API"
                )
            else:
                yield from _validate_direct_openstreetmap_element(region, tag, element)
            continue
        yield from _validate_openstreetmap_result(region, tag, result, lookup_results)


@LINT.add(
    "openstreetmap_parent_containment",
    required_resources={LintResource.SLOW},
    skip_virtual=True,
)
def check_openstreetmap_parent_containment(
    region: Region, cfg: LintConfig
) -> Iterable[str]:
    if region.parent is None:
        return
    child_boundary = coordinate_lint.get_direct_region_boundary(
        region, allow_network=False
    )
    if child_boundary is None:
        return
    parent_boundary = coordinate_lint.get_region_boundary(
        region.parent, allow_network=False
    )
    if parent_boundary is None:
        return
    assert child_boundary.result.geometry is not None
    assert parent_boundary.result.geometry is not None
    if coordinates.geometry_is_within_geometry(
        child_boundary.result.geometry,
        parent_boundary.result.geometry,
        tolerance_km=coordinate_lint.REGION_BOUNDARY_TOLERANCE_KM,
    ):
        return
    yield (
        f"OpenStreetMap polygon extends more than "
        f"{coordinate_lint.REGION_BOUNDARY_TOLERANCE_KM:g} km outside linked "
        f"ancestor Region {parent_boundary.region.name!r}"
    )


@LINT.add("infer_openstreetmap", requires_network=True)
def infer_openstreetmap(region: Region, cfg: LintConfig) -> Iterable[LintResult]:
    if region.kind not in _ADMINISTRATIVE_REGION_KINDS or any(
        isinstance(tag, RegionTag.OpenStreetMap) for tag in region.tags
    ):
        return
    attempted: list[
        tuple[
            str,
            list[nominatim.GeocodeResult],
            dict[tuple[str | None, int | None, str], nominatim.GeocodeResult],
        ]
    ] = []
    selected = None
    for query in get_nominatim_queries(region):
        results = nominatim.search_geocodejson(query, limit=10)
        candidates = {
            (result.osm_type, result.osm_id, result.category): result
            for result in results
            if _is_matching_result(region, result)
        }
        attempted.append((query, results, candidates))
        query_selection = _select_inference_candidate(region, candidates)
        if query_selection is not None and (
            selected is None
            or _is_preferred_inference_result(region, query_selection[1])
        ):
            selected = query_selection
        if selected is not None and _is_preferred_inference_result(region, selected[1]):
            break
    if (
        selected is None or not _is_preferred_inference_result(region, selected[1])
    ) and (structured_query := _get_structured_nominatim_query(region)) is not None:
        results = nominatim.search_geocodejson_structured(**structured_query, limit=10)
        candidates = {
            (result.osm_type, result.osm_id, result.category): result
            for result in results
            if _is_matching_result(region, result)
        }
        query = f"structured {structured_query!r}"
        attempted.append((query, results, candidates))
        structured_selection = _select_inference_candidate(region, candidates)
        if structured_selection is not None and (
            selected is None
            or _is_preferred_inference_result(region, structured_selection[1])
        ):
            selected = structured_selection
    if selected is None or not _is_preferred_inference_result(region, selected[1]):
        query, results, candidates = attempted[-1]
        candidate_labels = [
            (
                result.osm_type,
                result.osm_id,
                result.display_name,
                result.address_type,
                _result_admin_level(result),
            )
            for result in candidates.values()
        ]
        if candidate_labels:
            yield (
                f"has multiple possible OpenStreetMap administrative boundaries "
                f"for Nominatim query {query!r}: "
                f"{candidate_labels!r}"
            )
        else:
            yield (
                f"has no exact OpenStreetMap administrative boundary match among "
                f"{len(results)} Nominatim results for query "
                f"{query!r}"
            )
        return
    (osm_type, osm_id, category), result = selected
    assert osm_type is not None and osm_id is not None
    tag = RegionTag.OpenStreetMap(osm_type, osm_id, category)
    yield add_tag_issue(
        f"inferred {tag} from Nominatim result {result.display_name!r}", region, tag
    )
