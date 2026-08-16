"""Lint steps for Regions."""

from collections.abc import Iterable

from taxonomy.apis import nominatim
from taxonomy.db import helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.base import LintConfig
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


def get_ignores(region: Region) -> Iterable[IgnoreLint]:
    return ()


LINT = Lint(Region, get_ignores)


def _normalize_name(name: str) -> str:
    return helpers.simplify_string(name, clean_words=False).replace("-", "")


def _unqualified_name(region: Region) -> str:
    name = region.name
    if region.parent is not None:
        parent_suffix = f", {region.parent.name}"
        if name.endswith(parent_suffix):
            name = name.removesuffix(parent_suffix)
        parenthetical_suffix = f" ({region.parent.name})"
        if name.endswith(parenthetical_suffix):
            name = name.removesuffix(parenthetical_suffix)
    return name


def _region_name_aliases(region: Region) -> set[str]:
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
    if region.kind is RegionKind.country:
        aliases.add(nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name))
    return aliases


def _query_name(region: Region) -> str:
    if region.kind is RegionKind.country:
        return nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(region.name, region.name)
    aliases = _region_name_aliases(region)
    return min(aliases, key=lambda name: (len(name), name))


def get_nominatim_query(region: Region) -> str:
    components = [_query_name(region)]
    seen = {_normalize_name(components[0])}
    for parent in region.all_parents():
        if parent.kind not in _ADMINISTRATIVE_REGION_KINDS:
            continue
        name = _query_name(parent)
        normalized = _normalize_name(name)
        if normalized not in seen:
            components.append(name)
            seen.add(normalized)
    return ", ".join(components)


def _expected_address_type(region: Region) -> str | None:
    if region.kind is RegionKind.country:
        return "country"
    if region.parent is None:
        return None
    if region.parent.kind is RegionKind.country:
        return "state"
    return "county"


def _address_names(result: nominatim.GeocodeResult) -> set[str]:
    return {
        _normalize_name(value)
        for value in (*result.address.values(), *result.administrative.values())
    }


def _is_matching_result(region: Region, result: nominatim.GeocodeResult) -> bool:
    if (
        result.osm_type is None
        or result.osm_id is None
        or result.category != "boundary"
        or result.feature_type != "administrative"
        or result.address_type != _expected_address_type(region)
    ):
        return False
    expected_names = {_normalize_name(name) for name in _region_name_aliases(region)}
    if _normalize_name(result.name) not in expected_names:
        return False
    address_names = _address_names(result)
    return all(
        not address_names.isdisjoint(
            {_normalize_name(name) for name in _region_name_aliases(parent)}
        )
        for parent in region.all_parents()
        if parent.kind in _ADMINISTRATIVE_REGION_KINDS
    )


@LINT.add("infer_openstreetmap", requires_network=True)
def infer_openstreetmap(region: Region, cfg: LintConfig) -> Iterable[LintResult]:
    if region.kind not in _ADMINISTRATIVE_REGION_KINDS or any(
        isinstance(tag, RegionTag.OpenStreetMap) for tag in region.tags
    ):
        return
    results = nominatim.search_geocodejson(get_nominatim_query(region), limit=10)
    candidates = {
        (result.osm_type, result.osm_id, result.category): result
        for result in results
        if _is_matching_result(region, result)
    }
    if len(candidates) != 1:
        return
    (osm_type, osm_id, category), result = next(iter(candidates.items()))
    assert osm_type is not None and osm_id is not None
    tag = RegionTag.OpenStreetMap(osm_type, osm_id, category)
    yield add_tag_issue(
        f"inferred {tag} from Nominatim result {result.display_name!r}", region, tag
    )
