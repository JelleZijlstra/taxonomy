from io import StringIO
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy import coordinates
from taxonomy.apis import nominatim, openstreetmap
from taxonomy.db import coordinate_lint
from taxonomy.db.constants import RegionKind
from taxonomy.db.models import region_lint
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.region import Region, RegionTag


def _region(
    name: str,
    kind: RegionKind,
    parent: Region | None = None,
    *,
    region_id: int = 1,
    tags: tuple[RegionTag, ...] = (),
) -> Region:
    def all_parents() -> tuple[Region, ...]:
        if parent is None:
            return ()
        return (parent, *parent.all_parents())

    region = SimpleNamespace(
        id=region_id,
        name=name,
        kind=kind,
        parent=parent,
        tags=tags,
        all_parents=all_parents,
    )
    region.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    region.add_tag = lambda tag: setattr(region, "tags", (*region.tags, tag))
    return cast(Region, region)


def test_display_type_localities_uses_concise_recursive_display() -> None:
    display = Mock()
    region = cast(Region, SimpleNamespace(display=display))
    output = StringIO()

    Region.display_type_localities(region, depth=4, file=output)

    display.assert_called_once_with(
        full=False, depth=4, file=output, children=True, locations=True
    )


def test_concise_region_display_recurses_through_subregions() -> None:
    child = Mock()
    region = cast(
        Region,
        SimpleNamespace(
            comment=None,
            is_empty=Mock(return_value=False),
            sorted_locations=Mock(return_value=[]),
            sorted_children=Mock(return_value=[child]),
        ),
    )
    output = StringIO()

    Region.display(region, full=False, children=True, locations=True, file=output)

    child.display.assert_called_once_with(
        full=False, depth=4, file=output, children=True, skip_empty=True, locations=True
    )


def test_display_type_localities_is_adt_callback() -> None:
    region = object.__new__(Region)

    callbacks = region.get_adt_callbacks()

    assert callbacks["display_type_localities"] == region.display_type_localities


def test_region_has_tag() -> None:
    region = cast(
        Region,
        SimpleNamespace(
            tags=(RegionTag.IncompletelyDivided, RegionTag.MustHavePreciseTypeLocality)
        ),
    )

    assert Region.has_tag(region, RegionTag.IncompletelyDivided)
    assert Region.has_tag(region, RegionTag.MustHavePreciseTypeLocality)


def test_region_redirect_and_deleted_are_invalid() -> None:
    target = _region("Target", RegionKind.country, region_id=2)
    redirect = _region("Old name", RegionKind.redirect, target, region_id=3)
    deleted = _region("Removed", RegionKind.deleted, region_id=4)

    assert Region.get_redirect_target(redirect) is target
    assert Region.is_invalid(redirect)
    assert Region.should_skip(redirect)
    assert Region.get_redirect_target(deleted) is None
    assert Region.is_invalid(deleted)
    assert Region.should_skip(deleted)
    assert (
        list(
            Region.lint_invalid(redirect, LintConfig(autofix=False, interactive=False))
        )
        == []
    )


def test_region_merge_reassigns_references_and_creates_redirect() -> None:
    reference = SimpleNamespace(id=10, region=None)
    field = SimpleNamespace(attribute_name="region")
    target = cast(
        Region,
        SimpleNamespace(
            id=2,
            name="Target",
            kind=RegionKind.country,
            is_invalid=Mock(return_value=False),
            has_parent=Mock(return_value=False),
        ),
    )
    source = cast(
        Region,
        SimpleNamespace(
            id=1,
            name="Source",
            kind=RegionKind.country,
            parent=None,
            tags=(RegionTag.IncompletelyDivided,),
            is_invalid=Mock(return_value=False),
            get_direct_backrefs=Mock(return_value=iter(((field, reference),))),
        ),
    )

    Region.merge(source, target)

    assert reference.region is target
    assert source.parent is target
    assert source.kind is RegionKind.redirect
    assert source.tags == ()
    source.get_direct_backrefs.assert_called_once_with(include_invalid=True)  # type: ignore[attr-defined]


def test_region_merge_rejects_descendant_target() -> None:
    target = cast(
        Region,
        SimpleNamespace(
            is_invalid=Mock(return_value=False), has_parent=Mock(return_value=True)
        ),
    )
    source = cast(Region, SimpleNamespace(is_invalid=Mock(return_value=False)))

    with pytest.raises(ValueError, match="descendants"):
        Region.merge(source, target)


def test_region_remove_requires_no_references() -> None:
    referenced = cast(
        Region,
        SimpleNamespace(
            kind=RegionKind.country,
            tags=(),
            is_invalid=Mock(return_value=False),
            get_direct_backrefs=Mock(
                return_value=[
                    (SimpleNamespace(attribute_name="region"), SimpleNamespace(id=10))
                ]
            ),
        ),
    )
    with pytest.raises(ValueError, match="valid references"):
        Region.remove(referenced)

    unreferenced = cast(
        Region,
        SimpleNamespace(
            kind=RegionKind.country,
            tags=(RegionTag.IncompletelyDivided,),
            is_invalid=Mock(return_value=False),
            get_direct_backrefs=Mock(return_value=[]),
        ),
    )
    Region.remove(unreferenced)
    assert unreferenced.kind is RegionKind.deleted
    assert unreferenced.tags == (RegionTag.IncompletelyDivided,)


def test_openstreetmap_tag_round_trip() -> None:
    tag = RegionTag.OpenStreetMap("relation", 113722, "boundary")

    assert RegionTag.unserialize(tag.serialize()) == tag


def test_region_ignore_lint_tag_round_trip() -> None:
    tag = RegionTag.IgnoreLint(
        "openstreetmap_parent_containment", comment="Reviewed disputed boundary."
    )

    assert RegionTag.unserialize(tag.serialize()) == tag


def test_region_lint_uses_and_adds_ignore_tags() -> None:
    existing = RegionTag.IgnoreLint("existing", comment="Reviewed.")
    region = _region("Example", RegionKind.country, tags=(existing,))

    assert list(region_lint.get_ignores(region)) == [existing]
    region_lint.add_ignore(region, "new", "Also reviewed.")
    assert region.tags[-1] == RegionTag.IgnoreLint("new", comment="Also reviewed.")


def test_openstreetmap_parent_containment_reports_outside_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = _region("Parent", RegionKind.state, region_id=1)
    child = _region("Child", RegionKind.county, parent, region_id=2)
    outer = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]}
    )
    outside = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[3, 1], [5, 1], [5, 2], [3, 2], [3, 1]]]}
    )
    child_boundary = coordinate_lint.RegionBoundary(
        child,
        nominatim.BoundaryResult(
            "Child",
            "Child, Parent",
            "boundary",
            "administrative",
            "relation",
            2,
            {},
            outside,
        ),
    )
    parent_boundary = coordinate_lint.RegionBoundary(
        parent,
        nominatim.BoundaryResult(
            "Parent", "Parent", "boundary", "administrative", "relation", 1, {}, outer
        ),
    )
    monkeypatch.setattr(
        coordinate_lint, "get_direct_region_boundary", Mock(return_value=child_boundary)
    )
    monkeypatch.setattr(
        coordinate_lint, "get_region_boundary", Mock(return_value=parent_boundary)
    )

    assert list(
        region_lint.check_openstreetmap_parent_containment.linter(child, LintConfig())
    ) == [
        "OpenStreetMap polygon extends more than 0.5 km outside linked ancestor Region 'Parent'"
    ]


def test_openstreetmap_parent_containment_accepts_contained_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = _region("Parent", RegionKind.state, region_id=1)
    child = _region("Child", RegionKind.county, parent, region_id=2)
    outer = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]}
    )
    inside = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]]}
    )
    monkeypatch.setattr(
        coordinate_lint,
        "get_direct_region_boundary",
        Mock(
            return_value=coordinate_lint.RegionBoundary(
                child,
                nominatim.BoundaryResult(
                    "Child",
                    "Child, Parent",
                    "boundary",
                    "administrative",
                    "relation",
                    2,
                    {},
                    inside,
                ),
            )
        ),
    )
    monkeypatch.setattr(
        coordinate_lint,
        "get_region_boundary",
        Mock(
            return_value=coordinate_lint.RegionBoundary(
                parent,
                nominatim.BoundaryResult(
                    "Parent",
                    "Parent",
                    "boundary",
                    "administrative",
                    "relation",
                    1,
                    {},
                    outer,
                ),
            )
        ),
    )

    assert (
        list(
            region_lint.check_openstreetmap_parent_containment.linter(
                child, LintConfig()
            )
        )
        == []
    )


def test_redirect_names_are_region_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    united_states = _region("United States", RegionKind.country, region_id=2)
    rhode_island = _region(
        "Rhode Island", RegionKind.state, united_states, region_id=465
    )
    washington = _region(
        "Washington County, Rhode Island",
        RegionKind.county,
        rhode_island,
        region_id=3550,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_redirect_alias_names_by_target_id",
        lambda: {3550: frozenset({"South County, Rhode Island"})},
    )
    monkeypatch.setattr(region_lint, "_OSM_NAME_ALIASES", {})

    assert region_lint.get_openstreetmap_name_aliases(washington) == {
        "South County",
        "South County, Rhode Island",
    }


def test_infer_openstreetmap_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    country = _region("Ecuador", RegionKind.country)
    province = _region("Pichincha Province", RegionKind.province, country)
    province_result = nominatim.GeocodeResult(
        name="Pichincha",
        display_name="Pichincha, Ecuador",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Pichincha", "country": "Ecuador"},
        administrative={"level4": "Pichincha"},
        osm_type="relation",
        osm_id=113722,
    )
    same_name_canton = nominatim.GeocodeResult(
        name="Pichincha",
        display_name="Pichincha, Manabí, Ecuador",
        category="boundary",
        feature_type="administrative",
        address_type="county",
        address={"county": "Pichincha", "state": "Manabí", "country": "Ecuador"},
        administrative={"level6": "Pichincha", "level4": "Manabí"},
        osm_type="relation",
        osm_id=112901,
    )
    search = Mock(return_value=[province_result, same_name_canton])
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            province, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    issue = cast(LintIssue, issues[0])
    assert "Pichincha, Ecuador" in str(issue)
    assert issue.fix is not None
    assert issue.fix.apply()
    assert province.tags == (RegionTag.OpenStreetMap("relation", 113722, "boundary"),)
    search.assert_called_once_with("Pichincha, Ecuador", limit=10)


def test_infer_openstreetmap_rejects_wrong_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _region("Ecuador", RegionKind.country)
    province = _region("Pichincha Province", RegionKind.province, country)
    result = nominatim.GeocodeResult(
        name="Pichincha",
        display_name="Pichincha, Colombia",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Pichincha", "country": "Colombia"},
        administrative={"level4": "Pichincha"},
        osm_type="relation",
        osm_id=1234,
    )
    monkeypatch.setattr(nominatim, "search_geocodejson", Mock(return_value=[result]))
    monkeypatch.setattr(
        nominatim, "search_geocodejson_structured", Mock(return_value=[result])
    )

    expected = (
        "has no exact OpenStreetMap administrative boundary match among 1 "
        "Nominatim results for query \"structured {'state': 'Pichincha Province', "
        "'country': 'Ecuador'}\""
    )
    assert list(
        region_lint.infer_openstreetmap.linter(
            province, LintConfig(autofix=False, interactive=False)
        )
    ) == [expected]


def test_infer_openstreetmap_uses_structured_search_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    brazil = _region("Brazil", RegionKind.country)
    amazonas = _region("Amazonas", RegionKind.state, brazil)
    result = nominatim.GeocodeResult(
        name="Amazonas",
        display_name="Amazonas, North Region, Brazil",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Amazonas", "country": "Brazil"},
        administrative={"level4": "Amazonas", "level3": "North Region"},
        osm_type="relation",
        osm_id=332476,
    )
    monkeypatch.setattr(nominatim, "search_geocodejson", Mock(return_value=[]))
    structured = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search_geocodejson_structured", structured)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            amazonas, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert cast(LintIssue, issues[0]).fix is not None
    structured.assert_called_once_with(state="Amazonas", country="Brazil", limit=10)


def test_inference_accepts_restored_north_sumatra_relation() -> None:
    indonesia = _region("Indonesia", RegionKind.country)
    north_sumatra = _region("North Sumatra", RegionKind.province, indonesia)
    result = nominatim.GeocodeResult(
        name="North Sumatra",
        display_name="North Sumatra, Indonesia",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "North Sumatra", "country": "Indonesia"},
        administrative={"level4": "North Sumatra"},
        osm_type="relation",
        osm_id=2390843,
    )

    assert region_lint._is_matching_result(north_sumatra, result)


def test_infer_openstreetmap_skips_nonadministrative_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    island = _region("Galápagos Islands", RegionKind.island)
    search = Mock()
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    assert (
        list(
            region_lint.infer_openstreetmap.linter(
                island, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )
    search.assert_not_called()


def test_country_search_alias_does_not_conflate_overseas_region() -> None:
    guadeloupe = _region("Guadeloupe", RegionKind.country)

    assert region_lint.get_nominatim_query(guadeloupe) == "Guadeloupe"
    assert region_lint._region_name_aliases(guadeloupe) == {"Guadeloupe"}
    assert region_lint._address_name_aliases(guadeloupe) == {"Guadeloupe", "France"}


def test_country_inference_does_not_require_sovereign_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    netherlands = _region("Netherlands", RegionKind.country)
    bonaire = _region("Bonaire", RegionKind.country, netherlands)
    result = nominatim.GeocodeResult(
        name="Bonaire",
        display_name="Bonaire, Netherlands",
        category="boundary",
        feature_type="administrative",
        address_type="city",
        address={"city": "Bonaire", "country": "Netherlands"},
        administrative={"level8": "Bonaire"},
        osm_type="relation",
        osm_id=2324450,
    )
    search = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            bonaire, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert cast(LintIssue, issues[0]).fix is not None
    search.assert_called_once_with("Bonaire", limit=10)


def test_inference_tries_explicit_osm_name_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    russia = _region("Russia", RegionKind.country)
    adygea = _region("Adygea", RegionKind.subnational, russia)
    result = nominatim.GeocodeResult(
        name="Republic of Adygea",
        display_name="Republic of Adygea, Russia",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Republic of Adygea", "country": "Russia"},
        administrative={"level4": "Republic of Adygea"},
        osm_type="relation",
        osm_id=253256,
    )
    search = Mock(side_effect=[[], [result]])
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            adygea, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert cast(LintIssue, issues[0]).fix is not None
    assert search.call_args_list == [
        (("Adygea, Russia",), {"limit": 10}),
        (("Republic of Adygea, Russia",), {"limit": 10}),
    ]


def test_infer_openstreetmap_prefers_shallow_overseas_relation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    guadeloupe = _region("Guadeloupe", RegionKind.country)
    level3 = nominatim.GeocodeResult(
        name="Guadeloupe",
        display_name="Guadeloupe, France",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"country": "France"},
        administrative={"level3": "Guadeloupe"},
        osm_type="relation",
        osm_id=1401835,
    )
    level4 = nominatim.GeocodeResult(
        name="Guadeloupe",
        display_name="Guadeloupe, France",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Guadeloupe", "country": "France"},
        administrative={"level4": "Guadeloupe", "level3": "Guadeloupe"},
        osm_type="relation",
        osm_id=2562137,
    )
    monkeypatch.setattr(
        nominatim, "search_geocodejson", Mock(return_value=[level4, level3])
    )

    issues = list(
        region_lint.infer_openstreetmap.linter(
            guadeloupe, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    issue = cast(LintIssue, issues[0])
    assert "relation', 1401835" in str(issue)
    assert issue.fix is not None


def test_infer_openstreetmap_skips_unlinked_convenience_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    japan = _region("Japan", RegionKind.country)
    chubu = _region("Chūbu Region", RegionKind.region, japan)
    aichi = _region("Aichi Prefecture", RegionKind.prefecture, chubu)
    result = nominatim.GeocodeResult(
        name="Aichi",
        display_name="Aichi Prefecture, Japan",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "Aichi Prefecture", "country": "Japan"},
        administrative={"level4": "Aichi Prefecture"},
        osm_type="relation",
        osm_id=358674,
    )
    search = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            aichi, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert cast(LintIssue, issues[0]).fix is not None
    search.assert_called_once_with("Aichi, Japan", limit=10)


def test_county_query_removes_unqualified_parent_name() -> None:
    united_states = _region("United States", RegionKind.country)
    georgia = _region(
        "Georgia (United States)",
        RegionKind.state,
        united_states,
        tags=(RegionTag.OpenStreetMap("relation", 161957, "boundary"),),
    )
    county = _region("Appling County, Georgia", RegionKind.county, georgia)

    assert region_lint.get_nominatim_query(county) == "Appling, Georgia, United States"


def test_county_structured_query_includes_state() -> None:
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region("Middlesex County, Connecticut", RegionKind.county, connecticut)

    assert region_lint._get_structured_nominatim_query(county) == {
        "county": "Middlesex County",
        "country": "United States",
        "state": "Connecticut",
    }


def test_infer_openstreetmap_prefers_full_county_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    united_states = _region("United States", RegionKind.country)
    texas = _region("Texas", RegionKind.state, united_states)
    county = _region("Bandera County, Texas", RegionKind.county, texas)
    county_result = nominatim.GeocodeResult(
        name="Bandera County",
        display_name="Bandera County, Texas, United States",
        category="boundary",
        feature_type="administrative",
        address_type="county",
        address={"state": "Texas", "country": "United States"},
        administrative={"level6": "Bandera County", "level4": "Texas"},
        osm_type="relation",
        osm_id=1651195,
    )
    city_result = nominatim.GeocodeResult(
        name="Bandera",
        display_name="Bandera, Texas, United States",
        category="boundary",
        feature_type="administrative",
        address_type="county",
        address={"state": "Texas", "country": "United States"},
        administrative={"level6": "Bandera", "level4": "Texas"},
        osm_type="way",
        osm_id=33168790,
    )
    monkeypatch.setattr(
        nominatim, "search_geocodejson", Mock(return_value=[city_result, county_result])
    )

    issues = list(
        region_lint.infer_openstreetmap.linter(
            county, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert "relation', 1651195" in str(issues[0])


def test_infer_openstreetmap_continues_past_same_named_city(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    united_states = _region("United States", RegionKind.country)
    missouri = _region("Missouri", RegionKind.state, united_states)
    county = _region("St. Louis, Missouri", RegionKind.county, missouri)
    city_result = nominatim.GeocodeResult(
        name="Saint Louis",
        display_name="Saint Louis, Missouri, United States",
        category="boundary",
        feature_type="administrative",
        address_type="city",
        address={"state": "Missouri", "country": "United States"},
        administrative={"level6": "Saint Louis", "level4": "Missouri"},
        osm_type="relation",
        osm_id=1180533,
    )
    county_result = nominatim.GeocodeResult(
        name="St. Louis County",
        display_name="St. Louis County, Missouri, United States",
        category="boundary",
        feature_type="administrative",
        address_type="county",
        address={"state": "Missouri", "country": "United States"},
        administrative={"level6": "St. Louis County", "level4": "Missouri"},
        osm_type="relation",
        osm_id=1806791,
    )
    search = Mock(side_effect=[[city_result], [county_result]])
    monkeypatch.setattr(nominatim, "search_geocodejson", search)

    issues = list(
        region_lint.infer_openstreetmap.linter(
            county, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert "relation', 1806791" in str(issues[0])
    assert search.call_count == 2


def test_infer_openstreetmap_rejects_same_named_connecticut_city(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region("Hartford County, Connecticut", RegionKind.county, connecticut)
    city = nominatim.GeocodeResult(
        name="Hartford",
        display_name="Hartford, Connecticut, United States",
        category="boundary",
        feature_type="administrative",
        address_type="city",
        address={
            "city": "Hartford",
            "state": "Connecticut",
            "country": "United States",
        },
        administrative={"level8": "Hartford", "level4": "Connecticut"},
        osm_type="relation",
        osm_id=3909995,
    )
    monkeypatch.setattr(nominatim, "search_geocodejson", Mock(return_value=[city]))
    structured = Mock(return_value=[])
    monkeypatch.setattr(nominatim, "search_geocodejson_structured", structured)

    messages = list(
        region_lint.infer_openstreetmap.linter(
            county, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(messages) == 1
    assert "has no exact OpenStreetMap administrative boundary match" in str(
        messages[0]
    )
    structured.assert_called_once_with(
        county="Hartford County", country="United States", state="Connecticut", limit=10
    )


def test_infer_openstreetmap_accepts_historic_connecticut_county(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region("Middlesex County, Connecticut", RegionKind.county, connecticut)
    historic_county = nominatim.GeocodeResult(
        name="Middlesex County",
        display_name=(
            "Middlesex County, Lower Connecticut River Valley Planning Region, "
            "Connecticut, United States"
        ),
        category="boundary",
        feature_type="historic",
        address_type="locality",
        address={"state": "Connecticut", "country": "United States"},
        administrative={"level4": "Connecticut"},
        osm_type="relation",
        osm_id=2554043,
    )
    monkeypatch.setattr(
        nominatim, "search_geocodejson", Mock(return_value=[historic_county])
    )

    issues = list(
        region_lint.infer_openstreetmap.linter(
            county, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == 1
    assert "relation', 2554043" in str(issues[0])


def test_validate_openstreetmap_accepts_matching_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _region(
        "Ecuador",
        RegionKind.country,
        region_id=51,
        tags=(RegionTag.OpenStreetMap("relation", 108089, "boundary"),),
    )
    province = _region(
        "Pichincha Province",
        RegionKind.province,
        country,
        region_id=4551,
        tags=(RegionTag.OpenStreetMap("relation", 113722, "boundary"),),
    )
    result = nominatim.SearchResult(
        latitude="-0.1",
        longitude="-78.5",
        name="Pichincha",
        display_name="Pichincha, Ecuador",
        category="boundary",
        feature_type="administrative",
        address={"state": "Pichincha", "country": "Ecuador"},
        osm_type="relation",
        osm_id=113722,
        names={"name:en": "Pichincha"},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 113722): (province,)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {("relation", 113722): result},
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                province,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )


def test_validate_openstreetmap_reports_wrong_name_and_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 2202162, "boundary")
    france = _region("France", RegionKind.country, region_id=60, tags=(tag,))
    guadeloupe = _region("Guadeloupe", RegionKind.country, region_id=234, tags=(tag,))
    result = nominatim.SearchResult(
        latitude="46",
        longitude="2",
        name="France",
        display_name="France",
        category="boundary",
        feature_type="administrative",
        address={"country": "France"},
        osm_type="relation",
        osm_id=2202162,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 2202162): (france, guadeloupe)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {("relation", 2202162): result},
    )

    messages = list(
        region_lint.validate_openstreetmap.linter(
            guadeloupe,
            LintConfig(
                autofix=False,
                interactive=False,
                available_resources=frozenset({LintResource.NETWORK}),
            ),
        )
    )

    assert len(messages) == 2
    assert "also linked to [(60, 'France')]" in str(messages[0])
    assert "is named 'France'" in str(messages[1])


def test_validate_openstreetmap_rejects_city_linked_as_us_county(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 3909995, "boundary")
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region(
        "Hartford County, Connecticut",
        RegionKind.county,
        connecticut,
        region_id=3539,
        tags=(tag,),
    )
    result = nominatim.SearchResult(
        latitude="41.76",
        longitude="-72.68",
        name="Hartford",
        display_name="Hartford, Connecticut, United States",
        category="boundary",
        feature_type="administrative",
        address_type="city",
        address={
            "city": "Hartford",
            "state": "Connecticut",
            "country": "United States",
        },
        osm_type="relation",
        osm_id=3909995,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 3909995): (county,)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {("relation", 3909995): result},
    )

    messages = list(
        region_lint.validate_openstreetmap.linter(
            county,
            LintConfig(
                autofix=False,
                interactive=False,
                available_resources=frozenset({LintResource.NETWORK}),
            ),
        )
    )

    assert any(
        "address type 'city', expected one of ['county', 'historic', 'locality']"
        in str(message)
        for message in messages
    )


def test_validate_openstreetmap_accepts_historic_us_county(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 2554043, "boundary")
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region(
        "Middlesex County, Connecticut",
        RegionKind.county,
        connecticut,
        region_id=3541,
        tags=(tag,),
    )
    result = nominatim.SearchResult(
        latitude="41.45",
        longitude="-72.52",
        name="Middlesex County",
        display_name=(
            "Middlesex County, Lower Connecticut River Valley Planning Region, "
            "Connecticut, United States"
        ),
        category="boundary",
        feature_type="historic",
        address_type="historic",
        address={"state": "Connecticut", "country": "United States"},
        osm_type="relation",
        osm_id=2554043,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 2554043): (county,)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {("relation", 2554043): result},
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                county,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )


def test_validate_openstreetmap_accepts_ceremonial_english_county(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    england_tag = RegionTag.OpenStreetMap("relation", 58447, "boundary")
    shropshire_tag = RegionTag.OpenStreetMap("relation", 57511, "boundary")
    united_kingdom = _region("United Kingdom", RegionKind.country, region_id=340)
    england = _region(
        "England",
        RegionKind.subnational,
        united_kingdom,
        region_id=341,
        tags=(england_tag,),
    )
    shropshire = _region(
        "Shropshire", RegionKind.county, england, region_id=4209, tags=(shropshire_tag,)
    )
    england_result = nominatim.SearchResult(
        latitude="52.79",
        longitude="-1.47",
        name="England",
        display_name="England, United Kingdom",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"state": "England", "country": "United Kingdom"},
        osm_type="relation",
        osm_id=58447,
    )
    shropshire_result = nominatim.SearchResult(
        latitude="52.65",
        longitude="-2.73",
        name="Shropshire (Ceremonial)",
        display_name="Shropshire (Ceremonial), England, United Kingdom",
        category="boundary",
        feature_type="ceremonial",
        address_type="ceremonial",
        address={
            "ceremonial": "Shropshire",
            "state": "England",
            "country": "United Kingdom",
        },
        osm_type="relation",
        osm_id=57511,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 58447): (england,), ("relation", 57511): (shropshire,)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {
            ("relation", 58447): england_result,
            ("relation", 57511): shropshire_result,
        },
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                shropshire,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )
    assert "Shropshire (Ceremonial)" in region_lint.get_region_name_aliases(shropshire)


def test_english_county_address_types_include_lieutenancy_variants() -> None:
    united_kingdom = _region("United Kingdom", RegionKind.country)
    england = _region("England", RegionKind.subnational, united_kingdom)
    county = _region("Greater Manchester", RegionKind.county, england)

    assert region_lint._expected_address_types(county) == frozenset(
        {"ceremonial", "city", "county", "district", "state_district"}
    )


def test_validate_openstreetmap_accepts_region_address_type_for_hokkaido(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 3792634, "boundary")
    japan = _region("Japan", RegionKind.country, region_id=84)
    hokkaido = _region(
        "Hokkaido", RegionKind.prefecture, japan, region_id=4354, tags=(tag,)
    )
    result = nominatim.SearchResult(
        latitude="43.45",
        longitude="142.82",
        name="Hokkaido",
        display_name="Hokkaido, Japan",
        category="boundary",
        feature_type="administrative",
        address_type="region",
        address={"region": "Hokkaido", "country": "Japan"},
        osm_type="relation",
        osm_id=3792634,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 3792634): (hokkaido,)},
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_lookup_results",
        lambda: {("relation", 3792634): result},
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                hokkaido,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )


def test_prefecture_address_types_exclude_city() -> None:
    japan = _region("Japan", RegionKind.country, region_id=84)
    prefecture = _region("Aichi Prefecture", RegionKind.prefecture, japan)

    assert region_lint._expected_address_types(prefecture) == frozenset(
        {"province", "region", "state"}
    )


@pytest.mark.parametrize(
    ("kind", "address_type"),
    [
        (RegionKind.country, "archipelago"),
        (RegionKind.country, "disputed"),
        (RegionKind.department, "state_district"),
        (RegionKind.province, "province"),
        (RegionKind.province, "state_district"),
        (RegionKind.region, "archipelago"),
        (RegionKind.region, "region"),
        (RegionKind.subnational, "region"),
        (RegionKind.territory, "territory"),
    ],
)
def test_expected_address_types_include_nominatim_linked_place_variants(
    kind: RegionKind, address_type: str
) -> None:
    assert address_type in region_lint._expected_address_types(_region("Example", kind))


@pytest.mark.parametrize("address_type", ["city", "suburb", "town"])
def test_us_county_accepts_linked_place_only_at_admin_level_six(
    address_type: str,
) -> None:
    united_states = _region("United States", RegionKind.country)
    state = _region("Example State", RegionKind.state, united_states)
    county = _region("Example County", RegionKind.county, state)

    def result(admin_level: str) -> nominatim.SearchResult:
        return nominatim.SearchResult(
            latitude="1",
            longitude="2",
            name="Example",
            display_name="Example, Example State, United States",
            category="boundary",
            feature_type="administrative",
            address_type=address_type,
            address={"state": "Example State", "country": "United States"},
            osm_type="relation",
            osm_id=1,
            extra={"admin_level": admin_level},
        )

    assert region_lint._has_acceptable_address_type(county, result("6"))
    assert not region_lint._has_acceptable_address_type(county, result("8"))


@pytest.mark.parametrize(
    ("kind", "accepted", "rejected"),
    [
        (RegionKind.county, "6", "8"),
        (RegionKind.prefecture, "4", "8"),
        (RegionKind.province, "6", "8"),
        (RegionKind.region, "5", "8"),
        (RegionKind.state, "4", "8"),
        (RegionKind.subnational, "7", "8"),
        (RegionKind.territory, "4", "8"),
    ],
)
def test_administrative_region_kind_rejects_municipality_admin_level(
    kind: RegionKind, accepted: str, rejected: str
) -> None:
    region = _region("Example", kind)

    def result(admin_level: str) -> nominatim.SearchResult:
        return nominatim.SearchResult(
            latitude="1",
            longitude="2",
            name="Example",
            display_name="Example",
            category="boundary",
            feature_type="administrative",
            address_type="state",
            address={},
            osm_type="relation",
            osm_id=1,
            extra={"admin_level": admin_level},
        )

    assert region_lint._has_acceptable_admin_level(region, result(accepted))
    assert not region_lint._has_acceptable_admin_level(region, result(rejected))


def test_historic_county_may_omit_admin_level() -> None:
    county = _region("Example County", RegionKind.county)
    result = nominatim.SearchResult(
        latitude="1",
        longitude="2",
        name="Example County",
        display_name="Example County",
        category="boundary",
        feature_type="historic",
        address_type="historic",
        address={},
        osm_type="relation",
        osm_id=1,
    )

    assert region_lint._has_acceptable_admin_level(county, result)


def test_validate_openstreetmap_reports_missing_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 999, "boundary")
    region = _region("Missing", RegionKind.state, region_id=99, tags=(tag,))
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 999): (region,)},
    )
    monkeypatch.setattr(region_lint, "_get_openstreetmap_lookup_results", dict)
    monkeypatch.setattr(
        region_lint, "_stored_reference_is_searchable", lambda _region, _tag: False
    )
    monkeypatch.setattr(
        openstreetmap, "lookup_element", lambda _osm_type, _osm_id: None
    )

    assert list(
        region_lint.validate_openstreetmap.linter(
            region,
            LintConfig(
                autofix=False,
                interactive=False,
                available_resources=frozenset({LintResource.NETWORK}),
            ),
        )
    ) == [
        (
            "OpenStreetMap relation 999 is not returned by Nominatim and does not "
            "exist in the OpenStreetMap API"
        )
    ]


def test_validate_openstreetmap_accepts_direct_osm_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 2554044, "boundary")
    united_states = _region("United States", RegionKind.country)
    connecticut = _region("Connecticut", RegionKind.state, united_states)
    county = _region(
        "New London County, Connecticut",
        RegionKind.county,
        connecticut,
        region_id=3543,
        tags=(tag,),
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 2554044): (county,)},
    )
    monkeypatch.setattr(region_lint, "_get_openstreetmap_lookup_results", dict)
    monkeypatch.setattr(
        region_lint, "_stored_reference_is_searchable", lambda _region, _tag: False
    )
    monkeypatch.setattr(
        openstreetmap,
        "lookup_element",
        lambda _osm_type, _osm_id: openstreetmap.Element(
            osm_type="relation",
            osm_id=2554044,
            tags={"boundary": "historic", "name": "New London County"},
        ),
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                county,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )


def test_validate_openstreetmap_runs_duplicate_check_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 123, "boundary")
    first = _region("First", RegionKind.state, region_id=1, tags=(tag,))
    second = _region("Second", RegionKind.state, region_id=2, tags=(tag,))
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 123): (first, second)},
    )
    lookup = Mock()
    monkeypatch.setattr(region_lint, "_get_openstreetmap_lookup_results", lookup)

    messages = list(
        region_lint.validate_openstreetmap.linter(
            first,
            LintConfig(
                autofix=False, interactive=False, available_resources=frozenset()
            ),
        )
    )

    assert messages == ["OpenStreetMap relation 123 is also linked to [(2, 'Second')]"]
    lookup.assert_not_called()


def test_validate_openstreetmap_accepts_exact_search_when_lookup_lags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tag = RegionTag.OpenStreetMap("relation", 2390843, "boundary")
    indonesia = _region("Indonesia", RegionKind.country)
    sumatra = _region("Sumatra", RegionKind.other, indonesia)
    north_sumatra = _region(
        "North Sumatra", RegionKind.province, sumatra, region_id=4508, tags=(tag,)
    )
    result = nominatim.GeocodeResult(
        name="North Sumatra",
        display_name="North Sumatra, Indonesia",
        category="boundary",
        feature_type="administrative",
        address_type="state",
        address={"country": "Indonesia"},
        administrative={"level4": "North Sumatra"},
        osm_type="relation",
        osm_id=2390843,
    )
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 2390843): (north_sumatra,)},
    )
    monkeypatch.setattr(region_lint, "_get_openstreetmap_lookup_results", dict)
    monkeypatch.setattr(nominatim, "search_geocodejson", Mock(return_value=[result]))

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                north_sumatra,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )


def test_validate_openstreetmap_accepts_parent_iso_for_disputed_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    argentina_tag = RegionTag.OpenStreetMap("relation", 286393, "boundary")
    province_tag = RegionTag.OpenStreetMap("relation", 153550, "boundary")
    argentina = _region(
        "Argentina", RegionKind.country, region_id=7, tags=(argentina_tag,)
    )
    province = _region(
        "Tierra del Fuego",
        RegionKind.province,
        argentina,
        region_id=366,
        tags=(province_tag,),
    )
    argentina_result = nominatim.SearchResult(
        latitude="-34",
        longitude="-64",
        name="Argentina",
        display_name="Argentina",
        category="boundary",
        feature_type="administrative",
        address={"country": "Argentina", "country_code": "ar"},
        osm_type="relation",
        osm_id=286393,
        extra={"ISO3166-1:alpha2": "AR"},
    )
    province_result = nominatim.SearchResult(
        latitude="-54",
        longitude="-67",
        name="Tierra del Fuego",
        display_name="Tierra del Fuego, Falkland Islands",
        category="boundary",
        feature_type="administrative",
        address={
            "state": "Tierra del Fuego",
            "ISO3166-2-lvl4": "AR-V",
            "country": "Falkland Islands",
            "country_code": "fk",
        },
        osm_type="relation",
        osm_id=153550,
    )
    results = {
        ("relation", 286393): argentina_result,
        ("relation", 153550): province_result,
    }
    monkeypatch.setattr(
        region_lint,
        "_get_openstreetmap_tag_owners",
        lambda: {("relation", 153550): (province,)},
    )
    monkeypatch.setattr(
        region_lint, "_get_openstreetmap_lookup_results", lambda: results
    )

    assert (
        list(
            region_lint.validate_openstreetmap.linter(
                province,
                LintConfig(
                    autofix=False,
                    interactive=False,
                    available_resources=frozenset({LintResource.NETWORK}),
                ),
            )
        )
        == []
    )
