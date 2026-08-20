from io import StringIO
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.apis import nominatim
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

    return cast(
        Region,
        SimpleNamespace(
            id=region_id,
            name=name,
            kind=kind,
            parent=parent,
            tags=tags,
            all_parents=all_parents,
        ),
    )


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


def test_openstreetmap_tag_round_trip() -> None:
    tag = RegionTag.OpenStreetMap("relation", 113722, "boundary")

    assert RegionTag.unserialize(tag.serialize()) == tag


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


def test_inference_rejects_stale_search_identity() -> None:
    indonesia = _region("Indonesia", RegionKind.country)
    north_sumatra = _region("North Sumatra", RegionKind.province, indonesia)
    stale = nominatim.GeocodeResult(
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

    assert not region_lint._is_matching_result(north_sumatra, stale)


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

    assert list(
        region_lint.validate_openstreetmap.linter(
            region,
            LintConfig(
                autofix=False,
                interactive=False,
                available_resources=frozenset({LintResource.NETWORK}),
            ),
        )
    ) == ["OpenStreetMap relation 999 is not returned by Nominatim lookup"]


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
