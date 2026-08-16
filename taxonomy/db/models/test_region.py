from io import StringIO
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from taxonomy.apis import nominatim
from taxonomy.db.constants import RegionKind
from taxonomy.db.models import region_lint
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.region import Region, RegionTag


def _region(
    name: str,
    kind: RegionKind,
    parent: Region | None = None,
    *,
    tags: tuple[RegionTag, ...] = (),
) -> Region:
    def all_parents() -> tuple[Region, ...]:
        if parent is None:
            return ()
        return (parent, *parent.all_parents())

    return cast(
        Region,
        SimpleNamespace(
            name=name, kind=kind, parent=parent, tags=tags, all_parents=all_parents
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

    assert (
        list(
            region_lint.infer_openstreetmap.linter(
                province, LintConfig(autofix=False, interactive=False)
            )
        )
        == []
    )


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
