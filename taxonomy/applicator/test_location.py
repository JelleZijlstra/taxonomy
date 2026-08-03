import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pytest

from taxonomy.applicator import location as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import RegionKind
from taxonomy.db.models import Location, Region
from taxonomy.db.models.location import LocationStatus
from taxonomy.db.models.tags import LocationTag


@dataclass
class FakeNamed:
    id: int
    name: str


@dataclass
class FakeLocation:
    id: int
    name: str
    region: FakeNamed = field(default_factory=lambda: FakeNamed(10, "Example Region"))
    min_period: FakeNamed | None = field(
        default_factory=lambda: FakeNamed(171, "Recent")
    )
    max_period: FakeNamed | None = field(
        default_factory=lambda: FakeNamed(171, "Recent")
    )
    stratigraphic_unit: FakeNamed | None = None
    min_age: int | None = None
    max_age: int | None = None
    source: object | None = None
    deleted: LocationStatus = LocationStatus.valid
    parent: FakeLocation | None = None
    latitude: str | None = None
    longitude: str | None = None
    comment: str | None = None
    location_detail: str | None = None
    age_detail: str | None = None
    tags: tuple[object, ...] = ()
    merge_calls: list[int] = field(default_factory=list)

    def is_invalid(self) -> bool:
        return self.deleted is not LocationStatus.valid

    def merge(self, other: recommendations.LocationLike) -> None:
        self.merge_calls.append(other.id)
        self.deleted = LocationStatus.alias
        self.parent = other  # type: ignore[assignment]

    def add_tag(self, tag: object) -> None:
        self.tags = (*self.tags, tag)


def as_location(location: FakeLocation) -> recommendations.LocationLike:
    return cast(recommendations.LocationLike, location)


def location_spec(location_id: int, name: str) -> dict[str, object]:
    return {
        "location_id": location_id,
        "location_name": name,
        "region_id": 10,
        "region_name": "Example Region",
        "min_period_id": 171,
        "min_period_name": "Recent",
        "max_period_id": 171,
        "max_period_name": "Recent",
        "stratigraphic_unit_id": None,
        "stratigraphic_unit_name": None,
    }


def rename_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "rename_location",
        "confidence": "high",
        "reason": "Expand a generic geographic abbreviation.",
        "evidence": [{"kind": "naming_convention", "text": "Use Mount, not Mt."}],
        "location": location_spec(1, "Mt. Example"),
        "new_name": "Mount Example",
    }


def merge_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "merge_location",
        "confidence": "high",
        "reason": "The two spellings denote the same place.",
        "evidence": [{"kind": "source", "text": "The source uses both spellings."}],
        "source": location_spec(2, "Example-Cave"),
        "target": location_spec(3, "Example Cave"),
    }


def edit_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "edit_location",
        "confidence": "high",
        "reason": "Add reviewed coordinate bounds and their source tag.",
        "evidence": [{"kind": "PLSS", "text": "Resolved section polygon."}],
        "location": location_spec(1, "Example Quarry"),
        "changes": [
            {"field": "latitude", "old_value": None, "new_value": "1°N-2°N"},
            {"field": "longitude", "old_value": None, "new_value": "3°W-2°W"},
        ],
        "add_tags": [
            LocationTag.PLSS(
                "T1N R1W Sec. 1, 6th Meridian", "CO060010N0010W0"
            ).serialize()
        ],
    }


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))


def test_read_recommendations(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    write_rows(path, [rename_row(), merge_row()])

    rows = recommendations.read_recommendations(path)

    assert [row.action for row in rows] == [
        recommendations.RENAME_LOCATION,
        recommendations.MERGE_LOCATION,
    ]
    assert rows[0].new_name == "Mount Example"
    assert rows[1].source is not None
    assert rows[1].source.location_id == 2


def test_read_edit_recommendation(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    write_rows(path, [edit_row()])

    rows = recommendations.read_recommendations(path)

    assert rows[0].action == recommendations.EDIT_LOCATION
    assert rows[0].changes == (
        recommendations.FieldChange("latitude", None, "1°N-2°N"),
        recommendations.FieldChange("longitude", None, "3°W-2°W"),
    )
    assert rows[0].add_tags == (
        LocationTag.PLSS("T1N R1W Sec. 1, 6th Meridian", "CO060010N0010W0"),
    )


def test_reads_location_review_note(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = edit_row()
    row["review_note"] = "Review this caveat in full."
    write_rows(path, [row])

    parsed = recommendations.read_recommendations(path)

    assert parsed[0].review_note == "Review this caveat in full."


def test_rejects_location_used_by_multiple_recommendations(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    second = rename_row()
    second["new_name"] = "Mount Other"
    write_rows(path, [rename_row(), second])

    with pytest.raises(
        recommendations.RecommendationError,
        match="mutated by more than one recommendation",
    ):
        recommendations.read_recommendations(path)


def test_allows_multiple_merges_into_same_target(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    second = merge_row()
    second_source = location_spec(4, "Another Example-Cave")
    second["source"] = second_source
    write_rows(path, [merge_row(), second])

    rows = recommendations.read_recommendations(path)

    assert len(rows) == 2
    assert rows[0].target == rows[1].target


def test_build_plan_rejects_stale_name() -> None:
    row = recommendations.parse_recommendation(rename_row(), 1)
    location = FakeLocation(1, "Unexpected")

    with pytest.raises(recommendations.RecommendationError, match="name changed"):
        recommendations.build_plan(
            [row],
            get_location=lambda _: as_location(location),
            find_location_by_name=lambda _: None,
        )


def test_build_plan_rejects_rename_collision() -> None:
    row = recommendations.parse_recommendation(rename_row(), 1)
    location = FakeLocation(1, "Mt. Example")
    collision = FakeLocation(9, "Mount Example")

    with pytest.raises(
        recommendations.RecommendationError, match="already used by Location 9"
    ):
        recommendations.build_plan(
            [row],
            get_location=lambda _: as_location(location),
            find_location_by_name=lambda _: as_location(collision),
        )


def test_build_plan_rejects_two_renames_to_same_final_name() -> None:
    first_data = rename_row()
    second_data = rename_row()
    second_data["location"] = location_spec(2, "Mt. Other")
    rows = [
        recommendations.parse_recommendation(first_data, 1),
        recommendations.parse_recommendation(second_data, 2),
    ]
    locations = {
        1: as_location(FakeLocation(1, "Mt. Example")),
        2: as_location(FakeLocation(2, "Mt. Other")),
    }

    with pytest.raises(
        recommendations.RecommendationError,
        match=r"Locations 1 and 2 would both be named 'Mount Example'",
    ):
        recommendations.build_plan(
            rows,
            get_location=locations.__getitem__,
            find_location_by_name=lambda _: None,
        )


def test_build_plan_orders_rename_that_vacates_target_name_first() -> None:
    first_data = rename_row()
    first_data["new_name"] = "Mt. Other"
    second_data = rename_row()
    second_data["location"] = location_spec(2, "Mt. Other")
    second_data["new_name"] = "Mount Other"
    rows = [
        recommendations.parse_recommendation(first_data, 1),
        recommendations.parse_recommendation(second_data, 2),
    ]
    locations = {
        1: as_location(FakeLocation(1, "Mt. Example")),
        2: as_location(FakeLocation(2, "Mt. Other")),
    }

    def find_by_name(name: str) -> recommendations.LocationLike | None:
        return next((item for item in locations.values() if item.name == name), None)

    plan = recommendations.build_plan(
        rows, get_location=locations.__getitem__, find_location_by_name=find_by_name
    )

    assert [
        cast(recommendations.PlannedRename, action).location.id
        for action in plan.actions
    ] == [2, 1]


def test_build_plan_rejects_merge_context_mismatch() -> None:
    row = recommendations.parse_recommendation(merge_row(), 1)
    source = FakeLocation(2, "Example-Cave")
    target = FakeLocation(3, "Example Cave", region=FakeNamed(11, "Different Region"))
    locations = {2: as_location(source), 3: as_location(target)}

    with pytest.raises(recommendations.RecommendationError, match="Region changed"):
        recommendations.build_plan(
            [row],
            get_location=locations.__getitem__,
            find_location_by_name=lambda _: None,
        )


def test_build_plan_rejects_stale_edited_field() -> None:
    row = recommendations.parse_recommendation(edit_row(), 1)
    location = FakeLocation(1, "Example Quarry", latitude="unexpected")

    with pytest.raises(recommendations.RecommendationError, match="field 'latitude'"):
        recommendations.build_plan(
            [row],
            get_location=lambda _: as_location(location),
            find_location_by_name=lambda _: None,
        )


def test_build_plan_rejects_stale_unchanged_coordinate_snapshot() -> None:
    data = edit_row()
    data["changes"] = []
    location_data = cast(dict[str, object], data["location"])
    location_data["latitude"] = "1°N"
    location_data["longitude"] = "2°E"
    row = recommendations.parse_recommendation(data, 1)
    location = FakeLocation(1, "Example Quarry", latitude="unexpected", longitude="2°E")

    with pytest.raises(recommendations.RecommendationError, match="latitude changed"):
        recommendations.build_plan(
            [row],
            get_location=lambda _: as_location(location),
            find_location_by_name=lambda _: None,
        )


def test_old_location_snapshot_without_coordinates_remains_supported() -> None:
    data = edit_row()
    data["changes"] = []
    row = recommendations.parse_recommendation(data, 1)
    location = FakeLocation(1, "Example Quarry", latitude="later", longitude="change")

    plan = recommendations.build_plan(
        [row],
        get_location=lambda _: as_location(location),
        find_location_by_name=lambda _: None,
    )

    assert len(plan.actions) == 1


def test_build_plan_allows_missing_source_stratigraphic_context() -> None:
    data = merge_row()
    target_spec = data["target"]
    assert isinstance(target_spec, dict)
    target_spec["stratigraphic_unit_id"] = 12
    target_spec["stratigraphic_unit_name"] = "Example Formation"
    row = recommendations.parse_recommendation(data, 1)
    source = FakeLocation(2, "Example-Cave")
    target = FakeLocation(
        3, "Example Cave", stratigraphic_unit=FakeNamed(12, "Example Formation")
    )
    locations = {2: as_location(source), 3: as_location(target)}

    plan = recommendations.build_plan(
        [row], get_location=locations.__getitem__, find_location_by_name=lambda _: None
    )

    assert len(plan.actions) == 1


def test_build_plan_allows_explicit_temporal_context_conflict() -> None:
    data = merge_row()
    source_spec = data["source"]
    assert isinstance(source_spec, dict)
    source_spec["min_period_id"] = 172
    source_spec["min_period_name"] = "Older period"
    source_spec["max_period_id"] = 172
    source_spec["max_period_name"] = "Older period"
    data["allow_temporal_context_conflicts"] = True
    row = recommendations.parse_recommendation(data, 1)
    source = FakeLocation(
        2,
        "Example-Cave",
        min_period=FakeNamed(172, "Older period"),
        max_period=FakeNamed(172, "Older period"),
    )
    target = FakeLocation(3, "Example Cave")
    locations = {2: as_location(source), 3: as_location(target)}

    plan = recommendations.build_plan(
        [row], get_location=locations.__getitem__, find_location_by_name=lambda _: None
    )

    assert row.allow_temporal_context_conflicts
    assert len(plan.actions) == 1


def test_temporal_context_override_does_not_allow_region_conflict() -> None:
    data = merge_row()
    target_spec = data["target"]
    assert isinstance(target_spec, dict)
    target_spec["region_id"] = 11
    target_spec["region_name"] = "Different Region"
    data["allow_temporal_context_conflicts"] = True
    row = recommendations.parse_recommendation(data, 1)
    source = FakeLocation(2, "Example-Cave")
    target = FakeLocation(3, "Example Cave", region=FakeNamed(11, "Different Region"))
    locations = {2: as_location(source), 3: as_location(target)}

    with pytest.raises(recommendations.RecommendationError, match="same Region"):
        recommendations.build_plan(
            [row],
            get_location=locations.__getitem__,
            find_location_by_name=lambda _: None,
        )


def test_dry_run_does_not_mutate(capsys: pytest.CaptureFixture[str]) -> None:
    rows = [
        recommendations.parse_recommendation(rename_row(), 1),
        recommendations.parse_recommendation(merge_row(), 2),
    ]
    rename_location = FakeLocation(1, "Mt. Example")
    source = FakeLocation(
        2,
        "Example-Cave",
        latitude="1°N",
        longitude="2°E",
        location_detail="Source evidence.",
    )
    target = FakeLocation(3, "Example Cave")
    locations = {
        1: as_location(rename_location),
        2: as_location(source),
        3: as_location(target),
    }
    plan = recommendations.build_plan(
        rows, get_location=locations.__getitem__, find_location_by_name=lambda _: None
    )

    recommendations.execute_plan(plan, apply=False)

    assert rename_location.name == "Mt. Example"
    assert source.deleted is LocationStatus.valid
    output = capsys.readouterr().out
    assert "WOULD_RENAME_LOCATION location_id=1" in output
    assert "WOULD_MERGE_LOCATION source_id=2" in output
    assert "SOURCE_METADATA_WILL_BE_MERGED_WHERE_COMPATIBLE" in output
    assert "No database changes made" in output


def test_apply_is_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    rows = [
        recommendations.parse_recommendation(rename_row(), 1),
        recommendations.parse_recommendation(merge_row(), 2),
    ]
    rename_location = FakeLocation(1, "Mt. Example")
    source = FakeLocation(2, "Example-Cave")
    target = FakeLocation(3, "Example Cave")
    locations = {
        1: as_location(rename_location),
        2: as_location(source),
        3: as_location(target),
    }

    def find_by_name(name: str) -> recommendations.LocationLike | None:
        return next((item for item in locations.values() if item.name == name), None)

    plan = recommendations.build_plan(
        rows, get_location=locations.__getitem__, find_location_by_name=find_by_name
    )
    recommendations.execute_plan(plan, apply=True, clear_caches=lambda: None)

    assert rename_location.name == "Mount Example"
    assert source.deleted is LocationStatus.alias
    assert source.parent is target
    assert source.merge_calls == [3]

    second_plan = recommendations.build_plan(
        rows, get_location=locations.__getitem__, find_location_by_name=find_by_name
    )
    assert all(action.already_applied for action in second_plan.actions)
    recommendations.execute_plan(second_plan, apply=False, clear_caches=lambda: None)
    assert capsys.readouterr().out.count("SKIP_ALREADY_APPLIED") == 2


def test_apply_edit_is_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    row = recommendations.parse_recommendation(edit_row(), 1)
    location = FakeLocation(1, "Example Quarry")

    plan = recommendations.build_plan(
        [row],
        get_location=lambda _: as_location(location),
        find_location_by_name=lambda _: None,
    )
    recommendations.execute_plan(plan, apply=True, clear_caches=lambda: None)

    assert location.latitude == "1°N-2°N"
    assert location.longitude == "3°W-2°W"
    assert location.tags == (
        LocationTag.PLSS("T1N R1W Sec. 1, 6th Meridian", "CO060010N0010W0"),
    )

    second_plan = recommendations.build_plan(
        [row],
        get_location=lambda _: as_location(location),
        find_location_by_name=lambda _: None,
    )
    assert second_plan.actions[0].already_applied


def test_apply_edit_replaces_tag() -> None:
    old_tag = LocationTag.PLSS("T3S R8W Sec. 1, 25", "MS250030S0080W0")
    new_tag = LocationTag.PLSS(
        "T3S R8W Sec. 1, St. Stephens Meridian", "MS250030S0080W0"
    )
    data = edit_row()
    data["changes"] = []
    data["add_tags"] = [new_tag.serialize()]
    data["remove_tags"] = [old_tag.serialize()]
    row = recommendations.parse_recommendation(data, 1)
    location = FakeLocation(1, "Example Quarry", tags=(old_tag,))

    plan = recommendations.build_plan(
        [row],
        get_location=lambda _: as_location(location),
        find_location_by_name=lambda _: None,
    )
    recommendations.execute_plan(plan, apply=True, clear_caches=lambda: None)

    assert location.tags == (new_tag,)


def test_apply_edit_sorts_location_tags() -> None:
    later = LocationTag.IgnoreLintLocation("nominatim_coordinates")
    earlier = LocationTag.IgnoreLintLocation("coordinate_collision")
    data = edit_row()
    data["changes"] = []
    data["add_tags"] = [earlier.serialize()]
    data["remove_tags"] = []
    row = recommendations.parse_recommendation(data, 1)
    location = FakeLocation(1, "Example Quarry", tags=(later,))

    plan = recommendations.build_plan(
        [row],
        get_location=lambda _: as_location(location),
        find_location_by_name=lambda _: None,
    )
    recommendations.execute_plan(plan, apply=True, clear_caches=lambda: None)

    assert location.tags == tuple(sorted((earlier, later)))


def test_virtual_merge_proposal_combines_models_without_database_backrefs() -> None:
    region = Region.virtual(name="Example Region", kind=RegionKind.country, tags=())
    source = Location.virtual(
        name="Example-Cave",
        region=region,
        latitude="1°N",
        longitude="2°E",
        location_detail="Source evidence.",
        age_detail="",
        tags=(),
        deleted=LocationStatus.valid,
    )
    target = Location.virtual(
        name="Example Cave",
        region=region,
        location_detail="",
        age_detail="",
        tags=(),
        deleted=LocationStatus.valid,
    )
    row = recommendations.parse_recommendation(merge_row(), 1)
    plan = recommendations.RecommendationPlan(
        (
            recommendations.PlannedMerge(
                row,
                cast(recommendations.LocationLike, source),
                cast(recommendations.LocationLike, target),
                already_applied=False,
            ),
        ),
        Counter({recommendations.MERGE_LOCATION: 1}),
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposals = builder.build()
    assert len(proposals) == 2
    proposed_source = proposals[0].model
    proposed_target = proposals[1].model
    assert isinstance(proposed_source, Location)
    assert isinstance(proposed_target, Location)
    assert proposed_source.deleted is LocationStatus.alias
    assert proposed_source.parent is proposed_target
    assert (proposed_target.latitude, proposed_target.longitude) == ("1°N", "2°E")
    assert proposed_target.location_detail == "Source evidence."
    assert source.deleted is LocationStatus.valid
    assert target.latitude is None
