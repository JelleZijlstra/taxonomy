from collections.abc import Mapping

import pytest

from taxonomy.applicator import generic as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import AltitudeUnit
from taxonomy.db.models import BaseModel, Location
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
from taxonomy.db.models.tags import LocationTag


def _make_location(
    *,
    name: str = "Borchers Fauna",
    latitude: str | None = "37°10'N",
    tags: tuple[LocationTag, ...] = (),
) -> Location:
    return Location.virtual(name=name, latitude=latitude, tags=tags)


def _common(action: str, field: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": action,
        "confidence": "high",
        "reason": "Reviewed evidence supports the change.",
        "evidence": [{"kind": "source", "text": "Exact source evidence."}],
        "object": {"model": "Location", "id": 2300, "label": "Borchers Fauna"},
        "field": field,
    }


def _create_location_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": recommendations.CREATE_OBJECT,
        "confidence": "high",
        "reason": "Create a reviewed precise locality.",
        "evidence": [{"kind": "source", "text": "Exact source evidence."}],
        "object": {"model": "Location", "ref": "precise_site", "label": "Precise site"},
        "values": {"name": "Precise site"},
    }


def test_create_object_and_reference_it_from_later_action() -> None:
    child = _make_location(name="Broad site", latitude=None)
    set_parent = _common(recommendations.SET_FIELD, "parent")
    set_parent.update(
        {
            "object": {"model": "Location", "id": 2300, "label": "Broad site"},
            "old_value": None,
            "new_value": {
                "model": "Location",
                "ref": "precise_site",
                "label": "Precise site",
            },
        }
    )
    rows = [
        recommendations.parse_recommendation(_create_location_row(), 1),
        recommendations.parse_recommendation(set_parent, 2),
    ]
    created: list[Location] = []
    plan = recommendations.build_plan(
        rows,
        model_registry={"Location": Location},
        get_object=lambda _model, _id: child,
        find_objects_by_label=lambda _model, _label: [],
    )

    def create_object(
        model: type[BaseModel], values: Mapping[str, object]
    ) -> BaseModel:
        obj = model.virtual(**values)
        created.append(obj)
        return obj

    recommendations.execute_plan(plan, apply=True, create_object=create_object)

    assert len(created) == 1
    assert created[0].name == "Precise site"
    assert child.parent is created[0]


def test_later_action_can_edit_created_object_by_ref() -> None:
    edit = _common(recommendations.SET_FIELD, "comment")
    edit.update(
        {
            "object": {
                "model": "Location",
                "ref": "precise_site",
                "label": "Precise site",
            },
            "old_value": None,
            "new_value": "Reviewed after creation.",
        }
    )
    plan = recommendations.build_plan(
        [
            recommendations.parse_recommendation(_create_location_row(), 1),
            recommendations.parse_recommendation(edit, 2),
        ],
        model_registry={"Location": Location},
        find_objects_by_label=lambda _model, _label: [],
    )
    created: list[BaseModel] = []

    def create_object(
        model: type[BaseModel], values: Mapping[str, object]
    ) -> BaseModel:
        obj = model.virtual(**values)
        created.append(obj)
        return obj

    recommendations.execute_plan(plan, apply=True, create_object=create_object)

    assert len(created) == 1
    assert created[0].comment == "Reviewed after creation."


def test_create_object_participates_in_virtual_lint_proposal_graph() -> None:
    child = _make_location(name="Broad site", latitude=None)
    set_parent = _common(recommendations.SET_FIELD, "parent")
    set_parent.update(
        {
            "object": {"model": "Location", "id": 2300, "label": "Broad site"},
            "old_value": None,
            "new_value": {
                "model": "Location",
                "ref": "precise_site",
                "label": "Precise site",
            },
        }
    )
    plan = recommendations.build_plan(
        [
            recommendations.parse_recommendation(_create_location_row(), 1),
            recommendations.parse_recommendation(set_parent, 2),
        ],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: child,
        find_objects_by_label=lambda _model, _label: [],
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposals = builder.build()
    new_site = next(p.model for p in proposals if p.model.name == "Precise site")
    broad_site = next(p.model for p in proposals if p.model.name == "Broad site")
    assert broad_site.parent is new_site


def test_set_field_plan_and_apply(capsys: pytest.CaptureFixture[str]) -> None:
    data = _common(recommendations.SET_FIELD, "latitude")
    data.update({"old_value": "37°10'N", "new_value": "37.1°N-37.2°N"})
    row = recommendations.parse_recommendation(data, 1)
    location = _make_location()

    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(plan, apply=True)

    assert location.latitude == "37.1°N-37.2°N"
    assert "SET_FIELD object=Location:2300" in capsys.readouterr().out


def test_add_and_remove_tag() -> None:
    tag = LocationTag.PLSS("T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0")
    add_data = _common(recommendations.ADD_TAG, "tags")
    add_data["tag"] = tag.serialize()
    remove_data = _common(recommendations.REMOVE_TAG, "tags")
    remove_data["tag"] = tag.serialize()
    location = _make_location()

    add_plan = recommendations.build_plan(
        [recommendations.parse_recommendation(add_data, 1)],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(add_plan, apply=True)
    assert location.tags == (tag,)

    remove_plan = recommendations.build_plan(
        [recommendations.parse_recommendation(remove_data, 1)],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(remove_plan, apply=True)
    assert not location.tags


def test_review_renders_adt_constructor_instead_of_numeric_tag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    tag = LocationTag.IgnoreLintLocation(
        "coordinate_provenance", comment="Reviewed evidence."
    )
    data = _common(recommendations.ADD_TAG, "tags")
    data["tag"] = tag.serialize()
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "IgnoreLintLocation(" in output
    assert "label='coordinate_provenance'" in output
    assert f"tags: [{tag._tag}," not in output


def test_review_renders_enum_names_in_adt_tags(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _common(recommendations.ADD_TAG, "tags")
    data["object"] = {
        "model": "OccurrenceRecord",
        "id": 2300,
        "label": "Reviewed occurrence",
    }
    data["tag"] = OccurrenceRecordTag.Elevation("1200", AltitudeUnit.m).serialize()
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "Elevation(elevation='1200', unit=AltitudeUnit.m)" in output


def test_multiple_tag_additions_compose() -> None:
    first = LocationTag.PLSS("T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0")
    second = LocationTag.CoordinatesFromPLSS("KS060330S0280W0")
    rows: list[recommendations.Recommendation] = []
    for tag in (first, second):
        data = _common(recommendations.ADD_TAG, "tags")
        data["tag"] = tag.serialize()
        rows.append(recommendations.parse_recommendation(data, len(rows) + 1))
    location = _make_location()

    plan = recommendations.build_plan(
        rows,
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(plan, apply=True)

    assert location.tags == tuple(sorted((first, second)))


def test_plan_can_be_applied_to_virtual_copy_without_mutating_original() -> None:
    data = _common(recommendations.SET_FIELD, "latitude")
    data.update({"old_value": "37°10'N", "new_value": "37.1°N-37.2°N"})
    row = recommendations.parse_recommendation(data, 1)
    location = _make_location()
    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    (proposed,) = builder.build()
    assert isinstance(proposed.model, Location)
    assert proposed.model.latitude == "37.1°N-37.2°N"
    assert location.latitude == "37°10'N"
    assert proposed.contexts == ("generic manifest line 1",)


def test_tag_mutations_canonicalize_unordered_adt_fields() -> None:
    later = LocationTag.IgnoreLintLocation("nominatim_coordinates")
    earlier = LocationTag.IgnoreLintLocation("coordinate_collision")
    data = _common(recommendations.ADD_TAG, "tags")
    data["tag"] = earlier.serialize()
    location = _make_location(tags=(later,))

    plan = recommendations.build_plan(
        [recommendations.parse_recommendation(data, 1)],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(plan, apply=True)

    assert location.tags == tuple(sorted((earlier, later)))


def test_rejects_stale_field() -> None:
    data = _common(recommendations.SET_FIELD, "latitude")
    data.update({"old_value": "unexpected", "new_value": "37.1°N-37.2°N"})
    row = recommendations.parse_recommendation(data, 1)

    with pytest.raises(recommendations.RecommendationError, match="neither"):
        recommendations.build_plan(
            [row],
            model_registry={"Location": Location},
            get_object=lambda _model, _id: _make_location(),
        )


def test_reference_values_are_snapshotted() -> None:
    data = _common(recommendations.SET_FIELD, "region")
    data.update(
        {
            "old_value": {"model": "Region", "id": 1, "label": "Old"},
            "new_value": {"model": "Region", "id": 2, "label": "New"},
        }
    )
    row = recommendations.parse_recommendation(data, 1)

    assert row.old_value == {"model": "Region", "id": 1, "label": "Old"}


def test_model_registry_contains_exported_models() -> None:
    registry = recommendations.get_model_registry()

    assert registry["Location"] is Location
    assert all(issubclass(model, BaseModel) for model in registry.values())


def test_manual_review_validates_object_but_never_mutates(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _common(recommendations.MANUAL_REVIEW, "unused")
    data.pop("field")
    data["reason"] = "The available evidence does not support one coordinate."
    row = recommendations.parse_recommendation(data, 1)
    location = _make_location()

    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(plan, apply=True)

    assert location.latitude == "37°10'N"
    output = capsys.readouterr().out
    assert "MANUAL_REVIEW object=Location:2300" in output
    assert "0 generic update(s)" in output
    assert "1 manual review(s)" in output


def test_edit_only_manual_review_can_resolve_object_after_label_change() -> None:
    data = _common(recommendations.MANUAL_REVIEW, "unused")
    data.pop("field")
    row = recommendations.parse_recommendation(data, 1)
    location = _make_location(name="Renamed Borchers Fauna")

    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
        allow_manual_label_changes=True,
    )

    assert plan.actions[0].object is location


def test_manual_review_prints_full_evidence(capsys: pytest.CaptureFixture[str]) -> None:
    data = _common(recommendations.MANUAL_REVIEW, "unused")
    data.pop("field")
    data["reason"] = "A complete unresolved reason."
    data["evidence"] = [{"kind": "NameComment", "text": "Complete comment text."}]
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_full_manual_reviews([row])

    output = capsys.readouterr().out
    assert "MANUAL_REVIEW object='Location 2300 Borchers Fauna'" in output
    assert "A complete unresolved reason." in output
    assert "Complete comment text." in output
