from dataclasses import dataclass

import pytest

from taxonomy.applicator import generic as recommendations
from taxonomy.db.models import BaseModel, Location
from taxonomy.db.models.tags import LocationTag


@dataclass
class FakeLocation:
    id: int = 2300
    name: str = "Borchers Fauna"
    latitude: str | None = "37°10'N"
    tags: tuple[LocationTag, ...] = ()


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


def test_set_field_plan_and_apply(capsys: pytest.CaptureFixture[str]) -> None:
    data = _common(recommendations.SET_FIELD, "latitude")
    data.update({"old_value": "37°10'N", "new_value": "37.1°N-37.2°N"})
    row = recommendations.parse_recommendation(data, 1)
    location = FakeLocation()

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
    location = FakeLocation()

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


def test_multiple_tag_additions_compose() -> None:
    first = LocationTag.PLSS("T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0")
    second = LocationTag.CoordinatesFromPLSS("KS060330S0280W0")
    rows: list[recommendations.Recommendation] = []
    for tag in (first, second):
        data = _common(recommendations.ADD_TAG, "tags")
        data["tag"] = tag.serialize()
        rows.append(recommendations.parse_recommendation(data, len(rows) + 1))
    location = FakeLocation()

    plan = recommendations.build_plan(
        rows,
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    recommendations.execute_plan(plan, apply=True)

    assert location.tags == tuple(sorted((first, second)))


def test_tag_mutations_canonicalize_unordered_adt_fields() -> None:
    later = LocationTag.IgnoreLintLocation("nominatim_coordinates")
    earlier = LocationTag.IgnoreLintLocation("coordinate_collision")
    data = _common(recommendations.ADD_TAG, "tags")
    data["tag"] = earlier.serialize()
    location = FakeLocation(tags=(later,))

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
            get_object=lambda _model, _id: FakeLocation(),
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
    location = FakeLocation()

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
    location = FakeLocation(name="Renamed Borchers Fauna")

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
