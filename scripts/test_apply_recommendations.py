import json
import sys
from collections import Counter
from pathlib import Path

import pytest

from scripts import apply_recommendations
from taxonomy.applicator import generic as generic_recommendations
from taxonomy.applicator import location as location_recommendations
from taxonomy.applicator import type_locality as type_recommendations


def _location_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "edit_location",
        "confidence": "high",
        "reason": "Add a reviewed comment.",
        "evidence": [{"kind": "source", "text": "Reviewed source."}],
        "location": {
            "location_id": 1,
            "location_name": "Example",
            "region_id": 2,
            "region_name": "Region",
            "min_period_id": None,
            "min_period_name": None,
            "max_period_id": None,
            "max_period_name": None,
            "stratigraphic_unit_id": None,
            "stratigraphic_unit_name": None,
        },
        "changes": [{"field": "comment", "old_value": None, "new_value": "Reviewed."}],
    }


def _type_row() -> dict[str, object]:
    return {
        "schema_version": 2,
        "name_id": 3,
        "name": "Example name",
        "current_location_id": 4,
        "current_location_name": "Broad place",
        "current_location_tags": [],
        "action": "manual_review",
        "confidence": "medium",
        "reason": "The source is ambiguous.",
        "tag_comment": None,
        "target": None,
        "evidence": [{"source_id": 5, "source_name": "Source", "text": "Place"}],
    }


def _generic_manual_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "manual_review",
        "confidence": "low",
        "reason": "The coordinate history remains unresolved.",
        "evidence": [
            {"kind": "NameComment", "text": "The earlier evidence is ambiguous."}
        ],
        "object": {"model": "Location", "id": 7, "label": "Unresolved place"},
    }


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_reads_mixed_recommendation_file(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])

    recommendations = apply_recommendations.read_recommendations(path)

    assert recommendations.count == 2
    assert not recommendations.generic_rows
    assert isinstance(
        recommendations.location_rows[0], location_recommendations.Recommendation
    )
    assert isinstance(
        recommendations.type_locality_rows[0], type_recommendations.Recommendation
    )


def test_rejects_unknown_action(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [{"schema_version": 1, "action": "unknown"}])

    with pytest.raises(apply_recommendations.RecommendationError, match="unsupported"):
        apply_recommendations.read_recommendations(path)


def test_review_prints_both_families(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])

    apply_recommendations.print_review(apply_recommendations.read_recommendations(path))

    output = capsys.readouterr().out
    assert "LOCATION RECOMMENDATIONS" in output
    assert "TYPE-LOCALITY RECOMMENDATIONS" in output


def test_review_filters_actions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])

    apply_recommendations.print_review(
        apply_recommendations.read_recommendations(path), actions={"manual_review"}
    )

    output = capsys.readouterr().out
    assert "LOCATION RECOMMENDATIONS" not in output
    assert "TYPE-LOCALITY RECOMMENDATIONS" in output


def test_review_reports_when_no_actions_match(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])

    apply_recommendations.print_review(
        apply_recommendations.read_recommendations(path), actions={"rename_location"}
    )

    assert (
        capsys.readouterr().out == "No recommendations match the requested actions.\n"
    )


def test_review_action_cli_filter(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--review",
            "--review-action",
            "manual_review",
        ],
    )

    apply_recommendations.main()

    output = capsys.readouterr().out
    assert "LOCATION RECOMMENDATIONS" not in output
    assert "TYPE-LOCALITY RECOMMENDATIONS" in output


def test_reads_generic_recommendation(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(
        path,
        [
            {
                "schema_version": 1,
                "action": "set_field",
                "confidence": "high",
                "reason": "Correct a reviewed value.",
                "evidence": [{"kind": "source", "text": "The source gives the value."}],
                "object": {"model": "Location", "id": 1, "label": "Example"},
                "field": "name",
                "old_value": "Example",
                "new_value": "Example locality",
            }
        ],
    )

    recommendations = apply_recommendations.read_recommendations(path)

    assert recommendations.count == 1
    assert len(recommendations.generic_rows) == 1
    assert not recommendations.location_rows
    assert not recommendations.type_locality_rows


def test_dispatches_generic_and_type_locality_manual_reviews(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _type_row()])

    recommendations = apply_recommendations.read_recommendations(path)

    assert len(recommendations.generic_rows) == 1
    assert len(recommendations.type_locality_rows) == 1


def test_manual_review_prints_complete_manual_and_actionable_notes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    manual = _type_row()
    manual["reason"] = "A complete reason that must not be shortened or omitted."
    manual["evidence"] = [
        {
            "source_id": 5,
            "source_name": "Source",
            "text": "Complete evidence text that must remain visible in full.",
        }
    ]
    actionable = _type_row()
    actionable.update(
        {
            "name_id": 6,
            "name": "Actionable name",
            "action": "create_location",
            "reason": "Create the precise site while retaining its broad period.",
            "review_note": (
                "The historical Pliocene assignment requires chronology review."
            ),
            "target": {
                "location_id": None,
                "location_name": "Precise site",
                "region_id": 7,
                "region_name": "Region",
                "latitude": None,
                "longitude": None,
                "coordinate_source": None,
                "coordinate_note": None,
                "location_tags": [],
            },
        }
    )
    location = _location_row()
    location["review_note"] = "The Location coordinate is only a regional proxy."
    _write(path, [_generic_manual_row(), location, manual, actionable])

    apply_recommendations.print_manual_reviews(
        apply_recommendations.read_recommendations(path)
    )

    output = capsys.readouterr().out
    assert "MANUAL_REVIEW object='Location 7 Unresolved place'" in output
    assert "The earlier evidence is ambiguous." in output
    assert "REVIEW_NOTE action=edit_location" in output
    assert "The Location coordinate is only a regional proxy." in output
    assert "MANUAL_REVIEW name_id=3" in output
    assert "A complete reason that must not be shortened or omitted." in output
    assert "Complete evidence text that must remain visible in full." in output
    assert "REVIEW_NOTE action=create_location name_id=6" in output
    assert "The historical Pliocene assignment requires chronology review." in output
    assert "Create the precise site while retaining its broad period." in output
    assert "LOCATION RECOMMENDATIONS" not in output
    assert "…" not in output


def test_review_manual_cli_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row(), _type_row()])
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--review-manual"]
    )

    apply_recommendations.main()

    output = capsys.readouterr().out
    assert "MANUAL_REVIEW name_id=3" in output
    assert "The source is ambiguous." in output


def test_manual_review_reports_when_manifest_has_none(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_location_row()])

    apply_recommendations.print_manual_reviews(
        apply_recommendations.read_recommendations(path)
    )

    assert (
        capsys.readouterr().out
        == "No manual_review recommendations or actionable review notes.\n"
    )


class _Editable:
    def __init__(self, label: str, events: list[str]) -> None:
        self.label = label
        self.events = events

    def edit(self) -> None:
        self.events.append(self.label)
        print(f"EDIT {self.label}")


def _generic_manual_plan(
    row: generic_recommendations.Recommendation, obj: _Editable
) -> generic_recommendations.RecommendationPlan:
    return generic_recommendations.RecommendationPlan(
        (
            generic_recommendations.PlannedAction(
                row, obj, None, None, already_applied=False
            ),
        ),
        Counter({generic_recommendations.MANUAL_REVIEW: 1}),
    )


def test_edit_manual_reviews_prints_note_then_edits_in_file_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_type_row(), _generic_manual_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    events: list[str] = []
    generic_obj = _Editable("generic", events)
    name_obj = _Editable("name", events)
    generic_plan = _generic_manual_plan(recommendations.generic_rows[0], generic_obj)

    apply_recommendations.edit_manual_reviews(
        recommendations,
        generic_plan,
        get_name=lambda name_id: name_obj,
        label_name=lambda name: "Example name",
    )

    output = capsys.readouterr().out
    assert events == ["name", "generic"]
    assert "/=" in output
    assert "\N{ESC}[34mEvidence\N{ESC}[0m" in output
    assert "\N{ESC}[33mReason\N{ESC}[0m" in output
    assert output.index("Place") < output.index("The source is ambiguous.")
    assert output.index("The source is ambiguous.") < output.index("EDIT name")
    assert output.index("The earlier evidence is ambiguous.") < output.index(
        "The coordinate history remains unresolved."
    )
    assert output.index("The coordinate history remains unresolved.") < output.index(
        "EDIT generic"
    )
    assert output.index("EDIT name") < output.index("EDIT generic")


def test_edit_manual_reviews_validates_all_objects_before_editing(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _type_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    events: list[str] = []
    generic_obj = _Editable("generic", events)
    name_obj = _Editable("name", events)
    generic_plan = _generic_manual_plan(recommendations.generic_rows[0], generic_obj)

    with pytest.raises(
        apply_recommendations.RecommendationError, match="Name 3 changed"
    ):
        apply_recommendations.edit_manual_reviews(
            recommendations,
            generic_plan,
            get_name=lambda name_id: name_obj,
            label_name=lambda name: "Changed name",
        )

    assert events == []


def test_edit_manual_cli_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    called: list[object] = []
    fake_generic_plan = object()
    manual_items = (object(),)

    def resolve_manual_reviews(
        recommendations: object, generic_plan: object, **kwargs: object
    ) -> tuple[object, ...]:
        called.append(("resolve", generic_plan))
        return manual_items

    monkeypatch.setattr(
        apply_recommendations,
        "build_plans",
        lambda recommendations: (fake_generic_plan, object(), object()),
    )
    monkeypatch.setattr(
        apply_recommendations, "_resolve_manual_review_objects", resolve_manual_reviews
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: called.append(("edit", items)),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--edit-manual"]
    )

    apply_recommendations.main()

    assert called == [("resolve", fake_generic_plan), ("edit", manual_items)]


def test_edit_manual_warns_and_continues_when_actionable_plan_is_stale(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    events: list[str] = []
    manual_plan = object()
    manual_items = (object(),)

    def validate_manual_reviews(recommendations: object) -> object:
        events.append("manual validation")
        return manual_plan

    def resolve_manual_reviews(
        recommendations: object, generic_plan: object, **kwargs: object
    ) -> tuple[object, ...]:
        events.append("manual resolution")
        return manual_items

    def stale_plan(recommendations: object) -> None:
        raise apply_recommendations.RecommendationError(
            "line 2: Location 123 changed from 'Old' to 'New'"
        )

    monkeypatch.setattr(apply_recommendations, "build_plans", stale_plan)
    monkeypatch.setattr(
        apply_recommendations,
        "build_generic_manual_review_plan",
        validate_manual_reviews,
    )
    monkeypatch.setattr(
        apply_recommendations, "_resolve_manual_review_objects", resolve_manual_reviews
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("edit"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--edit-manual"]
    )

    apply_recommendations.main()

    output = capsys.readouterr().out
    assert "Warning: the complete manifest no longer matches the database" in output
    assert "Location 123 changed from 'Old' to 'New'" in output
    assert events == ["manual validation", "manual resolution", "edit"]


def test_apply_and_edit_manual_validates_then_applies_then_edits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    events: list[str] = []
    fake_plans = (object(), object(), object())
    manual_items = (object(),)

    def resolve_manual_reviews(
        recommendations: object, generic_plan: object
    ) -> tuple[object, ...]:
        events.append("resolve")
        return manual_items

    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations, "_resolve_manual_review_objects", resolve_manual_reviews
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply" if apply else "dry-run"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("edit"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--apply", "--edit-manual"]
    )

    apply_recommendations.main()

    assert events == ["resolve", "apply", "edit"]
