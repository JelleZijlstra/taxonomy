from collections.abc import Mapping

import pytest

from taxonomy.applicator import generic as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import AltitudeUnit
from taxonomy.db.models import BaseModel, Collection, Location, Name, OccurrenceRecord
from taxonomy.db.models.name import NameTag
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
        assert isinstance(obj, Location)
        created.append(obj)
        return obj

    recommendations.execute_plan(plan, apply=True, create_object=create_object)

    assert len(created) == 1
    assert created[0].name == "Precise site"
    assert child.parent is created[0]


def test_schema_v2_create_object_allows_forward_references() -> None:
    common = {
        "schema_version": 2,
        "action": recommendations.CREATE_OBJECT,
        "confidence": "high",
        "reason": "The source defines both localities.",
        "evidence": [{"kind": "source", "text": "Exact source evidence."}],
    }
    child = {
        **common,
        "object": {"model": "Location", "ref": "child", "label": "Child"},
        "match": {"name": "Child", "parent": {"model": "Location", "ref": "parent"}},
        "values": {"name": "Child", "parent": {"model": "Location", "ref": "parent"}},
    }
    parent = {
        **common,
        "object": {"model": "Location", "ref": "parent", "label": "Parent"},
        "match": {"name": "Parent"},
        "values": {"name": "Parent"},
    }
    plan = recommendations.build_plan(
        [
            recommendations.parse_recommendation(child, 1),
            recommendations.parse_recommendation(parent, 2),
        ],
        model_registry={"Location": Location},
        find_objects_by_match=lambda _model, _match: [],
    )
    created: list[Location] = []

    def create_object(
        model: type[BaseModel], values: Mapping[str, object]
    ) -> BaseModel:
        obj = model.virtual(**values)
        assert isinstance(obj, Location)
        created.append(obj)
        return obj

    recommendations.execute_plan(plan, apply=True, create_object=create_object)

    assert [obj.name for obj in created] == ["Parent", "Child"]
    assert created[1].parent is created[0]


def test_review_expands_create_object_fields_and_match_guards(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _create_location_row()
    data.update(
        {
            "schema_version": 2,
            "match": {"name": "Precise site"},
            "values": {
                "name": "Precise site",
                "latitude": "37°N",
                "tags": [LocationTag.General.serialize()],
            },
        }
    )
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "create with fields name, latitude, tags" in output
    assert "    - match: name='Precise site'" in output
    assert "    - field: name='Precise site'" in output
    assert "    - field: latitude='37°N'" in output
    assert "    - field: tags=[General]" in output


def test_schema_v2_update_object_applies_multiple_guarded_changes() -> None:
    location = _make_location(latitude=None)
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "The source supplies coordinates and context.",
            "evidence": [{"kind": "source", "text": "Exact source evidence."}],
            "object": {"model": "Location", "id": 2300, "label": "Borchers Fauna"},
            "changes": [
                {
                    "operation": "set",
                    "field": "latitude",
                    "old_value": None,
                    "new_value": "37°N",
                },
                {
                    "operation": "set",
                    "field": "comment",
                    "old_value": None,
                    "new_value": "Reviewed.",
                },
            ],
        },
        1,
    )
    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )

    recommendations.execute_plan(plan, apply=True)

    assert location.latitude == "37°N"
    assert location.comment == "Reviewed."


def _merge_collection_row() -> dict[str, object]:
    return {
        "schema_version": 2,
        "action": recommendations.MERGE_COLLECTION,
        "confidence": "high",
        "reason": "The repositories are the same institution.",
        "evidence": [{"kind": "registry", "text": "OLD is obsolete as NEW."}],
        "object": {"model": "Collection", "id": 1, "label": "OLD"},
        "target": {"model": "Collection", "id": 2, "label": "NEW"},
        "object_updates": [
            {
                "object": {"model": "Name", "id": 3, "label": "Test name"},
                "changes": [
                    {
                        "operation": "set",
                        "field": "collection",
                        "old_value": {"model": "Collection", "id": 1, "label": "OLD"},
                        "new_value": {"model": "Collection", "id": 2, "label": "NEW"},
                    },
                    {
                        "operation": "set",
                        "field": "type_specimen",
                        "old_value": "OLD 123",
                        "new_value": "NEW 123",
                    },
                ],
            }
        ],
    }


def _build_collection_merge_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[recommendations.RecommendationPlan, Collection, Collection, Name]:
    source = Collection.virtual(label="OLD", name="Old collection")
    target = Collection.virtual(label="NEW", name="New collection")
    name = Name.virtual(
        corrected_original_name="Test name",
        collection=source,
        type_specimen="OLD 123",
        type_tags=(),
    )

    def get_object(model: type[BaseModel], object_id: int) -> BaseModel:
        return {(Collection, 1): source, (Collection, 2): target, (Name, 3): name}[
            (model, object_id)
        ]

    monkeypatch.setattr(recommendations, "_get_object", get_object)
    row = recommendations.parse_recommendation(_merge_collection_row(), 1)
    plan = recommendations.build_plan(
        [row],
        model_registry={"Collection": Collection, "Name": Name},
        get_object=get_object,
        find_collection_references=lambda _source: [(name, "collection")],
    )
    return plan, source, target, name


def test_merge_collection_updates_references_and_specimen_text(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    plan, source, target, name = _build_collection_merge_plan(monkeypatch)

    recommendations.execute_plan(plan, apply=False)

    assert "WOULD_MERGE_COLLECTION source=Collection:1" in capsys.readouterr().out
    assert name.collection is source
    assert not source.removed

    recommendations.execute_plan(plan, apply=True)

    assert name.collection is target
    assert name.type_specimen == "NEW 123"
    assert source.parent is target
    assert source.removed

    row = recommendations.parse_recommendation(_merge_collection_row(), 1)
    rebuilt = recommendations.build_plan(
        [row],
        model_registry={"Collection": Collection, "Name": Name},
        get_object=lambda model, object_id: {
            (Collection, 1): source,
            (Collection, 2): target,
            (Name, 3): name,
        }[(model, object_id)],
        find_collection_references=lambda _source: [],
    )
    assert rebuilt.actions[0].already_applied


def test_merge_collection_participates_in_virtual_proposal_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, source, target, name = _build_collection_merge_plan(monkeypatch)
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposed_models = [proposal.model for proposal in builder.build()]
    proposed_source = next(
        model
        for model in proposed_models
        if isinstance(model, Collection) and model.label == "OLD"
    )
    proposed_name = next(model for model in proposed_models if isinstance(model, Name))
    assert proposed_source.removed
    assert proposed_source.parent is target
    assert proposed_name.collection is target
    assert proposed_name.type_specimen == "NEW 123"
    assert not source.removed
    assert name.collection is source


def test_merge_collection_rejects_unaccounted_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Collection.virtual(label="OLD", name="Old collection")
    target = Collection.virtual(label="NEW", name="New collection")
    name = Name.virtual(
        corrected_original_name="Test name", type_specimen="OLD 123", type_tags=()
    )

    def get_object(model: type[BaseModel], object_id: int) -> BaseModel:
        return {(Collection, 1): source, (Collection, 2): target, (Name, 3): name}[
            (model, object_id)
        ]

    monkeypatch.setattr(recommendations, "_get_object", get_object)
    row_data = _merge_collection_row()
    changes = row_data["object_updates"][0]["changes"]  # type: ignore[index]
    changes.pop(0)
    row = recommendations.parse_recommendation(row_data, 1)

    with pytest.raises(recommendations.RecommendationError, match="does not account"):
        recommendations.build_plan(
            [row],
            model_registry={"Collection": Collection, "Name": Name},
            get_object=get_object,
            find_collection_references=lambda _source: [(name, "collection")],
        )


def test_collection_reference_scan_uses_only_indexed_backrefs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Collection.virtual(label="OLD", name="Old collection")
    name = Name.virtual(corrected_original_name="Test name", collection=source)

    monkeypatch.setattr(
        Collection,
        "get_direct_backrefs",
        lambda self, *, include_invalid=False: iter(((Name.collection, name),)),
    )

    def reject_table_scan(cls: type[BaseModel]) -> None:
        raise AssertionError(f"unexpected table scan of {cls.__name__}")

    for model in (Name, OccurrenceRecord, Collection):
        monkeypatch.setattr(model, "select", classmethod(reject_table_scan))

    assert recommendations._find_collection_references(source) == [(name, "collection")]


def test_merge_collection_composes_with_target_label_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Collection.virtual(label="OLD", name="Old collection")
    target = Collection.virtual(label="NEW", name="New collection")
    name = Name.virtual(
        corrected_original_name="Test name",
        collection=source,
        type_specimen="OLD 123",
        type_tags=(),
    )

    def get_object(model: type[BaseModel], object_id: int) -> BaseModel:
        return {(Collection, 1): source, (Collection, 2): target, (Name, 3): name}[
            (model, object_id)
        ]

    monkeypatch.setattr(recommendations, "_get_object", get_object)
    rename = {
        "schema_version": 2,
        "action": recommendations.UPDATE_OBJECT,
        "confidence": "high",
        "reason": "Use the registered code.",
        "evidence": [{"kind": "registry", "text": "FINAL is current."}],
        "object": {"model": "Collection", "id": 2, "label": "NEW"},
        "changes": [
            {
                "operation": "set",
                "field": "label",
                "old_value": "NEW",
                "new_value": "FINAL",
            }
        ],
    }
    merge = _merge_collection_row()
    merge["target"] = {"model": "Collection", "id": 2, "label": "FINAL"}
    merge["object_updates"][0]["changes"][0]["new_value"] = {  # type: ignore[index]
        "model": "Collection",
        "id": 2,
        "label": "FINAL",
    }
    rows = [
        recommendations.parse_recommendation(rename, 1),
        recommendations.parse_recommendation(merge, 2),
    ]
    plan = recommendations.build_plan(
        rows,
        model_registry={"Collection": Collection, "Name": Name},
        get_object=get_object,
        find_collection_references=lambda _source: [(name, "collection")],
    )

    recommendations.execute_plan(plan, apply=True)

    assert target.label == "FINAL"
    assert name.collection is target
    rebuilt = recommendations.build_plan(
        rows,
        model_registry={"Collection": Collection, "Name": Name},
        get_object=get_object,
        find_collection_references=lambda _source: [],
    )
    assert all(action.already_applied for action in rebuilt.actions)


def test_review_expands_schema_v2_guarded_changes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "The current repository and previous repository are explicit.",
            "evidence": [{"kind": "source", "text": "Exact source evidence."}],
            "object": {"model": "Name", "id": 4903, "label": "Example name"},
            "changes": [
                {
                    "operation": "set",
                    "field": "collection",
                    "old_value": {"model": "Collection", "id": 44, "label": "AM"},
                    "new_value": {"model": "Collection", "id": 6, "label": "MZB"},
                },
                {
                    "operation": "add",
                    "field": "type_tags",
                    "value": {
                        "tag": "FormerRepository",
                        "arguments": {
                            "repository": {
                                "model": "Collection",
                                "id": 44,
                                "label": "AM",
                            }
                        },
                    },
                },
                {
                    "operation": "remove_raw",
                    "field": "type_tags",
                    "raw_value": [22, 148618, ""],
                },
            ],
        },
        1,
    )

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "3 guarded change(s)" in output
    assert (
        "    - change: set collection: Collection(id=44, label='AM') -> "
        "Collection(id=6, label='MZB')" in output
    )
    assert (
        "    - change: add type_tags: "
        "FormerRepository(repository=Collection(id=44, label='AM'))" in output
    )
    assert "    - change: remove_raw type_tags: [22, 148618, '']" in output


def test_update_object_can_remove_one_malformed_raw_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_name = Name.virtual(corrected_original_name="Hyla kanaima", tags=())
    combination = Name.virtual(
        corrected_original_name="Nesorohyla kanaima", tags=(), type_tags=()
    )
    malformed = [22, 148618, ""]

    def get_raw_tags_field(self: Name, field: str) -> list[list[object]]:
        if self is combination and field == "type_tags":
            return [malformed]
        return []

    monkeypatch.setattr(Name, "get_raw_tags_field", get_raw_tags_field)
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "Move a Name tag out of the TypeTag field.",
            "evidence": [{"kind": "raw_database_value", "text": repr(malformed)}],
            "object": {"model": "Name", "id": 148619, "label": "Nesorohyla kanaima"},
            "changes": [
                {
                    "operation": "remove_raw",
                    "field": "type_tags",
                    "raw_value": malformed,
                },
                {
                    "operation": "add",
                    "field": "tags",
                    "value": {
                        "tag": "NameCombinationOf",
                        "arguments": {
                            "name": {
                                "model": "Name",
                                "id": 148618,
                                "label": "Hyla kanaima",
                            }
                        },
                    },
                },
            ],
        },
        1,
    )

    def get_object(_model: type[BaseModel], object_id: int) -> BaseModel:
        return combination if object_id == 148619 else base_name

    plan = recommendations.build_plan(
        [row], model_registry={"Name": Name}, get_object=get_object
    )
    recommendations.execute_plan(plan, apply=True)

    assert combination.type_tags == ()
    (combination_tag,) = combination.tags
    assert isinstance(combination_tag, NameTag.NameCombinationOf)
    assert combination_tag.name.id == 148618


def test_structured_tag_resolves_manifest_reference() -> None:
    collection = Collection.virtual(label="MUSM")
    tag = recommendations._decode_tag(
        OccurrenceRecord.tags,
        {
            "tag": "Voucher",
            "arguments": {
                "text": "MUSM 19358",
                "collection": {"model": "Collection", "ref": "musm"},
            },
        },
        context="test tag",
        references={"musm": collection},
    )

    assert isinstance(tag, OccurrenceRecordTag.Voucher)
    assert tag.collection is collection


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


def test_review_renders_argumentless_adt_tag_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _common(recommendations.ADD_TAG, "tags")
    data["tag"] = LocationTag.General.serialize()
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "tags: General" in output


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


def test_review_prints_manual_review_evidence(
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = _common(recommendations.MANUAL_REVIEW, "unused")
    data.pop("field")
    data["evidence"] = [
        {
            "kind": "source_page",
            "text": "First line of exact evidence.\nSecond line of exact evidence.",
        }
    ]
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "requires manual review" in output
    assert "    - evidence (source_page): First line of exact evidence." in output
    assert "      Second line of exact evidence." in output
