from collections.abc import Mapping

import pytest

from taxonomy.applicator import generic as recommendations
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import AltitudeUnit, NamingConvention, PersonType, RegionKind
from taxonomy.db.models import (
    Article,
    BaseModel,
    CitationGroup,
    ClassificationEntry,
    Collection,
    IssueDate,
    Location,
    Name,
    NameComment,
    OccurrenceRecord,
    Person,
    Region,
)
from taxonomy.db.models.location import LocationStatus
from taxonomy.db.models.name import NameTag
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
from taxonomy.db.models.region import RegionTag
from taxonomy.db.models.tags import LocationTag, PersonTag


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


def test_classification_entry_create_allows_comma_separated_pages() -> None:
    article = Article.virtual(name="Test classification.pdf")

    recommendations._validate_create_invariants(
        ClassificationEntry,
        {"article": article, "page": "12, 188", "parent": None},
        context="line 1",
    )


def test_classification_entry_create_rejects_page_ranges() -> None:
    article = Article.virtual(name="Test classification.pdf")

    with pytest.raises(
        recommendations.RecommendationError, match="comma-separated page numbers"
    ):
        recommendations._validate_create_invariants(
            ClassificationEntry,
            {"article": article, "page": "12-13", "parent": None},
            context="line 1",
        )


def test_classification_entry_create_preserves_raw_page() -> None:
    article = Article.virtual(name="Bilderbuch.pdf")
    page = "@no. 58 unnumbered p. 1"
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": "create_object",
            "object": {
                "model": "ClassificationEntry",
                "ref": "species",
                "label": "Simia Sylvatica",
            },
            "match": {"name": "Simia Sylvatica", "page": page},
            "values": {
                "article": {"model": "Article", "ref": "volume"},
                "name": "Simia Sylvatica",
                "rank": "species",
                "page": page,
            },
            "confidence": "high",
            "reason": "Unnumbered page within a numbered leaf.",
            "evidence": [{"kind": "source", "text": "Leaf 58, German text."}],
        },
        1,
    )
    plan = recommendations.build_plan(
        [row],
        initial_references={"volume": article},
        find_objects_by_match=lambda _model, _match: [],
    )
    assert plan.actions[0].new_value["page"] == page


@pytest.mark.parametrize(
    "page", ["@", "@ ", "@@no. 58", "@no. 58, @no. 59", "@no. 58\npage 1", "@no. 58 ["]
)
def test_classification_entry_create_rejects_malformed_raw_page(page: str) -> None:
    with pytest.raises(recommendations.RecommendationError, match="raw page"):
        recommendations._validate_create_invariants(
            ClassificationEntry, {"page": page}, context="line 1"
        )


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


def test_create_object_rejects_unique_collision_hidden_by_detailed_match() -> None:
    existing = _make_location(name="Precise site", latitude=None)
    data = _create_location_row()
    data.update(
        {
            "schema_version": 2,
            "match": {"name": "Precise site", "latitude": "37°N"},
            "values": {"name": "Precise site", "latitude": "37°N"},
        }
    )

    with pytest.raises(
        recommendations.RecommendationError,
        match=r"UNIQUE constraint Location\(name\).*existing Location",
    ):
        recommendations.build_plan(
            [recommendations.parse_recommendation(data, 1)],
            model_registry={"Location": Location},
            find_objects_by_match=lambda _model, _match: [],
            get_unique_constraints=lambda _model: (("name",),),
            find_objects_by_unique_constraint=lambda _model, _fields, _values: [
                existing
            ],
        )


def test_create_object_allows_unique_value_freed_by_earlier_update() -> None:
    existing = _make_location(name="Precise site", latitude=None)
    rename = _common(recommendations.SET_FIELD, "name")
    rename.update(
        {
            "object": {"model": "Location", "id": 2300, "label": "Precise site"},
            "old_value": "Precise site",
            "new_value": "Renamed site",
        }
    )
    create = _create_location_row()
    create.update(
        {
            "schema_version": 2,
            "match": {"name": "Precise site", "latitude": "37°N"},
            "values": {"name": "Precise site", "latitude": "37°N"},
        }
    )

    plan = recommendations.build_plan(
        [
            recommendations.parse_recommendation(rename, 1),
            recommendations.parse_recommendation(create, 2),
        ],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: existing,
        find_objects_by_match=lambda _model, _match: [],
        get_unique_constraints=lambda _model: (("name",),),
        find_objects_by_unique_constraint=lambda _model, _fields, _values: [existing],
    )

    assert len(plan.actions) == 2


def test_create_object_allows_auto_generated_id_label() -> None:
    citation_group = CitationGroup.virtual(name="Journal of Test Evidence")
    values: dict[str, object] = {
        "citation_group": {
            "model": "CitationGroup",
            "ref": "journal",
            "label": "Journal of Test Evidence",
        },
        "series": None,
        "volume": "12",
        "issue": "2",
        "start_page": "71",
        "end_page": "129",
        "date": "1878-02",
        "tags": [],
    }
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.CREATE_OBJECT,
            "confidence": "high",
            "reason": "The evidence supports one shared issue date.",
            "evidence": [{"kind": "source", "text": "Exact source evidence."}],
            "object": {
                "model": "IssueDate",
                "ref": "issue_date",
                "label": "Journal of Test Evidence 12(2):71-129 (1878-02)",
            },
            "match": values,
            "values": values,
        },
        1,
    )
    matches: list[Mapping[str, object]] = []

    def find_objects_by_match(
        _model: type[BaseModel], match: Mapping[str, object]
    ) -> list[BaseModel]:
        matches.append(match)
        return []

    plan = recommendations.build_plan(
        [row],
        model_registry={"IssueDate": IssueDate},
        find_objects_by_match=find_objects_by_match,
        initial_references={"journal": citation_group},
    )

    issue_date = plan.actions[0].object
    assert isinstance(issue_date, IssueDate)
    assert issue_date.citation_group is citation_group
    assert issue_date.date == "1878-02"
    assert len(matches) == 1
    assert matches[0]["citation_group"] is citation_group
    assert matches[0]["date"] == "1878-02"
    assert matches[0]["tags"] == ()


def test_create_name_comment_without_source() -> None:
    name = Name.virtual(corrected_original_name="Mus testus")
    match = {
        "name": {"model": "Name", "ref": "name", "label": "Mus testus"},
        "kind": {"enum": "CommentKind", "name": "type_specimen"},
        "text": "Two catalogued specimens match the original type data.",
    }
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.CREATE_OBJECT,
            "confidence": "high",
            "reason": "Retain unresolved specimen alternatives.",
            "evidence": [{"kind": "catalogue", "text": "Matching specimen data."}],
            "object": {
                "model": "NameComment",
                "ref": "comment",
                "label": "Type alternatives for Mus testus",
            },
            "match": match,
            "values": {**match, "date": 1788609600, "source": None, "page": None},
        },
        1,
    )
    plan = recommendations.build_plan(
        [row],
        model_registry={"NameComment": NameComment},
        find_objects_by_match=lambda _model, _match: [],
        initial_references={"name": name},
    )
    comment = plan.actions[0].object
    assert isinstance(comment, NameComment)
    assert comment.name is name
    assert comment.text == match["text"]
    assert comment.source is None
    resumed = recommendations.build_plan(
        [row],
        model_registry={"NameComment": NameComment},
        find_objects_by_match=lambda _model, _match: [comment],
        initial_references={"name": name},
    )
    assert resumed.actions[0].already_applied


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
    assert "    - field: name='Precise site'" not in output
    assert "    - field: latitude='37°N'" in output
    assert "    - field: tags=[General]" in output


def test_create_object_review_omits_empty_values_and_keeps_falsey_scalars(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.CREATE_OBJECT,
            "confidence": "high",
            "reason": "Create a reviewed issue date.",
            "evidence": [{"kind": "source", "text": "Exact source evidence."}],
            "object": {
                "model": "IssueDate",
                "ref": "issue_date",
                "label": "Reviewed issue date",
            },
            "match": {"date": "1878-02", "series": None, "tags": []},
            "values": {
                "date": "1878-02",
                "series": None,
                "issue": "",
                "tags": [],
                "start_page": 0,
                "volume": False,
            },
        },
        1,
    )

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "create with fields date, start_page, volume" in output
    assert "    - match: date='1878-02'" in output
    assert "    - field: date='1878-02'" not in output
    assert "    - match: series=" not in output
    assert "    - field: series=" not in output
    assert "    - field: issue=" not in output
    assert "    - match: tags=" not in output
    assert "    - field: tags=" not in output
    assert "    - field: start_page=0" in output
    assert "    - field: volume=False" in output


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


def test_schema_v2_update_object_normalizes_unordered_adt_field() -> None:
    general = LocationTag.General
    osm = LocationTag.CoordinatesFromGeoNames(123)
    location = _make_location(tags=(general, osm, general))
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "Normalize an unordered tag field.",
            "evidence": [{"kind": "lint", "text": "Tags are unsorted."}],
            "object": {"model": "Location", "id": 2300, "label": "Borchers Fauna"},
            "changes": [{"operation": "normalize", "field": "tags"}],
        },
        1,
    )

    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )
    assert not plan.actions[0].already_applied

    recommendations.execute_plan(plan, apply=True)

    assert location.tags == tuple(sorted({general, osm}))


def test_schema_v2_update_object_normalize_is_idempotent() -> None:
    general = LocationTag.General
    osm = LocationTag.CoordinatesFromGeoNames(123)
    location = _make_location(tags=tuple(sorted({general, osm})))
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "Normalize an unordered tag field.",
            "evidence": [{"kind": "lint", "text": "Tags are unsorted."}],
            "object": {"model": "Location", "id": 2300, "label": "Borchers Fauna"},
            "changes": [{"operation": "normalize", "field": "tags"}],
        },
        1,
    )

    plan = recommendations.build_plan(
        [row],
        model_registry={"Location": Location},
        get_object=lambda _model, _id: location,
    )

    assert plan.actions[0].already_applied


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


def _person_guard(person: Person) -> dict[str, object]:
    return {
        "family_name": person.family_name,
        "given_names": person.given_names,
        "initials": person.initials,
        "suffix": person.suffix,
        "tussenvoegsel": person.tussenvoegsel,
        "birth": person.birth,
        "death": person.death,
        "tags": [tag.serialize() for tag in person.tags],
        "naming_convention": {
            "enum": "NamingConvention",
            "name": person.naming_convention.name,
        },
        "type": {"enum": "PersonType", "name": person.type.name},
        "target": (
            None
            if person.target is None
            else {
                "model": "Person",
                "id": person.target.id,
                "label": person.target.family_name,
            }
        ),
        "bio": person.bio,
        "ol_id": person.ol_id,
    }


def _merge_person_row(source: Person, target: Person) -> dict[str, object]:
    references: dict[str, list[object]] = {
        field_name: [] for field_name in recommendations._PERSON_MERGE_REFERENCE_FIELDS
    }
    return {
        "schema_version": 2,
        "action": recommendations.MERGE_PERSON,
        "confidence": "high",
        "reason": "The shared ORCID and publication record establish one identity.",
        "evidence": [
            {
                "kind": "ORCID and article author evidence",
                "text": "Both records map to ORCID 0000-0001-2345-6789.",
            }
        ],
        "object": {"model": "Person", "id": 1, "label": source.family_name},
        "target": {"model": "Person", "id": 2, "label": target.family_name},
        "guard": {
            "source": _person_guard(source),
            "target": _person_guard(target),
            "references": references,
        },
    }


def _build_person_merge_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[recommendations.RecommendationPlan, Person, Person]:
    source = Person.virtual(
        family_name="Smith",
        given_names="Jane Anne",
        birth="1970",
        tags=(PersonTag.ORCID("0000-0001-2345-6789"),),
        naming_convention=NamingConvention.english,
        type=PersonType.unchecked,
    )
    target = Person.virtual(
        family_name="Smith",
        initials="J.A.",
        naming_convention=NamingConvention.english,
        type=PersonType.checked,
        tags=(PersonTag.ORCID("0000-0003-3641-8321"),),
    )

    class EmptyQuery:
        def filter(self, *_args: object) -> tuple[()]:
            return ()

    monkeypatch.setattr(Person, "select", classmethod(lambda _cls: EmptyQuery()))

    def get_object(model: type[BaseModel], object_id: int) -> BaseModel:
        return {(Person, 1): source, (Person, 2): target}[(model, object_id)]

    row = recommendations.parse_recommendation(_merge_person_row(source, target), 1)
    plan = recommendations.build_plan(
        [row], model_registry={"Person": Person}, get_object=get_object
    )
    return plan, source, target


def test_merge_person_is_guarded_restart_safe_and_transfers_metadata(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    plan, source, target = _build_person_merge_plan(monkeypatch)
    reassigned: list[Person] = []

    def reassign_references(
        self: Person, target: Person | None = None, *, respect_ignore_lint: bool = True
    ) -> None:
        assert self is source
        assert target is not None
        assert not respect_ignore_lint
        reassigned.append(target)

    monkeypatch.setattr(Person, "reassign_references", reassign_references)

    recommendations.execute_plan(plan, apply=False)
    assert "WOULD_MERGE_PERSON source=Person:1" in capsys.readouterr().out
    assert source.type is PersonType.unchecked

    recommendations.execute_plan(plan, apply=True)
    assert reassigned == [target]
    assert source.type is PersonType.hard_redirect  # type: ignore[comparison-overlap]
    assert source.target is target
    assert source.tags == (PersonTag.ORCID("0000-0001-2345-6789"),)
    assert source.birth is None
    assert target.birth == "1970"
    assert set(target.tags) == {
        PersonTag.ORCID("0000-0001-2345-6789"),
        PersonTag.ORCID("0000-0003-3641-8321"),
        PersonTag.IgnoreLint(
            "multiple_orcids",
            comment=(
                "Person records with independently matching public ORCID identities "
                "were merged."
            ),
        ),
    }

    rebuilt = recommendations.build_plan(
        [recommendations.parse_recommendation(_merge_person_row_from_plan(plan), 1)],
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )
    assert rebuilt.actions[0].already_applied

    # Redirects merged by older applicator versions had their tags cleared. Keep
    # those manifests restart-safe while retaining tags for all new merges.
    source.tags = ()
    legacy_rebuilt = recommendations.build_plan(
        [recommendations.parse_recommendation(_merge_person_row_from_plan(plan), 1)],
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )
    assert legacy_rebuilt.actions[0].already_applied


def test_merge_person_copies_composed_source_tag_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _plan, source, target = _build_person_merge_plan(monkeypatch)
    old_orcid = PersonTag.ORCID("0000-0001-2345-6789")
    work_exception = PersonTag.IgnoreORCIDWork(
        orcid="0000-0001-2345-6789",
        doi="10.1234/example",
        comment="Reviewed exception.",
    )
    update_data = {
        "schema_version": 2,
        "action": recommendations.UPDATE_OBJECT,
        "confidence": "high",
        "reason": "Replace stale source metadata before merging.",
        "evidence": [{"kind": "ORCID record", "text": "The work was reviewed."}],
        "object": {"model": "Person", "id": 1, "label": source.family_name},
        "changes": [
            {"operation": "remove", "field": "tags", "value": old_orcid.serialize()},
            {"operation": "add", "field": "tags", "value": work_exception.serialize()},
        ],
    }
    rows = [
        recommendations.parse_recommendation(update_data, 1),
        recommendations.parse_recommendation(_merge_person_row(source, target), 2),
    ]
    composed = recommendations.build_plan(
        rows,
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )

    target_values = composed.actions[1].new_value["target_values"]
    assert set(target_values["tags"]) == {
        PersonTag.ORCID("0000-0003-3641-8321"),
        work_exception,
    }

    builder = ProposalBuilder()
    recommendations.add_virtual_models(composed, builder)
    proposed_source = next(
        proposal.model
        for proposal in builder.build()
        if isinstance(proposal.model, Person)
        and proposal.model.given_names == "Jane Anne"
    )
    assert proposed_source.type is PersonType.hard_redirect
    assert proposed_source.tags == (work_exception,)

    def reassign_references(
        self: Person, target: Person | None = None, *, respect_ignore_lint: bool = True
    ) -> None:
        assert self is source
        assert target is not None
        assert not respect_ignore_lint

    monkeypatch.setattr(Person, "reassign_references", reassign_references)
    recommendations.execute_plan(composed, apply=True)
    assert source.tags == (work_exception,)
    assert set(target.tags) == {PersonTag.ORCID("0000-0003-3641-8321"), work_exception}


def test_merge_person_combines_general_with_specific_naming_convention() -> None:
    assert (
        recommendations._combine_person_naming_conventions(
            NamingConvention.general, NamingConvention.portuguese
        )
        is NamingConvention.portuguese
    )
    with pytest.raises(
        recommendations.RecommendationError, match="conflicting naming conventions"
    ):
        recommendations._combine_person_naming_conventions(
            NamingConvention.portuguese, NamingConvention.spanish
        )


def test_merge_person_allows_evidence_backed_naming_convention_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Person.virtual(
        family_name="Lin",
        given_names="Si-min",
        naming_convention=NamingConvention.pinyin,
        type=PersonType.unchecked,
    )
    target = Person.virtual(
        family_name="Lin",
        given_names="Si-Min",
        naming_convention=NamingConvention.chinese,
        type=PersonType.unchecked,
    )

    class EmptyQuery:
        def filter(self, *_args: object) -> tuple[()]:
            return ()

    monkeypatch.setattr(Person, "select", classmethod(lambda _cls: EmptyQuery()))
    row_data = _merge_person_row(source, target)
    row_data["resolved_naming_convention"] = {
        "enum": "NamingConvention",
        "name": "pinyin",
    }
    row = recommendations.parse_recommendation(row_data, 1)
    plan = recommendations.build_plan(
        [row],
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )
    assert plan.actions[0].new_value["target_values"]["naming_convention"] is (
        NamingConvention.pinyin
    )


def _merge_person_row_from_plan(
    plan: recommendations.RecommendationPlan,
) -> dict[str, object]:
    row = plan.actions[0].recommendation
    assert row.target is not None
    assert row.match is not None
    return {
        "schema_version": row.schema_version,
        "action": row.action,
        "confidence": row.confidence,
        "reason": row.reason,
        "evidence": [
            {"kind": evidence.kind, "text": evidence.text} for evidence in row.evidence
        ],
        "object": {"model": "Person", "id": 1, "label": row.object.label},
        "target": {"model": "Person", "id": 2, "label": row.target.label},
        "guard": {
            "source": dict(row.match["source"]),
            "target": dict(row.match["target"]),
            "references": dict(row.match["references"]),
        },
    }


def test_merge_person_rejects_stale_reference_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, source, target = _build_person_merge_plan(monkeypatch)
    row = plan.actions[0].recommendation
    monkeypatch.setattr(
        recommendations,
        "_person_reference_snapshot",
        lambda _person: {
            **{
                field_name: []
                for field_name in recommendations._PERSON_MERGE_REFERENCE_FIELDS
            },
            "articles": [99],
        },
    )

    with pytest.raises(recommendations.RecommendationError, match="references changed"):
        recommendations.build_plan(
            [row],
            model_registry={"Person": Person},
            get_object=lambda _model, object_id: source if object_id == 1 else target,
        )


def test_merge_person_participates_in_virtual_proposal_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, _source, target = _build_person_merge_plan(monkeypatch)
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposed_people = [
        proposal.model
        for proposal in builder.build()
        if isinstance(proposal.model, Person)
    ]
    proposed_source = next(
        person for person in proposed_people if person.given_names == "Jane Anne"
    )
    proposed_target = next(
        person for person in proposed_people if person.initials == "J.A."
    )
    assert proposed_source.type is PersonType.hard_redirect
    assert proposed_source.target is proposed_target
    assert proposed_target.birth == "1970"
    assert target.birth is None


def _reassign_person_references_row(
    source: Person, target: Person
) -> dict[str, object]:
    references: dict[str, list[object]] = {
        field_name: []
        for field_name in recommendations._PERSON_REASSIGN_REFERENCE_FIELDS
    }
    return {
        "schema_version": 2,
        "action": recommendations.REASSIGN_PERSON_REFERENCES,
        "confidence": "high",
        "reason": (
            "The shared ORCID verifies these existing references, but the abbreviated "
            "source name is not globally identity-safe."
        ),
        "evidence": [
            {
                "kind": "ORCID and Article author evidence",
                "text": "Both records map to ORCID 0000-0001-9748-2947.",
            }
        ],
        "object": {"model": "Person", "id": 1, "label": source.family_name},
        "target": {"model": "Person", "id": 2, "label": target.family_name},
        "orcid": "0000-0001-9748-2947",
        "guard": {
            "source": _person_guard(source),
            "target": _person_guard(target),
            "references": references,
        },
    }


def test_reassign_person_references_moves_orcid_without_redirecting_source(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Person.virtual(
        family_name="Bennett",
        initials="N.C.",
        tags=(PersonTag.ORCID("0000-0001-9748-2947"),),
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )
    target = Person.virtual(
        family_name="Bennett",
        given_names="Nigel C.",
        tags=(),
        naming_convention=NamingConvention.general,
        type=PersonType.unchecked,
    )

    class EmptyQuery:
        def filter(self, *_args: object) -> tuple[()]:
            return ()

    monkeypatch.setattr(Person, "select", classmethod(lambda _cls: EmptyQuery()))
    monkeypatch.setattr(
        recommendations,
        "_person_reassign_reference_snapshot",
        lambda _person: {
            field_name: []
            for field_name in recommendations._PERSON_REASSIGN_REFERENCE_FIELDS
        },
    )
    reassigned: list[Person] = []

    def reassign_references(
        self: Person, target: Person | None = None, *, respect_ignore_lint: bool = True
    ) -> None:
        assert self is source
        assert target is not None
        assert not respect_ignore_lint
        reassigned.append(target)

    monkeypatch.setattr(Person, "reassign_references", reassign_references)
    row_data = _reassign_person_references_row(source, target)
    row = recommendations.parse_recommendation(row_data, 1)
    plan = recommendations.build_plan(
        [row],
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )

    builder = ProposalBuilder()
    recommendations.add_virtual_models(plan, builder)
    proposed_people = [
        proposal.model
        for proposal in builder.build()
        if isinstance(proposal.model, Person)
    ]
    proposed_source = next(person for person in proposed_people if person.initials)
    proposed_target = next(person for person in proposed_people if person.given_names)
    assert proposed_source.type is PersonType.unchecked
    assert proposed_source.target is None
    assert proposed_source.tags == ()
    assert proposed_target.tags == (PersonTag.ORCID("0000-0001-9748-2947"),)

    recommendations.execute_plan(plan, apply=False)
    assert "WOULD_REASSIGN_PERSON_REFERENCES source=Person:1" in (
        capsys.readouterr().out
    )
    recommendations.execute_plan(plan, apply=True)
    assert reassigned == [target]
    assert source.type is PersonType.unchecked
    assert source.target is None
    assert source.tags == ()
    assert target.tags == (PersonTag.ORCID("0000-0001-9748-2947"),)

    rebuilt = recommendations.build_plan(
        [recommendations.parse_recommendation(row_data, 1)],
        model_registry={"Person": Person},
        get_object=lambda _model, object_id: source if object_id == 1 else target,
    )
    assert rebuilt.actions[0].already_applied


def test_reassign_person_references_rejects_stale_reference_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Person.virtual(
        family_name="Jones",
        initials="C.",
        tags=(PersonTag.ORCID("0000-0001-9748-2947"),),
        naming_convention=NamingConvention.unspecified,
        type=PersonType.unchecked,
    )
    target = Person.virtual(
        family_name="Jones",
        given_names="Craig M.",
        tags=(),
        naming_convention=NamingConvention.general,
        type=PersonType.unchecked,
    )
    monkeypatch.setattr(
        recommendations,
        "_person_reassign_reference_snapshot",
        lambda _person: {
            **{
                field_name: []
                for field_name in recommendations._PERSON_REASSIGN_REFERENCE_FIELDS
            },
            "articles": [99],
        },
    )
    row = recommendations.parse_recommendation(
        _reassign_person_references_row(source, target), 1
    )

    with pytest.raises(recommendations.RecommendationError, match="references changed"):
        recommendations.build_plan(
            [row],
            model_registry={"Person": Person},
            get_object=lambda _model, object_id: source if object_id == 1 else target,
        )


def _merge_region_row(*, target_label: str = "Canonical Region") -> dict[str, object]:
    return {
        "schema_version": 2,
        "action": recommendations.MERGE_REGION,
        "confidence": "high",
        "reason": "The source is an obsolete administrative Region.",
        "evidence": [{"kind": "official boundary", "text": "The Regions merged."}],
        "object": {"model": "Region", "id": 1, "label": "Obsolete Region"},
        "target": {"model": "Region", "id": 2, "label": target_label},
    }


def _delete_region_row() -> dict[str, object]:
    return {
        "schema_version": 2,
        "action": recommendations.DELETE_REGION,
        "confidence": "high",
        "reason": "The obsolete Region is empty after redistribution.",
        "evidence": [
            {"kind": "reference audit", "text": "No valid references remain."}
        ],
        "object": {"model": "Region", "id": 1, "label": "Obsolete Region"},
        "guard": {"kind": {"enum": "RegionKind", "name": "county"}, "comment": None},
    }


def test_delete_region_is_guarded_restart_safe_and_virtual(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Region.virtual(
        name="Obsolete Region",
        kind=RegionKind.county,
        tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
    )
    monkeypatch.setattr(
        Region, "get_direct_backrefs", lambda self, *, include_invalid=False: iter(())
    )
    row = recommendations.parse_recommendation(_delete_region_row(), 1)
    plan = recommendations.build_plan(
        [row], model_registry={"Region": Region}, get_object=lambda _model, _id: source
    )

    builder = ProposalBuilder()
    recommendations.add_virtual_models(plan, builder)
    proposed = builder.build()[0].model
    assert isinstance(proposed, Region)
    assert proposed.kind is RegionKind.deleted
    assert proposed.tags == (RegionTag.OpenStreetMap("relation", 123, "boundary"),)

    recommendations.execute_plan(plan, apply=False)
    assert "WOULD_DELETE_REGION source=Region:1" in capsys.readouterr().out
    assert source.kind is RegionKind.county

    recommendations.execute_plan(plan, apply=True)
    assert source.kind is RegionKind.deleted  # type: ignore[comparison-overlap]
    assert source.tags == (RegionTag.OpenStreetMap("relation", 123, "boundary"),)

    rebuilt = recommendations.build_plan(
        [row], model_registry={"Region": Region}, get_object=lambda _model, _id: source
    )
    assert rebuilt.actions[0].already_applied


def test_delete_region_rejects_valid_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    source = Region.virtual(name="Obsolete Region", kind=RegionKind.county, tags=())
    location = Location.virtual(name="Referenced site", region=source, tags=())
    monkeypatch.setattr(
        Region,
        "get_direct_backrefs",
        lambda self, *, include_invalid=False: iter(((Location.region, location),)),
    )
    row = recommendations.parse_recommendation(_delete_region_row(), 1)

    with pytest.raises(
        recommendations.RecommendationError,
        match="cannot delete a Region with valid references",
    ):
        recommendations.build_plan(
            [row],
            model_registry={"Region": Region},
            get_object=lambda _model, _id: source,
        )


def test_merge_region_composes_with_target_rename_and_is_restart_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Region.virtual(
        name="Obsolete Region",
        kind=RegionKind.territory,
        tags=(RegionTag.IncompletelyDivided,),
    )
    target = Region.virtual(name="Canonical Region", kind=RegionKind.territory, tags=())
    location = Location.virtual(name="Referenced site", region=source, tags=())

    def get_object(model: type[BaseModel], object_id: int) -> BaseModel:
        return {(Region, 1): source, (Region, 2): target}[(model, object_id)]

    monkeypatch.setattr(
        Region,
        "get_direct_backrefs",
        lambda self, *, include_invalid=False: iter(
            ((Location.region, location),) if self is source else ()
        ),
    )
    rename = {
        "schema_version": 2,
        "action": recommendations.UPDATE_OBJECT,
        "confidence": "high",
        "reason": "Use the current administrative name.",
        "evidence": [{"kind": "official boundary", "text": "The target was renamed."}],
        "object": {"model": "Region", "id": 2, "label": "Canonical Region"},
        "changes": [
            {
                "operation": "set",
                "field": "name",
                "old_value": "Canonical Region",
                "new_value": "Renamed Region",
            }
        ],
    }
    rows = [
        recommendations.parse_recommendation(rename, 1),
        recommendations.parse_recommendation(
            _merge_region_row(target_label="Renamed Region"), 2
        ),
    ]
    plan = recommendations.build_plan(
        rows, model_registry={"Region": Region}, get_object=get_object
    )

    recommendations.execute_plan(plan, apply=True)

    assert target.name == "Renamed Region"
    assert location.region is target
    assert source.parent is target
    assert source.kind is RegionKind.redirect
    assert source.tags == (RegionTag.IncompletelyDivided,)
    rebuilt = recommendations.build_plan(
        rows, model_registry={"Region": Region}, get_object=get_object
    )
    assert all(action.already_applied for action in rebuilt.actions)


def test_merge_region_participates_in_virtual_proposal_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Region.virtual(
        name="Obsolete Region",
        kind=RegionKind.territory,
        tags=(RegionTag.IncompletelyDivided,),
    )
    target = Region.virtual(name="Canonical Region", kind=RegionKind.territory, tags=())
    location = Location.virtual(name="Referenced site", region=source, tags=())
    monkeypatch.setattr(
        Region,
        "get_direct_backrefs",
        lambda self, *, include_invalid=False: iter(
            ((Location.region, location),) if self is source else ()
        ),
    )
    row = recommendations.parse_recommendation(_merge_region_row(), 1)
    plan = recommendations.build_plan(
        [row],
        model_registry={"Region": Region},
        get_object=lambda _model, object_id: {1: source, 2: target}[object_id],
    )
    builder = ProposalBuilder()

    recommendations.add_virtual_models(plan, builder)

    proposed_models = [proposal.model for proposal in builder.build()]
    proposed_source = next(
        model
        for model in proposed_models
        if isinstance(model, Region) and model.name == "Obsolete Region"
    )
    proposed_location = next(
        model for model in proposed_models if isinstance(model, Location)
    )
    assert proposed_source.kind is RegionKind.redirect
    assert proposed_source.parent is target
    assert proposed_source.tags == (RegionTag.IncompletelyDivided,)
    assert proposed_location.region is target
    assert source.kind is RegionKind.territory
    assert location.region is source


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


def test_review_omits_noop_guarded_set_changes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": recommendations.UPDATE_OBJECT,
            "confidence": "high",
            "reason": "The publication date is supported by direct evidence.",
            "evidence": [{"kind": "source", "text": "Exact source evidence."}],
            "object": {"model": "Article", "id": 123, "label": "Example.pdf"},
            "changes": [
                {
                    "operation": "set",
                    "field": "year",
                    "old_value": "1994-12",
                    "new_value": "1994-12",
                },
                {
                    "operation": "set",
                    "field": "title",
                    "old_value": None,
                    "new_value": "Example",
                },
            ],
        },
        1,
    )

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "1 guarded change(s)" in output
    assert "set title: None -> 'Example'" in output
    assert "set year" not in output
    assert "1994-12" not in output


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


def test_structured_tag_accepts_legacy_constructor_name() -> None:
    tag = recommendations._decode_tag(
        Location.tags,
        {"tag": "IgnoreLintLocation", "arguments": {"label": "coordinate_provenance"}},
        context="test tag",
    )

    assert isinstance(tag, LocationTag.IgnoreLint)
    assert tag.label == "coordinate_provenance"


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
    tag = LocationTag.IgnoreLint("coordinate_provenance", comment="Reviewed evidence.")
    data = _common(recommendations.ADD_TAG, "tags")
    data["tag"] = tag.serialize()
    row = recommendations.parse_recommendation(data, 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "IgnoreLint(" in output
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
    later = LocationTag.IgnoreLint("nominatim_coordinates")
    earlier = LocationTag.IgnoreLint("coordinate_collision")
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


@pytest.mark.parametrize(
    "action", [recommendations.SET_FIELD, recommendations.UPDATE_OBJECT]
)
@pytest.mark.parametrize("status", [LocationStatus.deleted, LocationStatus.alias])
def test_rename_rejects_name_reserved_by_invalid_location(
    action: str, status: LocationStatus
) -> None:
    current = _make_location(name="Old spelling")
    reserved = _make_location(name="Modern spelling")
    reserved.deleted = status
    data = _common(action, "name")
    data["object"] = {"model": "Location", "id": 2300, "label": "Old spelling"}
    if action == recommendations.SET_FIELD:
        data.update(old_value="Old spelling", new_value="Modern spelling")
    else:
        data.pop("field")
        data.update(
            schema_version=2,
            changes=[
                {
                    "operation": "set",
                    "field": "latitude",
                    "old_value": "37°10'N",
                    "new_value": "38°N",
                },
                {
                    "operation": "set",
                    "field": "name",
                    "old_value": "Old spelling",
                    "new_value": "Modern spelling",
                },
            ],
        )
    with pytest.raises(
        recommendations.RecommendationError, match=r"UNIQUE constraint Location\(name\)"
    ):
        recommendations.build_plan(
            [recommendations.parse_recommendation(data, 1)],
            model_registry={"Location": Location},
            get_object=lambda _model, _id: current,
            get_unique_constraints=lambda _model: (("name",),),
            find_objects_by_unique_constraint=lambda _model, _fields, _values: [
                reserved
            ],
        )
    assert current.name == "Old spelling"
    assert current.latitude == "37°10'N"


def test_rename_rejects_name_taken_by_earlier_manifest_create() -> None:
    current = _make_location(name="Old spelling")
    rename = _common(recommendations.SET_FIELD, "name")
    rename.update(
        object={"model": "Location", "id": 2300, "label": "Old spelling"},
        old_value="Old spelling",
        new_value="Precise site",
    )
    with pytest.raises(
        recommendations.RecommendationError, match=r"UNIQUE constraint Location\(name\)"
    ):
        recommendations.build_plan(
            [
                recommendations.parse_recommendation(row, index)
                for index, row in enumerate([_create_location_row(), rename], 1)
            ],
            model_registry={"Location": Location},
            get_object=lambda _model, _id: current,
            find_objects_by_label=lambda _model, _label: [],
            get_unique_constraints=lambda _model: (("name",),),
            find_objects_by_unique_constraint=lambda _model, _fields, _values: [],
        )
