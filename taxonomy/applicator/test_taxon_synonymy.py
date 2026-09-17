import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from taxonomy.applicator import generic, taxon_synonymy
from taxonomy.applicator.proposals import ProposalBuilder
from taxonomy.db.constants import AgeClass, NomenclatureStatus, Rank, Status
from taxonomy.db.models import Location, Name, Occurrence, OccurrenceRecord, Taxon
from taxonomy.db.models.name import TypeTag


@pytest.fixture
def taxa(monkeypatch: pytest.MonkeyPatch) -> tuple[Taxon, Taxon, dict[int, Any]]:
    # Keep real model descriptors and virtual writes, with stable fixture IDs.
    objects: dict[int, Any] = {}
    for i, label in [(1, "Old species"), (2, "New species"), (3, "Child taxon")]:
        taxon = Taxon.virtual(
            valid_name=label, rank=Rank.species, age=AgeClass.extant, parent=None
        )
        taxon._clirm_virtual_values["id"] = i
        name = Name.virtual(
            original_name=label,
            corrected_original_name=label,
            taxon=taxon,
            status=Status.valid,
            nomenclature_status=NomenclatureStatus.available,
        )
        name._clirm_virtual_values["id"] = i + 10
        taxon.base_name = name
        objects[i] = taxon
        objects[i + 10] = name
    source, target = objects[1], objects[2]
    objects[3].parent = source
    record = OccurrenceRecord.virtual(taxon=source)
    record._clirm_virtual_values["id"] = 20
    objects[20] = record
    monkeypatch.setattr(
        Taxon,
        "get_names",
        lambda self: [
            o for o in objects.values() if isinstance(o, Name) and o.taxon == self
        ],
    )
    monkeypatch.setattr(
        Taxon,
        "get_children",
        lambda self: [
            o
            for o in objects.values()
            if isinstance(o, Taxon) and o.parent == self and not o.is_invalid()
        ],
    )
    monkeypatch.setattr(
        Taxon,
        "occurrence_records",
        property(
            lambda self: [
                o
                for o in objects.values()
                if isinstance(o, OccurrenceRecord) and o.taxon == self
            ]
        ),
    )
    monkeypatch.setattr(
        Taxon,
        "occurrences",
        property(
            lambda self: [
                o
                for o in objects.values()
                if isinstance(o, Occurrence) and o.taxon == self
            ]
        ),
    )
    monkeypatch.setattr(Taxon, "reload", lambda self: self)
    monkeypatch.setattr(Name, "get", lambda *args: source.base_name)
    return source, target, objects


def _row(source: Taxon, target: Taxon) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "action": "synonymize_taxon",
        "confidence": "high",
        "reason": "The original descriptions refer to the same species.",
        "evidence": [{"kind": "source", "text": "Original account."}],
        "object": {"model": "Taxon", "id": source.id, "label": source.valid_name},
        "target": {"model": "Taxon", "id": target.id, "label": target.valid_name},
        "guard": taxon_synonymy.snapshot(source, target),
    }


def _plan(
    data: list[dict[str, Any]], objects: dict[int, Any]
) -> generic.RecommendationPlan:
    return generic.build_plan(
        [generic.parse_recommendation(row, i) for i, row in enumerate(data, 1)],
        get_object=lambda model, object_id: objects[object_id],
        get_unique_constraints=lambda model: (),
    )


def test_dry_run_apply_and_restart(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, target, objects = taxa
    source.base_name.status = Status.species_inquirenda
    original_status = target.base_name.status
    row = _row(source, target)
    plan = _plan([row], objects)
    real_synonymize = Taxon.synonymize
    call = Mock(side_effect=real_synonymize)
    monkeypatch.setattr(Taxon, "synonymize", lambda self, other: call(self, other))
    generic.execute_plan(plan, apply=False)
    call.assert_not_called()
    assert (source.age, source.parent) == (AgeClass.extant, None)
    generic.execute_plan(plan, apply=True)
    call.assert_called_once_with(source, target)
    assert source.age is AgeClass.redirect and source.parent == target
    assert source.base_name.status is Status.synonym
    assert source.base_name.taxon == target
    assert objects[3].parent == target
    assert objects[20].taxon == target
    assert target.base_name.status is original_status
    retry = _plan([row], objects)
    assert retry.actions[0].already_applied
    generic.execute_plan(retry, apply=True)
    assert call.call_count == 1
    assert "SKIP_ALREADY_APPLIED" in capsys.readouterr().out


def test_virtual_effects_and_review(
    taxa: tuple[Taxon, Taxon, dict[int, Any]], capsys: pytest.CaptureFixture[str]
) -> None:
    source, target, objects = taxa
    row = _row(source, target)
    plan = _plan([row], objects)
    builder = ProposalBuilder()
    generic.add_virtual_models(plan, builder)
    builder.build()
    assert builder.replacement(source).age is AgeClass.redirect
    assert builder.replacement(source.base_name).status is Status.synonym
    assert builder.replacement(source.base_name).taxon is builder.replacement(target)
    assert builder.replacement(objects[3]).parent is builder.replacement(target)
    assert builder.replacement(objects[20]).taxon is builder.replacement(target)
    assert source.age is AgeClass.extant and objects[3].parent == source
    generic.print_review_table([generic.parse_recommendation(row, 1)])
    output = capsys.readouterr().out
    assert "Taxon.synonymize" in output and "references=" in output


@pytest.mark.parametrize(
    "change",
    [
        lambda s, t, objects: setattr(t, "age", AgeClass.redirect),
        lambda s, t, objects: setattr(t, "parent", s),
        lambda s, t, objects: setattr(s, "data", "new data"),
        lambda s, t, objects: setattr(t.base_name, "status", Status.nomen_dubium),
        lambda s, t, objects: setattr(objects[3], "parent", t),
        lambda s, t, objects: setattr(objects[20], "taxon", t),
    ],
)
def test_stale_or_unsafe_merge_rejected(
    taxa: tuple[Taxon, Taxon, dict[int, Any]], change: Callable[..., None]
) -> None:
    source, target, objects = taxa
    row = _row(source, target)
    change(source, target, objects)
    with pytest.raises(generic.RecommendationError):
        _plan([row], objects)


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_mutation_rejected_in_either_order(
    taxa: tuple[Taxon, Taxon, dict[int, Any]], *, reverse: bool
) -> None:
    source, target, objects = taxa
    update = {
        "schema_version": 1,
        "action": "set_field",
        "confidence": "high",
        "reason": "Test",
        "evidence": [{"kind": "source", "text": "Test"}],
        "object": {
            "model": "Name",
            "id": source.base_name.id,
            "label": source.base_name.original_name,
        },
        "field": "status",
        "old_value": {"enum": "Status", "name": "valid"},
        "new_value": {"enum": "Status", "name": "nomen_dubium"},
    }
    rows = [_row(source, target), update]
    with pytest.raises(generic.RecommendationError, match="overlaps synonymize_taxon"):
        _plan(rows[::-1] if reverse else rows, objects)


def test_parser_rejects_missing_guard_and_self_merge(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
) -> None:
    source, target, _ = taxa
    row = _row(source, target)
    del row["guard"]
    with pytest.raises(generic.RecommendationError, match="guard"):
        generic.parse_recommendation(row, 1)
    row = _row(source, source)
    with pytest.raises(generic.RecommendationError, match="must differ"):
        generic.parse_recommendation(row, 1)


def test_legacy_occurrence_preview_preserves_provenance(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
) -> None:
    source, target, objects = taxa
    shared = Location.virtual(name="Shared site")
    separate = Location.virtual(name="Separate site")
    for oid, taxon, location, comment in [
        (30, source, shared, "Source evidence"),
        (31, target, shared, "Target evidence"),
        (32, source, separate, ""),
    ]:
        obj = Occurrence.virtual(
            taxon=taxon, location=location, comment=comment, source=None
        )
        obj._clirm_virtual_values["id"] = oid
        objects[oid] = obj
    plan = _plan([_row(source, target)], objects)
    builder = ProposalBuilder()
    generic.add_virtual_models(plan, builder)
    assert builder.replacement(objects[32]).taxon is builder.replacement(target)
    assert "Previously under _Old species_." in builder.replacement(objects[32]).comment
    assert "Source evidence" in builder.replacement(objects[31]).comment
    assert "Target evidence" in builder.replacement(objects[31]).comment
    assert objects[31].comment == "Target evidence"


def test_duplicate_and_overlapping_merges_rejected(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
) -> None:
    source, target, objects = taxa
    row = _row(source, target)
    with pytest.raises(generic.RecommendationError, match="duplicate"):
        _plan([row, row], objects)
    # A reverse merge would be individually valid against live state but unsafe
    # when applied with the first merge.
    with pytest.raises(generic.RecommendationError, match="overlapping"):
        _plan([row, _row(target, source)], objects)


def test_retry_rejects_reference_moved_elsewhere(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
) -> None:
    source, target, objects = taxa
    row = _row(source, target)
    generic.execute_plan(_plan([row], objects), apply=True)
    objects[20].taxon = objects[3]
    with pytest.raises(generic.RecommendationError, match="moved reference changed"):
        _plan([row], objects)


def test_cli_reads_and_reviews_synonymize_taxon(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from scripts import apply_recommendations

    source, target, _ = taxa
    path = tmp_path / "synonymy.jsonl"
    path.write_text(json.dumps(_row(source, target)) + "\n")
    recommendations = apply_recommendations.read_recommendations(path)
    assert recommendations.count == 1
    assert recommendations.generic_rows[0].action == "synonymize_taxon"
    apply_recommendations.print_review(recommendations)
    assert "Taxon.synonymize" in capsys.readouterr().out


def test_safe_companion_evidence_edit_is_allowed(
    taxa: tuple[Taxon, Taxon, dict[int, Any]],
) -> None:
    source, target, objects = taxa
    row = _row(source, target)
    evidence = {
        "schema_version": 1,
        "action": "add_tag",
        "confidence": "high",
        "reason": "Source evidence.",
        "evidence": [{"kind": "source", "text": "Evidence."}],
        "object": {
            "model": "Name",
            "id": source.base_name.id,
            "label": source.base_name.corrected_original_name,
        },
        "field": "type_tags",
        "tag": TypeTag.VerbatimName("Old species").serialize(),
    }
    plan = _plan([row, evidence], objects)
    generic.execute_plan(plan, apply=True)
    assert source.base_name.taxon == target
    assert len(source.base_name.type_tags) == 1
