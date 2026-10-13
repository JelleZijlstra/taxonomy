import copy
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date
from functools import cache, partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, Self, cast

import pytest

from scripts import apply_recommendations
from taxonomy import events as model_events
from taxonomy import getinput
from taxonomy.applicator import article as article_recommendations
from taxonomy.applicator import generic as generic_recommendations
from taxonomy.applicator import item_file as item_file_recommendations
from taxonomy.applicator import location as location_recommendations
from taxonomy.applicator import type_locality as type_recommendations
from taxonomy.applicator.proposals import ProposedModel
from taxonomy.db.constants import ArticleType, Rank, RegionKind
from taxonomy.db.models import (
    Article,
    CitationGroup,
    ClassificationEntry,
    ItemFile,
    Location,
    Name,
    Person,
    Region,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.citation_group.cg import CitationGroupStatus
from taxonomy.db.models.classification_entry.lint import check_predates_mapped_name
from taxonomy.db.models.lint_types import LintIssue, LintResult
from taxonomy.db.models.name.lint.variants import take_over_name_issue


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


def _article_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "create_article",
        "confidence": "high",
        "reason": "The PDF and DOI identify the work.",
        "evidence": [
            {"kind": "landing_page", "text": "https://doi.org/10.1234/example"}
        ],
        "article": {
            "name": "Endodontidae.pdf",
            "doi": "10.1234/example",
            "citation_group": {"id": 12, "name": "Journal of Mollusks"},
        },
        "file": {
            "source_path": "download.pdf",
            "destination_folder": "Mollusca",
            "sha256": "0" * 64,
            "size": 123,
        },
    }


def _item_file_row() -> dict[str, object]:
    return {
        "schema_version": 1,
        "action": "create_item_file",
        "confidence": "high",
        "reason": "The title page identifies the complete source item.",
        "evidence": [{"kind": "title_page", "text": "Volume 12."}],
        "item_file": {
            "filename": "raw volume.pdf",
            "citation_group": {"id": 12, "name": "Journal of Mollusks"},
            "fields": {"volume": "12"},
            "tags": [],
        },
        "file": {
            "source_path": "Burst/Old/raw volume.pdf",
            "sha256": "0" * 64,
            "size": 123,
        },
    }


def _classification_entry_row(
    *,
    ref: str,
    name: str,
    rank: str,
    page: str,
    article: Mapping[str, object] | None = None,
    parent: Mapping[str, object] | None = None,
    authority: str | None = None,
    year: str | None = None,
) -> dict[str, object]:
    if article is None:
        article = {"model": "Article", "id": 10, "label": "Example article"}
    return {
        "schema_version": 1,
        "action": "create_object",
        "confidence": "high",
        "reason": "Transcribed from the source classification.",
        "evidence": [{"kind": "source", "text": f"Source entry {name}."}],
        "object": {"model": "ClassificationEntry", "ref": ref, "label": name},
        "values": {
            "article": article,
            "name": name,
            "rank": rank,
            "page": page,
            "parent": parent,
            "authority": authority,
            "year": year,
        },
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


def test_article_static_review_does_not_plan_or_access_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_article_row()])

    recommendations = apply_recommendations.read_recommendations(path)
    apply_recommendations.print_review(recommendations)

    assert isinstance(
        recommendations.article_rows[0], article_recommendations.Recommendation
    )
    output = capsys.readouterr().out
    assert "ARTICLE RECOMMENDATIONS" in output
    assert "Endodontidae.pdf" in output


def test_item_file_static_review_does_not_plan_or_access_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_item_file_row()])

    recommendations = apply_recommendations.read_recommendations(path)
    apply_recommendations.print_review(recommendations)

    assert isinstance(
        recommendations.item_file_rows[0], item_file_recommendations.Recommendation
    )
    output = capsys.readouterr().out
    assert "ITEM FILE RECOMMENDATIONS" in output
    assert "raw volume.pdf" in output


@pytest.mark.parametrize("conflicting", [False, True])
def test_article_and_item_share_inline_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, conflicting: bool
) -> None:
    pdf = b"%PDF-1.4\nfixture\n%%EOF\n"
    options = SimpleNamespace(
        new_path=tmp_path / "new",
        library_path=tmp_path / "library",
        item_file_path=tmp_path / "items",
    )
    options.new_path.mkdir()
    (options.library_path / "Mollusca").mkdir(parents=True)
    options.item_file_path.mkdir()
    region = Region.virtual(name="Fiji")
    group = {
        "name": "New Journal of Mollusks",
        "type": "JOURNAL",
        "region": {"id": region.id, "name": region.name},
        "tags": [],
    }
    article_row = cast(dict[str, Any], _article_row())
    article_row["article"] = {
        "name": "Endodontidae.pdf",
        "type": "JOURNAL",
        "citation_group": group,
        "fields": {"title": "Example publication", "year": "2020"},
        "authors": [{"family_name": "Smith", "given_names": "Jane"}],
    }
    item_row = cast(dict[str, Any], _item_file_row())
    item_row["item_file"]["citation_group"] = copy.deepcopy(group)
    if conflicting:
        item_row["item_file"]["citation_group"]["type"] = "BOOK"
    for index, row in enumerate((article_row, item_row)):
        row["file"].update(
            source_path=f"source{index}.pdf",
            sha256=hashlib.sha256(pdf).hexdigest(),
            size=len(pdf),
        )
        (options.new_path / row["file"]["source_path"]).write_bytes(pdf)
    monkeypatch.setattr(
        article_recommendations,
        "build_plan",
        partial(
            article_recommendations.build_plan,
            options=options,
            get_article=lambda _name: None,
            is_catalog_folder=lambda _path: True,
            citation_groups_named=lambda _name: (),
            get_region=lambda _id: region,
        ),
    )
    monkeypatch.setattr(
        item_file_recommendations,
        "build_plan",
        partial(
            item_file_recommendations.build_plan,
            options=options,
            get_item_file=lambda _name: None,
            citation_groups_named=lambda _name: (),
            get_region=lambda _id: region,
        ),
    )
    recommendations = apply_recommendations.Recommendations(
        (article_recommendations.parse_recommendation(article_row, 2),),
        (),
        (),
        (),
        item_file_rows=(item_file_recommendations.parse_recommendation(item_row, 1),),
    )
    if conflicting:
        with pytest.raises(
            apply_recommendations.RecommendationError,
            match="conflicting definitions for new CitationGroup",
        ):
            apply_recommendations.build_plans(recommendations)
        assert len(list(options.new_path.iterdir())) == 2
        assert not list(options.item_file_path.iterdir())
        return

    plans = apply_recommendations.build_plans(recommendations)
    assert isinstance(plans, apply_recommendations.UnifiedRecommendationPlan)
    proposals = [proposal.model for proposal in plans.proposals]
    journals = [model for model in proposals if isinstance(model, CitationGroup)]
    assert len(journals) == 1
    assert all(
        model.citation_group is journals[0]
        for model in proposals
        if isinstance(model, (Article, ItemFile))
    )
    created_groups: list[CitationGroup] = []
    created_records: list[Article | ItemFile] = []

    def create_group(
        planned: article_recommendations.PlannedCitationGroup,
    ) -> CitationGroup:
        cg = CitationGroup.virtual(
            name=planned.spec.name,
            type=ArticleType.JOURNAL,
            region=region,
            status=CitationGroupStatus.normal,
            tags=planned.tags,
        )
        created_groups.append(cg)
        return cg

    def create_article(name: str, values: Mapping[str, Any]) -> Article:
        article = Article.virtual(name=name, **values)
        created_records.append(article)
        return article

    def create_item(values: Mapping[str, Any]) -> ItemFile:
        item = ItemFile.virtual(**values)
        created_records.append(item)
        return item

    monkeypatch.setattr(
        article_recommendations,
        "execute_plan",
        partial(
            article_recommendations.execute_plan,
            create_citation_group=create_group,
            create_article=create_article,
            get_or_create_person=lambda **kwargs: Person.virtual(**kwargs),
            run_auxiliary=lambda _article, *, add_history: None,
        ),
    )
    monkeypatch.setattr(
        item_file_recommendations,
        "execute_plan",
        partial(
            item_file_recommendations.execute_plan,
            create_citation_group=create_group,
            create_item_file=create_item,
        ),
    )
    apply_recommendations.execute_plans(plans, apply=False)
    assert not created_groups and not created_records
    assert len(list(options.new_path.iterdir())) == 2
    apply_recommendations.execute_plans(plans, apply=True)
    assert len(created_groups) == 1
    assert len(created_records) == 2
    assert all(record.citation_group is created_groups[0] for record in created_records)
    assert not list(options.new_path.iterdir())


def test_classification_review_overlays_plan_on_existing_hierarchy(
    capsys: pytest.CaptureFixture[str],
) -> None:
    article = Article.virtual(name="Example article")
    existing_order = ClassificationEntry.virtual(
        article=article, name="Carnivora", rank=Rank.order, parent=None, page="1"
    )
    existing_genus = ClassificationEntry.virtual(
        article=article, name="Existingus", rank=Rank.genus, parent=None, page="2"
    )
    existing_species = ClassificationEntry.virtual(
        article=article,
        name="Existingus oldus",
        rank=Rank.species,
        parent=existing_genus,
        page="3",
    )
    family_ref = {"model": "ClassificationEntry", "ref": "ce:family"}
    order_id = {"model": "ClassificationEntry", "id": 98, "label": "Carnivora"}
    genus_id = {"model": "ClassificationEntry", "id": 99, "label": "Existingus"}
    family = ClassificationEntry.virtual(
        article=article,
        name="Exampleidae",
        rank=Rank.family,
        parent=existing_order,
        page="1",
    )
    new_species = ClassificationEntry.virtual(
        article=article,
        name="Existingus novus",
        rank=Rank.species,
        parent=existing_genus,
        page="4",
    )
    create_family = generic_recommendations.parse_recommendation(
        _classification_entry_row(
            ref="ce:family",
            name="Exampleidae",
            rank="family",
            page="1",
            parent=order_id,
        ),
        1,
    )
    update_genus = generic_recommendations.parse_recommendation(
        {
            "schema_version": 2,
            "action": "update_object",
            "confidence": "high",
            "reason": "Attach the existing source heading to its source parent.",
            "evidence": [{"kind": "source", "text": "Nested under Exampleidae."}],
            "object": genus_id,
            "changes": [
                {
                    "operation": "set",
                    "field": "parent",
                    "old_value": None,
                    "new_value": family_ref,
                }
            ],
        },
        2,
    )
    create_species = generic_recommendations.parse_recommendation(
        _classification_entry_row(
            ref="ce:new-species",
            name="Existingus novus",
            rank="species",
            page="4",
            parent=genus_id,
        ),
        3,
    )
    plan = generic_recommendations.RecommendationPlan(
        (
            generic_recommendations.PlannedAction(
                create_family,
                family,
                None,
                {
                    "article": article,
                    "name": "Exampleidae",
                    "rank": Rank.family,
                    "parent": existing_order,
                    "page": "1",
                    "authority": None,
                    "year": None,
                },
                already_applied=False,
            ),
            generic_recommendations.PlannedAction(
                update_genus,
                existing_genus,
                None,
                None,
                already_applied=False,
                changes=(("set", "parent", None, family),),
            ),
            generic_recommendations.PlannedAction(
                create_species,
                new_species,
                None,
                {
                    "article": article,
                    "name": "Existingus novus",
                    "rank": Rank.species,
                    "parent": existing_genus,
                    "page": "4",
                    "authority": None,
                    "year": None,
                },
                already_applied=False,
            ),
        ),
        Counter({"create_object": 2, "update_object": 1}),
        {"ce:family": family, "ce:new-species": new_species},
    )
    apply_recommendations.print_classification_review(
        plan,
        get_existing_entries=lambda _article: [
            existing_order,
            existing_genus,
            existing_species,
        ],
    )

    output = capsys.readouterr().out
    assert "Legend: + new, ~ updated, = existing" in output
    assert "  = Carnivora (order) [page 1]" in output
    assert "    + Exampleidae (family) [page 1]" in output
    assert "      ~ Existingus (genus) [page 2]" in output
    assert "        + Existingus novus (species) [page 4]" in output
    assert "Existingus oldus" not in output


def test_review_classification_cli_builds_database_plan(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(
        path,
        [
            _classification_entry_row(
                ref="ce:root", name="Exampleidae", rank="family", page="2"
            )
        ],
    )
    generic_plan = generic_recommendations.RecommendationPlan((), Counter(), {})
    built = False

    def build_plans(_recommendations: object) -> tuple[object, object, object, object]:
        nonlocal built
        built = True
        return object(), generic_plan, object(), object()

    reviewed: list[generic_recommendations.RecommendationPlan] = []
    monkeypatch.setattr(apply_recommendations, "build_plans", build_plans)
    monkeypatch.setattr(
        apply_recommendations,
        "print_classification_review",
        lambda plan: reviewed.append(plan),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--review-classification"]
    )

    apply_recommendations.main()

    assert built
    assert reviewed == [generic_plan]
    assert capsys.readouterr().out == ""


def test_dispatcher_allows_rename_of_location_merge_target() -> None:
    target_rename = location_recommendations.parse_recommendation(
        {
            "schema_version": 1,
            "action": "rename_location",
            "confidence": "high",
            "reason": "Use the canonical target name.",
            "evidence": [{"kind": "source", "text": "Canonical spelling."}],
            "location": {
                "location_id": 3,
                "location_name": "Example Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
            "new_name": "Example Cavern",
        },
        1,
    )
    merge = location_recommendations.parse_recommendation(
        {
            "schema_version": 1,
            "action": "merge_location",
            "confidence": "high",
            "reason": "Merge the duplicate.",
            "evidence": [{"kind": "source", "text": "Same place."}],
            "source": {
                "location_id": 2,
                "location_name": "Example-Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
            "target": {
                "location_id": 3,
                "location_name": "Example Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
        },
        2,
    )

    apply_recommendations._validate_location_rows([merge, target_rename])


def test_dispatcher_allows_edit_of_location_merge_target() -> None:
    target_edit = location_recommendations.parse_recommendation(
        {
            "schema_version": 1,
            "action": "edit_location",
            "confidence": "high",
            "reason": "Add reviewed coordinate bounds.",
            "evidence": [{"kind": "source", "text": "Mapped extent."}],
            "location": {
                "location_id": 3,
                "location_name": "Example Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
            "changes": [
                {"field": "latitude", "old_value": None, "new_value": "1°N-2°N"}
            ],
        },
        1,
    )
    merge = location_recommendations.parse_recommendation(
        {
            "schema_version": 1,
            "action": "merge_location",
            "confidence": "high",
            "reason": "Merge the duplicate.",
            "evidence": [{"kind": "source", "text": "Same place."}],
            "source": {
                "location_id": 2,
                "location_name": "Example-Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
            "target": {
                "location_id": 3,
                "location_name": "Example Cave",
                "region_id": 10,
                "region_name": "Example Region",
                "min_period_id": None,
                "min_period_name": None,
                "max_period_id": None,
                "max_period_name": None,
                "stratigraphic_unit_id": None,
                "stratigraphic_unit_name": None,
            },
        },
        2,
    )

    apply_recommendations._validate_location_rows([merge, target_edit])


def test_build_plans_passes_separately_planned_target_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row_data = _location_row()
    row_data["changes"] = [
        {"field": "name", "old_value": "Example", "new_value": "Modern Example"}
    ]
    location_row = location_recommendations.parse_recommendation(row_data, 1)
    expected_plans = (
        article_recommendations.RecommendationPlan((), Counter()),
        object(),
        object(),
        object(),
    )

    monkeypatch.setattr(
        article_recommendations, "build_plan", lambda rows: expected_plans[0]
    )

    monkeypatch.setattr(
        generic_recommendations, "build_plan", lambda rows: expected_plans[1]
    )
    monkeypatch.setattr(
        location_recommendations, "build_plan", lambda rows: expected_plans[2]
    )

    def build_type_plan(
        rows: object, *, allowed_target_names: dict[int, set[str]]
    ) -> object:
        assert allowed_target_names == {1: {"Modern Example"}}
        return expected_plans[3]

    monkeypatch.setattr(type_recommendations, "build_plan", build_type_plan)
    monkeypatch.setattr(
        type_recommendations, "add_virtual_models", lambda plan, builder: None
    )
    monkeypatch.setattr(
        location_recommendations, "add_virtual_models", lambda plan, builder: None
    )
    monkeypatch.setattr(
        generic_recommendations, "add_virtual_models", lambda plan, builder: None
    )

    plans = apply_recommendations.build_plans(
        apply_recommendations.Recommendations((), (), (location_row,), ())
    )

    assert plans == expected_plans


def test_rejects_cross_family_proposed_location_name_collision() -> None:
    region = Region.virtual(name="Cameroon", kind=RegionKind.country, tags=())
    created = Location.virtual(name="Mount Cameroon", region=region, tags=())
    renamed_origin = Location.virtual(name="Cameroon Mountains", region=region, tags=())
    renamed = renamed_origin.virtual_copy()
    renamed.name = "Mount Cameroon"

    with pytest.raises(
        apply_recommendations.RecommendationError,
        match="proposed Location name 'Mount Cameroon' is not unique",
    ):
        apply_recommendations._validate_proposed_location_name_uniqueness(
            (
                ProposedModel(created, ("type-locality manifest line 1",)),
                ProposedModel(renamed, ("Location manifest line 2",)),
            )
        )


def test_deduplicate_objects_excludes_unresolved_virtual_proposals() -> None:
    redirect = Region.virtual(
        name="Old name",
        kind=RegionKind.redirect,
        parent=Region.virtual(
            name="Current name", kind=RegionKind.country, tags=(), comment=None
        ),
        tags=(),
        comment=None,
    )

    assert apply_recommendations._deduplicate_objects((redirect,)) == ()


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


def test_individual_review_resolves_editors_in_file_order(tmp_path: Path) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_type_row(), _generic_manual_row(), _location_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    events: list[str] = []
    name = _Editable("name", events)
    generic = _Editable("generic", events)
    location = _Editable("location", events)
    generic_plan = _generic_manual_plan(recommendations.generic_rows[0], generic)
    location_plan = location_recommendations.RecommendationPlan(
        (
            location_recommendations.PlannedEdit(
                recommendations.location_rows[0],
                cast(Any, location),
                already_applied=False,
            ),
        ),
        Counter({location_recommendations.EDIT_LOCATION: 1}),
    )

    items = apply_recommendations._resolve_individual_review_items(
        recommendations,
        (
            cast(article_recommendations.RecommendationPlan, object()),
            generic_plan,
            location_plan,
            cast(type_recommendations.RecommendationPlan, object()),
        ),
        get_name=lambda name_id: name,
        label_name=lambda obj: "Example name",
    )

    assert [item.line_number for item in items] == [1, 2, 3]
    assert [item.edit_object for item in items] == [name, generic, location]


def test_individual_review_applies_yes_skips_no_and_edits_immediately(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _location_row(), _type_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    events: list[str] = []
    editable = _Editable("name", events)
    rows = apply_recommendations._all_recommendation_rows(recommendations)
    items = tuple(
        apply_recommendations.IndividualReviewItem(
            row.line_number, row, family, editable if row.line_number == 3 else None
        )
        for family, row in rows
    )
    choices: Iterator[Literal["yes", "no", "edit"]] = iter(("yes", "no", "edit"))
    editable_options: list[bool] = []

    def choose(*, can_edit: bool) -> Literal["yes", "no", "edit"]:
        editable_options.append(can_edit)
        return next(choices)

    applied_selections: list[set[int]] = []

    def apply_selected(
        selected_lines: set[int],
    ) -> apply_recommendations.ExecutionResult:
        applied_selections.append(set(selected_lines))
        events.append(f"apply {sorted(selected_lines)}")
        return apply_recommendations.ExecutionResult(())

    result = apply_recommendations.review_recommendations_individually(
        items, apply_selected=apply_selected, choose=choose
    )

    output = capsys.readouterr().out
    assert result.applied_lines == {1}
    assert not result.aborted
    assert applied_selections == [{1}]
    assert editable_options == [False, False, True]
    assert events == ["apply [1]", "name"]
    assert "Recommendation 1/3 (manifest line 1)" in output
    assert "Recommendation 2/3 (manifest line 2)" in output
    assert "Recommendation 3/3 (manifest line 3)" in output
    assert "The coordinate history remains unresolved." in output
    assert "Add a reviewed comment." in output
    assert "The source is ambiguous." in output
    assert output.index("Recommendation 1/3") < output.index("Recommendation 2/3")
    assert output.index("Recommendation 2/3") < output.index("Recommendation 3/3")
    assert output.index("Recommendation 3/3") < output.index("EDIT name")
    assert output.index("Applied recommendation from manifest line 1") < output.index(
        "Recommendation 2/3"
    )


def test_individual_review_prompt_only_offers_edit_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prompts: list[str] = []

    def get_line(prompt: str, **kwargs: object) -> str:
        prompts.append(prompt)
        return "edit" if "[e]dit" in prompt else "no"

    monkeypatch.setattr(getinput, "get_line", get_line)

    assert (
        apply_recommendations._prompt_individual_review_choice(can_edit=False) == "no"
    )
    assert (
        apply_recommendations._prompt_individual_review_choice(can_edit=True) == "edit"
    )
    assert "[e]dit" not in prompts[0]
    assert "[e]dit" in prompts[1]


def test_individual_review_abort_keeps_earlier_applied_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _location_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    items = tuple(
        apply_recommendations.IndividualReviewItem(
            row.line_number, row, family, edit_object=None
        )
        for family, row in apply_recommendations._all_recommendation_rows(
            recommendations
        )
    )
    choice_count = 0

    def choose(*, can_edit: bool) -> Literal["yes", "no", "edit"]:
        nonlocal choice_count
        choice_count += 1
        if choice_count == 1:
            return "yes"
        raise getinput.StopException

    applied_selections: list[set[int]] = []

    def apply_selected(
        selected_lines: set[int],
    ) -> apply_recommendations.ExecutionResult:
        applied_selections.append(set(selected_lines))
        return apply_recommendations.ExecutionResult(())

    result = apply_recommendations.review_recommendations_individually(
        items, apply_selected=apply_selected, choose=choose
    )

    assert result.aborted
    assert result.applied_lines == {1}
    assert applied_selections == [{1}]
    assert (
        "1 accepted recommendation(s) were already applied" in capsys.readouterr().out
    )


def test_individual_review_failure_does_not_hide_earlier_applied_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _location_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    items = tuple(
        apply_recommendations.IndividualReviewItem(
            row.line_number, row, family, edit_object=None
        )
        for family, row in apply_recommendations._all_recommendation_rows(
            recommendations
        )
    )
    applied_selections: list[set[int]] = []

    def apply_selected(
        selected_lines: set[int],
    ) -> apply_recommendations.ExecutionResult:
        applied_selections.append(set(selected_lines))
        if selected_lines == {2}:
            raise RuntimeError("simulated failure")
        return apply_recommendations.ExecutionResult(())

    with pytest.raises(RuntimeError, match="simulated failure"):
        apply_recommendations.review_recommendations_individually(
            items, apply_selected=apply_selected, choose=lambda **kwargs: "yes"
        )

    assert applied_selections == [{1}, {2}]
    assert "Earlier accepted changes" in capsys.readouterr().out


def test_review_each_replans_only_current_row_and_applies_each_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _location_row()])
    build_calls: list[list[int]] = []
    events: list[str] = []
    fake_plans = (
        object(),
        generic_recommendations.RecommendationPlan((), Counter()),
        object(),
    )

    def build_plans(
        recommendations: apply_recommendations.Recommendations, **kwargs: Any
    ) -> tuple[object, object, object]:
        build_calls.append(
            [
                row.line_number
                for _, row in apply_recommendations._all_recommendation_rows(
                    recommendations
                )
            ]
        )
        return fake_plans

    monkeypatch.setattr(apply_recommendations, "build_plans", build_plans)
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_individual_review_items",
        lambda recommendations, plans: (object(),),
    )

    def review_each(
        items: object,
        *,
        apply_selected: Callable[[set[int]], apply_recommendations.ExecutionResult],
    ) -> apply_recommendations.IndividualReviewResult:
        first = apply_selected({1})
        second = apply_selected({2})
        return apply_recommendations.IndividualReviewResult(
            frozenset({1, 2}),
            apply_recommendations.ExecutionResult(
                (*first.affected_objects, *second.affected_objects)
            ),
            aborted=False,
        )

    monkeypatch.setattr(
        apply_recommendations, "review_recommendations_individually", review_each
    )

    def execute_plans(
        plans: object, *, apply: bool
    ) -> apply_recommendations.ExecutionResult:
        events.append("apply" if apply else "dry-run")
        return apply_recommendations.ExecutionResult(())

    monkeypatch.setattr(apply_recommendations, "execute_plans", execute_plans)
    monkeypatch.setattr(
        apply_recommendations,
        "run_virtual_lint",
        lambda plans, *, issues_only: events.append("virtual lint"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "readonly",
        lambda: pytest.fail("--review-each entered read-only context"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["apply_recommendations.py", str(path), "--review-each", "--virtual-lint"],
    )

    apply_recommendations.main()

    assert build_calls == [[1, 2], [1], [2]]
    assert events == ["virtual lint", "apply", "apply"]


def test_review_each_abort_before_yes_does_not_apply_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (
        object(),
        generic_recommendations.RecommendationPlan((), Counter()),
        object(),
    )
    build_count = 0

    def build_plans(recommendations: object) -> tuple[object, object, object]:
        nonlocal build_count
        build_count += 1
        return fake_plans

    monkeypatch.setattr(apply_recommendations, "build_plans", build_plans)
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_individual_review_items",
        lambda recommendations, plans: (object(),),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "review_recommendations_individually",
        lambda items, *, apply_selected: apply_recommendations.IndividualReviewResult(
            frozenset(), apply_recommendations.ExecutionResult(()), aborted=True
        ),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda *args, **kwargs: pytest.fail("aborted review applied rows"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--review-each"]
    )

    apply_recommendations.main()

    assert build_count == 1


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
        lambda recommendations: (object(), fake_generic_plan, object(), object()),
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


def test_virtual_lint_runs_before_dry_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    events: list[str] = []
    fake_plans = (object(), object(), object())

    @contextmanager
    def readonly() -> Iterator[None]:
        events.append("read-only enter")
        try:
            yield
        finally:
            events.append("read-only exit")

    monkeypatch.setattr(apply_recommendations, "readonly", readonly)
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "run_virtual_lint",
        lambda plans, *, issues_only: events.append(
            "issues only" if issues_only else "virtual lint"
        ),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply" if apply else "dry-run"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--virtual-lint"]
    )

    apply_recommendations.main()

    assert events == ["read-only enter", "virtual lint", "dry-run", "read-only exit"]


def test_virtual_lint_issues_only_implies_virtual_lint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object())
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "run_virtual_lint",
        lambda plans, *, issues_only: events.append(
            "issues only" if issues_only else "virtual lint"
        ),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda *_args, **_kwargs: pytest.fail("issues-only mode ran a dry run"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["apply_recommendations.py", str(path), "--virtual-lint-issues-only"],
    )

    apply_recommendations.main()

    assert events == ["issues only"]


def test_apply_mode_does_not_enter_read_only_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object())
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations, "execute_plans", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        apply_recommendations,
        "readonly",
        lambda: pytest.fail("apply mode entered read-only context"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--apply",
            "--no-edit-applied",
            "--no-edit-manual",
        ],
    )

    apply_recommendations.main()


def test_build_plans_skipping_invalid_retries_line_attributable_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    rows = [_generic_manual_row() for _ in range(3)]
    _write(path, rows)
    recommendations = apply_recommendations.read_recommendations(path)
    calls: list[list[int]] = []
    fake_plans = object()

    def build_plans(selected: apply_recommendations.Recommendations) -> object:
        lines = [
            row.line_number
            for _, row in apply_recommendations._all_recommendation_rows(selected)
        ]
        calls.append(lines)
        if 2 in lines:
            raise apply_recommendations.RecommendationError(
                "database validation failed:\n- line 2: stale second row"
            )
        if 3 in lines:
            raise apply_recommendations.RecommendationError(
                "database validation failed:\n- line 3: stale third row"
            )
        return fake_plans

    monkeypatch.setattr(apply_recommendations, "build_plans", build_plans)

    filtered, plans, skipped = apply_recommendations.build_plans_skipping_invalid(
        recommendations
    )

    assert calls == [[1, 2, 3], [1, 3], [1]]
    assert plans is fake_plans
    assert [row.line_number for row in filtered.generic_rows] == [1]
    assert [(item.line_number, item.reason) for item in skipped] == [
        (2, "stale second row"),
        (3, "stale third row"),
    ]


def test_build_plans_skipping_invalid_rejects_unattributed_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    recommendations = apply_recommendations.read_recommendations(path)
    monkeypatch.setattr(
        apply_recommendations,
        "build_plans",
        lambda _recommendations: (_ for _ in ()).throw(
            apply_recommendations.RecommendationError("global reference conflict")
        ),
    )

    with pytest.raises(
        apply_recommendations.RecommendationError,
        match=r"could not attribute.*refusing to guess",
    ):
        apply_recommendations.build_plans_skipping_invalid(recommendations)


def test_skip_invalid_cli_applies_only_revalidated_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row(), _generic_manual_row()])
    fake_plans = object()
    build_calls: list[list[int]] = []
    applied: list[object] = []

    def build_plans(selected: apply_recommendations.Recommendations) -> object:
        lines = [
            row.line_number
            for _, row in apply_recommendations._all_recommendation_rows(selected)
        ]
        build_calls.append(lines)
        if 2 in lines:
            raise apply_recommendations.RecommendationError(
                "database validation failed:\n- line 2: stale row"
            )
        return fake_plans

    def execute_plans(plans: object, *, apply: bool) -> object:
        assert apply is True
        applied.append(plans)
        return apply_recommendations.ExecutionResult(())

    monkeypatch.setattr(apply_recommendations, "build_plans", build_plans)
    monkeypatch.setattr(apply_recommendations, "execute_plans", execute_plans)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--apply",
            "--skip-invalid",
            "--no-edit-applied",
            "--no-edit-manual",
        ],
    )

    apply_recommendations.main()

    assert build_calls == [[1, 2], [1]]
    assert applied == [fake_plans]
    output = capsys.readouterr().out
    assert "SKIP_INVALID line=2 family=generic action=manual_review" in output
    assert "1 recommendation(s) remain eligible" in output


def test_virtual_lint_combines_with_review_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object())
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "run_virtual_lint",
        lambda plans, *, issues_only: events.append(
            "issues only" if issues_only else "virtual lint"
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["apply_recommendations.py", str(path), "--review", "--virtual-lint"],
    )

    apply_recommendations.main()

    assert "GENERIC RECOMMENDATIONS" in capsys.readouterr().out
    assert events == ["virtual lint"]


def test_review_each_combines_with_review_manual_and_edit_manual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (
        object(),
        generic_recommendations.RecommendationPlan((), Counter()),
        object(),
    )
    manual_items = (object(),)
    events: list[str] = []

    def review_each(
        items: object,
        *,
        apply_selected: Callable[[set[int]], apply_recommendations.ExecutionResult],
    ) -> apply_recommendations.IndividualReviewResult:
        events.append("review each")
        result = apply_selected({1})
        return apply_recommendations.IndividualReviewResult(
            frozenset({1}), result, aborted=False
        )

    monkeypatch.setattr(
        apply_recommendations,
        "print_manual_reviews",
        lambda recommendations: events.append("review manual"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "build_plans",
        lambda recommendations, **kwargs: fake_plans,
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_individual_review_items",
        lambda recommendations, plans: (object(),),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_manual_review_objects",
        lambda recommendations, generic_plan: manual_items,
    )
    monkeypatch.setattr(
        apply_recommendations, "review_recommendations_individually", review_each
    )

    def execute_plans(
        plans: object, *, apply: bool
    ) -> apply_recommendations.ExecutionResult:
        events.append("apply" if apply else "dry-run")
        return apply_recommendations.ExecutionResult(())

    monkeypatch.setattr(apply_recommendations, "execute_plans", execute_plans)
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("edit manual"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--review-manual",
            "--review-each",
            "--edit-manual",
        ],
    )

    apply_recommendations.main()

    assert events == ["review manual", "review each", "apply", "edit manual"]


def test_views_dry_run_lint_apply_and_edit_run_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object())
    manual_items = (object(),)
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations,
        "print_review",
        lambda recommendations, *, actions=None: events.append("review"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "print_manual_reviews",
        lambda recommendations: events.append("review manual"),
    )
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_manual_review_objects",
        lambda recommendations, generic_plan: manual_items,
    )
    monkeypatch.setattr(
        apply_recommendations,
        "run_virtual_lint",
        lambda plans, *, issues_only: events.append(
            "issues only" if issues_only else "virtual lint"
        ),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply" if apply else "dry-run"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("edit manual"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--review",
            "--review-manual",
            "--virtual-lint",
            "--dry-run",
            "--apply",
            "--edit-manual",
        ],
    )

    apply_recommendations.main()

    assert events == [
        "review",
        "review manual",
        "virtual lint",
        "dry-run",
        "apply",
        "edit manual",
    ]


def test_apply_remains_incompatible_with_review_each(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--apply", "--review-each"]
    )

    with pytest.raises(SystemExit):
        apply_recommendations.main()

    assert "--review-each selects individual rows" in capsys.readouterr().err


def test_edit_applied_requires_an_application_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--edit-applied"]
    )

    with pytest.raises(SystemExit):
        apply_recommendations.main()

    assert "--edit-applied requires --apply or --review-each" in capsys.readouterr().err


def test_edit_each_requires_an_application_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--edit-each"]
    )

    with pytest.raises(SystemExit):
        apply_recommendations.main()

    assert "--edit-each requires --apply or --review-each" in capsys.readouterr().err


def test_edit_each_requires_post_apply_cleanup(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--apply",
            "--edit-each",
            "--no-edit-applied",
        ],
    )

    with pytest.raises(SystemExit):
        apply_recommendations.main()

    assert "--edit-each cannot be combined with --no-edit-applied" in (
        capsys.readouterr().err
    )


@pytest.mark.parametrize(
    ("extra_args", "expected_edit_each"), [([], False), (["--edit-each"], True)]
)
def test_apply_runs_cleanup_and_manual_edit_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    extra_args: list[str],
    expected_edit_each: bool,
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object(), object())
    result = apply_recommendations.ExecutionResult(())
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_manual_review_objects",
        lambda recommendations, generic_plan: (object(),),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply") or result,  # type: ignore[func-returns-value]
    )
    monkeypatch.setattr(
        apply_recommendations,
        "edit_applied_objects",
        lambda execution_result, *, edit_each: events.append(f"cleanup:{edit_each}")  # type: ignore[func-returns-value]
        or True,
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("manual"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--apply", *extra_args]
    )

    apply_recommendations.main()

    assert events == ["apply", f"cleanup:{expected_edit_each}", "manual"]


def test_incomplete_cleanup_warns_and_continues_to_manual_edit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object(), object())
    result = apply_recommendations.ExecutionResult(())
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_manual_review_objects",
        lambda recommendations, generic_plan: (object(),),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply") or result,  # type: ignore[func-returns-value]
    )
    monkeypatch.setattr(
        apply_recommendations,
        "edit_applied_objects",
        lambda execution_result, *, edit_each: events.append(f"cleanup:{edit_each}")  # type: ignore[func-returns-value]
        or False,
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_edit_manual_review_objects",
        lambda items: events.append("manual"),
    )
    monkeypatch.setattr(sys, "argv", ["apply_recommendations.py", str(path), "--apply"])

    apply_recommendations.main()

    captured = capsys.readouterr()
    assert events == ["apply", "cleanup:False", "manual"]
    assert "Warning: post-apply cleanup was incomplete" in captured.out
    assert "Continuing." in captured.out
    assert "usage:" not in captured.out
    assert captured.err == ""


def test_apply_cleanup_and_manual_edit_can_be_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(path, [_generic_manual_row()])
    fake_plans = (object(), object(), object(), object())
    result = apply_recommendations.ExecutionResult(())
    events: list[str] = []
    monkeypatch.setattr(
        apply_recommendations, "build_plans", lambda recommendations: fake_plans
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda plans, *, apply: events.append("apply") or result,  # type: ignore[func-returns-value]
    )
    monkeypatch.setattr(
        apply_recommendations,
        "edit_applied_objects",
        lambda execution_result, *, edit_each: pytest.fail("cleanup was not disabled"),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "_resolve_manual_review_objects",
        lambda *_args, **_kwargs: pytest.fail("manual editing was not disabled"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "apply_recommendations.py",
            str(path),
            "--apply",
            "--no-edit-applied",
            "--no-edit-manual",
        ],
    )

    apply_recommendations.main()

    assert events == ["apply"]


@pytest.mark.parametrize("edit_each", [False, True])
@pytest.mark.parametrize(
    ("has_issues", "discover_on_edit"), [(False, False), (True, False), (True, True)]
)
def test_cleanup_includes_objects_created_by_autofix(
    monkeypatch: pytest.MonkeyPatch,
    *,
    edit_each: bool,
    has_issues: bool,
    discover_on_edit: bool,
) -> None:
    events: list[str] = []
    active_recorder: list[Any] = []

    class CleanupObject:
        label_field = "label"

        @classmethod
        def clear_lint_caches(cls) -> None:
            pass

        def __init__(
            self, object_id: int, label: str, discovered: CleanupObject | None = None
        ) -> None:
            self.id = object_id
            self.label = label
            self.discovered = discovered

        def reload(self) -> None:
            events.append(f"reload:{self.label}")

        def general_lint(self, cfg: Any) -> list[str]:
            assert cfg.autofix
            assert not cfg.interactive
            events.append(f"automatic:{self.label}")
            if self.discovered is not None and not discover_on_edit:
                active_recorder[0]._objects.append(self.discovered)
                active_recorder[0].event_count += 1
                self.discovered = None
            return [f"issue:{self.label}"] if has_issues else []

        def display(self) -> None:
            events.append(f"display:{self.label}")

        def edit_until_clean(self, *, cfg: Any) -> None:
            assert cfg.autofix
            assert cfg.interactive
            events.append(f"edit:{self.label}")
            if self.discovered is not None:
                active_recorder[0]._objects.append(self.discovered)
                self.discovered = None

        def edit(self) -> None:
            events.append(f"interactive:{self.label}")

        def is_lint_clean(self, *, cfg: Any) -> bool:
            assert not cfg.autofix
            assert not cfg.interactive
            events.append(f"lint:{self.label}")
            return True

    child = CleanupObject(2, "child")
    parent = CleanupObject(1, "parent", child)

    class Recorder:
        def __init__(self) -> None:
            self._objects: list[Any] = []
            self.event_count = 0

        def __enter__(self) -> Self:
            active_recorder[:] = [self]
            return self

        def __exit__(self, *_args: object) -> None:
            active_recorder.clear()

        @property
        def objects(self) -> tuple[Any, ...]:
            return tuple(self._objects)

        def last_event(self, obj: Any) -> int:
            return 0

    monkeypatch.setattr(apply_recommendations, "AppliedObjectRecorder", Recorder)

    assert apply_recommendations.edit_applied_objects(
        apply_recommendations.ExecutionResult((cast(Any, parent),)), edit_each=edit_each
    )
    expected = ["reload:parent", "automatic:parent"]
    if not discover_on_edit:
        expected.extend(["reload:child", "automatic:child"])
        expected.extend(["reload:parent", "automatic:parent"])
        if has_issues:
            expected.extend(["reload:child", "automatic:child"])
    if has_issues or edit_each:
        for label in ("parent", "child"):
            if label == "child" and discover_on_edit:
                expected.extend(["reload:child", "automatic:child"])
            expected.append(f"reload:{label}")
            if has_issues:
                expected.extend([f"display:{label}", f"edit:{label}"])
            if edit_each:
                expected.append(f"interactive:{label}")
            expected.extend([f"reload:{label}", f"lint:{label}"])
    assert events == expected


@pytest.mark.parametrize("fix_kind", ["structured", "legacy"])
@pytest.mark.parametrize("fix_count", [0, 2, 10, 12])
@pytest.mark.parametrize("unresolved", [False, True])
def test_cleanup_repeats_lint_until_stable_or_limit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fix_kind: str,
    fix_count: int,
    *,
    unresolved: bool,
) -> None:
    calls: list[str] = []

    class CleanupObject:
        label_field = "id"
        id = 1
        save_event: model_events.Event[Any] = model_events.Event()

        @classmethod
        def clear_lint_caches(cls) -> None:
            pass

        def __init__(self) -> None:
            self.fixes_applied = 0

        def reload(self) -> None:
            pass

        def general_lint(self, cfg: Any) -> list[str]:
            assert not cfg.interactive
            calls.append("automatic" if cfg.autofix else "check")
            if cfg.autofix and self.fixes_applied < fix_count:
                self.fixes_applied += 1
                if fix_kind == "structured":
                    cfg.fix_callback(LintIssue("applied fix"))
                else:
                    self.save_event.trigger(self)
                # Simulate a message yielded before an autofix resolved it.
                return ["stale issue"]
            return (
                ["unresolved"] if unresolved or self.fixes_applied < fix_count else []
            )

        def display(self) -> None:
            pass

        def edit_until_clean(self, *, cfg: Any) -> None:
            assert cfg.interactive
            calls.append("edit")

        def is_lint_clean(self, *, cfg: Any) -> bool:
            return True

    obj = CleanupObject()
    recorder = apply_recommendations.AppliedObjectRecorder([cast(Any, CleanupObject)])
    monkeypatch.setattr(
        apply_recommendations, "AppliedObjectRecorder", lambda: recorder
    )
    assert apply_recommendations.edit_applied_objects(
        apply_recommendations.ExecutionResult((cast(Any, obj),))
    )
    expected = ["automatic"] * min(fix_count + 1, 10)
    if fix_count >= 10:
        expected.append("check")
    if unresolved or fix_count > 10:
        expected.append("edit")
    assert calls == expected
    assert obj.fixes_applied == min(fix_count, 10)
    assert ("reached 10 automatic lint passes" in capsys.readouterr().out) == (
        fix_count >= 10
    )
    assert not CleanupObject.save_event.handlers


@pytest.mark.parametrize("include_name", [False, True])
def test_cleanup_revisits_entry_after_mapped_name_autofix(
    monkeypatch: pytest.MonkeyPatch, *, include_name: bool
) -> None:
    """The CE warning is resolved by a later Name fix, with fresh lint caches."""
    earlier = Article.virtual(
        name="Thomas 1901", year="1901-09-01", author_tags=(), type=ArticleType.JOURNAL
    )
    later = Article.virtual(name="Thomas 1902", year="1902-04-01")
    name = Name.virtual(
        corrected_original_name="Eligmodontia callosa",
        original_name="Eligmodontia callosa",
        original_citation=later,
        year=later.year,
        page_described="241",
        author_tags=(),
        type_tags=(),
    )
    entry = ClassificationEntry.virtual(
        name="Eligmodontia callosa",
        rank=Rank.species,
        article=earlier,
        mapped_name=name,
        page="253",
    )
    calls: list[str] = []

    @cache
    def name_date(nam: Name) -> date:
        assert nam.year is not None
        return date.fromisoformat(nam.year)

    def entry_lint(self: ClassificationEntry, cfg: Any) -> list[LintResult]:
        calls.append("ce")
        return list(check_predates_mapped_name.linter(self, cfg))

    def name_lint(self: Name, cfg: Any) -> list[Any]:
        calls.append("name")
        if self.original_citation == earlier:
            return []
        return list(
            self._process_lint_results(
                [take_over_name_issue("use earlier citation", self, entry)], cfg
            )
        )

    monkeypatch.setattr(Article, "is_unpublished", lambda _: False)
    monkeypatch.setattr(
        Article, "get_date_object", lambda self: date.fromisoformat(self.year)
    )
    monkeypatch.setattr(Name, "get_date_object", name_date)
    monkeypatch.setattr(
        Name, "__str__", lambda self: f"Eligmodontia callosa ({self.year})"
    )
    monkeypatch.setattr(Name, "general_lint", name_lint)
    monkeypatch.setattr(ClassificationEntry, "general_lint", entry_lint)
    for model in (Name, ClassificationEntry):
        monkeypatch.setattr(model, "reload", lambda _: None)
        monkeypatch.setattr(model, "clear_lint_caches", name_date.cache_clear)
        monkeypatch.setattr(
            model,
            "edit_until_clean",
            lambda *args, **kwargs: pytest.fail("stale issue"),
        )
    # Exercise the cleanup scheduler on isolated virtual objects, including the
    # real CE check and Name fix, without opening or changing the live database.
    monkeypatch.setattr(apply_recommendations, "_deduplicate_objects", tuple)
    assert entry_lint(entry, LintConfig(autofix=False)) == [
        "predates mapped name Eligmodontia callosa (1902-04-01)"
    ]
    calls.clear()
    objects = (entry, name) if include_name else (entry,)
    assert apply_recommendations.edit_applied_objects(
        apply_recommendations.ExecutionResult(objects)
    )
    assert calls == ["ce", "name", "ce", "name"]
    assert name.original_citation == earlier
    assert name.year == "1901-09-01"
    assert name.page_described == "253"


@pytest.mark.parametrize("edit_each", [False, True])
@pytest.mark.parametrize("change_clean", [False, True])
def test_cleanup_skips_clean_objects_unless_saved_by_another_lint(
    monkeypatch: pytest.MonkeyPatch, *, edit_each: bool, change_clean: bool
) -> None:
    calls: Counter[int] = Counter()
    edits: list[int] = []

    class CleanupObject:
        label_field = "id"
        save_event: model_events.Event[Any] = model_events.Event()

        def __init__(self, object_id: int) -> None:
            self.id = object_id
            self.dirty = False

        @classmethod
        def clear_lint_caches(cls) -> None:
            pass

        def reload(self) -> None:
            pass

        def general_lint(self, cfg: Any) -> list[str]:
            assert cfg.autofix and not cfg.interactive
            calls[self.id] += 1
            if self.dirty:
                self.dirty = False
                cfg.fix_callback(LintIssue("fixed after related save"))
            if self.id == 2 and calls[self.id] <= 2:
                cfg.fix_callback(LintIssue("applied fix without remaining issues"))
                if change_clean:
                    clean.dirty = True
                    self.save_event.trigger(clean)
            return ["unresolved"] if self.id == 3 else []

        def display(self) -> None:
            pass

        def edit_until_clean(self, *, cfg: Any) -> None:
            assert self.id == 3

        def edit(self) -> None:
            edits.append(self.id)

        def is_lint_clean(self, *, cfg: Any) -> bool:
            return True

    clean, fixer, unresolved = (CleanupObject(i) for i in (1, 2, 3))
    recorder = apply_recommendations.AppliedObjectRecorder([cast(Any, CleanupObject)])
    monkeypatch.setattr(
        apply_recommendations, "AppliedObjectRecorder", lambda: recorder
    )
    assert apply_recommendations.edit_applied_objects(
        apply_recommendations.ExecutionResult(cast(Any, (clean, fixer, unresolved))),
        edit_each=edit_each,
    )
    assert calls == {1: 4 if change_clean else 1, 2: 3, 3: 4 if change_clean else 3}
    assert edits == ([1, 2, 3] if edit_each else [])
    assert not clean.dirty


def test_cleanup_continues_after_lint_failure_and_reports_residual_issues(
    capsys: pytest.CaptureFixture[str],
) -> None:
    events: list[str] = []

    class CleanupObject:
        label_field = "id"

        @classmethod
        def clear_lint_caches(cls) -> None:
            pass

        def __init__(self, object_id: int) -> None:
            self.id = object_id

        def reload(self) -> None:
            pass

        def general_lint(self, cfg: Any) -> list[str]:
            events.append(f"automatic:{self.id}")
            if self.id == 1:
                raise ValueError("broken lint")
            return ["unresolved"]

        def display(self) -> None:
            pass

        def edit_until_clean(self, *, cfg: Any) -> None:
            # Returning without fixing models a user leaving the editor.
            events.append(f"edit:{self.id}")

        def is_lint_clean(self, *, cfg: Any) -> bool:
            return self.id == 3

    objects = tuple(cast(Any, CleanupObject(i)) for i in (1, 2, 3))
    assert not apply_recommendations.edit_applied_objects(
        apply_recommendations.ExecutionResult(objects)
    )
    assert events == ["automatic:1", "automatic:2", "automatic:3", "edit:2", "edit:3"]
    captured = capsys.readouterr()
    assert "CleanupObject:1: broken lint" in captured.out
    assert "CleanupObject:2 has residual lint" in captured.out
    assert "total=3 clean=1 incomplete=2" in captured.out


def test_replacement_action_registration_and_interactions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from taxonomy.applicator import staged_file

    row = {
        "schema_version": 1,
        "action": "replace_article_file",
        "confidence": "high",
        "reason": "Replace incomplete PDF with verified full source.",
        "evidence": [{"kind": "source", "text": "Both pages verified."}],
        "article": {"id": 1, "name": "Source.pdf", "path": None},
        "old_file": {"sha256": "a" * 64, "size": 10},
        "file": {"source_path": "replacement.pdf", "sha256": "b" * 64, "size": 20},
    }
    mutation = {
        "schema_version": 1,
        "action": "set_field",
        "confidence": "high",
        "reason": "Conflicting rename.",
        "evidence": [{"kind": "source", "text": "Test."}],
        "object": {"model": "Article", "id": 1, "label": "Source.pdf"},
        "field": "name",
        "old_value": "Source.pdf",
        "new_value": "Other.pdf",
    }
    manifest = tmp_path / "rows.jsonl"
    manifest.write_text(json.dumps(row) + "\n" + json.dumps(mutation) + "\n")
    parsed = apply_recommendations.read_recommendations(manifest)
    assert parsed.count == 2
    assert parsed.staged_file_rows[0].action == "replace_article_file"
    monkeypatch.setattr(
        staged_file,
        "build_plan",
        lambda _rows: staged_file.RecommendationPlan((), Counter()),
    )
    with pytest.raises(
        apply_recommendations.RecommendationError, match="overlaps PDF replacement"
    ):
        apply_recommendations.build_plans(parsed)


def test_intake_rejects_cross_family_file_destinations(tmp_path: Path) -> None:
    action = SimpleNamespace(
        source_path=tmp_path / "source1",
        destination_path=tmp_path / "destination",
        recommendation=SimpleNamespace(line_number=1, action="create_article"),
    )
    conflicting = SimpleNamespace(
        source_path=tmp_path / "source2",
        destination_path=tmp_path / "destination",
        recommendation=SimpleNamespace(line_number=2, action="replace_article_file"),
    )
    with pytest.raises(
        apply_recommendations.RecommendationError, match="destination is also used"
    ):
        apply_recommendations._validate_unique_intake_sources(
            cast(Any, SimpleNamespace(actions=(action,))),
            cast(Any, SimpleNamespace(actions=())),
            cast(Any, SimpleNamespace(actions=(conflicting,))),
        )
    conflicting.destination_path = tmp_path / "source1"
    with pytest.raises(
        apply_recommendations.RecommendationError,
        match="intake source is also a file destination",
    ):
        apply_recommendations._validate_unique_intake_sources(
            cast(Any, SimpleNamespace(actions=(action,))),
            cast(Any, SimpleNamespace(actions=())),
            cast(Any, SimpleNamespace(actions=(conflicting,))),
        )


@pytest.mark.parametrize("schema_version", [1, 2])
def test_review_each_keeps_created_references_without_replaying_mutated_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_version: int
) -> None:
    path = tmp_path / "recommendations.jsonl"
    rows = [
        _classification_entry_row(ref="parent", name="Mus", rank="genus", page="1"),
        _classification_entry_row(
            ref="child",
            name="Mus testus",
            rank="species",
            page="2",
            parent={"model": "ClassificationEntry", "ref": "parent", "label": "Mus"},
        ),
        _classification_entry_row(
            ref="other",
            name="Mus alter",
            rank="species",
            page="3",
            parent={"model": "ClassificationEntry", "ref": "parent", "label": "Mus"},
        ),
    ]
    for row in rows:
        row["schema_version"] = schema_version
        if schema_version == 2:
            row["match"] = {"name": cast(dict[str, Any], row["values"])["name"]}
    _write(path, rows)
    article = Article.virtual(name="Example article")
    monkeypatch.setattr(generic_recommendations, "_get_object", lambda *args: article)
    original_build = generic_recommendations.build_plan
    built_rows: list[list[int]] = []

    def build_generic(rows: Any, **kwargs: Any) -> Any:
        built_rows.append([row.line_number for row in rows])
        return original_build(
            rows,
            **kwargs,
            find_objects_by_label=lambda *args: [],
            find_objects_by_match=lambda *args: [],
            get_unique_constraints=lambda model: (),
        )

    monkeypatch.setattr(generic_recommendations, "build_plan", build_generic)
    created: list[ClassificationEntry] = []

    def create_object(model: Any, values: Any) -> ClassificationEntry:
        obj = model.virtual(**values)
        if created:
            assert obj.parent is created[0]
            # Replaying the parent's original creation would now fail its guards.
            created[0].page = "1-2"
        created.append(obj)
        return obj

    monkeypatch.setattr(
        generic_recommendations,
        "execute_plan",
        partial(generic_recommendations.execute_plan, create_object=create_object),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "review_recommendations_individually",
        partial(
            apply_recommendations.review_recommendations_individually,
            choose=lambda **kwargs: "yes",
        ),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--review-each"]
    )
    apply_recommendations.main()
    assert built_rows == [[1, 2, 3], [1], [2], [3]]
    assert len(created) == 3
    assert created[0].page == "1-2"


def test_review_each_skips_applied_rows_without_prompting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "recommendations.jsonl"
    row = _classification_entry_row(ref="existing", name="Mus", rank="genus", page="1")
    _write(path, [row])
    article = Article.virtual(name="Example article")
    ce = ClassificationEntry.virtual(
        article=article, name="Mus", rank=Rank.genus, page="1"
    )
    monkeypatch.setattr(generic_recommendations, "_get_object", lambda *args: article)
    generic_plan = generic_recommendations.build_plan(
        apply_recommendations.read_recommendations(path).generic_rows,
        find_objects_by_label=lambda *args: [ce],
    )
    plans = (
        article_recommendations.RecommendationPlan((), Counter()),
        generic_plan,
        location_recommendations.RecommendationPlan((), Counter()),
        object(),
    )
    items = apply_recommendations._resolve_individual_review_items(
        apply_recommendations.read_recommendations(path), cast(Any, plans)
    )
    assert items[0].already_applied
    result = apply_recommendations.review_recommendations_individually(
        items,
        apply_selected=lambda lines: pytest.fail("applied an existing row"),
        choose=lambda **kwargs: pytest.fail("prompted for an existing row"),
    )
    assert not result.applied_lines
    assert "SKIP_ALREADY_APPLIED line=1 action=create_object" in capsys.readouterr().out


def test_review_each_rejects_dependency_on_declined_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "recommendations.jsonl"
    _write(
        path,
        [
            _classification_entry_row(ref="parent", name="Mus", rank="genus", page="1"),
            _classification_entry_row(
                ref="child",
                name="Mus testus",
                rank="species",
                page="2",
                parent={
                    "model": "ClassificationEntry",
                    "ref": "parent",
                    "label": "Mus",
                },
            ),
        ],
    )
    article = Article.virtual(name="Example article")
    monkeypatch.setattr(generic_recommendations, "_get_object", lambda *args: article)
    monkeypatch.setattr(
        generic_recommendations,
        "build_plan",
        partial(
            generic_recommendations.build_plan,
            find_objects_by_label=lambda *args: [],
            get_unique_constraints=lambda model: (),
        ),
    )
    choices: Iterator[Literal["yes", "no", "edit"]] = iter(("no", "yes"))
    monkeypatch.setattr(
        apply_recommendations,
        "review_recommendations_individually",
        partial(
            apply_recommendations.review_recommendations_individually,
            choose=lambda **kwargs: next(choices),
        ),
    )
    monkeypatch.setattr(
        apply_recommendations,
        "execute_plans",
        lambda *args, **kwargs: pytest.fail("must not write"),
    )
    monkeypatch.setattr(
        sys, "argv", ["apply_recommendations.py", str(path), "--review-each"]
    )
    with pytest.raises(SystemExit):
        apply_recommendations.main()
