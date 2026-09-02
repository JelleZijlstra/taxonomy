import copy
import hashlib
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy.applicator import item_file as recommendations
from taxonomy.db.constants import ArticleType
from taxonomy.db.models import CitationGroup, ItemFile
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.item_file import ItemFileTag, lint_detect_url


def _pdf_bytes() -> bytes:
    return b"%PDF-1.4\nminimal item fixture\n%%EOF\n"


def _row(pdf: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "create_item_file",
        "confidence": "high",
        "reason": "The reviewed title page identifies the source item.",
        "evidence": [{"kind": "title_page", "text": "Journal of Examples. Volume 12."}],
        "item_file": {
            "filename": "source item.pdf",
            "citation_group": {"id": 12, "name": "Journal of Examples"},
            "fields": {
                "title": "Reviewed item",
                "volume": "12",
                "url": "https://example.com/item/12",
            },
            "tags": [[1, "Complete source item."]],
        },
        "file": {
            "source_path": "Burst/Old/source item.pdf",
            "sha256": hashlib.sha256(pdf).hexdigest(),
            "size": len(pdf),
        },
    }


def _citation_group() -> CitationGroup:
    return cast(
        CitationGroup,
        SimpleNamespace(
            id=12,
            name="Journal of Examples",
            type=ArticleType.JOURNAL,
            is_invalid=lambda: False,
        ),
    )


def _options(tmp_path: Path) -> SimpleNamespace:
    new_path = tmp_path / "new"
    item_file_path = tmp_path / "items"
    (new_path / "Burst" / "Old").mkdir(parents=True)
    item_file_path.mkdir()
    return SimpleNamespace(new_path=new_path, item_file_path=item_file_path)


def _build(tmp_path: Path) -> recommendations.RecommendationPlan:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    (options.new_path / "Burst" / "Old" / "source item.pdf").write_bytes(pdf)
    return recommendations.build_plan(
        (recommendations.parse_recommendation(_row(pdf), 1),),
        options=options,
        get_citation_group=lambda _id: _citation_group(),
        get_item_file=lambda _filename: None,
    )


def test_parse_rejects_nested_destination_filename() -> None:
    row = _row(_pdf_bytes())
    row["item_file"]["filename"] = "nested/item.pdf"

    with pytest.raises(
        recommendations.RecommendationError, match="must be a plain filename"
    ):
        recommendations.parse_recommendation(row, 1)


def test_build_plan_validates_file_and_metadata(tmp_path: Path) -> None:
    plan = _build(tmp_path)

    action = plan.actions[0]
    assert action.destination_path.name == "source item.pdf"
    assert action.fields["volume"] == "12"
    assert action.tags[0].text == "Complete source item."
    assert not action.already_applied


def test_build_plan_rejects_stale_citation_group_name(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    source = options.new_path / "Burst" / "Old" / "source item.pdf"
    source.write_bytes(pdf)
    stale = cast(
        CitationGroup,
        SimpleNamespace(id=12, name="Renamed Journal", is_invalid=lambda: False),
    )

    with pytest.raises(
        recommendations.RecommendationError, match="is not valid with name"
    ):
        recommendations.build_plan(
            (recommendations.parse_recommendation(_row(pdf), 1),),
            options=options,
            get_citation_group=lambda _id: stale,
            get_item_file=lambda _filename: None,
        )


def test_build_plan_rejects_conflicting_existing_item_file(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    source = options.new_path / "Burst" / "Old" / "source item.pdf"
    source.write_bytes(pdf)
    existing = cast(
        ItemFile,
        SimpleNamespace(
            filename="source item.pdf",
            citation_group=_citation_group(),
            tags=(),
            title="Different title",
            series=None,
            volume="12",
            issue=None,
            start_page=None,
            end_page=None,
            url="https://example.com/item/12",
        ),
    )

    with pytest.raises(recommendations.RecommendationError, match="conflicts"):
        recommendations.build_plan(
            (recommendations.parse_recommendation(_row(pdf), 1),),
            options=options,
            get_citation_group=lambda _id: _citation_group(),
            get_item_file=lambda _filename: existing,
        )


def test_build_plan_rejects_changed_checksum(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    source = options.new_path / "Burst" / "Old" / "source item.pdf"
    source.write_bytes(pdf + b"changed")

    with pytest.raises(recommendations.RecommendationError, match="size changed"):
        recommendations.build_plan(
            (recommendations.parse_recommendation(_row(pdf), 1),),
            options=options,
            get_citation_group=lambda _id: _citation_group(),
            get_item_file=lambda _filename: None,
        )


def test_build_plan_rejects_duplicate_source(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    source = options.new_path / "Burst" / "Old" / "source item.pdf"
    source.write_bytes(pdf)
    second = copy.deepcopy(_row(pdf))
    second["item_file"]["filename"] = "second.pdf"
    rows = (
        recommendations.parse_recommendation(_row(pdf), 1),
        recommendations.parse_recommendation(second, 2),
    )

    with pytest.raises(
        recommendations.RecommendationError, match="used by more than one"
    ):
        recommendations.build_plan(
            rows,
            options=options,
            get_citation_group=lambda _id: _citation_group(),
            get_item_file=lambda _filename: None,
        )


def test_execute_installs_file_creates_object_and_removes_source(
    tmp_path: Path,
) -> None:
    plan = _build(tmp_path)
    action = plan.actions[0]
    created_values: list[Mapping[str, Any]] = []
    created = cast(ItemFile, SimpleNamespace(filename="source item.pdf"))

    def create_item_file(values: Mapping[str, Any]) -> ItemFile:
        created_values.append(values)
        return created

    result = recommendations.execute_plan(
        plan, apply=True, create_item_file=create_item_file
    )

    assert result == (created,)
    assert action.destination_path.read_bytes() == _pdf_bytes()
    assert not action.source_path.exists()
    assert created_values[0]["filename"] == "source item.pdf"
    assert created_values[0]["volume"] == "12"
    assert created_values[0]["issue"] is None


def test_dry_run_does_not_move_or_create(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    action = plan.actions[0]
    created = False

    def create_item_file(_values: Mapping[str, Any]) -> ItemFile:
        nonlocal created
        created = True
        return cast(ItemFile, SimpleNamespace(filename="source item.pdf"))

    recommendations.execute_plan(plan, apply=False, create_item_file=create_item_file)

    assert action.source_path.exists()
    assert not action.destination_path.exists()
    assert not created


def test_restart_after_file_install_creates_missing_database_object(
    tmp_path: Path,
) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    destination = options.item_file_path / "source item.pdf"
    destination.write_bytes(pdf)
    row = recommendations.parse_recommendation(_row(pdf), 1)

    plan = recommendations.build_plan(
        (row,),
        options=options,
        get_citation_group=lambda _id: _citation_group(),
        get_item_file=lambda _filename: None,
    )

    assert not plan.actions[0].already_applied


def test_existing_object_and_destination_are_idempotent(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    destination = options.item_file_path / "source item.pdf"
    destination.write_bytes(pdf)
    tag = ItemFileTag.IFComment("Complete source item.")
    existing = cast(
        ItemFile,
        SimpleNamespace(
            filename="source item.pdf",
            citation_group=_citation_group(),
            tags=(tag,),
            title="Reviewed item",
            series=None,
            volume="12",
            issue=None,
            start_page=None,
            end_page=None,
            url="https://example.com/item/12",
        ),
    )
    row = recommendations.parse_recommendation(_row(pdf), 1)

    plan = recommendations.build_plan(
        (row,),
        options=options,
        get_citation_group=lambda _id: _citation_group(),
        get_item_file=lambda _filename: existing,
    )

    assert plan.actions[0].already_applied


def test_existing_item_file_removes_downloads_source_on_apply(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    options = _options(tmp_path)
    downloads_path = tmp_path / "downloads"
    downloads_path.mkdir()
    source = downloads_path / "source item.pdf"
    source.write_bytes(pdf)
    destination = options.item_file_path / "source item.pdf"
    destination.write_bytes(pdf)
    tag = ItemFileTag.IFComment("Complete source item.")
    existing = cast(
        ItemFile,
        SimpleNamespace(
            filename="source item.pdf",
            citation_group=_citation_group(),
            tags=(tag,),
            title="Reviewed item",
            series=None,
            volume="12",
            issue=None,
            start_page=None,
            end_page=None,
            url="https://example.com/item/12",
        ),
    )
    row_data = _row(pdf)
    row_data["file"]["source_root"] = "downloads"
    row_data["file"]["source_path"] = "source item.pdf"
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(row_data, 1),),
        options=SimpleNamespace(
            new_path=options.new_path,
            downloads_path=downloads_path,
            item_file_path=options.item_file_path,
        ),
        get_citation_group=lambda _id: _citation_group(),
        get_item_file=lambda _filename: existing,
    )

    recommendations.execute_plan(plan, apply=True)

    assert plan.actions[0].already_applied
    assert not source.exists()


def test_detect_url_lint_skips_new_virtual_item_file() -> None:
    item_file = ItemFile.virtual(
        filename="future.pdf",
        title=None,
        citation_group=CitationGroup.virtual(name="Journal of Examples"),
        series=None,
        volume=None,
        issue=None,
        start_page=None,
        end_page=None,
        url=None,
        tags=(),
    )

    assert list(lint_detect_url(item_file, LintConfig())) == []
