import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from data_import import ce_file, lib
from taxonomy.db import models
from taxonomy.db.constants import ObservationKind, OccurrenceBasis, Rank
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag


def test_serialize_ce() -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "7",
        "name": "R. rattus",
        "rank": Rank.species,
        "parent": "Rattus",
        "parent_rank": Rank.genus,
        "corrected_name": "Rattus rattus",
    }
    assert ce_file.serialize_ce(ce) == {
        "article": "source.pdf",
        "page": "7",
        "name": "R. rattus",
        "rank": "species",
        "parent": "Rattus",
        "parent_rank": "genus",
        "corrected_name": "Rattus rattus",
    }


def test_read_ce_file_allows_multiple_articles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "combined.ce.jsonl"
    path.write_text('{"source": "first"}\n{"source": "second"}\n')
    first = SimpleNamespace(id=1)
    second = SimpleNamespace(id=2)
    entries: list[lib.CEDict] = [
        {"article": first},  # type: ignore[typeddict-item]
        {"article": second},  # type: ignore[typeddict-item]
    ]
    monkeypatch.setattr(
        ce_file, "deserialize_ce", lambda data, line_number: entries[line_number - 1]
    )

    assert ce_file.read_ce_file(path) == entries


def test_validate_ce_parents_uses_corrected_name_for_genus_check() -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    entries: list[lib.CEDict] = [
        {"page": "7", "name": "Rattus", "rank": Rank.genus, "article": article},
        {
            "page": "7",
            "name": "R. rattus",
            "corrected_name": "Rattus rattus",
            "rank": Rank.species,
            "parent": "Rattus",
            "parent_rank": Rank.genus,
            "article": article,
        },
    ]

    assert list(lib.validate_ce_parents(entries)) == entries


def test_serialize_occurrences() -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "7",
        "name": "Rattus rattus",
        "rank": Rank.species,
        "occurrences": [
            {
                "locality": "near the port",
                "mapped_location": "Port locality, Ecuador",
                "basis": OccurrenceBasis.observation,
                "tags": [OccurrenceRecordTag.ObservationKind(ObservationKind.visual)],
            }
        ],
    }

    assert ce_file.serialize_ce(ce)["occurrences"] == [
        {
            "locality": "near the port",
            "mapped_location": "Port locality, Ecuador",
            "basis": "observation",
            "tags": [{"kind": "ObservationKind", "data": [1, 1]}],
        }
    ]


def test_serialize_fallback_raw_data_with_classification_entry_tags() -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "7",
        "name": "Vespertilionidae",
        "rank": Rank.family,
        "tags": [ClassificationEntryTag.TreatedAsDubious],
    }

    assert json.loads(lib._serialize_ce_raw_data(ce)) == {
        "page": "7",
        "name": "Vespertilionidae",
        "rank": Rank.family,
        "tags": [{"kind": "TreatedAsDubious", "data": [13]}],
    }


def test_get_existing_for_import_allows_normalized_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "183–184",
        "name": "Myotis nesopolus",
        "rank": Rank.species,
        "parent": "Myotis",
        "authority": "Miller",
        "year": "1900",
    }
    imported_ce = object()
    calls: list[tuple[bool, str | None]] = []

    def get_existing(candidate: lib.CEDict, *, strict: bool = False) -> object | None:
        calls.append((strict, candidate.get("page")))
        return imported_ce if strict and "page" not in candidate else None

    monkeypatch.setattr(lib, "get_existing", get_existing)

    assert lib.get_existing_for_import(ce, strict=True) is imported_ce
    assert calls == [(True, "183–184"), (True, None)]


def test_get_existing_for_import_preserves_non_page_fields_when_normalizing_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "183–184",
        "name": "Myotis nesopolus",
        "rank": Rank.species,
        "parent": "Myotis",
        "authority": "Miller",
        "year": "1900",
    }
    calls: list[tuple[lib.CEDict, bool]] = []

    def get_existing(candidate: lib.CEDict, *, strict: bool = False) -> None:
        calls.append((candidate, strict))

    monkeypatch.setattr(lib, "get_existing", get_existing)

    assert lib.get_existing_for_import(ce, strict=True) is None
    assert calls[1][0]["authority"] == "Miller"
    assert calls[1][0]["year"] == "1900"
    assert calls[1][0]["parent"] == "Myotis"
    assert [strict for _, strict in calls] == [True, True, False]


def test_get_existing_for_import_allows_missing_existing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "11",
        "name": "Artibeus jamaicensis",
        "rank": Rank.species,
        "parent": "Artibeus",
        "authority": "Leach",
        "year": "1821",
    }
    existing = type("Existing", (), {"authority": None, "year": None, "parent": None})()

    def get_existing(candidate: lib.CEDict, *, strict: bool = False) -> object | None:
        return None if strict else existing

    monkeypatch.setattr(lib, "get_existing", get_existing)

    assert lib.get_existing_for_import(ce, strict=True) is existing


def test_get_existing_for_import_rejects_populated_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = type("Article", (), {"name": "source.pdf"})()
    ce: lib.CEDict = {
        "article": article,
        "page": "11",
        "name": "Artibeus jamaicensis",
        "rank": Rank.species,
        "parent": "Artibeus",
        "authority": "Leach",
        "year": "1821",
    }
    existing = type(
        "Existing", (), {"authority": "Gosse", "year": "1851", "parent": None}
    )()

    def get_existing(candidate: lib.CEDict, *, strict: bool = False) -> object | None:
        return None if strict else existing

    monkeypatch.setattr(lib, "get_existing", get_existing)

    assert lib.get_existing_for_import(ce, strict=True) is None


def test_deserialize_occurrence() -> None:
    occurrence = ce_file._deserialize_occurrence(
        {
            "locality": "near the port",
            "basis": "observation",
            "tags": [{"kind": "ObservationKind", "data": [1, 2]}],
        },
        line_number=3,
        occurrence_number=2,
    )

    assert occurrence["basis"] is OccurrenceBasis.observation
    assert occurrence["tags"] == [
        OccurrenceRecordTag.ObservationKind(ObservationKind.acoustic)
    ]


def test_deserialize_occurrence_rejects_incompatible_basis_tag() -> None:
    with pytest.raises(ce_file.CEFileError, match="requires observation basis"):
        ce_file._deserialize_occurrence(
            {
                "locality": "near the port",
                "basis": "listing",
                "tags": [{"kind": "ObservationKind", "data": [1, 1]}],
            },
            line_number=3,
            occurrence_number=2,
        )


def test_validate_structure() -> None:
    article = type("Article", (), {"id": 1})()
    entries: list[lib.CEDict] = [
        {"article": article, "page": "7", "name": "Rattus", "rank": Rank.genus},
        {
            "article": article,
            "page": "7",
            "name": "Rattus rattus",
            "rank": Rank.species,
            "parent": "Rattus",
            "parent_rank": Rank.genus,
        },
    ]
    assert ce_file.validate_structure(entries) == entries


def test_validate_structure_scopes_hierarchies_to_article() -> None:
    first = cast(models.Article, SimpleNamespace(id=1))
    second = cast(models.Article, SimpleNamespace(id=2))
    entries: list[lib.CEDict] = [
        {"article": first, "page": "1", "name": "Rattus", "rank": Rank.genus},
        {"article": second, "page": "2", "name": "Rattus", "rank": Rank.genus},
        {
            "article": first,
            "page": "1",
            "name": "Rattus rattus",
            "rank": Rank.species,
            "parent": "Rattus",
            "parent_rank": Rank.genus,
        },
        {
            "article": second,
            "page": "2",
            "name": "Rattus norvegicus",
            "rank": Rank.species,
            "parent": "Rattus",
            "parent_rank": Rank.genus,
        },
    ]

    assert ce_file.validate_structure(entries) == entries


def test_validate_structure_does_not_share_parents_between_articles() -> None:
    first = cast(models.Article, SimpleNamespace(id=1))
    second = cast(models.Article, SimpleNamespace(id=2))
    entries: list[lib.CEDict] = [
        {"article": first, "page": "1", "name": "Rattus", "rank": Rank.genus},
        {
            "article": second,
            "page": "2",
            "name": "Rattus rattus",
            "rank": Rank.species,
            "parent": "Rattus",
            "parent_rank": Rank.genus,
        },
    ]

    with pytest.raises(ValueError, match="parent Rattus"):
        ce_file.validate_structure(entries)
