from __future__ import annotations

import json

import pytest

from data_import import ce_file, lib
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
    article = type("Article", (), {})()
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
