from __future__ import annotations

import pytest

from data_import import ce_file, lib
from taxonomy.db.constants import ObservationKind, OccurrenceBasis, Rank
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
