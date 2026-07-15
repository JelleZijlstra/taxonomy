from __future__ import annotations

from data_import import ce_file, lib
from taxonomy.db.constants import Rank


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
