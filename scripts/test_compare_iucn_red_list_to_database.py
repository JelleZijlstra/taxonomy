import csv
from pathlib import Path

import pytest

from scripts import compare_iucn_red_list_to_database as compare_iucn_db


def make_iucn(
    taxon_id: str, name: str, order: str = "CHIROPTERA"
) -> compare_iucn_db.IUCNRow:
    return {
        "internalTaxonId": taxon_id,
        "scientificName": name,
        "orderName": order,
        "familyName": "EXAMPLIDAE",
        "authority": "Author, 1900",
        "taxonomicNotes": "Example note",
    }


def make_database(
    taxon_id: int, name: str, order: str = "Chiroptera"
) -> compare_iucn_db.DatabaseSpecies:
    return compare_iucn_db.DatabaseSpecies(
        scientific_name=name,
        taxon_id=taxon_id,
        name_id=taxon_id + 100,
        order=order,
        family="Examplidae",
        age="extant",
        authority="Author",
        year="1900",
    )


def make_name_target(
    species: compare_iucn_db.DatabaseSpecies, *name_ids: int
) -> compare_iucn_db.NameTableTarget:
    return compare_iucn_db.NameTableTarget(species, name_ids)


def test_order_filter_is_case_insensitive_on_both_sources() -> None:
    iucn = [make_iucn("1", "Bat alpha"), make_iucn("2", "Mouse beta", "RODENTIA")]
    database = [
        make_database(1, "Bat alpha"),
        make_database(2, "Mouse beta", "Rodentia"),
    ]

    assert compare_iucn_db.filter_iucn_by_order(iucn, " chiroptera ") == [iucn[0]]
    assert compare_iucn_db.filter_database_by_order(database, "CHIROPTERA") == [
        database[0]
    ]
    index = {
        "Old alpha": (
            make_name_target(database[0], 101),
            make_name_target(database[1], 102),
        )
    }
    assert compare_iucn_db.filter_name_table_by_order(index, "CHIROPTERA") == {
        "Old alpha": (make_name_target(database[0], 101),)
    }


def test_empty_order_filter_is_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        compare_iucn_db.filter_iucn_by_order([], "  ")


def test_compare_reports_exact_and_unmatched_names() -> None:
    iucn = [make_iucn("1", "Shared alpha"), make_iucn("2", "Iucn beta")]
    database = [make_database(1, "Shared alpha"), make_database(2, "Database gamma")]

    comparison = compare_iucn_db.compare(
        iucn,
        database,
        {},
        {"1": {"assessmentId": "10", "redlistCategory": "Least Concern"}},
        iucn_source=Path("taxonomy.csv"),
        database_scope="test scope",
    )

    assert comparison.counts == {"exact_match": 1, "iucn_only": 1, "database_only": 1}
    assert [row["match_status"] for row in comparison.rows] == [
        "iucn_only",
        "database_only",
        "exact_match",
    ]
    assert comparison.rows[0]["iucn_taxonomic_notes_if_unmatched"] == "Example note"


def test_compare_uses_unique_name_table_mapping() -> None:
    iucn = [make_iucn("1", "Acerodon mackloti")]
    database_species = make_database(12093, "Acerodon macklotii")
    index = {"Acerodon mackloti": (make_name_target(database_species, 104996),)}

    comparison = compare_iucn_db.compare(
        iucn,
        [database_species],
        index,
        {},
        iucn_source=Path("taxonomy.csv"),
        database_scope="test scope",
    )

    assert comparison.counts == {"name_table_match": 1}
    row = comparison.rows[0]
    assert row["match_status"] == "name_table_match"
    assert row["scientific_name"] == "Acerodon mackloti"
    assert row["database_valid_name"] == "Acerodon macklotii"
    assert row["database_taxon_id"] == 12093
    assert row["name_table_name_ids"] == "104996"
    assert row["name_table_candidate_count"] == 1


def test_compare_does_not_choose_ambiguous_name_table_mapping() -> None:
    iucn = [make_iucn("1", "Example ambiguus")]
    first = make_database(1, "First species")
    second = make_database(2, "Second species")
    index = {
        "Example ambiguus": (
            make_name_target(first, 101),
            make_name_target(second, 102),
        )
    }

    comparison = compare_iucn_db.compare(
        iucn,
        [first, second],
        index,
        {},
        iucn_source=Path("taxonomy.csv"),
        database_scope="test scope",
    )

    assert comparison.counts == {"ambiguous_name_table": 1, "database_only": 2}
    ambiguous = comparison.rows[0]
    assert ambiguous["database_valid_name"] == ""
    assert ambiguous["database_taxon_id"] == ""
    assert ambiguous["name_table_candidate_count"] == 2
    assert ambiguous["name_table_candidate_valid_names"] == (
        "First species [taxon_id=1];Second species [taxon_id=2]"
    )
    assert ambiguous["iucn_taxonomic_notes_if_unmatched"] == "Example note"


def test_directory_input_prefers_plain_taxonomy_file(tmp_path: Path) -> None:
    plain = tmp_path / "taxonomy.csv"
    html = tmp_path / "taxonomy_with_html.csv"
    plain.touch()
    html.touch()

    assert compare_iucn_db.resolve_taxonomy_path(tmp_path) == plain


def test_write_report_has_stable_columns(tmp_path: Path) -> None:
    comparison = compare_iucn_db.compare(
        [make_iucn("1", "Shared alpha")],
        [make_database(1, "Shared alpha")],
        {},
        {},
        iucn_source=Path("taxonomy.csv"),
        database_scope="test scope",
    )
    output = tmp_path / "report.csv"

    assert compare_iucn_db.write_report(output, comparison.rows) == 1
    with output.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        assert tuple(reader.fieldnames or ()) == compare_iucn_db.FIELDNAMES
        rows = list(reader)
    assert rows[0]["match_status"] == "exact_match"
