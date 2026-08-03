import csv
from pathlib import Path

import pytest

from scripts import compare_iucn_red_list_taxonomy as compare_iucn


def make_row(
    taxon_id: str,
    name: str,
    *,
    order: str = "EXAMPLIFORMES",
    family: str = "EXAMPLIDAE",
    authority: str = "Author, 1900",
    notes: str = "",
) -> compare_iucn.TaxonomyRow:
    genus, species = name.split()
    return {
        "internalTaxonId": taxon_id,
        "scientificName": name,
        "kingdomName": "ANIMALIA",
        "phylumName": "CHORDATA",
        "className": "MAMMALIA",
        "orderName": order,
        "familyName": family,
        "genusName": genus,
        "speciesName": species,
        "infraType": "",
        "infraName": "",
        "infraAuthority": "",
        "subpopulationName": "",
        "authority": authority,
        "taxonomicNotes": notes,
    }


def test_compare_classifies_material_changes_and_identifier_replacements() -> None:
    old = [
        make_row("1", "Oldus alpha"),
        make_row("2", "Stable beta"),
        make_row("3", "Identifier gamma"),
        make_row("4", "Removed delta"),
        make_row("5", "Authority epsilon"),
    ]
    new = [
        make_row("1", "Newus alpha", authority="Other, 1901"),
        make_row("2", "Stable beta", family="NEWFAMILY"),
        make_row("30", "Identifier gamma"),
        make_row("5", "Authority epsilon", authority="Other, 1901"),
        make_row("6", "Added zeta", notes="Formerly included Removed delta."),
    ]

    comparison = compare_iucn.compare_taxonomies(old, new)

    assert comparison.counts == {
        "renamed": 1,
        "taxonomy_changed": 1,
        "id_changed_same_name": 1,
        "removed": 1,
        "added": 1,
        "authority_changed": 1,
    }
    assert [row["change_type"] for row in comparison.rows] == [
        "renamed",
        "taxonomy_changed",
        "id_changed_same_name",
        "removed",
        "added",
    ]
    renamed = comparison.rows[0]
    assert renamed["changed_fields"] == "scientificName|genusName|authority"
    removed = comparison.rows[3]
    assert removed["new_taxa_whose_notes_mention_old_name"] == "6:Added zeta"


def test_compare_can_include_authority_only_rows() -> None:
    old = [make_row("1", "Stable alpha")]
    new = [make_row("1", "Stable alpha", authority="Other, 1901")]

    comparison = compare_iucn.compare_taxonomies(old, new, include_authority_only=True)

    assert len(comparison.rows) == 1
    assert comparison.rows[0]["change_type"] == "authority_changed"
    assert comparison.rows[0]["changed_fields"] == "authority"


def test_duplicate_ids_are_rejected() -> None:
    rows = [make_row("1", "First alpha"), make_row("1", "Second beta")]

    with pytest.raises(ValueError, match="Duplicate internalTaxonId"):
        compare_iucn.compare_taxonomies(rows, [])


def test_filter_by_order_is_case_insensitive() -> None:
    rows = [
        make_row("1", "Bat alpha", order="CHIROPTERA"),
        make_row("2", "Rodent beta", order="RODENTIA"),
    ]

    assert compare_iucn.filter_by_order(rows, " chiroptera ") == [rows[0]]


def test_filter_by_order_rejects_empty_value() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        compare_iucn.filter_by_order([], "  ")


def test_directory_input_prefers_plain_taxonomy_file(tmp_path: Path) -> None:
    plain = tmp_path / "taxonomy.csv"
    html = tmp_path / "taxonomy_with_html.csv"
    plain.touch()
    html.touch()

    assert compare_iucn.resolve_taxonomy_path(tmp_path) == plain


def test_write_report_has_stable_columns(tmp_path: Path) -> None:
    comparison = compare_iucn.compare_taxonomies(
        [make_row("1", "Oldus alpha")], [make_row("1", "Newus alpha")]
    )
    output = tmp_path / "nested" / "report.csv"

    count = compare_iucn.write_report(output, comparison.rows)

    assert count == 1
    with output.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        assert tuple(reader.fieldnames or ()) == compare_iucn.REPORT_COLUMNS
        rows = list(reader)
    assert rows[0]["change_type"] == "renamed"
