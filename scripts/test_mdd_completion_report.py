import pytest

from scripts import mdd_completion_report


def test_rows_from_values_pads_short_rows() -> None:
    assert mdd_completion_report.rows_from_values(
        [["first", "second"], ["one"], ["two", "three"]]
    ) == [{"first": "one", "second": ""}, {"first": "two", "second": "three"}]


def test_needs_type_data_uses_all_nomenclature_statuses() -> None:
    needs_type = mdd_completion_report.needs_type_data

    assert needs_type({"MDD_nomenclature_status": "available"})
    assert needs_type({"MDD_nomenclature_status": "preoccupied | as_emended"})
    assert not needs_type({"MDD_nomenclature_status": "nomen_novum"})
    assert not needs_type({"MDD_nomenclature_status": "nomen_novum | preoccupied"})
    assert not needs_type({"MDD_nomenclature_status": "unknown_status"})
    assert not needs_type({"MDD_nomenclature_status": ""})


def test_print_names_stats_uses_type_data_denominator(
    capsys: pytest.CaptureFixture[str],
) -> None:
    names = [
        {
            "MDD_nomenclature_status": "available",
            "MDD_original_type_locality": "Locality one",
            "MDD_type_latitude": "1",
            "MDD_type_longitude": "2",
        },
        {"MDD_nomenclature_status": "preoccupied", "MDD_type_latitude": "3"},
        {
            "MDD_nomenclature_status": "nomen_novum",
            "MDD_type_latitude": "4",
            "MDD_type_longitude": "5",
        },
        {"MDD_nomenclature_status": "name_combination"},
        {"MDD_nomenclature_status": "nomen_novum | preoccupied"},
        {"MDD_nomenclature_status": "unknown_status"},
    ]

    mdd_completion_report.print_names_stats(names)
    output = capsys.readouterr().out

    assert "Names that establish type data: 2/6 (33.3%)" in output
    assert "Type locality latitude: 2/2 (100.0%)" in output
    assert "Type locality longitude: 1/2 (50.0%)" in output
    assert "Both type locality coordinates: 1/2 (50.0%)" in output
    assert "Only one type locality coordinate: 1/2 (50.0%)" in output
    assert "unknown_status" in output


def test_print_species_stats_uses_all_species_denominator(
    capsys: pytest.CaptureFixture[str],
) -> None:
    species = [
        {
            "typeLocality": "Locality one",
            "typeLocalityLatitude": "1",
            "typeLocalityLongitude": "2",
        },
        {"typeLocality": "Locality two", "typeLocalityLongitude": "3"},
    ]

    mdd_completion_report.print_species_stats(species)
    output = capsys.readouterr().out

    assert "Type locality: 2/2 (100.0%)" in output
    assert "Type locality latitude: 1/2 (50.0%)" in output
    assert "Type locality longitude: 2/2 (100.0%)" in output
    assert "Both type locality coordinates: 1/2 (50.0%)" in output
    assert "Only one type locality coordinate: 1/2 (50.0%)" in output


def test_zero_denominator_is_reported_without_division_by_zero() -> None:
    assert mdd_completion_report.Coverage("Field", 0, 0).format() == "Field: 0/0 (n/a)"
