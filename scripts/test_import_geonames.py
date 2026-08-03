import sqlite3
from pathlib import Path

import pytest

from scripts import import_geonames
from taxonomy.apis import geonames


def _row(
    geoname_id: int,
    name: str,
    latitude: float,
    longitude: float,
    *,
    ascii_name: str | None = None,
    alternate_names: str = "",
    country_code: str = "FR",
    feature_class: str = "P",
    feature_code: str = "PPL",
    population: int = 0,
    admin1_code: str = "93",
    admin2_code: str = "83",
    admin3_code: str = "",
    admin4_code: str = "",
) -> str:
    return "\t".join(
        (
            str(geoname_id),
            name,
            ascii_name or name,
            alternate_names,
            str(latitude),
            str(longitude),
            feature_class,
            feature_code,
            country_code,
            "",
            admin1_code,
            admin2_code,
            admin3_code,
            admin4_code,
            str(population),
            "",
            "100",
            "Europe/Paris",
            "2026-01-01",
        )
    )


@pytest.fixture
def geonames_db(tmp_path: Path) -> Path:
    source = tmp_path / "allCountries.txt"
    source.write_text(
        "\n".join(
            (
                _row(
                    1,
                    "Collobrières",
                    43.2374,
                    6.3089533,
                    ascii_name="Collobrieres",
                    alternate_names="Collobrières,Collobrieres",
                    population=1967,
                ),
                _row(
                    2,
                    "Roc Meler",
                    42.58765,
                    1.7418,
                    alternate_names="Roc Mele,Roc Mélé",
                    country_code="AD",
                    feature_class="T",
                    feature_code="PK",
                ),
                _row(3, "Dateline East", 0, 179.99, country_code="FJ"),
                _row(4, "Dateline West", 0, -179.99, country_code="FJ"),
                _row(5, "Far Away", 45, 10),
                _row(
                    6,
                    "Provence-Alpes-Côte d'Azur",
                    44,
                    6,
                    feature_class="A",
                    feature_code="ADM1",
                    admin2_code="",
                ),
                _row(7, "Var", 43.4, 6.3, feature_class="A", feature_code="ADM2"),
            )
        )
        + "\n"
    )
    output = tmp_path / "geonames.sqlite"
    assert (
        import_geonames.build_database(source, output, progress_every=0, batch_size=2)
        == 7
    )
    return output


def test_build_database_records_metadata(geonames_db: Path) -> None:
    connection = sqlite3.connect(geonames_db)
    assert connection.execute("SELECT count(*) FROM geonames").fetchone() == (7,)
    assert connection.execute(
        "SELECT value FROM metadata WHERE key = 'row_count'"
    ).fetchone() == ("7",)
    assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)


def test_build_database_rejects_bad_row(tmp_path: Path) -> None:
    source = tmp_path / "allCountries.txt"
    source.write_text("too\tfew\tfields\n")
    output = tmp_path / "geonames.sqlite"

    with pytest.raises(ValueError, match="expected 19"):
        import_geonames.build_database(source, output, progress_every=0)

    assert not output.exists()
    assert not (tmp_path / ".geonames.sqlite.tmp").exists()


def test_search_name_matches_primary_ascii_and_alternate(geonames_db: Path) -> None:
    primary = geonames.search_name(
        "Collobrières", country_code="FR", db_path=geonames_db
    )
    ascii_matches = geonames.search_name(
        "Collobrieres", country_code="FR", db_path=geonames_db
    )
    alternate = geonames.search_name("Roc Mélé", country_code="AD", db_path=geonames_db)

    assert [(match.record.geoname_id, match.match_kind) for match in primary] == [
        (1, "name")
    ]
    assert [(match.record.geoname_id, match.match_kind) for match in ascii_matches] == [
        (1, "name")
    ]
    assert [(match.record.geoname_id, match.match_kind) for match in alternate] == [
        (2, "alternate_name")
    ]


def test_search_name_applies_feature_filters(geonames_db: Path) -> None:
    assert (
        geonames.search_name("Roc Mélé", feature_classes={"P"}, db_path=geonames_db)
        == []
    )
    assert (
        len(
            geonames.search_name(
                "Roc Mélé",
                feature_classes={"T"},
                feature_codes={"PK"},
                db_path=geonames_db,
            )
        )
        == 1
    )


def test_get_by_id(geonames_db: Path) -> None:
    record = geonames.get_by_id(1, db_path=geonames_db)
    assert record is not None
    assert record.name == "Collobrières"
    assert geonames.get_by_id(999, db_path=geonames_db) is None


def test_get_administrative_hierarchy(geonames_db: Path) -> None:
    record = geonames.get_by_id(1, db_path=geonames_db)
    assert record is not None

    hierarchy = geonames.get_administrative_hierarchy(record, db_path=geonames_db)

    assert [(item.feature_code, item.name) for item in hierarchy] == [
        ("ADM1", "Provence-Alpes-Côte d'Azur"),
        ("ADM2", "Var"),
    ]


def test_find_nearby_orders_and_filters_results(geonames_db: Path) -> None:
    nearby = geonames.find_nearby(
        43.2374,
        6.3089533,
        radius_km=100,
        country_code="FR",
        feature_classes={"P"},
        db_path=geonames_db,
    )

    assert [item.record.geoname_id for item in nearby] == [1]
    assert nearby[0].distance_km == 0


def test_find_nearby_handles_antimeridian(geonames_db: Path) -> None:
    nearby = geonames.find_nearby(
        0, 180, radius_km=5, country_code="FJ", db_path=geonames_db
    )

    assert [item.record.geoname_id for item in nearby] == [3, 4]
