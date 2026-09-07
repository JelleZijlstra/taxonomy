import pytest

from scripts.generate_games_data import (
    generate_family_order_data,
    generate_geography_data,
    generate_order_families_data,
    parse_continents,
)
from taxonomy.db.constants import Rank, Status
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.tags import TaxonTag


def make_species(epithet: str = "bus", ids: tuple[str, ...] = ("1",)) -> Taxon:
    order = Taxon.virtual(rank=Rank.order, valid_name="Localorder", parent=None)
    family = Taxon.virtual(rank=Rank.family, valid_name="Localidae", parent=order)
    subfamily = Taxon.virtual(rank=Rank.subfamily, parent=family)
    genus = Taxon.virtual(rank=Rank.genus, valid_name="Localgenus", parent=subfamily)
    return Taxon.virtual(
        rank=Rank.species,
        valid_name=f"Localgenus {epithet}",
        parent=genus,
        base_name=Name.virtual(root_name=epithet, status=Status.valid),
        tags=tuple(TaxonTag.MDD(mid) for mid in ids),
    )


def mdd_row(mid: str = "1", continents: str = "North America|Asia?") -> dict[str, str]:
    return {"id": mid, "sciName": "Oldgenus_bus", "continentDistribution": continents}


def test_continents_preserve_uncertainty_and_ignore_non_geographic_values() -> None:
    assert parse_continents("North America |Asia?|NA|Domesticated|North America") == [
        "North America"
    ]
    assert parse_continents("Asia?|NA") == []
    assert parse_continents("Oceania (Continent)") == ["Oceania (Continent)"]
    with pytest.raises(ValueError, match="Unknown MDD continent"):
        parse_continents("Atlantis")


def test_geography_uses_existing_ids_and_local_classification() -> None:
    species = make_species()
    data, report = generate_geography_data([mdd_row()], [species])
    assert data["species"] == {"Localgenus": {"bus": ["North America"]}}
    assert data["genera"] == {"Localgenus": ["North America"]}
    assert data["families"] == {"Localidae": ["North America"]}
    assert report["records"][0] == {
        "taxon_id": species.id,
        "name": "Localgenus bus",
        "mdd_ids": ["1"],
        "mdd_name": "Oldgenus_bus",
        "continentDistribution": "North America|Asia?",
        "status": "matched",
    }


@pytest.mark.parametrize(
    ("ids", "rows", "status"),
    [
        ((), [mdd_row()], "missing_id"),
        (("1", "2"), [mdd_row()], "multiple_ids"),
        (("1",), [mdd_row("2")], "missing_mdd_row"),
        (("1",), [mdd_row(), mdd_row()], "duplicate_mdd_id"),
        (("1",), [mdd_row(continents="Asia?|NA")], "no_confirmed_continent"),
    ],
)
def test_unsafe_ranges_are_reported_and_not_guessed(
    ids: tuple[str, ...], rows: list[dict[str, str]], status: str
) -> None:
    data, report = generate_geography_data(rows, [make_species(ids=ids)])
    assert data["species"] == {}
    assert report["coverage"] == {status: 1}


def test_shared_local_mdd_id_is_ambiguous() -> None:
    data, report = generate_geography_data(
        [mdd_row()], [make_species(), make_species("dus")]
    )
    assert data["genera"] == {}
    assert report["coverage"] == {"shared_local_id": 2}


def test_aggregate_ranges_from_species_without_broadening_species_answers() -> None:
    first = make_species()
    second = make_species("dus", ids=("2",))
    data, report = generate_geography_data(
        [mdd_row(), mdd_row("2", "Asia")], [first, second]
    )
    assert data["genera"] == {"Localgenus": ["Asia", "North America"]}
    assert data["families"] == {"Localidae": ["Asia", "North America"]}
    assert data["species"]["Localgenus"] == {"bus": ["North America"], "dus": ["Asia"]}
    assert report["coverage"] == {"matched": 2}


def test_order_by_family_traverses_intermediate_ranks() -> None:
    order = Taxon.virtual(rank=Rank.order, valid_name="Rodentia", parent=None)
    suborder = Taxon.virtual(rank=Rank.suborder, parent=order)
    families = [
        Taxon.virtual(rank=Rank.family, valid_name="Muridae", parent=suborder),
        Taxon.virtual(rank=Rank.family, valid_name="Cricetidae", parent=suborder),
    ]
    assert generate_family_order_data(families) == [
        {"family": "Cricetidae", "order": "Rodentia"},
        {"family": "Muridae", "order": "Rodentia"},
    ]


def test_families_by_order_groups_sorts_and_deduplicates() -> None:
    assert generate_order_families_data(
        [
            {"family": "Muridae", "order": "Rodentia"},
            {"family": "Felidae", "order": "Carnivora"},
            {"family": "Cricetidae", "order": "Rodentia"},
            {"family": "Muridae", "order": "Rodentia"},
        ]
    ) == [
        {"order": "Carnivora", "families": ["Felidae"]},
        {"order": "Rodentia", "families": ["Cricetidae", "Muridae"]},
    ]
    assert generate_order_families_data([]) == []
