import pytest

from taxonomy import shell
from taxonomy.db import models
from taxonomy.db.constants import AgeClass, Group, NomenclatureStatus, Status
from taxonomy.db.models import Name, Taxon, TypeTag


def _location(
    name: str,
    *,
    region_name: str = "Country",
    recent: bool = True,
    tags: tuple[models.tags.LocationTag, ...] = (),
    coordinates: bool = False,
) -> models.Location:
    period = models.Period.virtual(name="Recent" if recent else "Jurassic")
    return models.Location.virtual(
        name=name,
        region=models.Region.virtual(name=region_name),
        min_period=period,
        max_period=period,
        tags=tags,
        latitude="1°N" if coordinates else None,
        longitude="2°E" if coordinates else None,
    )


def _name(
    type_locality: models.Location | None,
    age: AgeClass,
    *,
    locality_required: bool = True,
    imprecise: bool = False,
    group: Group = Group.species,
) -> Name:
    return Name.virtual(
        status=Status.valid,
        nomenclature_status=(
            NomenclatureStatus.available
            if locality_required
            else NomenclatureStatus.informal
        ),
        group=group,
        taxon=Taxon.virtual(age=age),
        type_locality=type_locality,
        type_tags=(TypeTag.ImpreciseLocality(),) if imprecise else (),
        original_name=None,
        corrected_original_name=None,
        author_tags=(),
        year=None,
        original_citation=None,
        verbatim_citation=None,
        collection=None,
        type_specimen=None,
    )


def test_type_locality_summary_labels_are_independent_of_tree() -> None:
    missing = _name(None, AgeClass.fossil, locality_required=False)
    regionwide = _name(
        _location("Country", coordinates=True), AgeClass.extant, imprecise=True
    )
    fossil_unplaced = _name(
        _location(
            "Unknown bed", recent=False, tags=(models.tags.LocationTag.Unplaced(),)
        ),
        AgeClass.fossil,
    )

    assert shell._type_locality_summary_labels(missing) == {
        "type_locality_set": False,
        "type_locality_required": False,
        "taxon_age": "fossil",
    }
    assert shell._type_locality_summary_labels(regionwide) == {
        "type_locality_set": True,
        "location_age": "Recent",
        "location_kind": "regionwide",
        "imprecise_locality": True,
        "coordinates_set": True,
    }
    assert shell._type_locality_summary_labels(fossil_unplaced) == {
        "type_locality_set": True,
        "location_age": "fossil",
        "location_kind": "Unplaced",
        "imprecise_locality": False,
        "coordinates_set": False,
    }


def test_type_locality_summary_lines() -> None:
    names = [
        _name(None, AgeClass.extant, locality_required=False),
        _name(None, AgeClass.fossil, locality_required=False),
        _name(None, AgeClass.extant),
        _name(_location("Country"), AgeClass.extant, imprecise=True),
        _name(
            _location("Broad area", tags=(models.tags.LocationTag.General,)),
            AgeClass.extant,
        ),
        _name(
            _location("Unknown site", tags=(models.tags.LocationTag.Unplaced(),)),
            AgeClass.extant,
        ),
        _name(_location("Exact site", coordinates=True), AgeClass.extant),
        _name(
            _location("Country", recent=False, tags=(models.tags.LocationTag.General,)),
            AgeClass.fossil,
        ),
        _name(
            _location(
                "Unknown bed", recent=False, tags=(models.tags.LocationTag.Unplaced(),)
            ),
            AgeClass.fossil,
        ),
        _name(_location("Country", recent=False), AgeClass.fossil),
    ]

    assert shell._type_locality_summary_lines(names) == [
        "Of 10 names (100.0% of total):",
        "- 3 names (30.0% of total): type locality not set",
        "  - 2 names (20.0% of total): type locality not required",
        "    - 1 name (10.0% of total): fossil or ichnotaxon",
        "    - 1 name (10.0% of total): extant or recently extinct",
        "  - 1 name (10.0% of total): type locality required",
        "    - 0 names (0.0% of total): fossil or ichnotaxon",
        "    - 1 name (10.0% of total): extant or recently extinct",
        "- 7 names (70.0% of total): type locality set",
        "  - 4 names (40.0% of total): Recent location",
        "    - 1 name (10.0% of total): regionwide location",
        "      - 1 name (10.0% of total): ImpreciseLocality tag set",
        "      - 0 names (0.0% of total): no ImpreciseLocality tag",
        "    - 1 name (10.0% of total): General location",
        "      - 0 names (0.0% of total): ImpreciseLocality tag set",
        "      - 1 name (10.0% of total): no ImpreciseLocality tag",
        "    - 1 name (10.0% of total): Unplaced location",
        "    - 0 names (0.0% of total): partial type localities",
        "    - 1 name (10.0% of total): precise location",
        "      - 1 name (10.0% of total): coordinates set",
        "      - 0 names (0.0% of total): coordinates not set",
        "  - 3 names (30.0% of total): fossil location (all non-Recent locations)",
        "    - 1 name (10.0% of total): General location",
        "    - 1 name (10.0% of total): Unplaced location",
        "    - 0 names (0.0% of total): partial type localities",
        "    - 1 name (10.0% of total): other location",
    ]


def test_type_locality_summary_lines_empty() -> None:
    lines = shell._type_locality_summary_lines([])

    assert lines[0] == "Of 0 names (0.0% of total):"
    assert all("(0.0% of total)" in line for line in lines)


def test_type_locality_summary_filters_taxon_names_to_species_group(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    species_name = _name(None, AgeClass.extant, locality_required=False)
    genus_name = _name(
        None, AgeClass.extant, locality_required=False, group=Group.genus
    )
    taxon = Taxon.virtual(age=AgeClass.extant)
    monkeypatch.setattr(Taxon, "all_names", lambda _self: {species_name, genus_name})

    shell.type_locality_summary(taxon)

    assert capsys.readouterr().out.startswith("Of 1 name (100.0% of total):\n")
