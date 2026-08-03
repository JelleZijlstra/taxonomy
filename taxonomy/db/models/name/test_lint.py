from types import SimpleNamespace
from typing import cast

import pytest

from taxonomy.db import coordinate_lint, models
from taxonomy.db.constants import (
    AgeClass,
    Group,
    OccurrenceValidity,
    Rank,
    SpeciesGroupType,
    SpecimenOrgan,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location

from .lint import (
    check_collector_lifespan,
    check_coordinates,
    check_location_detail_coordinates,
    check_location_detail_plss,
    check_type_locality_age,
    check_type_locality_distribution_rules,
    check_type_locality_validity,
    infer_included_species,
    infer_tags_from_mapped_entries,
    parse_date,
)
from .name import Name, NameTag, TypeTag


def test_parse_date() -> None:
    assert parse_date("Feb 2013") == "2013-02"
    assert parse_date("1 Feb 2013") == "2013-02-01"
    assert parse_date("23 Feb 2013") == "2013-02-23"
    assert parse_date("July 2013") == "2013-07"
    assert parse_date("7 July 2013") == "2013-07-07"


def _name_with_collector_dates(
    *, death: str | None, dates: tuple[str, ...], additional_collector: bool = False
) -> Name:
    person = SimpleNamespace(death=death)
    tags = (
        TypeTag.CollectedBy(cast(models.Person, person)),
        *(
            (TypeTag.CollectedBy(cast(models.Person, SimpleNamespace(death=None))),)
            if additional_collector
            else ()
        ),
        *(TypeTag.Date(date) for date in dates),
    )
    return cast(
        Name,
        SimpleNamespace(
            type_tags=tags,
            get_tags=lambda values, tag_type: (
                tag for tag in values if isinstance(tag, tag_type)
            ),
        ),
    )


def test_collector_lifespan_flags_collection_after_death() -> None:
    name = _name_with_collector_dates(death="1900", dates=("2 January 1901",))

    messages = list(check_collector_lifespan(name, LintConfig()))

    assert len(messages) == 1
    assert (
        "collector namespace(death='1900') died in 1900, before collection date 2 January 1901"
        in messages[0]
    )


@pytest.mark.parametrize(
    "date", ["2 January 1899", "2 January 1900", "<2 January 1901"]
)
def test_collector_lifespan_allows_possible_lifetime_dates(date: str) -> None:
    name = _name_with_collector_dates(death="1900", dates=(date,))

    assert list(check_collector_lifespan(name, LintConfig())) == []


def test_collector_lifespan_requires_known_death() -> None:
    name = _name_with_collector_dates(death=None, dates=("2 January 1901",))

    assert list(check_collector_lifespan(name, LintConfig())) == []


def test_collector_lifespan_checks_all_collectors() -> None:
    name = _name_with_collector_dates(
        death="1900", dates=("2 January 1901",), additional_collector=True
    )

    assert len(list(check_collector_lifespan(name, LintConfig()))) == 1


def _name_with_coordinates(
    name_coordinates: tuple[str, str], location_coordinates: tuple[str, str] | None
) -> Name:
    tag = TypeTag.Coordinates(*name_coordinates)
    if location_coordinates is None:
        location = None
    else:
        location = SimpleNamespace(
            latitude=location_coordinates[0],
            longitude=location_coordinates[1],
            region=object(),
        )
    name = SimpleNamespace(
        type_locality=location,
        type_tags=(tag,),
        get_tags=lambda tags, tag_type: (
            candidate for candidate in tags if isinstance(candidate, tag_type)
        ),
    )
    return cast(Name, name)


def test_name_coordinates_require_type_locality() -> None:
    name = _name_with_coordinates(("40.5°N", "74.25°W"), None)

    messages = list(check_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "Coordinates('40.5°N', '74.25°W') is present" in messages[0]
    assert "type locality is not set" in messages[0]


def test_name_coordinates_allow_five_kilometres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("40°31'N", "74°15'W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig())) == []


def test_name_coordinates_must_match_location(monkeypatch: pytest.MonkeyPatch) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("41°N", "74°15'W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    messages = list(check_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]


def test_name_coordinate_range_matches_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _name_with_coordinates(("40°N-41°N", "75°W-74°W"), ("40.5°N", "74.5°W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig())) == []


def test_name_coordinates_report_exact_location_duplicate() -> None:
    name = _name_with_coordinates(("40.5°N", "74.25°W"), ("40.5°N", "74.25°W"))

    messages = list(check_coordinates(name, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "coordinates exactly match Location" in messages[0]
    assert TypeTag.Coordinates("40.5°N", "74.25°W") in name.type_tags


def test_name_coordinates_drop_exact_location_duplicate() -> None:
    name = _name_with_coordinates(("40.5°N", "74.25°W"), ("40.5°N", "74.25°W"))

    assert list(check_coordinates(name, LintConfig(autofix=True))) == []
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_name_coordinates_respect_ignore_lint_during_autofix() -> None:
    name = _name_with_coordinates(("40.5°N", "74.25°W"), ("40.5°N", "74.25°W"))
    coordinates = name.type_tags[0]
    name.type_tags = (  # type: ignore[assignment]
        coordinates,
        TypeTag.IgnoreLintName("coordinates", comment="retain source coordinates"),
    )

    assert list(check_coordinates(name, LintConfig(autofix=True))) == []
    assert coordinates in name.type_tags


def test_name_coordinates_keep_equivalent_different_representation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("40.5°N", "74.25°W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig(autofix=True))) == []
    assert TypeTag.Coordinates("40°30'N", "74°15'W") in name.type_tags


def _name_with_location_details(
    texts: tuple[str, ...],
    *,
    location_coordinates: tuple[str | None, str | None] | None,
    location_tags: tuple[object, ...] = (),
) -> Name:
    source = cast(models.Article, object())
    tags = tuple(TypeTag.LocationDetail(text, source) for text in texts)
    if location_coordinates is None:
        location = None
    else:
        location = SimpleNamespace(
            name="Example locality",
            latitude=location_coordinates[0],
            longitude=location_coordinates[1],
            tags=location_tags,
            get_tags=lambda values, tag_type: (
                tag for tag in values if isinstance(tag, tag_type)
            ),
        )
    return cast(
        Name,
        SimpleNamespace(
            type_locality=location,
            type_tags=tags,
            species_type_kind=None,
            has_lint_ignore=lambda label: False,
            get_tags=lambda values, tag_type: (
                tag for tag in values if isinstance(tag, tag_type)
            ),
        ),
    )


def test_location_detail_does_not_duplicate_location_coordinates() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=("40.5°N", "74.25°W")
    )

    assert list(check_location_detail_coordinates(name, LintConfig(autofix=True))) == []
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_location_detail_coordinates_must_match_location() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=("41°N", "74.25°W")
    )

    messages = list(check_location_detail_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]


def test_neotype_location_detail_coordinates_ignore_original_locality() -> None:
    original_source = cast(models.Article, SimpleNamespace(id=98))
    neotype_source = cast(models.Article, SimpleNamespace(id=99))
    tags = (
        TypeTag.NeotypeDesignation(
            optional_source=neotype_source, neotype="USNM 1", valid=True
        ),
        TypeTag.LocationDetail("original locality at 10°N, 20°E", original_source),
        TypeTag.LocationDetail("neotype locality at 11°N, 21°E", neotype_source),
    )
    name = cast(
        Name,
        SimpleNamespace(
            species_type_kind=SpeciesGroupType.neotype,
            type_locality=SimpleNamespace(
                name="Neotype locality", latitude="11°N", longitude="21°E"
            ),
            type_tags=tags,
            get_tags=lambda values, tag_type: (
                tag for tag in values if isinstance(tag, tag_type)
            ),
        ),
    )

    assert list(check_location_detail_coordinates(name, LintConfig())) == []


def test_location_detail_infers_coordinates_when_location_has_none() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=(None, None)
    )

    assert list(check_location_detail_coordinates(name, LintConfig(autofix=True))) == []
    assert TypeTag.Coordinates("40°30'N", "74°15'W") in name.type_tags


def test_conflicting_location_detail_coordinates_are_not_inferred() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.", "Locality reported as 41°N, 74°15'W."),
        location_coordinates=(None, None),
    )

    messages = list(check_location_detail_coordinates(name, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "cannot infer Coordinates tag" in messages[0]
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_distinct_nearby_location_detail_coordinates_are_not_combined() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.", "Locality reported as 40°31'N, 74°15'W."),
        location_coordinates=(None, None),
    )

    messages = list(check_location_detail_coordinates(name, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "multiple coordinate pairs within 5 km" in messages[0]
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_location_detail_plss_reports_name_local_conflict() -> None:
    name = _name_with_location_details(
        ("T27S R31E Sec. 3", "T27S R31W Sec. 3"), location_coordinates=(None, None)
    )

    messages = list(check_location_detail_plss(name, LintConfig()))

    assert len(messages) == 1
    assert "incompatible PLSS descriptions" in messages[0]


def test_location_detail_plss_allows_compatible_precision() -> None:
    name = _name_with_location_details(
        ("T27S R31E", "T27S R31E Sec. 3"), location_coordinates=(None, None)
    )

    assert list(check_location_detail_plss(name, LintConfig())) == []


def test_location_detail_plss_reports_conflict_with_reviewed_location() -> None:
    name = _name_with_location_details(
        ("T33S R25W Sec. 21 NW1/4 NE1/4",),
        location_coordinates=(None, None),
        location_tags=(
            models.tags.LocationTag.PLSS(
                "T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0"
            ),
        ),
    )

    messages = list(check_location_detail_plss(name, LintConfig()))

    assert len(messages) == 1
    assert "conflicts with type locality" in messages[0]
    assert "T33S R25W Sec. 21 NW¼NE¼" in messages[0]


def test_location_detail_plss_allows_narrower_reviewed_location_evidence() -> None:
    name = _name_with_location_details(
        ("T33S R28W Sec. 21 NW1/4 NE1/4",),
        location_coordinates=(None, None),
        location_tags=(
            models.tags.LocationTag.PLSS(
                "T33S R28W Sec. 21, 6th Meridian", "KS060330S0280W0"
            ),
        ),
    )

    assert list(check_location_detail_plss(name, LintConfig())) == []


def test_location_detail_plss_allows_reviewed_section_from_explicit_alternative() -> (
    None
):
    name = _name_with_location_details(
        ("Sections 30-31, T6N R3E",),
        location_coordinates=(None, None),
        location_tags=(
            models.tags.LocationTag.PLSS(
                "T6N R3E Sec. 31, Wind River Meridian", "WY340060N0030E0"
            ),
        ),
    )

    assert list(check_location_detail_plss(name, LintConfig())) == []


def test_location_detail_coordinates_must_match_existing_tag() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=(None, None)
    )
    name.type_tags = [*name.type_tags, TypeTag.Coordinates("41°N", "74°15'W")]  # type: ignore[assignment]

    messages = list(check_location_detail_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "conflict with all Coordinates tags" in messages[0]


def test_nominate_subspecies_does_not_readd_merged_location_detail() -> None:
    source = cast(models.Article, object())
    parent_ce = cast(models.ClassificationEntry, object())
    child_ce = cast(
        models.ClassificationEntry,
        SimpleNamespace(
            rank=Rank.subspecies,
            parent=parent_ce,
            article=source,
            type_locality="Pantar",
            tags=(),
            citation=None,
        ),
    )
    location_tag = TypeTag.LocationDetail(
        "Pantar", source, classification_entry=parent_ce
    )
    tag_name = SimpleNamespace(
        type_tags=(location_tag,),
        original_citation=None,
        get_tags=lambda values, tag_type: (
            tag for tag in values if isinstance(tag, tag_type)
        ),
    )
    combination_name = SimpleNamespace(
        group=Group.species,
        get_classification_entries=lambda: [child_ce],
        resolve_variant=lambda **_: tag_name,
        get_mapped_classification_entry=lambda: None,
    )

    messages = list(
        infer_tags_from_mapped_entries(
            cast(Name, combination_name), LintConfig(autofix=False)
        )
    )

    assert messages == []


class _VariantName:
    def __init__(self, combination_of: Name | None = None) -> None:
        self.combination_of = combination_of

    def get_tag_target(self, tag_type: object) -> Name | None:
        if tag_type is NameTag.NameCombinationOf:
            return self.combination_of
        return None


def test_included_species_does_not_readd_original_preempted_by_combination() -> None:
    original_name = cast(Name, _VariantName())
    combination_name = cast(Name, _VariantName(original_name))
    combination_ce = cast(models.ClassificationEntry, object())
    existing_tag = TypeTag.IncludedSpecies(
        combination_name, classification_entry=combination_ce
    )
    genus_ce = SimpleNamespace(rank=Rank.genus)
    child_ce = cast(
        models.ClassificationEntry,
        SimpleNamespace(mapped_name=original_name, parent=genus_ce, page="28"),
    )
    genus_ce.get_children_of_rank = lambda rank: [child_ce]
    genus_name = SimpleNamespace(
        group=Group.genus,
        original_citation=object(),
        type_tags=(existing_tag,),
        get_mapped_classification_entry=lambda: genus_ce,
    )

    messages = list(
        infer_included_species(cast(Name, genus_name), LintConfig(autofix=False))
    )

    assert messages == []


def _location_with_age(period_name: str, youngest_age: int) -> Location:
    period = SimpleNamespace(name=period_name, get_min_age=lambda: youngest_age)
    return cast(
        Location, SimpleNamespace(min_period=period, max_period=period, min_age=None)
    )


def _name_with_age(
    taxon_age: AgeClass, location: Location, *, organs: tuple[SpecimenOrgan, ...] = ()
) -> Name:
    tags = tuple(TypeTag.Organ(organ) for organ in organs)
    name = SimpleNamespace(
        type_locality=location,
        taxon=SimpleNamespace(age=taxon_age),
        type_tags=tags,
        get_tags=lambda tags, tag_type: (
            tag for tag in tags if isinstance(tag, tag_type)
        ),
    )
    return cast(Name, name)


def test_name_recent_type_locality_requires_recent_taxon() -> None:
    location = _location_with_age("Recent", 0)

    assert (
        list(
            check_type_locality_age(
                _name_with_age(AgeClass.extant, location), LintConfig()
            )
        )
        == []
    )
    messages = list(
        check_type_locality_age(_name_with_age(AgeClass.fossil, location), LintConfig())
    )

    assert len(messages) == 1
    assert "is Recent" in messages[0]
    assert "has age fossil" in messages[0]


@pytest.mark.parametrize("organ", [SpecimenOrgan.skin, SpecimenOrgan.in_alcohol])
def test_name_recent_organ_conflicts_with_fossil_locality(organ: SpecimenOrgan) -> None:
    name = _name_with_age(
        AgeClass.extant, _location_with_age("Pleistocene", 11_700), organs=(organ,)
    )

    messages = list(check_type_locality_age(name, LintConfig()))

    assert len(messages) == 1
    assert organ.name.replace("_", " ") in messages[0]
    assert "indicate a Recent type specimen" in messages[0]


def test_name_extant_taxon_may_have_pleistocene_type_locality() -> None:
    name = _name_with_age(AgeClass.extant, _location_with_age("Pleistocene", 11_700))

    assert list(check_type_locality_age(name, LintConfig())) == []


def test_name_mixed_recent_fossil_locality_is_left_alone() -> None:
    recent = SimpleNamespace(name="Recent", get_min_age=lambda: 0)
    pleistocene = SimpleNamespace(name="Pleistocene", get_min_age=lambda: 11_700)
    location = cast(
        Location,
        SimpleNamespace(min_period=recent, max_period=pleistocene, min_age=None),
    )
    name = _name_with_age(AgeClass.extant, location, organs=(SpecimenOrgan.skin,))

    assert list(check_type_locality_age(name, LintConfig())) == []


def test_name_extant_taxon_conflicts_with_pre_pleistocene_type_locality() -> None:
    name = _name_with_age(
        AgeClass.recently_extinct, _location_with_age("Pliocene", 2_590_000)
    )

    messages = list(check_type_locality_age(name, LintConfig()))

    assert len(messages) == 1
    assert "is pre-Pleistocene" in messages[0]
    assert "recently extinct taxon" in messages[0]


def test_name_fossil_taxon_allows_pre_pleistocene_type_locality() -> None:
    name = _name_with_age(AgeClass.fossil, _location_with_age("Pliocene", 2_590_000))

    assert list(check_type_locality_age(name, LintConfig())) == []


def _tagged_name(tags: tuple[object, ...]) -> Name:
    name = SimpleNamespace(type_tags=tags)
    name.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    return cast(Name, name)


def test_type_locality_validity_rejects_valid_tag() -> None:
    name = _tagged_name((TypeTag.TypeLocalityValidity(OccurrenceValidity.valid),))

    messages = list(check_type_locality_validity(name, LintConfig()))

    assert len(messages) == 1
    assert "only allows occurrence_dubious or classification_dubious" in messages[0]


def test_redirect_rule_flags_name_type_locality() -> None:
    region_data = SimpleNamespace()
    region_data.all_parents = lambda: iter(())
    region = cast(models.Region, region_data)
    target = cast(models.Taxon, SimpleNamespace())
    source = cast(models.Article, SimpleNamespace())
    rule = models.tags.TaxonTag.RedirectOccurrences(region, target, source)
    taxon = SimpleNamespace(tags=(rule,))
    taxon.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    name = SimpleNamespace(
        type_locality=SimpleNamespace(region=region), taxon=taxon, type_tags=()
    )
    name.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )

    messages = list(
        check_type_locality_distribution_rules(cast(Name, name), LintConfig())
    )

    assert len(messages) == 1
    assert "where RedirectOccurrences says" in messages[0]
