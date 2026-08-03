from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy.db import models
from taxonomy.db.constants import (
    AgeClass,
    AltitudeUnit,
    DistributionOrigin,
    DistributionPresence,
    ObservationKind,
    OccurrenceBasis,
    OccurrenceValidity,
    Rank,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location
from taxonomy.db.models.occurrence_record import (
    OccurrenceRecord,
    OccurrenceRecordStatus,
    OccurrenceRecordTag,
    lint,
)
from taxonomy.db.models.occurrence_record.lint import (
    check_basis_tags,
    check_coordinate_consistency,
    check_distribution_rules,
    check_duplicate,
    check_location_age,
    check_missing_location,
    check_missing_taxon,
    check_source_data_tags,
    check_status_tags,
    check_taxon_mapping,
    parse_verbatim_coordinates,
    parse_verbatim_date,
    parse_verbatim_elevation,
    remove_unused_ignores,
)
from taxonomy.db.models.taxon import Taxon


def _record(**kwargs: object) -> OccurrenceRecord:
    kwargs.setdefault("tags", ())
    kwargs.setdefault("taxon", None)
    record = SimpleNamespace(**kwargs)
    record.has_tag = lambda tag_cls: any(
        tag._tag == tag_cls._tag for tag in record.tags
    )
    record.get_tags = lambda tags, tag_cls: (
        tag for tag in tags if isinstance(tag, tag_cls)
    )
    record.remove_tags = lambda tag_cls: setattr(
        record,
        "tags",
        tuple(tag for tag in record.tags if not isinstance(tag, tag_cls)),
    )
    record.add_tag = lambda tag: setattr(record, "tags", (*record.tags, tag))
    return cast(OccurrenceRecord, record)


def _duplicate_record(id: int, **kwargs: object) -> OccurrenceRecord:
    return _record(
        id=id,
        classification_entry=object(),
        locality_text="locality",
        page="1",
        basis=OccurrenceBasis.voucher,
        **kwargs,
    )


def test_taxon_lint_autofills_from_classification_entry() -> None:
    taxon = cast(Taxon, SimpleNamespace(rank=Rank.species))
    taxon.resolve_redirect = lambda: taxon  # type: ignore[method-assign]
    mapped_name = SimpleNamespace(taxon=taxon)
    record = _record(
        taxon=None,
        classification_entry=SimpleNamespace(
            mapped_name=mapped_name, rank=Rank.species
        ),
    )

    assert list(check_missing_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is taxon


def test_taxon_lint_autofills_genus_for_indeterminate_species() -> None:
    taxon = cast(Taxon, SimpleNamespace(rank=Rank.genus))
    taxon.resolve_redirect = lambda: taxon  # type: ignore[method-assign]
    parent = SimpleNamespace(rank=Rank.genus, mapped_name=SimpleNamespace(taxon=taxon))
    record = _record(
        taxon=None,
        classification_entry=SimpleNamespace(
            name="Sorex sp.", rank=Rank.species, mapped_name=None, parent=parent
        ),
    )

    assert list(check_missing_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is taxon


def test_taxon_lint_uses_genus_above_mapped_nominate_subgenus() -> None:
    genus = cast(Taxon, SimpleNamespace(rank=Rank.genus))
    subgenus_obj = SimpleNamespace(rank=Rank.subgenus)
    subgenus_obj.resolve_redirect = lambda: subgenus_obj
    subgenus_obj.parent_of_rank = lambda rank: genus if rank is Rank.genus else None
    subgenus = cast(Taxon, subgenus_obj)
    parent = SimpleNamespace(
        rank=Rank.genus, mapped_name=SimpleNamespace(taxon=subgenus)
    )
    record = _record(
        taxon=None,
        classification_entry=SimpleNamespace(
            name="Sorex sp.", rank=Rank.species, mapped_name=None, parent=parent
        ),
    )

    assert list(check_missing_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is genus


def test_taxon_lint_does_not_infer_genus_for_unmapped_species() -> None:
    taxon = cast(Taxon, SimpleNamespace(rank=Rank.genus))
    parent = SimpleNamespace(rank=Rank.genus, mapped_name=SimpleNamespace(taxon=taxon))
    record = _record(
        taxon=None,
        classification_entry=SimpleNamespace(
            name="Sorex problematicus",
            rank=Rank.species,
            mapped_name=None,
            parent=parent,
        ),
    )

    messages = list(check_missing_taxon(record, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "cannot infer taxon" in messages[0]
    assert record.taxon is None


def _nominate_subspecies_mapping(
    classification_entry_rank: Rank,
) -> tuple[Taxon, Taxon, object]:
    species = cast(Taxon, SimpleNamespace(rank=Rank.species))
    subspecies_obj = SimpleNamespace(rank=Rank.subspecies)
    subspecies_obj.resolve_redirect = lambda: subspecies_obj
    subspecies_obj.is_nominate_subspecies = lambda: True
    subspecies_obj.parent_of_rank = lambda rank: (
        species if rank is Rank.species else None
    )
    subspecies = cast(Taxon, subspecies_obj)
    classification_entry = SimpleNamespace(
        mapped_name=SimpleNamespace(taxon=subspecies), rank=classification_entry_rank
    )
    return species, subspecies, classification_entry


def test_taxon_lint_autofills_species_from_implicit_nominate_subspecies() -> None:
    species, _, classification_entry = _nominate_subspecies_mapping(Rank.species)
    record = _record(taxon=None, classification_entry=classification_entry)

    assert list(check_missing_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is species


def test_taxon_mapping_allows_species_for_implicit_nominate_subspecies() -> None:
    species, _, classification_entry = _nominate_subspecies_mapping(Rank.species)
    record = _record(taxon=species, classification_entry=classification_entry, tags=())

    assert list(check_taxon_mapping(record, LintConfig())) == []


def test_taxon_mapping_autofixes_implicit_nominate_subspecies() -> None:
    species, subspecies, classification_entry = _nominate_subspecies_mapping(
        Rank.species
    )
    record = _record(
        taxon=subspecies, classification_entry=classification_entry, tags=()
    )

    assert list(check_taxon_mapping(record, LintConfig(autofix=True))) == []
    assert record.taxon is species


def test_taxon_mapping_reports_implicit_nominate_subspecies_without_autofix() -> None:
    _, subspecies, classification_entry = _nominate_subspecies_mapping(Rank.species)
    record = _record(
        taxon=subspecies, classification_entry=classification_entry, tags=()
    )

    messages = list(check_taxon_mapping(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "change taxon from" in messages[0]
    assert record.taxon is subspecies


def test_taxon_lint_keeps_explicit_nominate_subspecies() -> None:
    _, subspecies, classification_entry = _nominate_subspecies_mapping(Rank.subspecies)
    record = _record(taxon=None, classification_entry=classification_entry)

    assert list(check_missing_taxon(record, LintConfig(autofix=True))) == []
    assert record.taxon is subspecies


def test_observation_kind_requires_observation_basis() -> None:
    record = _record(
        basis=OccurrenceBasis.listing,
        tags=(OccurrenceRecordTag.ObservationKind(ObservationKind.acoustic),),
    )

    messages = list(check_basis_tags(record, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "ObservationKind requires observation basis [basis_tags]"
    )


def test_location_lint_autofills_from_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    location = cast(Location, object())
    record = _record(
        location=None,
        locality_text="verbatim locality",
        tags=(OccurrenceRecordTag.LocationHint("canonical locality"),),
    )
    monkeypatch.setattr(
        lint,
        "_get_location_by_name",
        lambda name: location if name == "canonical locality" else None,
    )

    assert list(check_missing_location(record, LintConfig(autofix=True))) == []
    assert record.location is location
    assert record.tags == ()


def _location_with_age(period_name: str, youngest_age: int) -> Location:
    period = SimpleNamespace(name=period_name, get_min_age=lambda: youngest_age)
    return cast(
        Location, SimpleNamespace(min_period=period, max_period=period, min_age=None)
    )


def _taxon_with_age(age: AgeClass) -> Taxon:
    return cast(Taxon, SimpleNamespace(age=age))


def test_occurrence_recent_location_requires_recent_taxon() -> None:
    location = _location_with_age("Recent", 0)
    record = _record(
        location=location,
        taxon=_taxon_with_age(AgeClass.holocene),
        basis=OccurrenceBasis.voucher,
    )

    messages = list(check_location_age(record, LintConfig()))

    assert len(messages) == 1
    assert "is Recent" in messages[0]
    assert "has age holocene" in messages[0]


def test_occurrence_recent_location_allows_recently_extinct_taxon() -> None:
    record = _record(
        location=_location_with_age("Recent", 0),
        taxon=_taxon_with_age(AgeClass.recently_extinct),
        basis=OccurrenceBasis.voucher,
    )

    assert list(check_location_age(record, LintConfig())) == []


def test_observation_requires_recent_location() -> None:
    record = _record(
        location=_location_with_age("Pleistocene", 11_700),
        taxon=_taxon_with_age(AgeClass.extant),
        basis=OccurrenceBasis.observation,
    )

    messages = list(check_location_age(record, LintConfig()))

    assert len(messages) == 1
    assert "observation" in messages[0]
    assert "expected to be Recent" in messages[0]


def test_extant_voucher_may_come_from_pleistocene_location() -> None:
    record = _record(
        location=_location_with_age("Pleistocene", 11_700),
        taxon=_taxon_with_age(AgeClass.extant),
        basis=OccurrenceBasis.voucher,
    )

    assert list(check_location_age(record, LintConfig())) == []


def test_extant_occurrence_conflicts_with_pre_pleistocene_location() -> None:
    record = _record(
        location=_location_with_age("Pliocene", 2_590_000),
        taxon=_taxon_with_age(AgeClass.extant),
        basis=OccurrenceBasis.listing,
    )

    messages = list(check_location_age(record, LintConfig()))

    assert len(messages) == 1
    assert "is pre-Pleistocene" in messages[0]
    assert "extant taxon" in messages[0]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("40°30'N, 74°15'W", ("40°30'N", "74°15'W")),
        ("40.5, -74.25", ("40.5°N", "74.25°W")),
        ("not coordinates", None),
    ],
)
def test_parse_verbatim_coordinates(
    text: str, expected: tuple[str, str] | None
) -> None:
    assert parse_verbatim_coordinates(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("elev. 1,200 m", ("1200", AltitudeUnit.m)),
        ("500-750 feet", ("500-750", AltitudeUnit.ft)),
        ("~50 ft", ("~50", AltitudeUnit.ft)),
        ("ca. 50 ft", ("~50", AltitudeUnit.ft)),
        ("approximately 1,600 m", ("~1600", AltitudeUnit.m)),
        ("elev. ca. 1,200 m", ("~1200", AltitudeUnit.m)),
        ("below the summit", None),
    ],
)
def test_parse_verbatim_elevation(
    text: str, expected: tuple[str, AltitudeUnit] | None
) -> None:
    assert parse_verbatim_elevation(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1992", "1992"),
        ("Feb. 1992", "1992-02"),
        ("24 February 1992", "1992-02-24"),
        ("1992-02-24", "1992-02-24"),
        ("July to September 2023", "2023-07/2023-09"),
        ("between 28 January and 6 February 1983", "1983-01-28/1983-02-06"),
        ("14 July–16 August 1996", "1996-07-14/1996-08-16"),
        ("5 October 1994–9 April 1995", "1994-10-05/1995-04-09"),
        ("1994-10-05/1995-04-09", "1994-10-05/1995-04-09"),
        ("31 February 1992", None),
    ],
)
def test_parse_verbatim_date(text: str, expected: str | None) -> None:
    assert parse_verbatim_date(text) == expected


def test_source_data_lint_adds_normalized_tags() -> None:
    record = _record(
        tags=(
            OccurrenceRecordTag.VerbatimCoordinates("40.5, -74.25"),
            OccurrenceRecordTag.VerbatimElevation("elevation 1,200 m"),
            OccurrenceRecordTag.VerbatimDate("24 February 1992"),
        )
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert OccurrenceRecordTag.Coordinates("40.5°N", "74.25°W") in record.tags
    assert OccurrenceRecordTag.Elevation("1200", AltitudeUnit.m) in record.tags
    assert OccurrenceRecordTag.Date("1992-02-24") in record.tags


def test_source_data_lint_refines_coarse_date_from_source_interval() -> None:
    coarse = OccurrenceRecordTag.Date("2023")
    interval = OccurrenceRecordTag.Date("2023-07/2023-09")
    record = _record(
        tags=(OccurrenceRecordTag.VerbatimDate("July to September 2023"), coarse)
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert interval in record.tags
    assert coarse not in record.tags


def test_source_data_lint_translates_approximate_elevation() -> None:
    exact = OccurrenceRecordTag.Elevation("50", AltitudeUnit.ft)
    approximate = OccurrenceRecordTag.Elevation("~50", AltitudeUnit.ft)
    record = _record(tags=(OccurrenceRecordTag.VerbatimElevation("ca. 50 ft"), exact))

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert approximate in record.tags
    assert exact not in record.tags


def test_source_data_lint_reports_approximate_elevation_without_autofix() -> None:
    exact = OccurrenceRecordTag.Elevation("50", AltitudeUnit.ft)
    record = _record(tags=(OccurrenceRecordTag.VerbatimElevation("ca. 50 ft"), exact))

    messages = list(check_source_data_tags(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "preserve source elevation precision" in messages[0]
    assert exact in record.tags


def test_source_data_lint_accepts_normalized_approximate_elevation() -> None:
    approximate = OccurrenceRecordTag.Elevation("~50", AltitudeUnit.ft)
    record = _record(
        tags=(OccurrenceRecordTag.VerbatimElevation("ca. 50 ft"), approximate)
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=False))) == []
    assert approximate in record.tags


def test_source_data_lint_allows_manual_normalization() -> None:
    coordinates = OccurrenceRecordTag.Coordinates("40°30'N", "74°15'W")
    record = _record(
        tags=(
            OccurrenceRecordTag.VerbatimCoordinates("coordinates on map"),
            coordinates,
        )
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=False))) == []
    assert record.tags[-1] == coordinates


def test_source_data_lint_preserves_source_coordinate_format() -> None:
    decimal = OccurrenceRecordTag.Coordinates("12.016667°N", "61.716667°W")
    source_format = OccurrenceRecordTag.Coordinates("12°1'N", "61°43'W")
    record = _record(
        tags=(OccurrenceRecordTag.VerbatimCoordinates("12°01'N, 61°43'W"), decimal)
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert source_format in record.tags
    assert decimal not in record.tags


def test_source_data_lint_reports_source_format_replacement_without_autofix() -> None:
    decimal = OccurrenceRecordTag.Coordinates("12.016667°N", "61.716667°W")
    record = _record(
        tags=(OccurrenceRecordTag.VerbatimCoordinates("12°01'N, 61°43'W"), decimal)
    )

    messages = list(check_source_data_tags(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "preserve source coordinate format" in messages[0]
    assert decimal in record.tags


def test_source_data_lint_allows_coordinates_within_shared_tolerance() -> None:
    coordinates = OccurrenceRecordTag.Coordinates("40°31'N", "74°15'W")
    record = _record(
        tags=(OccurrenceRecordTag.VerbatimCoordinates("40°30'N, 74°15'W"), coordinates)
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert coordinates in record.tags


def test_source_data_lint_reports_coordinates_beyond_shared_tolerance() -> None:
    record = _record(
        tags=(
            OccurrenceRecordTag.VerbatimCoordinates("40°30'N, 74°15'W"),
            OccurrenceRecordTag.Coordinates("40°40'N", "74°15'W"),
        )
    )

    messages = list(check_source_data_tags(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "km from closest normalized coordinates" in messages[0]


def test_source_data_lint_standardizes_coordinate_ranges() -> None:
    record = _record(
        tags=(
            OccurrenceRecordTag.VerbatimCoordinates("coordinates on map"),
            OccurrenceRecordTag.Coordinates("41°N-40°N", "73°W-74°W"),
        )
    )

    assert list(check_source_data_tags(record, LintConfig(autofix=True))) == []
    assert OccurrenceRecordTag.Coordinates("40°N-41°N", "74°W-73°W") in record.tags


def test_normalized_source_data_requires_verbatim_tag() -> None:
    record = _record(tags=(OccurrenceRecordTag.Date("1992-02-24"),))

    messages = list(check_source_data_tags(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "Date requires VerbatimDate" in messages[0]


def test_coordinate_consistency_allows_five_kilometres() -> None:
    location = SimpleNamespace(
        latitude="40°30'N", longitude="74°15'W", __str__=lambda: "location"
    )
    record = _record(
        location=location, tags=(OccurrenceRecordTag.Coordinates("40°31'N", "74°15'W"),)
    )

    assert list(check_coordinate_consistency(record, LintConfig())) == []


def test_coordinate_consistency_flags_distant_location() -> None:
    location = SimpleNamespace(latitude="41°N", longitude="74°15'W")
    record = _record(
        location=location, tags=(OccurrenceRecordTag.Coordinates("40°30'N", "74°15'W"),)
    )

    messages = list(check_coordinate_consistency(record, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]
    assert messages[0].endswith("[coordinate_location]")


def test_coordinate_consistency_allows_point_inside_range() -> None:
    location = SimpleNamespace(latitude="40.5°N", longitude="74.5°W")
    record = _record(
        location=location,
        tags=(OccurrenceRecordTag.Coordinates("40°N-41°N", "75°W-74°W"),),
    )

    assert list(check_coordinate_consistency(record, LintConfig())) == []


def test_status_tags_are_repeatable() -> None:
    record = _record(
        tags=(
            OccurrenceRecordTag.ValidityFromSource(
                OccurrenceValidity.occurrence_dubious
            ),
            OccurrenceRecordTag.ValidityFromSource(
                OccurrenceValidity.classification_dubious
            ),
            OccurrenceRecordTag.ValidityAssessment(OccurrenceValidity.rejected),
            OccurrenceRecordTag.OriginFromSource(DistributionOrigin.introduced),
            OccurrenceRecordTag.PresenceFromSource(DistributionPresence.vagrant),
        )
    )

    assert list(check_status_tags(record, LintConfig())) == []


def test_status_lint_flags_duplicate_status() -> None:
    record = _record(
        tags=(
            OccurrenceRecordTag.PresenceFromSource(DistributionPresence.vagrant),
            OccurrenceRecordTag.PresenceFromSource(
                DistributionPresence.vagrant, comment="explicitly stated"
            ),
        )
    )

    messages = list(check_status_tags(record, LintConfig()))

    assert len(messages) == 1
    assert "duplicate PresenceFromSource for vagrant" in messages[0]


def _region(name: str, parent: Any = None) -> models.Region:
    region = SimpleNamespace(name=name, parent=parent)

    def all_parents() -> object:
        current = parent
        while current is not None:
            yield current
            current = current.parent

    region.all_parents = all_parents
    region.__str__ = lambda: name  # type: ignore[method-assign]
    return cast(models.Region, region)


def _rule_taxon(name: str, tags: tuple[object, ...]) -> Taxon:
    taxon = SimpleNamespace(valid_name=name, tags=tags)
    taxon.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    taxon.add_tag = lambda tag: setattr(taxon, "tags", (*taxon.tags, tag))
    taxon.__str__ = lambda: name  # type: ignore[method-assign]
    return cast(Taxon, taxon)


def _article(name: str, year: int) -> models.Article:
    return cast(
        models.Article,
        SimpleNamespace(
            name=name, valid_numeric_year=lambda: year, __str__=lambda: name
        ),
    )


def test_reassess_rule_requires_review_for_old_record() -> None:
    continent = _region("North America")
    country = _region("Canada", continent)
    revision = _article("revision", 2020)
    taxon = _rule_taxon(
        "Example old",
        (
            models.tags.TaxonTag.ReassessOccurrences(
                continent, revision, comment="split species"
            ),
        ),
    )
    record = _record(
        taxon=taxon,
        location=SimpleNamespace(region=country),
        classification_entry=SimpleNamespace(article=_article("old", 1990)),
    )

    messages = list(check_distribution_rules(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "requires ReviewedInLightOf" in messages[0]


def test_reassess_rule_exempts_new_record() -> None:
    region = _region("Canada")
    revision = _article("revision", 2020)
    taxon = _rule_taxon(
        "Example new", (models.tags.TaxonTag.ReassessOccurrences(region, revision),)
    )
    record = _record(
        taxon=taxon,
        location=SimpleNamespace(region=region),
        classification_entry=SimpleNamespace(article=_article("new", 2021)),
    )

    assert list(check_distribution_rules(record, LintConfig())) == []


def test_reassess_rule_requires_review_in_cutoff_year() -> None:
    region = _region("Canada")
    revision = _article("revision", 2020)
    taxon = _rule_taxon(
        "Example contemporary",
        (models.tags.TaxonTag.ReassessOccurrences(region, revision),),
    )
    record = _record(
        taxon=taxon,
        location=SimpleNamespace(region=region),
        classification_entry=SimpleNamespace(article=_article("contemporary", 2020)),
    )

    messages = list(check_distribution_rules(record, LintConfig()))

    assert len(messages) == 1
    assert "requires ReviewedInLightOf" in messages[0]


def test_redirect_rule_moves_old_record() -> None:
    region = _region("South America")
    revision = _article("revision", 2020)
    target = _rule_taxon("Artibeus planirostris", ())
    source = _rule_taxon(
        "Artibeus jamaicensis",
        (models.tags.TaxonTag.RedirectOccurrences(region, target, revision),),
    )
    record = _record(
        taxon=source,
        location=SimpleNamespace(region=region),
        classification_entry=SimpleNamespace(article=_article("old", 1990)),
    )

    assert list(check_distribution_rules(record, LintConfig(autofix=True))) == []
    assert record.taxon is target
    assert any(
        isinstance(tag, OccurrenceRecordTag.CommentFromDatabase)
        and "Reassigned from" in tag.text
        for tag in record.tags
    )


def test_redirect_rule_preserves_reviewed_old_record() -> None:
    region = _region("South America")
    revision = _article("revision", 2020)
    target = _rule_taxon("Artibeus planirostris", ())
    source = _rule_taxon(
        "Artibeus jamaicensis",
        (models.tags.TaxonTag.RedirectOccurrences(region, target, revision),),
    )
    record = _record(
        taxon=source,
        location=SimpleNamespace(region=region),
        classification_entry=SimpleNamespace(article=_article("old", 1990)),
        tags=(
            OccurrenceRecordTag.ReviewedInLightOf(
                revision, source, "confirmed after revision"
            ),
        ),
    )

    assert list(check_distribution_rules(record, LintConfig())) == []
    assert record.taxon is source


def test_redirect_rule_requires_review_for_later_record() -> None:
    region = _region("South America")
    revision = _article("revision", 2020)
    target = _rule_taxon("Artibeus planirostris", ())
    source = _rule_taxon(
        "Artibeus jamaicensis",
        (models.tags.TaxonTag.RedirectOccurrences(region, target, revision),),
    )
    record = _record(
        taxon=source,
        location=SimpleNamespace(region=region),
        classification_entry=SimpleNamespace(article=_article("later", 2021)),
    )

    messages = list(check_distribution_rules(record, LintConfig()))

    assert len(messages) == 1
    assert "requires ReviewedInLightOf" in messages[0]
    assert record.taxon is source


def test_occurrence_record_lint_can_be_ignored() -> None:
    record = _record(
        basis=OccurrenceBasis.listing,
        tags=(
            OccurrenceRecordTag.ObservationKind(ObservationKind.acoustic),
            OccurrenceRecordTag.IgnoreLintOccurrenceRecord("basis_tags"),
        ),
    )

    assert list(check_basis_tags(record, LintConfig())) == []


def test_remove_unused_occurrence_record_ignore() -> None:
    record = _record(
        tags=(OccurrenceRecordTag.IgnoreLintOccurrenceRecord("basis_tags"),)
    )

    remove_unused_ignores(record, {"basis_tags"})

    assert record.tags == []


def test_duplicate_lint_skips_lower_id_record(monkeypatch: pytest.MonkeyPatch) -> None:
    record = _duplicate_record(1)
    higher = _duplicate_record(2)
    monkeypatch.setattr(
        OccurrenceRecord,
        "select_valid",
        lambda: SimpleNamespace(filter=lambda *conditions: [record, higher]),
    )

    assert list(check_duplicate(record, LintConfig(autofix=False))) == []


def test_duplicate_lint_reports_ids_without_autofix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _duplicate_record(1)
    record = _duplicate_record(2)
    monkeypatch.setattr(
        OccurrenceRecord,
        "select_valid",
        lambda: SimpleNamespace(filter=lambda *conditions: [primary, record]),
    )
    monkeypatch.setattr(OccurrenceRecord, "fields", lambda: ())

    messages = list(check_duplicate(record, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "OR#2 duplicates primary record OR#1" in messages[0]


def test_duplicate_lint_redirects_exact_higher_id_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _duplicate_record(1, value="same", status=OccurrenceRecordStatus.valid)
    record = _duplicate_record(2, value="same", status=OccurrenceRecordStatus.valid)
    monkeypatch.setattr(
        OccurrenceRecord,
        "select_valid",
        lambda: SimpleNamespace(filter=lambda *conditions: [primary, record]),
    )
    monkeypatch.setattr(OccurrenceRecord, "fields", lambda: ("value", "status"))

    assert list(check_duplicate(record, LintConfig(autofix=True))) == []
    assert record.status is OccurrenceRecordStatus.alias
    assert record.tags == (OccurrenceRecordTag.RedirectTarget(primary),)


def test_duplicate_lint_does_not_delete_records_with_different_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _duplicate_record(1, value="first", status=OccurrenceRecordStatus.valid)
    record = _duplicate_record(2, value="second", status=OccurrenceRecordStatus.valid)
    monkeypatch.setattr(
        OccurrenceRecord,
        "select_valid",
        lambda: SimpleNamespace(filter=lambda *conditions: [primary, record]),
    )
    monkeypatch.setattr(OccurrenceRecord, "fields", lambda: ("value", "status"))

    messages = list(check_duplicate(record, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert record.status is OccurrenceRecordStatus.valid


def test_occurrence_record_lint_registry_labels() -> None:
    assert [wrapper.label for wrapper in lint.LINT.linters] == [
        "missing_taxon",
        "taxon_mapping",
        "missing_location",
        "location_hint",
        "location_mapping",
        "location_age",
        "basis_tags",
        "source_data",
        "coordinate_location",
        "status_tags",
        "distribution_rules",
        "taxonomic_split",
        "duplicate",
    ]
