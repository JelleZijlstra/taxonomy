from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest
from clirm import VirtualReferenceError

from taxonomy.db import coordinate_lint, models
from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    ArticleType,
    Group,
    NamingConvention,
    NomenclatureStatus,
    OccurrenceValidity,
    PersonType,
    Rank,
    SpeciesGroupType,
    SpecimenOrgan,
    Status,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location
from taxonomy.db.models.person import AuthorTag

from .lint import (
    _create_name_variant_issue,
    _merge_name_issue,
    _redirect_name_issue,
    check_collector_lifespan,
    check_coordinates,
    check_general_type_locality,
    check_location_detail_coordinates,
    check_location_detail_plss,
    check_partial_type_locality,
    check_type_locality_age,
    check_type_locality_distribution_rules,
    check_type_locality_validity,
    check_unique_type_locality,
    infer_included_species,
    infer_tags_from_mapped_entries,
    parse_date,
    take_over_name_issue,
)
from .name import Name, NameTag, TypeTag


@pytest.mark.parametrize("second_text", ["At the river", "In the mountains"])
def test_unique_type_locality_compares_text_not_provenance(second_text: str) -> None:
    article = models.Article.virtual(name="source.pdf")
    name = Mock(spec=Name)
    name.type_locality = Location.virtual(name="Test locality")
    name.original_citation = article
    name.status = Status.valid
    name.taxon = models.Taxon.virtual(age=AgeClass.extant)
    name.has_type_tag.return_value = False
    name.type_tags = (
        TypeTag.LocationDetail("At the river", article),
        TypeTag.LocationDetail(second_text, article, page="10"),
    )

    issues = list(
        check_unique_type_locality.linter(
            name, LintConfig(autofix=False, interactive=False)
        )
    )

    assert len(issues) == (0 if second_text == "At the river" else 1)
    assert len(name.type_tags) == 2


def test_parse_date() -> None:
    assert parse_date("Feb 2013") == "2013-02"
    assert parse_date("1 Feb 2013") == "2013-02-01"
    assert parse_date("23 Feb 2013") == "2013-02-23"
    assert parse_date("July 2013") == "2013-07"
    assert parse_date("7 July 2013") == "2013-07-07"


def test_generic_string_cleanup_normalizes_safe_input_shortcuts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Name, "__str__", lambda _: "test name")
    article = models.Article.virtual(name="source.pdf", kind=ArticleKind.electronic)
    specimen_detail = TypeTag.SpecimenDetail("Adult [M], skin and skull.", article)
    etymology_detail = TypeTag.EtymologyDetail(
        "[M]agnus, from a speci®c epithet.", article
    )
    location_detail = TypeTag.LocationDetail("at 23*29'N, 68*54'W", article)
    name = Name.virtual(
        group=Group.species,
        root_name="example",
        status=Status.valid,
        taxon=models.Taxon.virtual(age=AgeClass.extant),
        original_name=None,
        corrected_original_name=None,
        nomenclature_status=NomenclatureStatus.available,
        target=None,
        author_tags=(),
        original_citation=None,
        page_described=None,
        verbatim_citation=None,
        citation_group=None,
        year=None,
        name_complex=None,
        species_name_complex=None,
        type=None,
        type_locality=None,
        type_specimen=None,
        collection=None,
        genus_type_kind=None,
        species_type_kind=None,
        type_tags=(specimen_detail, etymology_detail, location_detail),
        original_rank=None,
        original_parent=None,
        data=None,
        tags=(),
    )

    issues = list(name.check_all_fields(LintConfig(autofix=False, interactive=False)))
    assert len(issues) == 3
    for issue in issues:
        assert not isinstance(issue, str)
        assert issue.fix is not None
        assert issue.fix.apply() is True
    assert TypeTag.SpecimenDetail("Adult ♂, skin and skull.", article) in name.type_tags
    assert TypeTag.LocationDetail("at 23°29'N, 68°54'W", article) in name.type_tags
    assert (
        TypeTag.EtymologyDetail("[M]agnus, from a specific epithet.", article)
        in name.type_tags
    )


def test_redirect_name_issue_changes_only_redirect_fields() -> None:
    target = cast(Name, SimpleNamespace())
    name = cast(Name, SimpleNamespace(status=Status.synonym, target=None))

    issue = _redirect_name_issue("redirect", name, target)

    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert name.status is Status.redirect
    assert name.target is target


def test_take_over_name_issue_uses_explicit_fields_and_tag_removal() -> None:
    author = AuthorTag.Author(person=models.Person.virtual(family_name="Author"))
    citation = models.Article.virtual(
        name="citation",
        parent=None,
        author_tags=(author,),
        year="1900",
        type=ArticleType.JOURNAL,
    )
    ce = SimpleNamespace(article=citation, page="12", name="Original name")
    page_link = TypeTag.AuthorityPageLink(
        "https://example.com/page", confirmed=True, page="12"
    )
    name = cast(
        Name,
        SimpleNamespace(
            original_citation=None,
            page_described=None,
            original_name=None,
            author_tags=(),
            year=None,
            type_tags=(page_link,),
            get_tags=lambda tags, tag_cls: (
                tag for tag in tags if isinstance(tag, tag_cls)
            ),
        ),
    )

    issue = take_over_name_issue("take over", name, cast(Any, ce))

    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert name.original_citation is citation
    assert name.page_described == "12"
    assert name.original_name == "Original name"
    assert name.author_tags == (author,)
    assert name.year == "1900"
    assert name.type_tags == ()


def test_create_name_variant_issue_creates_without_interactive_cleanup() -> None:
    created = Name.virtual()
    add_variant = Mock(return_value=created)
    base_name = cast(Name, SimpleNamespace(add_variant=add_variant))
    article = object()
    ce = SimpleNamespace(
        article=article,
        page="12",
        name="Original name",
        rank=Rank.species,
        mapped_name=base_name,
    )

    issue = _create_name_variant_issue(
        "create variant",
        base_name,
        NomenclatureStatus.name_combination,
        cast(Any, ce),
        "Genus species",
    )

    assert issue.fix is not None
    assert issue.fix.apply() is True
    add_variant.assert_called_once_with(
        "species",
        status=NomenclatureStatus.name_combination,
        paper=article,
        page_described="12",
        original_name="Original name",
        interactive=False,
    )
    assert created.corrected_original_name == "Genus species"
    assert created.original_rank is Rank.species
    assert ce.mapped_name is created


def test_merge_name_issue_copies_empty_fields_and_redirects() -> None:
    taxon = models.Taxon.virtual(rank=Rank.species, valid_name="Genus species")
    target = Name.virtual(
        root_name="species", group=Group.species, status=Status.synonym, taxon=taxon
    )
    duplicate = Name.virtual(
        root_name="species",
        group=Group.species,
        status=Status.synonym,
        taxon=taxon,
        original_name="Genus species",
    )

    issue = _merge_name_issue("merge duplicate", duplicate, target)

    assert issue.fix is not None
    assert issue.fix.apply() is True
    assert target.original_name == "Genus species"
    assert duplicate.status is Status.redirect
    assert duplicate.target is target
    assert issue.fix.apply() is False


def test_merge_name_issue_does_not_autofix_valid_name() -> None:
    taxon = models.Taxon.virtual(rank=Rank.species, valid_name="Genus species")
    target = Name.virtual(
        root_name="species", group=Group.species, status=Status.synonym, taxon=taxon
    )
    duplicate = Name.virtual(
        root_name="species", group=Group.species, status=Status.valid, taxon=taxon
    )

    issue = _merge_name_issue("merge duplicate", duplicate, target)

    assert issue.fix is None
    assert "cannot autofix a Name with status valid" in issue.message


def _name_with_collector_dates(
    *, death: str | None, dates: tuple[str, ...], additional_collector: bool = False
) -> Name:
    def make_person(person_death: str | None) -> models.Person:
        return models.Person.virtual(
            family_name="Collector",
            death=person_death,
            naming_convention=NamingConvention.general,
            type=PersonType.checked,
        )

    person = make_person(death)
    tags = (
        TypeTag.CollectedBy(person),
        *((TypeTag.CollectedBy(make_person(None)),) if additional_collector else ()),
        *(TypeTag.Date(date) for date in dates),
    )
    return Name.virtual(type_tags=tags)


def test_collector_lifespan_flags_collection_after_death() -> None:
    name = _name_with_collector_dates(death="1900", dates=("2 January 1901",))

    messages = list(check_collector_lifespan(name, LintConfig()))

    assert len(messages) == 1
    assert str(messages[0]).startswith("<virtual Name ")
    assert "collector Collector" in str(messages[0])
    assert "died in 1900, before collection date 2 January 1901" in str(messages[0])


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


def test_virtual_adt_field_preserves_and_protects_model_references() -> None:
    name = _name_with_collector_dates(death="1900", dates=("2 January 1901",))
    (collector,) = name.get_tags(name.type_tags, TypeTag.CollectedBy)

    assert collector.person.is_virtual
    with pytest.raises(VirtualReferenceError):
        Name.type_tags.validate_persistent(name.type_tags)


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
    assert "Coordinates('40.5°N', '74.25°W') is present" in str(messages[0])
    assert "type locality is not set" in str(messages[0])


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
    assert "55.6 km from Location" in str(messages[0])


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
    assert "coordinates exactly match Location" in str(messages[0])
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
        TypeTag.IgnoreLint("coordinates", comment="retain source coordinates"),
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
    assert "55.6 km from Location" in str(messages[0])


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
    assert "cannot infer Coordinates tag" in str(messages[0])
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_distinct_nearby_location_detail_coordinates_are_not_combined() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.", "Locality reported as 40°31'N, 74°15'W."),
        location_coordinates=(None, None),
    )

    messages = list(check_location_detail_coordinates(name, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "multiple coordinate pairs within 5 km" in str(messages[0])
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_location_detail_plss_reports_name_local_conflict() -> None:
    name = _name_with_location_details(
        ("T27S R31E Sec. 3", "T27S R31W Sec. 3"), location_coordinates=(None, None)
    )

    messages = list(check_location_detail_plss(name, LintConfig()))

    assert len(messages) == 1
    assert "incompatible PLSS descriptions" in str(messages[0])


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
    assert "conflicts with type locality" in str(messages[0])
    assert "T33S R25W Sec. 21 NW¼NE¼" in str(messages[0])


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
    assert "conflict with all Coordinates tags" in str(messages[0])


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
    assert "is Recent" in str(messages[0])
    assert "has age fossil" in str(messages[0])


@pytest.mark.parametrize("organ", [SpecimenOrgan.skin, SpecimenOrgan.in_alcohol])
def test_name_recent_organ_conflicts_with_fossil_locality(organ: SpecimenOrgan) -> None:
    name = _name_with_age(
        AgeClass.extant, _location_with_age("Pleistocene", 11_700), organs=(organ,)
    )

    messages = list(check_type_locality_age(name, LintConfig()))

    assert len(messages) == 1
    assert organ.name.replace("_", " ") in str(messages[0])
    assert "indicate a Recent type specimen" in str(messages[0])


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
    assert "is pre-Pleistocene" in str(messages[0])
    assert "recently extinct taxon" in str(messages[0])


def test_name_fossil_taxon_allows_pre_pleistocene_type_locality() -> None:
    name = _name_with_age(AgeClass.fossil, _location_with_age("Pliocene", 2_590_000))

    assert list(check_type_locality_age(name, LintConfig())) == []


def _name_with_general_type_locality(
    *,
    location_name: str = "Example Region",
    region_tagged: bool = False,
    parent_tagged: bool = False,
    imprecise: bool = False,
    partial: bool = False,
) -> Name:
    parent = SimpleNamespace(name="Parent Region", has_tag=lambda tag: parent_tagged)
    region = SimpleNamespace(
        name="Example Region",
        has_tag=lambda tag: region_tagged,
        all_parents=lambda: iter((parent,)),
    )
    return cast(
        Name,
        SimpleNamespace(
            type_locality=SimpleNamespace(name=location_name, region=region),
            has_type_tag=lambda tag: (
                imprecise
                if tag is TypeTag.ImpreciseLocality
                else partial and tag is TypeTag.PartialTypeLocality
            ),
        ),
    )


def test_general_type_locality_lint_reports_tagged_region() -> None:
    name = _name_with_general_type_locality(region_tagged=True)

    assert list(check_general_type_locality.linter(name, LintConfig())) == [
        (
            "type locality 'Example Region' matches its Region name, and Region "
            "'Example Region' is tagged MustHavePreciseTypeLocality; use a more "
            "precise Location or add the ImpreciseLocality tag"
        )
    ]


def test_general_type_locality_lint_inherits_tag_from_parent() -> None:
    name = _name_with_general_type_locality(parent_tagged=True)

    messages = list(check_general_type_locality.linter(name, LintConfig()))

    assert len(messages) == 1
    assert "Region 'Parent Region' is tagged MustHavePreciseTypeLocality" in str(
        messages[0]
    )


def test_general_type_locality_lint_allows_more_precise_location() -> None:
    name = _name_with_general_type_locality(
        location_name="Precise site", region_tagged=True
    )

    assert list(check_general_type_locality.linter(name, LintConfig())) == []


def test_general_type_locality_lint_requires_region_tag() -> None:
    name = _name_with_general_type_locality()

    assert list(check_general_type_locality.linter(name, LintConfig())) == []


def test_general_type_locality_lint_allows_imprecise_locality_tag() -> None:
    name = _name_with_general_type_locality(region_tagged=True, imprecise=True)

    assert list(check_general_type_locality.linter(name, LintConfig())) == []


def test_general_type_locality_lint_allows_partial_type_locality_tag() -> None:
    name = _name_with_general_type_locality(region_tagged=True, partial=True)

    assert list(check_general_type_locality.linter(name, LintConfig())) == []


def _partial_type_locality_name(
    *,
    species_type_kind: SpeciesGroupType | None = SpeciesGroupType.syntypes,
    partial_count: int = 2,
    has_type_locality: bool = True,
    container_name: str = "Example Region fossil",
    container_period: str = "Phanerozoic",
    outside_region: bool = False,
) -> Name:
    parent_region = SimpleNamespace(id=1, name="Example Region", parent=None)
    other_region = SimpleNamespace(id=2, name="Other Region", parent=None)
    period = SimpleNamespace(name=container_period)
    container = SimpleNamespace(
        id=100,
        name=container_name,
        region=parent_region,
        min_period=period,
        max_period=period,
    )
    partials = []
    for index in range(partial_count):
        region = other_region if outside_region and index == 0 else parent_region
        location = cast(
            Location,
            SimpleNamespace(id=200 + index, name=f"Partial {index + 1}", region=region),
        )
        partials.append(TypeTag.PartialTypeLocality(location))
    name = SimpleNamespace(
        species_type_kind=species_type_kind,
        type_locality=container if has_type_locality else None,
        type_tags=tuple(partials),
    )
    name.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    return cast(Name, name)


@pytest.mark.parametrize("species_type_kind", [None, SpeciesGroupType.syntypes])
def test_partial_type_locality_lint_accepts_supported_type_kinds(
    species_type_kind: SpeciesGroupType | None,
) -> None:
    name = _partial_type_locality_name(species_type_kind=species_type_kind)

    assert list(check_partial_type_locality.linter(name, LintConfig())) == []


def test_partial_type_locality_lint_rejects_holotype() -> None:
    name = _partial_type_locality_name(species_type_kind=SpeciesGroupType.holotype)

    messages = list(check_partial_type_locality.linter(name, LintConfig()))

    assert len(messages) == 1
    assert isinstance(messages[0], str)
    assert "not syntypes or unset" in messages[0]


def test_partial_type_locality_lint_requires_multiple_locations() -> None:
    name = _partial_type_locality_name(partial_count=1)

    messages = list(check_partial_type_locality.linter(name, LintConfig()))

    assert len(messages) == 1
    assert isinstance(messages[0], str)
    assert "more than one distinct PartialTypeLocality" in messages[0]


def test_partial_type_locality_lint_requires_type_locality() -> None:
    name = _partial_type_locality_name(has_type_locality=False)

    messages = list(check_partial_type_locality.linter(name, LintConfig()))

    assert messages == ["has PartialTypeLocality tags but no type locality"]


def test_partial_type_locality_lint_requires_regionwide_container() -> None:
    name = _partial_type_locality_name(container_name="Example Region Pleistocene")

    messages = list(check_partial_type_locality.linter(name, LintConfig()))

    assert len(messages) == 1
    assert isinstance(messages[0], str)
    assert "must be either the Recent Location" in messages[0]


def test_partial_type_locality_lint_accepts_recent_container() -> None:
    name = _partial_type_locality_name(
        container_name="Example Region", container_period="Recent"
    )

    assert list(check_partial_type_locality.linter(name, LintConfig())) == []


def test_partial_type_locality_lint_requires_contained_locations() -> None:
    name = _partial_type_locality_name(outside_region=True)

    messages = list(check_partial_type_locality.linter(name, LintConfig()))

    assert len(messages) == 1
    assert isinstance(messages[0], str)
    assert "is outside type-locality Region 'Example Region'" in messages[0]


@pytest.mark.parametrize("fossil", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_partial_type_locality_lint_fixes_smallest_enclosing_region(
    monkeypatch: pytest.MonkeyPatch, *, fossil: bool, nested: bool
) -> None:
    continent = models.Region.virtual(name="Continent", parent=None)
    country = models.Region.virtual(name="Country", parent=continent)
    first_region = models.Region.virtual(name="First province", parent=country)
    second_region = models.Region.virtual(name="Second province", parent=country)
    period = models.Period.virtual(name="Phanerozoic" if fossil else "Recent")
    suffix = " fossil" if fossil else ""
    container = Location.virtual(
        name=f"Continent{suffix}",
        region=continent,
        min_period=period,
        max_period=period,
    )
    expected = Location.virtual(
        name=f"Country{suffix}", region=country, min_period=period, max_period=period
    )
    first = Location.virtual(
        name="First site", region=first_region if nested else country
    )
    second = Location.virtual(
        name="Second site", region=second_region if nested else country
    )
    tags = (TypeTag.PartialTypeLocality(first), TypeTag.PartialTypeLocality(second))
    name = Name.virtual(
        type_locality=container,
        species_type_kind=SpeciesGroupType.syntypes,
        type_tags=tags,
    )
    get_period = Mock(return_value=period)
    get_container = Mock(return_value=expected)
    monkeypatch.setattr(models.Period, "get", get_period)
    monkeypatch.setattr(Location, "get_or_create_general", get_container)

    issues = list(check_partial_type_locality.linter(name, LintConfig()))

    assert len(issues) == 1
    issue = issues[0]
    assert not isinstance(issue, str)
    assert "smallest enclosing Region 'Country'" in issue.message
    assert name.type_locality == container
    get_period.assert_not_called()
    get_container.assert_not_called()
    assert issue.fix is not None
    assert issue.fix.apply() is True
    get_period.assert_called_once_with(name="Phanerozoic" if fossil else "Recent")
    get_container.assert_called_once_with(country, period)
    assert name.type_locality == expected
    assert name.type_tags == tags
    assert issue.fix.apply() is False
    assert list(check_partial_type_locality.linter(name, LintConfig())) == []


def test_partial_type_locality_lint_accepts_lowest_common_ancestor() -> None:
    root = models.Region.virtual(name="Root", parent=None)
    first_region = models.Region.virtual(name="First region", parent=root)
    second_region = models.Region.virtual(name="Second region", parent=root)
    period = models.Period.virtual(name="Recent")
    container = Location.virtual(
        name="Root", region=root, min_period=period, max_period=period
    )
    name = Name.virtual(
        type_locality=container,
        species_type_kind=SpeciesGroupType.syntypes,
        type_tags=(
            TypeTag.PartialTypeLocality(
                Location.virtual(name="First", region=first_region)
            ),
            TypeTag.PartialTypeLocality(
                Location.virtual(name="Second", region=second_region)
            ),
            TypeTag.PartialTypeLocality(
                Location.virtual(name="Third", region=first_region)
            ),
        ),
    )

    assert list(check_partial_type_locality.linter(name, LintConfig())) == []


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
    assert "only allows occurrence_dubious or classification_dubious" in str(
        messages[0]
    )


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
    assert "where RedirectOccurrences says" in str(messages[0])


@pytest.mark.parametrize("group", list(Group))
@pytest.mark.parametrize(
    ("tag", "allowed"),
    [
        (
            TypeTag.InterpretedTypeTaxon("Evidence for the type taxon."),
            {Group.genus, Group.family},
        ),
        (
            TypeTag.InterpretedTypeSpecimen("Evidence for the specimen."),
            {Group.species},
        ),
        (
            TypeTag.InterpretedTypeLocality("Evidence for the locality."),
            {Group.species},
        ),
        (TypeTag.GenusCoelebs(), {Group.genus}),
        (TypeTag.NoOriginalParent, {Group.genus}),
        (
            TypeTag.SpecimenDetail(
                "Original specimen.", models.Article.virtual(name="test.pdf")
            ),
            {Group.species},
        ),
        (
            TypeTag.TypeSpeciesDetail(
                "Type species evidence.", models.Article.virtual(name="test.pdf")
            ),
            {Group.genus},
        ),
        (
            TypeTag.DescriptionDetail(
                "Description valid at any rank.",
                models.Article.virtual(name="test.pdf"),
            ),
            set(Group),
        ),
    ],
)
def test_type_tag_group_restrictions(
    group: Group, tag: TypeTag, allowed: set[Group]
) -> None:
    from .lint import ATTRIBUTES_BY_GROUP, check_disallowed_attributes

    name = Name.virtual(
        group=group, type_tags=(tag,), **dict.fromkeys(ATTRIBUTES_BY_GROUP)
    )
    messages = list(check_disallowed_attributes(name, LintConfig(autofix=True)))
    assert bool(messages) == (group not in allowed)
    if messages:
        assert type(tag).__name__ in str(messages[0])
    assert name.type_tags == (tag,)
