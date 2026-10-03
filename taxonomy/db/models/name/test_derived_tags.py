import pytest

from taxonomy.db.constants import (
    AgeClass,
    AltitudeUnit,
    Group,
    NamingConvention,
    PersonType,
    SpeciesGroupType,
    SpecimenAge,
    SpecimenGender,
    SpecimenOrgan,
)
from taxonomy.db.models import Article, Name, Person, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint_types import LintIssue

from . import derived_tags as extract
from .lint import derived_tags as lint
from .name import TypeTag

CFG = LintConfig(autofix=False, interactive=False, available_resources=frozenset())


def make_name(text: str, *, location: bool = False) -> Name:
    source = Article.virtual(name="source.pdf")
    cls = TypeTag.LocationDetail if location else TypeTag.SpecimenDetail
    return Name.virtual(
        group=Group.species,
        year="2000",
        species_type_kind=SpeciesGroupType.holotype,
        taxon=Taxon.virtual(age=AgeClass.extant),
        type_tags=(cls(text, source),),
    )


def test_gender_boundaries() -> None:
    assert extract.extract_gender("Adult female.")[0].value == TypeTag.Gender(
        SpecimenGender.female
    )
    assert extract.extract_gender("Type: ♂.")[0].value == TypeTag.Gender(
        SpecimenGender.male
    )
    assert not extract.extract_gender("Two females and one male.")
    assert not extract.extract_gender("Adult male?")
    assert not extract.extract_gender("The female paratype.")
    assert not extract.extract_gender("Sex unknown.")
    assert not extract.extract_gender("Male and female.")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Type. Adult female.", SpecimenAge.adult),
        ("Type ♂ ad.", SpecimenAge.adult),
        ("Subadult male", SpecimenAge.subadult),
        ("sub-adult", SpecimenAge.subadult),
        ("juvenile", SpecimenAge.juvenile),
        ("juv.", SpecimenAge.juvenile),
    ],
)
def test_age(text: str, expected: SpecimenAge) -> None:
    assert extract.extract_age(text) == [
        extract.Evidence(TypeTag.Age(expected), certain=True)
    ]


def test_age_validation_only() -> None:
    assert extract.extract_age("Young male.")[0].certain is False
    assert extract.extract_age("Young male.")[0].value == TypeTag.Age(
        SpecimenAge.juvenile
    )
    assert not extract.extract_age("Adult female with two embryos.")
    assert not extract.extract_age("Adult holotype and juvenile paratype.")


@pytest.mark.parametrize(
    "text",
    [
        "Collected 11 March, 1906, by Dr. W. J. Ansorge.",
        "Obtained on March 11th, 1906.",
        "Taken 11 Mar. 1906.",
        "[catalogNumber] 1909.10.1.49 ... [eventdate] 1906-03-11 ... [verbatimeventdate] 11 March 1906 [at https://example.org]",
    ],
)
def test_date(text: str) -> None:
    assert extract.extract_date(text) == [
        extract.Evidence(TypeTag.Date("11 March 1906"), certain=True)
    ]


@pytest.mark.parametrize(
    "text",
    [
        "B.M. No. 9.10.1.49. Original number 17.",
        "Collected 31 February 1906.",
        "Received 11 March 1906.",
        "Presented 11 March 1906.",
        "[eventdate] 1906-02-31 [at https://example.org]",
        "[eventdate] 1906-03-11/1906-03-15 [at https://example.org]",
    ],
)
def test_not_a_collection_date(text: str) -> None:
    assert extract.extract_date(text) == []


def test_partial_date_and_precision() -> None:
    assert extract.extract_date("[eventdate] 1906-03 [at https://example.org]")[
        0
    ].value == TypeTag.Date("March 1906")
    assert extract.tags_compatible(TypeTag.Date("1906"), TypeTag.Date("11 March 1906"))
    assert not extract.tags_compatible(
        TypeTag.Date("11 March 1906"), TypeTag.Date("12 March 1906")
    )
    assert not extract.tags_compatible(TypeTag.Date("<1906"), TypeTag.Date("1906"))
    assert not extract.extract_date("Type, 11 March 1906.")[0].certain


@pytest.mark.parametrize(
    "text", ["Altitude 3,200 ft.", "elevation: 3200 feet", "3200 ft above sea level"]
)
def test_altitude(text: str) -> None:
    assert extract.extract_altitude(text) == [
        extract.Evidence(TypeTag.Altitude("3200", AltitudeUnit.ft), certain=True)
    ]


@pytest.mark.parametrize(
    "text",
    [
        "500 m north of camp",
        "500 m. E of town",
        "depth 500 m",
        "500 m below the summit",
        "skin 500 mm long",
        "10° 30′ N",
    ],
)
def test_not_altitude(text: str) -> None:
    assert extract.extract_altitude(text) == []


def test_altitude_precision() -> None:
    assert extract.extract_altitude("Camp, 500 m")[0].certain is False
    assert extract.extract_altitude("altitude 500–600 m")[0].value == TypeTag.Altitude(
        "500-600", AltitudeUnit.m
    )
    assert extract.extract_altitude("altitude -20 m")[0].value == TypeTag.Altitude(
        "-20", AltitudeUnit.m
    )
    assert extract.tags_compatible(
        TypeTag.Altitude("1000", AltitudeUnit.ft),
        TypeTag.Altitude("305", AltitudeUnit.m),
    )
    assert extract.tags_compatible(
        TypeTag.Altitude("500-600", AltitudeUnit.m),
        TypeTag.Altitude("550", AltitudeUnit.m),
    )
    assert not extract.tags_compatible(
        TypeTag.Altitude("1000", AltitudeUnit.ft),
        TypeTag.Altitude("1000", AltitudeUnit.m),
    )


def test_preparations() -> None:
    expected = [
        extract.Evidence(TypeTag.Organ(organ), certain=True)
        for organ in (SpecimenOrgan.skin, SpecimenOrgan.skull)
    ]
    assert extract.extract_organ("Skin and skull.") == expected
    assert (
        extract.extract_organ("[preparations] Skin; Skull [at https://example.org]")
        == expected
    )
    assert extract.extract_organ("Holotype: skin and skull.") == expected
    assert extract.extract_organ("Skin, study; skull.") == expected
    assert extract.extract_organ("Skull length 25 mm. Skin dark brown.") == []
    assert extract.extract_organ("Skin and skull, the latter missing.") == []
    assert extract.extract_organ("Skin and partial skull.") == []
    assert extract.extract_organ("Skull of the paratype.") == []
    assert extract.extract_organ("Not preserved in alcohol.") == []


def test_catalog_fields_do_not_leak() -> None:
    text = "[determinationNames] Mus adultus 1906 ... [sex] female ... [lifestage] adult ... [eventdate] 1900-01-02 ... [preparations] Skin; Skull [at https://example.org]"
    assert extract.extract_gender(text)[0].value == TypeTag.Gender(
        SpecimenGender.female
    )
    assert extract.extract_age(text)[0].value == TypeTag.Age(SpecimenAge.adult)
    assert extract.extract_date(text)[0].value == TypeTag.Date("2 January 1900")
    assert len(extract.extract_organ(text)) == 2
    assert (
        extract.extract_gender(
            "[determinationNames] female mouse [at https://example.org]"
        )
        == []
    )


def test_reviewable_idempotent_addition() -> None:
    nam = make_name("Adult male.")
    original = nam.type_tags
    issues = list(lint.check_specimen_detail_gender(nam, CFG))
    assert nam.type_tags == original
    assert len(issues) == 1
    issue = issues[0]
    assert isinstance(issue, LintIssue)
    assert issue.fix is not None
    issue.fix.apply()
    assert nam.has_type_tag(TypeTag.Gender)
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []


def test_conflicting_sources_never_autofix() -> None:
    nam = make_name("Adult male.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.SpecimenDetail("Adult female.", Article.virtual(name="other.pdf")),
    )
    issues = list(lint.check_specimen_detail_gender(nam, CFG))
    assert len(issues) == 1
    assert "conflicting source evidence" in str(issues[0])
    assert isinstance(issues[0], LintIssue)
    assert issues[0].fix is None


def test_validate_existing_without_replacing() -> None:
    nam = make_name("Young male.")
    nam.type_tags = (*nam.type_tags, TypeTag.Age(SpecimenAge.adult))  # type: ignore[assignment]
    original = nam.type_tags
    issues = list(lint.check_specimen_detail_age(nam, CFG))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].fix is None
    assert nam.type_tags == original


def test_no_tag_requires_review() -> None:
    nam = make_name("Adult male.")
    nam.type_tags = (*nam.type_tags, TypeTag.NoAge(nam.type_tags[0].source))  # type: ignore[assignment]
    issues = list(lint.check_specimen_detail_age(nam, CFG))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].fix is None
    assert "NoAge" in str(issues[0])


@pytest.mark.parametrize(
    "kind", [SpeciesGroupType.syntypes, SpeciesGroupType.nonexistent, None]
)
def test_no_single_specimen_inference(kind: SpeciesGroupType | None) -> None:
    nam = make_name("Adult male.")
    nam.species_type_kind = kind
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []


def test_neotype_source_filter() -> None:
    nam = make_name("Adult male.")
    nam.species_type_kind = SpeciesGroupType.neotype
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []
    new_source = Article.virtual(name="neotype.pdf")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.NeotypeDesignation(
            neotype="X 1", optional_source=new_source, valid=True
        ),
        TypeTag.SpecimenDetail("Adult female.", new_source),
    )
    issues = list(lint.check_specimen_detail_gender(nam, CFG))
    assert len(issues) == 1
    assert "female" in str(issues[0])


def test_fossil_organ_is_out_of_scope() -> None:
    nam = make_name("Skin and skull.")
    nam.taxon.age = AgeClass.fossil
    assert list(lint.check_specimen_detail_organ(nam, CFG)) == []


def test_existing_organ_detail_is_preserved() -> None:
    nam = make_name("Skull.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.Organ(SpecimenOrgan.skull, condition="broken"),
    )
    assert list(lint.check_specimen_detail_organ(nam, CFG)) == []


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Collected 11 March, 1906, by Dr. W. J. Ansorge.", ["W. J. Ansorge"]),
        ("Collected by John Smith, 11 March 1906.", ["John Smith"]),
        (
            "Collected March 31, 1900 by E. W. Nelson and E. A. Goldman. Original No. 14074.",
            ["E. W. Nelson", "E. A. Goldman"],
        ),
        (
            "[recordedby] Collector(s): John Smith [at https://example.org]",
            ["John Smith"],
        ),
        ("Presented by John Smith.", []),
    ],
)
def test_collectors(text: str, expected: list[str]) -> None:
    assert [e.value for e in extract.extract_collectors(text)] == expected


def make_person(given: str = "John", family: str = "Smith") -> Person:
    return Person.virtual(
        family_name=family,
        given_names=given,
        initials=None,
        tussenvoegsel=None,
        naming_convention=NamingConvention.english,
        type=PersonType.checked,
    )


def test_collector_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    person = make_person()
    nam = make_name("Collected by John Smith.")
    monkeypatch.setattr(lint, "_person_index", lambda: {"johnsmith": {person}})
    issues = list(lint.check_specimen_detail_collector(nam, CFG))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    # A duplicate full-name match must disable inference.
    other = make_person()
    monkeypatch.setattr(lint, "_person_index", lambda: {"johnsmith": {person, other}})
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_collector_team_not_partially_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    person = make_person()
    monkeypatch.setattr(lint, "_person_index", lambda: {"johnsmith": {person}})
    nam = make_name("Collected by John Smith and J. Doe.")
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_ignore_suppresses_additions_and_conflicts() -> None:
    nam = make_name("Adult male.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.IgnoreLint("specimen_detail_gender", comment="Reviewed source"),
    )
    original = nam.type_tags
    cfg = LintConfig(autofix=True, interactive=False, available_resources=frozenset())
    assert list(lint.check_specimen_detail_gender(nam, cfg)) == []
    assert nam.type_tags == original
    nam.type_tags = (*nam.type_tags, TypeTag.Gender(SpecimenGender.female))  # type: ignore[assignment]
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []


def test_later_source_can_supply_originally_absent_data() -> None:
    nam = make_name("Adult male.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.NoAge(Article.virtual(name="original.pdf")),
    )
    issues = list(lint.check_specimen_detail_age(nam, CFG))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].fix is not None


@pytest.mark.parametrize(
    "text", ["Young adult male.", "Nearly adult male.", "Immature male."]
)
def test_ambiguous_age_is_validation_only(text: str) -> None:
    nam = make_name(text)
    assert list(lint.check_specimen_detail_age(nam, CFG)) == []
    nam.type_tags = (*nam.type_tags, TypeTag.Age(SpecimenAge.subadult))  # type: ignore[assignment]
    assert list(lint.check_specimen_detail_age(nam, CFG)) == []


def test_mixed_ages_not_reduced_to_one() -> None:
    assert extract.extract_age("Adult and sub-adult.") == []


def test_catalog_january_first_requires_corroboration() -> None:
    assert not extract.extract_date("[eventdate] 1890-01-01 [at https://example.org]")[
        0
    ].certain
    assert extract.extract_date(
        "[eventdate] 1890-01-01 ... [verbatimeventdate] 1 January 1890 [at https://example.org]"
    )[0].certain


def test_date_after_publication_not_added() -> None:
    nam = make_name("Collected 1 March 2001.")
    issues = list(lint.check_specimen_detail_date(nam, CFG))
    assert len(issues) == 1
    assert isinstance(issues[0], LintIssue)
    assert issues[0].fix is None
    assert "postdates" in str(issues[0])


def test_museum_transfer_and_date_range_not_collection() -> None:
    assert extract.extract_date("Ex: MZB (MZB 3112), 12 January 1950.") == []
    assert extract.extract_date("Collected 5–7 March 1909.") == []
    assert extract.extract_date("Died in the zoo on 5 March 1909.") == []


def test_acquisition_is_validation_only() -> None:
    text = "[eventdate] 1923-10-15 ... [occurrenceremarks] Purchased in Peking [at https://example.org]"
    assert not extract.extract_date(text)[0].certain


def test_altitude_ambiguous_decimal_and_horizontal_offset() -> None:
    assert extract.extract_altitude("Alt. 1.800 m") == []
    assert extract.extract_altitude("1600 ft (488 m) above lake") == []
    assert (
        extract.extract_altitude("Range: Southern India from 3000 ft to 6000 ft") == []
    )
    assert (
        extract.extract_altitude(
            "Langbian Peak rises higher than 2100 m above sea level"
        )
        == []
    )
    assert extract.extract_altitude("500 to 600 m") == []
    assert not extract.extract_altitude("about 500 m above sea level")[0].certain


def test_skull_and_skeleton_means_postcranium() -> None:
    evidence = extract.extract_organ("Skin, skull, and skeleton.")
    assert {item.value.organ for item in evidence} == {
        SpecimenOrgan.skin,
        SpecimenOrgan.skull,
        SpecimenOrgan.postcranial_skeleton,
    }


def test_neotype_mention_not_used_for_original_type() -> None:
    nam = make_name("The proposed neotype is an adult male.")
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []


def test_lectotype_requires_designation_source() -> None:
    nam = make_name("Adult male.")
    nam.species_type_kind = SpeciesGroupType.lectotype
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []
    source = nam.type_tags[0].source
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.LectotypeDesignation(
            lectotype="X 1", valid=True, optional_source=source
        ),
    )
    assert len(list(lint.check_specimen_detail_gender(nam, CFG))) == 1


def test_unsafe_other_account_blocks_inference() -> None:
    nam = make_name("Adult male.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.SpecimenDetail("Male or female?", Article.virtual(name="later.pdf")),
    )
    assert list(lint.check_specimen_detail_gender(nam, CFG)) == []


def test_catalog_paratype_and_negative_remarks() -> None:
    assert (
        extract.extract_gender(
            "[sex] male ... [typestatus] paratype [at https://example.org]"
        )
        == []
    )
    assert (
        extract.extract_organ(
            "[preparations] Skin; Skull ... [occurrenceremarks] skull lost [at https://example.org]"
        )
        == []
    )


def test_collector_shortened_initials_do_not_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = make_person(given="Henry Herbert")
    nam = make_name("Collected by H. Smith.")
    nam.type_tags = (*nam.type_tags, TypeTag.CollectedBy(person))  # type: ignore[assignment]
    monkeypatch.setattr(lint, "_person_index", dict)
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []
    assert "hsmith" in lint._person_forms(person)


def test_collector_one_initial_not_added(monkeypatch: pytest.MonkeyPatch) -> None:
    person = make_person()
    nam = make_name("Collected by J. Smith.")
    nam.type_tags = (*nam.type_tags, TypeTag.CollectedBy(person))  # type: ignore[assignment]
    monkeypatch.setattr(lint, "_person_index", lambda: {"jsmith": {person}})
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_unparsed_collector_team_blocks_other_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = make_person()
    nam = make_name("Collected by John Smith.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.SpecimenDetail(
            "Collected by John Smith and Doe.", Article.virtual(name="other.pdf")
        ),
    )
    monkeypatch.setattr(lint, "_person_index", lambda: {"johnsmith": {person}})
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_collector_cannot_be_description_author() -> None:
    text = "The type was collected at 6000 feet. Collected by the Pratt brothers in January 1920 and described by Oldfield Thomas in the same year."
    values = extract.extract_collectors(text)
    assert all("Thomas" not in item.value for item in values)
    assert (
        extract.extract_collectors("Collected in 1900 and presented by John Smith.")
        == []
    )


def test_preserved_skin_not_implied_by_skin_in_alcohol() -> None:
    assert extract.extract_organ("Skin in alcohol; skull removed.") == []
    assert (
        extract.extract_organ("[preparations] With Skin [at https://example.org]") == []
    )


def test_skeleton_equivalent_to_parts() -> None:
    nam = make_name("Skin and skeleton.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.Organ(SpecimenOrgan.skin),
        TypeTag.Organ(SpecimenOrgan.skull),
        TypeTag.Organ(SpecimenOrgan.postcranial_skeleton),
    )
    assert list(lint.check_specimen_detail_organ(nam, CFG)) == []


def test_collector_given_names_and_initials() -> None:
    assert lint._matches_person("J. Alden Smith", make_person(given="John Alden"))
    assert lint._matches_person("John Alden Smith", make_person(given="J. A."))
    assert not lint._matches_person("James Smith", make_person(given="John"))


@pytest.mark.parametrize("text", ["altitude 300 m (?)", "altitude 300 m or 400 m"])
def test_altitude_uncertainty_after_value(text: str) -> None:
    nam = make_name(text, location=True)
    assert list(lint.check_location_detail_altitude(nam, CFG)) == []


def test_normalized_catalog_date_is_validation_only() -> None:
    nam = make_name("[eventdate] 1900-04-03 [at https://example.org]")
    assert list(lint.check_specimen_detail_date(nam, CFG)) == []
    nam.type_tags = (*nam.type_tags, TypeTag.Date("3 April 1901"))  # type: ignore[assignment]
    issues = list(lint.check_specimen_detail_date(nam, CFG))
    assert len(issues) == 1
    assert issues[0].fix is None


@pytest.mark.parametrize(
    "text", ["Collected by John Smith, Jane Doe.", "Collected by John Smith; Jane Doe."]
)
def test_collector_team_not_truncated(
    text: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    person = make_person()
    nam = make_name(text)
    monkeypatch.setattr(lint, "_person_index", lambda: {"johnsmith": {person}})
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_overlapping_altitudes_validate_but_do_not_infer() -> None:
    nam = make_name("altitude 500-600 m", location=True)
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.LocationDetail("altitude 550-650 m", Article.virtual(name="other.pdf")),
    )
    issues = list(lint.check_location_detail_altitude(nam, CFG))
    assert len(issues) == 1
    assert issues[0].fix is None
    nam.type_tags = (*nam.type_tags, TypeTag.Altitude("575", AltitudeUnit.m))  # type: ignore[assignment]
    assert list(lint.check_location_detail_altitude(nam, CFG)) == []


@pytest.mark.parametrize("text", ["altitude -20--10 m", "altitude 10.5-20.5 m"])
def test_unsupported_altitude_tag_syntax_is_validation_only(text: str) -> None:
    assert not extract.extract_altitude(text)[0].certain


@pytest.mark.parametrize(
    "text", ["Collected Sept. 5, 1909, by E. Kemp.", "Type. Collector, E. Kemp."]
)
def test_collector_abbreviated_month_or_label(text: str) -> None:
    assert [item.value for item in extract.extract_collectors(text)] == ["E. Kemp"]


def test_unresolved_original_collector_blocks_catalog_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    person = make_person(given="Robin", family="Kemp")
    nam = make_name("Type. Collected Sept. 5, 1909, by E. Kemp.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.SpecimenDetail(
            "[recordedBy] Robin Kemp [at https://example.org]",
            Article.virtual(name="catalog"),
        ),
    )
    monkeypatch.setattr(lint, "_person_index", lambda: {"robinkemp": {person}})
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


@pytest.mark.parametrize("number", ["1 620", "2 438", "1\u00a0620", "1\u202f620"])
def test_altitude_space_thousands(number: str) -> None:
    value = "".join(number.split())
    assert extract.extract_altitude(f"Altitude {number} m") == [
        extract.Evidence(TypeTag.Altitude(value, AltitudeUnit.m), certain=True)
    ]


@pytest.mark.parametrize("number", ["1 62", "12 34", "1 62 000"])
def test_altitude_malformed_thousands(number: str) -> None:
    assert not extract.extract_altitude(f"Altitude {number} m")


@pytest.mark.parametrize("heading", ["Range:", "Range.—", "Range —"])
def test_altitude_ignores_following_species_range(heading: str) -> None:
    assert extract.extract_altitude(
        f"Altitude, 3,200 feet. {heading} Wooded slopes above 3,000 feet."
    ) == [extract.Evidence(TypeTag.Altitude("3200", AltitudeUnit.ft), certain=True)]


def test_subadult_abbreviation() -> None:
    assert extract.extract_age("Lektotypus: Nr. 4224, ♀ s. ad.") == [
        extract.Evidence(TypeTag.Age(SpecimenAge.subadult), certain=False)
    ]


def test_ages_belonging_to_different_individuals() -> None:
    assert not extract.extract_age(
        "The only specimen was the skeleton of a young animal. Since then, "
        "the institution has been presented with a skeleton of a very old individual."
    )


def test_gender_of_other_cotype() -> None:
    assert not extract.extract_gender(
        "COTIPOS: M. N. n.o 841 (o esqueleto) e 1441 (♀ pele) — (Lectotipo-841)."
    )


@pytest.mark.parametrize(
    "text",
    [
        "[MC: Skin cataloged Jul 1868; skull 31 Mar 1898].",
        "[MC: Cataloged 16 Dec 1886].",
        "Catalogued 16 December 1886.",
        "Ex: MZB (MZB 6363; 16/35), 12 January 1950.",
    ],
)
def test_collection_date_excludes_museum_events(text: str) -> None:
    assert not extract.extract_date(text)
    assert extract.extract_date(f"Collected 23 January 1935. {text}") == [
        extract.Evidence(TypeTag.Date("23 January 1935"), certain=True)
    ]


def test_paratopotype_date() -> None:
    assert not extract.extract_date(
        "Type. October 26, 1915. Adult ♂, skin and skull. "
        "One adult paratopotype ♂, skin and skull, August 3, 1918."
    )


def test_preparations_of_different_specimens() -> None:
    assert not extract.extract_organ(
        "a. Skin. Shaik Othman. 24. II. 95. b, c. Skins; d. In spirit. Lahej."
    )


def test_collector_native_name_order() -> None:
    person = make_person(given="Son", family="Nguyen")
    person.tussenvoegsel = "Truong"
    person.naming_convention = NamingConvention.vietnamese
    assert lint._matches_person("Nguyen Truong Son", person)
    assert "nguyentruongson" in lint._person_forms(person, abbreviated=False)
    nam = make_name("Collected by Nguyen Truong Son.")
    nam.type_tags = (*nam.type_tags, TypeTag.CollectedBy(person))  # type: ignore[assignment]
    assert list(lint.check_specimen_detail_collector(nam, CFG)) == []


def test_additional_preparation_is_not_a_contradiction() -> None:
    nam = make_name("Skin; skull; baculum.")
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.Organ(SpecimenOrgan.skin),
        TypeTag.Organ(SpecimenOrgan.skull),
    )
    (issue,) = lint.check_specimen_detail_organ(nam, CFG)
    assert "not represented in existing Organ tags" in issue.message
    assert issue.fix is None


def test_habitat_elevation_does_not_establish_type_elevation() -> None:
    text = (
        "It was common at Zebil, in the Bulgar Dagh, on the rocky bushy "
        "hill-sides, at an elevation of 3000–4000 feet."
    )
    assert not extract.extract_altitude(text)[0].certain
    nam = make_name(text, location=True)
    assert list(lint.check_location_detail_altitude(nam, CFG)) == []


@pytest.mark.parametrize(
    "event", ["Purchased by Sody", "Received", "Cataloged", "Died in captivity"]
)
def test_catalog_date_corroboration_does_not_establish_collection(event: str) -> None:
    nam = make_name(
        "[eventDate] 1933-02-08 ... [verbatimEventDate] 8 February 1933"
        " [at https://example.org]"
    )
    nam.type_tags = (  # type: ignore[assignment]
        *nam.type_tags,
        TypeTag.SpecimenDetail(
            f"Holotype. {event}, 8 February 1933.", Article.virtual(name="catalog.pdf")
        ),
    )
    assert list(lint.check_specimen_detail_date(nam, CFG)) == []


def test_purchase_date_is_not_collection_evidence() -> None:
    assert not extract.extract_date("Purchased by Sody, 8 February 1933.")
