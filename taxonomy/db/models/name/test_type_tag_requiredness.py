import pytest

from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    CoverageLevel,
    Group,
    NomenclatureStatus,
    Rank,
    SourceLanguage,
    SpeciesGroupType,
    SpeciesNameKind,
    Status,
    TypeSpeciesDesignation,
)
from taxonomy.db.models import Article, Collection, Name, Person, Taxon
from taxonomy.db.models.article import ArticleTag
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.collection import MULTIPLE_COLLECTION
from taxonomy.db.models.name_complex import SpeciesNameComplex
from taxonomy.db.models.tags import TaxonTag

from .name import TypeTag, TypeTagCons
from .required_fields import (
    Requiredness,
    get_missing_requirements,
    get_requiredness,
    get_type_tag_requiredness,
    get_type_tag_status,
)

CFG = LintConfig(autofix=False, interactive=False, available_resources=frozenset())
EXPERIMENTAL = LintConfig(
    autofix=False, interactive=False, experimental=True, available_resources=frozenset()
)


def make_name(*, group: Group = Group.species, year: str = "2021") -> Name:
    article = Article.virtual(name="original.pdf", kind=ArticleKind.electronic, tags=())
    taxon = Taxon.virtual(
        valid_name="Example",
        rank=Rank.genus,
        age=AgeClass.extant,
        tags=(TaxonTag.Coverage(CoverageLevel.high),),
    )
    return Name.virtual(
        taxon=taxon,
        group=group,
        root_name="example",
        status=Status.valid,
        nomenclature_status=NomenclatureStatus.available,
        year=year,
        original_rank=Rank.species,
        original_citation=article,
        type_tags=(),
        tags=(),
    )


@pytest.mark.parametrize(
    "tag_cls", [TypeTag.EtymologyDetail, TypeTag.LocationDetail, TypeTag.SpecimenDetail]
)
def test_detail_enforcement_starts_with_original_data(tag_cls: TypeTagCons) -> None:
    nam = make_name()
    original = nam.original_citation
    assert original is not None
    # Experimental mode must not bypass the deliberate no-data exemption.
    assert (
        get_type_tag_requiredness(nam, tag_cls, cfg=EXPERIMENTAL).requiredness
        is Requiredness.pending_required_high_coverage
    )
    other = Article.virtual(name="later.pdf")
    nam.type_tags = (TypeTag.CitationDetail("Later evidence", other),)  # type: ignore[assignment]
    assert (
        get_type_tag_requiredness(nam, tag_cls).requiredness
        is Requiredness.pending_required_high_coverage
    )
    nam.type_tags = (TypeTag.CitationDetail("Original evidence", original),)  # type: ignore[assignment]
    requirement = get_type_tag_requiredness(nam, tag_cls)
    assert requirement.requiredness is Requiredness.required_high_coverage
    assert requirement.must_be_from_original_citation
    assert (
        requirement.alternative_with_citation
        is {
            TypeTag.EtymologyDetail: TypeTag.NoEtymology,
            TypeTag.LocationDetail: TypeTag.NoLocation,
            TypeTag.SpecimenDetail: TypeTag.NoSpecimen,
        }[tag_cls]
    )
    # Explicitly recording absent data also counts as starting source extraction.
    nam.type_tags = (TypeTag.NoSpecimen(original),)  # type: ignore[assignment]
    assert (
        get_type_tag_requiredness(nam, tag_cls).requiredness
        is Requiredness.required_high_coverage
    )


@pytest.mark.parametrize(
    "source_state",
    ["missing", "no_copy", "NonOriginal", "Incomplete", "NeedsTranslation"],
)
def test_unusable_original_does_not_enforce_details(source_state: str) -> None:
    nam = make_name()
    original = nam.original_citation
    assert original is not None
    nam.type_tags = (TypeTag.CitationDetail("Evidence", original),)  # type: ignore[assignment]
    if source_state == "missing":
        nam.original_citation = None
    elif source_state == "no_copy":
        original.kind = ArticleKind.no_copy
    elif source_state == "NeedsTranslation":
        original.tags = (ArticleTag.NeedsTranslation(SourceLanguage.russian),)
    else:
        original.tags = (getattr(ArticleTag, source_state)(),)
    assert (
        get_type_tag_requiredness(
            nam, TypeTag.LocationDetail, cfg=EXPERIMENTAL
        ).requiredness
        is Requiredness.desired
    )


@pytest.mark.parametrize(
    ("year", "expected"),
    [
        ("2019", Requiredness.pending_required_high_coverage),
        ("2020", Requiredness.required_high_coverage),
    ],
)
def test_detail_experimental_cutoff(year: str, expected: Requiredness) -> None:
    nam = make_name(year=year)
    nam.type_tags = (TypeTag.CitationDetail("Evidence", nam.original_citation),)  # type: ignore[assignment]
    assert (
        get_type_tag_requiredness(nam, TypeTag.LocationDetail).requiredness is expected
    )
    assert (
        get_type_tag_requiredness(
            nam, TypeTag.LocationDetail, cfg=EXPERIMENTAL
        ).requiredness
        is Requiredness.required_high_coverage
    )


@pytest.mark.parametrize(("model", "cutoff"), [(Article, 69_000), (Name, 100_000)])
def test_detail_id_cutoffs(
    monkeypatch: pytest.MonkeyPatch, model: type[Article | Name], cutoff: int
) -> None:
    nam = make_name(year="2000")
    nam.type_tags = (TypeTag.CitationDetail("Evidence", nam.original_citation),)  # type: ignore[assignment]
    monkeypatch.setattr(model, "id", property(lambda _: cutoff - 1))
    assert (
        get_type_tag_requiredness(nam, TypeTag.LocationDetail).requiredness
        is Requiredness.pending_required_high_coverage
    )
    monkeypatch.setattr(model, "id", property(lambda _: cutoff))
    assert (
        get_type_tag_requiredness(nam, TypeTag.LocationDetail).requiredness
        is Requiredness.required_high_coverage
    )


@pytest.mark.parametrize("group", list(Group))
@pytest.mark.parametrize("year", ["1899", "1900", "1989", "1990"])
@pytest.mark.parametrize(
    "status",
    [
        NomenclatureStatus.available,
        NomenclatureStatus.variant,
        NomenclatureStatus.informal,
    ],
)
@pytest.mark.parametrize("patronym", [False, True])
def test_detail_desirability_by_group_year_and_status(
    group: Group, year: str, status: NomenclatureStatus, *, patronym: bool
) -> None:
    nam = make_name(group=group, year=year)
    nam.nomenclature_status = status
    if patronym:
        nam.species_name_complex = SpeciesNameComplex.virtual(
            kind=SpeciesNameKind.patronym_masculine
        )
    desired = set()
    if status is NomenclatureStatus.available:
        if group in (Group.genus, Group.species) and (
            int(year) >= 1990 or (group is Group.species and patronym)
        ):
            desired.add(TypeTag.EtymologyDetail)
        if group is Group.species and int(year) >= 1900:
            desired.update((TypeTag.LocationDetail, TypeTag.SpecimenDetail))
    # Genus variants still need an etymology; species variants do not.
    if status is NomenclatureStatus.variant and group is Group.genus and year == "1990":
        desired.add(TypeTag.EtymologyDetail)
    for tag_cls in (
        TypeTag.EtymologyDetail,
        TypeTag.LocationDetail,
        TypeTag.SpecimenDetail,
    ):
        assert get_type_tag_requiredness(nam, tag_cls).requiredness.is_desired() == (
            tag_cls in desired
        )


@pytest.mark.parametrize("age", [AgeClass.extant, AgeClass.fossil, AgeClass.ichno])
@pytest.mark.parametrize(
    "type_kind", [None, SpeciesGroupType.holotype, SpeciesGroupType.syntypes]
)
def test_derived_desirability_by_age_and_type_kind(
    age: AgeClass, type_kind: SpeciesGroupType | None
) -> None:
    nam = make_name()
    nam.taxon.age = age
    nam.type_specimen = "BMNH 1"
    nam.species_type_kind = type_kind
    nam.species_name_complex = SpeciesNameComplex.virtual(
        kind=SpeciesNameKind.patronym_masculine
    )
    desired = {TypeTag.NamedAfter}
    if type_kind is not SpeciesGroupType.syntypes:
        if age is not AgeClass.ichno:
            desired.add(TypeTag.Organ)
        if age is AgeClass.extant:
            desired.update(
                (TypeTag.Date, TypeTag.Age, TypeTag.Gender, TypeTag.CollectedBy)
            )
    if type_kind is not None:
        desired.add(TypeTag.ProbableRepository)
    for tag_cls in (
        TypeTag.Organ,
        TypeTag.Date,
        TypeTag.Age,
        TypeTag.Gender,
        TypeTag.CollectedBy,
        TypeTag.NamedAfter,
        TypeTag.ProbableRepository,
    ):
        assert get_type_tag_requiredness(nam, tag_cls).requiredness.is_desired() == (
            tag_cls in desired
        )


def test_derived_enforcement_requires_relevant_source_data() -> None:
    nam = make_name()
    nam.type_specimen = "BMNH 1"
    nam.species_type_kind = SpeciesGroupType.holotype
    nam.type_tags = (TypeTag.LocationDetail("Locality", nam.original_citation),)  # type: ignore[assignment]
    assert (
        get_type_tag_requiredness(
            nam, TypeTag.CollectedBy, cfg=EXPERIMENTAL
        ).requiredness
        is Requiredness.desired
    )
    nam.type_tags = (TypeTag.NoSpecimen(nam.original_citation),)  # type: ignore[assignment]
    requirement = get_type_tag_requiredness(nam, TypeTag.CollectedBy)
    assert requirement.requiredness is Requiredness.required_high_coverage
    assert requirement.alternative_with_citation is TypeTag.NoCollector
    assert requirement.alternative is TypeTag.Involved
    assert not requirement.must_be_from_original_citation
    # These alternatives do not themselves create additional obligations.
    for tag_cls in (TypeTag.NoCollector, TypeTag.Involved):
        assert (
            get_type_tag_requiredness(nam, tag_cls).requiredness
            is Requiredness.optional
        )


@pytest.mark.parametrize(
    "tag_cls",
    [
        TypeTag.Organ,
        TypeTag.Date,
        TypeTag.Age,
        TypeTag.Gender,
        TypeTag.CollectedBy,
        TypeTag.ProbableRepository,
    ],
)
@pytest.mark.parametrize("prerequisite", [TypeTag.SpecimenDetail, TypeTag.NoSpecimen])
@pytest.mark.parametrize("source", ["absent", "later", "original"])
@pytest.mark.parametrize("year", ["2000", "2021"])
@pytest.mark.parametrize("cfg", [CFG, EXPERIMENTAL], ids=["normal", "experimental"])
def test_specimen_prerequisites_and_rollout(
    tag_cls: TypeTagCons,
    prerequisite: TypeTagCons,
    source: str,
    year: str,
    cfg: LintConfig,
) -> None:
    nam = make_name(year=year)
    nam.type_specimen = "BMNH 1"
    nam.species_type_kind = SpeciesGroupType.holotype
    if source != "absent":
        article = (
            nam.original_citation
            if source == "original"
            else Article.virtual(name="later.pdf")
        )
        tag = (
            prerequisite("Specimen evidence", article)
            if prerequisite is TypeTag.SpecimenDetail
            else prerequisite(article)
        )
        nam.type_tags = (tag,)  # type: ignore[assignment]
    if source != "original":
        expected = Requiredness.desired
    elif year == "2021" or cfg.experimental:
        expected = Requiredness.required_high_coverage
    else:
        expected = Requiredness.pending_required_high_coverage
    assert get_type_tag_requiredness(nam, tag_cls, cfg=cfg).requiredness is expected


@pytest.mark.parametrize(
    "source_state", ["missing", "no_copy", "NonOriginal", "Incomplete"]
)
def test_specimen_prerequisite_requires_accessible_citation(source_state: str) -> None:
    nam = make_name()
    nam.type_specimen = "BMNH 1"
    original = nam.original_citation
    assert original is not None
    nam.type_tags = (TypeTag.NoSpecimen(original),)  # type: ignore[assignment]
    if source_state == "missing":
        nam.original_citation = None
    elif source_state == "no_copy":
        original.kind = ArticleKind.no_copy
    else:
        original.tags = (getattr(ArticleTag, source_state)(),)
    assert (
        get_type_tag_requiredness(nam, TypeTag.Organ, cfg=EXPERIMENTAL).requiredness
        is Requiredness.desired
    )
    assert "type tag Organ" not in dict(get_missing_requirements(nam))


@pytest.mark.parametrize("evidence", ["absent", "later", "positive", "negative"])
@pytest.mark.parametrize("year", ["2000", "2021"])
@pytest.mark.parametrize("cfg", [CFG, EXPERIMENTAL], ids=["normal", "experimental"])
def test_named_after_is_pending_without_etymology_prerequisite(
    evidence: str, year: str, cfg: LintConfig
) -> None:
    nam = make_name(year=year)
    nam.species_name_complex = SpeciesNameComplex.virtual(
        kind=SpeciesNameKind.patronym_masculine
    )
    if evidence == "negative":
        nam.type_tags = (TypeTag.NoEtymology(nam.original_citation),)  # type: ignore[assignment]
    elif evidence != "absent":
        source = (
            Article.virtual(name="later.pdf")
            if evidence == "later"
            else nam.original_citation
        )
        nam.type_tags = (TypeTag.EtymologyDetail("Named for Example", source),)  # type: ignore[assignment]
    expected = Requiredness.pending_required_high_coverage
    if evidence in ("positive", "negative") and (year == "2021" or cfg.experimental):
        expected = Requiredness.required_high_coverage
    assert (
        get_type_tag_requiredness(nam, TypeTag.NamedAfter, cfg=cfg).requiredness
        is expected
    )
    nam.original_citation = None
    assert (
        get_type_tag_requiredness(nam, TypeTag.NamedAfter, cfg=cfg).requiredness
        is Requiredness.desired
    )


@pytest.mark.parametrize("case", ["non_patronym", "variant", "genus"])
def test_named_after_retains_applicability_conditions(case: str) -> None:
    nam = make_name(group=Group.genus if case == "genus" else Group.species)
    if case != "non_patronym":
        nam.species_name_complex = SpeciesNameComplex.virtual(
            kind=SpeciesNameKind.patronym_masculine
        )
    if case == "variant":
        nam.nomenclature_status = NomenclatureStatus.variant
    assert (
        get_type_tag_requiredness(
            nam, TypeTag.NamedAfter, cfg=EXPERIMENTAL
        ).requiredness
        is Requiredness.optional
    )


def test_no_specimen_enables_presence_lint() -> None:
    from .lint.other import check_type_tags_presence

    nam = make_name()
    nam.type_specimen = "BMNH 1"
    nam.type_tags = (TypeTag.NoSpecimen(nam.original_citation),)  # type: ignore[assignment]
    assert any(
        "missing required type tag Organ" in str(issue)
        for issue in check_type_tags_presence(nam, CFG)
    )
    nam.type_tags = (*nam.type_tags, TypeTag.NoOrgan(nam.original_citation))  # type: ignore[assignment]
    assert not any(
        "missing required type tag Organ" in str(issue)
        for issue in check_type_tags_presence(nam, CFG)
    )


def test_always_enforced_derived_tags_do_not_need_original_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nam = make_name(year="1900")
    nam.collection = Collection.virtual(label="multiple")
    monkeypatch.setattr(Collection, "id", property(lambda _: MULTIPLE_COLLECTION))
    assert (
        get_type_tag_requiredness(nam, TypeTag.Repository).requiredness
        is Requiredness.required_high_coverage
    )
    nam.original_rank = Rank.other_species
    assert nam.original_rank.needs_textual_rank
    assert (
        get_type_tag_requiredness(nam, TypeTag.TextualOriginalRank).requiredness
        is Requiredness.required_high_coverage
    )
    nam.original_citation = None
    assert (
        get_type_tag_requiredness(nam, TypeTag.Repository).requiredness
        is Requiredness.desired
    )


def test_included_species_coverage_and_designation() -> None:
    nam = make_name(group=Group.genus, year="1900")
    requirement = get_type_tag_requiredness(nam, TypeTag.IncludedSpecies)
    assert requirement.requiredness is Requiredness.required_high_coverage
    assert requirement.alternative is TypeTag.GenusCoelebs
    nam.taxon.tags = (TaxonTag.Coverage(CoverageLevel.low),)
    assert get_type_tag_requiredness(nam, TypeTag.IncludedSpecies) == requirement
    nam.original_citation = None
    assert (
        get_type_tag_requiredness(nam, TypeTag.IncludedSpecies).requiredness
        is Requiredness.desired
    )
    nam.genus_type_kind = TypeSpeciesDesignation.monotypy
    assert not nam.genus_type_kind.requires_tag()
    assert (
        get_type_tag_requiredness(nam, TypeTag.IncludedSpecies).requiredness
        is Requiredness.optional
    )


@pytest.mark.parametrize("group", list(Group))
def test_every_tag_has_a_policy(group: Group) -> None:
    nam = make_name(group=group)
    for member in TypeTag._members:
        assert isinstance(
            get_type_tag_requiredness(nam, getattr(TypeTag, member)).requiredness,
            Requiredness,
        )
    assert get_type_tag_requiredness(nam, TypeTag.NoOriginalParent).requiredness is (
        Requiredness.optional if group is Group.genus else Requiredness.disallowed
    )


@pytest.mark.parametrize(
    ("tag_cls", "alternative"),
    [
        (TypeTag.LocationDetail, TypeTag.NoLocation),
        (TypeTag.SpecimenDetail, TypeTag.NoSpecimen),
        (TypeTag.EtymologyDetail, TypeTag.NoEtymology),
    ],
)
def test_detail_status_matches_original_source_and_alternatives(
    tag_cls: TypeTagCons, alternative: TypeTagCons
) -> None:
    nam = make_name()
    original = nam.original_citation
    other = Article.virtual(name="later.pdf")
    later_detail = tag_cls("Later evidence", other)
    original_detail = tag_cls("Original evidence", original)
    absent_in_original = alternative(original)
    absent_in_other = alternative(other)
    nam.type_tags = (later_detail, absent_in_other)  # type: ignore[assignment]
    status = get_type_tag_status(nam, tag_cls)
    assert not status.is_nonempty
    assert status.value == ()
    for matching_tag in (original_detail, absent_in_original):
        nam.type_tags = (later_detail, matching_tag, absent_in_other)  # type: ignore[assignment]
        status = get_type_tag_status(nam, tag_cls)
        assert status.requiredness is Requiredness.required_high_coverage
        assert status.is_nonempty
        assert status.value == (matching_tag,)
    nam.original_citation = None
    status = get_type_tag_status(nam, tag_cls)
    assert status.requiredness is Requiredness.desired
    assert not status.is_nonempty
    assert status.value == ()


def test_type_tag_status_accepts_unsourced_and_any_source_alternatives() -> None:
    nam = make_name()
    nam.type_specimen = "BMNH 1"
    other = Article.virtual(name="later.pdf")
    for tag in (TypeTag.Date("1900"), TypeTag.NoDate(other)):
        nam.type_tags = (tag,)  # type: ignore[assignment]
        status = get_type_tag_status(nam, TypeTag.Date)
        assert status.is_nonempty
        assert status.value == (tag,)
    person = Person.virtual(family_name="Example")
    for tag in (
        TypeTag.CollectedBy(person),
        TypeTag.NoCollector(other),
        TypeTag.Involved(person),
    ):
        nam.type_tags = (tag,)  # type: ignore[assignment]
        status = get_type_tag_status(nam, TypeTag.CollectedBy)
        assert status.is_nonempty
        assert status.value == (tag,)


def test_type_tag_status_preserves_all_matches() -> None:
    nam = make_name()
    tags = (
        TypeTag.LocationDetail("First quotation", nam.original_citation),
        TypeTag.LocationDetail("Second quotation", nam.original_citation),
    )
    nam.type_tags = tags  # type: ignore[assignment]
    status = get_type_tag_status(nam, TypeTag.LocationDetail)
    assert status.is_nonempty
    assert status.value == tags


@pytest.mark.parametrize("group", [Group.genus, Group.species])
def test_type_tag_status_handles_singletons_and_disallowed_tags(group: Group) -> None:
    nam = make_name(group=group)
    status = get_type_tag_status(nam, TypeTag.NoOriginalParent)
    assert not status.is_nonempty
    nam.type_tags = (TypeTag.NoOriginalParent,)  # type: ignore[assignment]
    status = get_type_tag_status(nam, TypeTag.NoOriginalParent)
    assert status.is_nonempty
    assert status.value == (TypeTag.NoOriginalParent,)
    if group is Group.species:
        assert status.requiredness is Requiredness.disallowed
        assert status.message is not None
    else:
        assert status.requiredness is Requiredness.optional


def test_type_tag_status_passes_config_and_leaves_coverage_to_caller() -> None:
    nam = make_name(year="2000")
    nam.taxon.tags = (TaxonTag.Coverage(CoverageLevel.low),)
    nam.type_tags = (TypeTag.SpecimenDetail("Evidence", nam.original_citation),)  # type: ignore[assignment]
    status = get_type_tag_status(nam, TypeTag.LocationDetail)
    assert status.requiredness is Requiredness.pending_required_high_coverage
    assert not status.is_nonempty
    status = get_type_tag_status(nam, TypeTag.LocationDetail, cfg=EXPERIMENTAL)
    assert status.requiredness is Requiredness.required_high_coverage
    assert not status.is_nonempty


def test_pending_requirements_are_desired_but_not_linted() -> None:
    from .lint.other import check_type_tags_presence, required_fields_linters

    nam = make_name(year="2000")
    nam.type_specimen = "BMNH 1"
    assert Requiredness.pending_required_high_coverage.is_desired()
    assert (
        get_type_tag_requiredness(nam, TypeTag.Organ).requiredness
        is Requiredness.desired
    )
    assert (
        get_type_tag_requiredness(nam, TypeTag.LocationDetail).requiredness
        is Requiredness.pending_required_high_coverage
    )
    # Without source data, detail tags stay pending and derived tags stay desired,
    # even in experimental mode.
    assert list(check_type_tags_presence(nam, EXPERIMENTAL)) == []
    nam.year = None
    assert (
        get_requiredness(nam, "year")[0] is Requiredness.pending_required_high_coverage
    )
    assert "year" in nam.get_required_fields()
    assert list(required_fields_linters["year"](nam, EXPERIMENTAL)) == []
    nam.original_citation = None
    assert (
        get_requiredness(nam, "verbatim_citation")[0]
        is Requiredness.pending_required_high_coverage
    )


def test_pending_report_counts_obligations_and_accepts_alternatives() -> None:
    nam = make_name(year="2000")
    nam.type_specimen = "BMNH 1"
    nam.type_tags = (  # type: ignore[assignment]
        TypeTag.NoSpecimen(nam.original_citation),
        TypeTag.NoOrgan(nam.original_citation),
    )
    missing = dict(get_missing_requirements(nam))
    assert "type tag Date" in missing
    assert "type tag Organ" not in missing
    assert "type tag NoOrgan" not in missing
    assert "type tag NoDate" not in missing
    assert "field type_specimen" not in missing
    assert dict(get_missing_requirements(nam, include_type_tags=False)) == {}
    assert all(
        label.startswith("type tag ")
        for label, _ in get_missing_requirements(nam, include_fields=False)
    )
    desired = dict(get_missing_requirements(nam, levels=(Requiredness.desired,)))
    assert "field collection" in desired
    assert "field year" not in desired
    nam.year = None
    assert set(dict(get_missing_requirements(nam, include_type_tags=False))) == {
        "field year"
    }


def test_presence_lint_explains_source_and_alternative() -> None:
    from .lint.other import check_type_tags_presence

    nam = make_name()
    other = Article.virtual(name="later.pdf")
    nam.type_tags = (  # type: ignore[assignment]
        TypeTag.SpecimenDetail("Evidence", nam.original_citation),
        TypeTag.LocationDetail("Later locality", other),
    )
    issues = list(check_type_tags_presence(nam, CFG))
    assert any(
        "expected LocationDetail or NoLocation from original_citation" in str(issue)
        for issue in issues
    )
    nam.type_tags = (*nam.type_tags, TypeTag.NoLocation(nam.original_citation))  # type: ignore[assignment]
    assert not any(
        "LocationDetail" in str(issue) for issue in check_type_tags_presence(nam, CFG)
    )


def test_data_level_checks_only_remaining_fields() -> None:
    from .lint.other import check_data_level

    nam = make_name()
    messages = [str(issue) for issue in check_data_level(nam, CFG)]
    assert len(messages) == 1
    assert "type_specimen" in messages[0]
    assert "collection" in messages[0]
    assert "type_tags" not in messages[0]
    # Missing enforced fields are left to their own linters.
    assert "original_name" not in messages[0]
    nam.type_tags = (TypeTag.CitationDetail("Evidence", nam.original_citation),)  # type: ignore[assignment]
    assert list(check_data_level(nam, CFG)) == []


@pytest.mark.parametrize(
    "reason", ["low_coverage", "no_citation", "incomplete", "translation"]
)
def test_data_level_preserves_exemptions(reason: str) -> None:
    from .lint.other import check_data_level

    nam = make_name()
    assert nam.original_citation is not None
    if reason == "low_coverage":
        nam.taxon.tags = (TaxonTag.Coverage(CoverageLevel.low),)
    elif reason == "no_citation":
        nam.original_citation = None
    elif reason == "incomplete":
        nam.original_citation.tags = (ArticleTag.Incomplete(),)
    else:
        nam.original_citation.tags = (
            ArticleTag.NeedsTranslation(SourceLanguage.russian),
        )
    assert list(check_data_level(nam, CFG)) == []
