"""Coverage inheritance and the distinction between missing and incorrect links."""

from unittest.mock import Mock

import pytest

from taxonomy.db.constants import (
    AgeClass,
    ArticleKind,
    ArticleType,
    CoverageLevel,
    Group,
    NomenclatureStatus,
    Rank,
    Status,
)
from taxonomy.db.models import Article, Name, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.name import TypeTag, required_fields
from taxonomy.db.models.name import lint as name_lint
from taxonomy.db.models.tags import TaxonTag
from taxonomy.db.models.taxon import lint as taxon_lint

CFG = LintConfig(autofix=False, interactive=False, available_resources=frozenset())


def make_taxon(
    name: str = "Example",
    level: CoverageLevel | None = None,
    parent: Taxon | None = None,
) -> Taxon:
    return Taxon.virtual(
        valid_name=name,
        rank=Rank.genus,
        age=AgeClass.extant,
        parent=parent,
        tags=() if level is None else (TaxonTag.Coverage(level),),
    )


def make_name(taxon: Taxon, group: Group = Group.genus) -> Name:
    name = Name.virtual(
        taxon=taxon,
        group=group,
        status=Status.valid,
        nomenclature_status=NomenclatureStatus.available,
        root_name="example",
        corrected_original_name=(
            "Example example" if group is Group.species else "Example"
        ),
        original_name="Example",
        original_rank=Rank.species if group is Group.species else Rank.genus,
        year="1900",
        tags=(),
        type_tags=(),
    )
    taxon.base_name = name
    return name


def test_inheritance_overrides_and_live_edits() -> None:
    root = make_taxon("Root")
    high = make_taxon("Covered", CoverageLevel.high, root)
    low = make_taxon("Exception", CoverageLevel.low, high)
    island = make_taxon("Island", CoverageLevel.high, low)
    leaf = make_taxon("Leaf", parent=island)
    assert root.get_coverage() == (CoverageLevel.low, None)
    assert low.get_coverage() == (CoverageLevel.low, low)
    assert leaf.get_coverage() == (CoverageLevel.high, island)
    island.tags = ()  # type: ignore[assignment]
    assert leaf.get_coverage() == (CoverageLevel.low, low)
    island.parent = high
    assert leaf.get_coverage() == (CoverageLevel.high, high)
    high.valid_name = "Renamed"
    assert leaf.is_high_coverage()
    high.tags = ()  # type: ignore[assignment]
    root.tags = (TaxonTag.Coverage(CoverageLevel.low),)  # type: ignore[assignment]
    assert leaf.get_coverage() == (CoverageLevel.low, root)


def test_redirect_uses_target_coverage() -> None:
    target = make_taxon(level=CoverageLevel.low)
    redirect = make_taxon(level=CoverageLevel.high, parent=target)
    redirect.age = AgeClass.redirect
    assert redirect.get_coverage() == (CoverageLevel.low, target)


def test_conflicts_and_cycles_are_errors_not_low_defaults() -> None:
    taxon = make_taxon()
    taxon.tags = (  # type: ignore[assignment]
        TaxonTag.Coverage(CoverageLevel.high),
        TaxonTag.Coverage(CoverageLevel.low),
    )
    with pytest.raises(ValueError, match="conflicting coverage"):
        taxon.get_coverage()
    assert any(
        "multiple coverage tags" in str(msg)
        for msg in taxon_lint.check_coverage(taxon, CFG)
    )
    taxon.tags = ()  # type: ignore[assignment]
    child = make_taxon(parent=taxon)
    taxon.parent = child
    with pytest.raises(ValueError, match="parent cycle"):
        taxon.get_coverage()
    assert any(
        "parent cycle" in str(msg) for msg in taxon_lint.check_coverage(taxon, CFG)
    )


def test_coverage_tag_serialization_and_existing_tags() -> None:
    assert TaxonTag.unserialize([12, 2]) == TaxonTag.Coverage(CoverageLevel.high)
    tag = TaxonTag.Coverage(CoverageLevel.low, comment="Incomplete living family")
    assert TaxonTag.unserialize(tag.serialize()) == tag
    assert TaxonTag.unserialize([7, "rank"]) == TaxonTag.IgnoreLint("rank")
    assert TaxonTag.unserialize([6]) == TaxonTag.Basal()


def test_root_coverage_proposal_does_not_require_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = make_taxon("root", CoverageLevel.low)
    assert any(
        "missing parent" in str(msg) for msg in taxon_lint.check_parent(root, CFG)
    )
    monkeypatch.setattr(Taxon, "virtual_origin_id", property(lambda _: 1))
    assert list(taxon_lint.check_parent(root, CFG)) == []


def test_included_species_requirement_and_cross_boundary_links() -> None:
    covered = make_taxon(level=CoverageLevel.high)
    incidental = make_taxon(level=CoverageLevel.low)
    genus = make_name(incidental)
    species = make_name(covered, Group.species)
    genus.type = species
    assert incidental.is_high_coverage() is False
    genus.original_citation = Article.virtual(kind=ArticleKind.electronic, tags=())
    requirement = required_fields.get_type_tag_requiredness(
        genus, TypeTag.IncludedSpecies
    )
    assert (
        requirement.requiredness is required_fields.Requiredness.required_high_coverage
    )
    assert not any(
        "IncludedSpecies" in str(issue)
        for issue in name_lint.other.check_type_tags_presence(genus, CFG)
    )
    incidental.tags = (TaxonTag.Coverage(CoverageLevel.high),)  # type: ignore[assignment]
    assert any(
        "IncludedSpecies" in str(issue)
        for issue in name_lint.other.check_type_tags_presence(genus, CFG)
    )


def test_existing_incorrect_original_parent_still_checked() -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    species = make_name(taxon, Group.species)
    species.original_parent = make_name(taxon, Group.species)
    assert any(
        "not a genus" in str(msg)
        for msg in name_lint.original_parent.check_original_parent(species, CFG)
    )


def test_existing_type_still_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    genus = make_name(taxon)
    genus.type = make_name(taxon, Group.family)
    monkeypatch.setattr(Taxon, "is_child_of", lambda *args: True)
    assert any(
        "not species" in str(msg)
        for msg in name_lint.typification.check_type(genus, CFG)
    )


def test_family_type_inference_still_runs_in_low_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    family = make_name(taxon, Group.family)
    genus = make_name(taxon)
    from taxonomy.db.models import NameComplex

    genus.name_complex = NameComplex.virtual()
    monkeypatch.setattr(Name, "get_family_group_stem", lambda _: "Example")
    monkeypatch.setattr(Name, "safe_get_stem", lambda _: "Example")
    monkeypatch.setattr(Taxon, "all_names", lambda _: [genus])
    messages = list(name_lint.typification.infer_family_group_type(family, CFG))
    assert len(messages) == 1
    assert "inferred type" in str(messages[0])
    assert family.type is None


def test_age_warning_requires_high_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    fossil = make_taxon(parent=taxon)
    fossil.age = AgeClass.fossil
    monkeypatch.setattr(Taxon, "get_children", lambda _: [fossil])
    assert list(taxon_lint.check_age(taxon, CFG)) == []
    taxon.tags = (TaxonTag.Coverage(CoverageLevel.high),)  # type: ignore[assignment]
    assert any(
        "no extant children" in str(msg) for msg in taxon_lint.check_age(taxon, CFG)
    )


def test_parent_species_requirement_is_skipped_before_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    name = make_name(taxon, Group.species)
    name.corrected_original_name = "Example missing subspecies"
    name.original_citation = Article.virtual(
        name="Example.pdf", kind=ArticleKind.electronic, type=ArticleType.JOURNAL
    )
    monkeypatch.setattr(
        Name, "select_valid", lambda: pytest.fail("unexpected database query")
    )
    assert list(name_lint.other.check_has_parent_species(name, CFG)) == []


def test_parent_species_requirement_in_high_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = make_name(make_taxon(level=CoverageLevel.high), Group.species)
    name.year = "2000"
    name.corrected_original_name = "Example missing subspecies"
    name.original_citation = Article.virtual(name="Example.pdf")
    query = Mock()
    query.filter.return_value.count.return_value = 0
    monkeypatch.setattr(Name, "select_valid", lambda: query)
    assert any(
        "missing parent species" in str(msg)
        for msg in name_lint.other.check_has_parent_species(name, CFG)
    )


def test_nominal_genus_missing_and_inferred(monkeypatch: pytest.MonkeyPatch) -> None:
    taxon = make_taxon(level=CoverageLevel.low)
    taxon.rank = Rank.species
    make_name(taxon, Group.species)
    monkeypatch.setattr(Taxon, "has_parent_of_rank", lambda *args: False)
    query = Mock()
    query.filter.return_value = []
    monkeypatch.setattr(Name, "select_valid", lambda: query)
    assert list(taxon_lint.check_nominal_genus(taxon, CFG)) == []
    taxon.tags = (TaxonTag.Coverage(CoverageLevel.high),)  # type: ignore[assignment]
    assert any(
        "should have NominalGenus tag" in str(msg)
        for msg in taxon_lint.check_nominal_genus(taxon, CFG)
    )
    taxon.tags = (TaxonTag.Coverage(CoverageLevel.low),)  # type: ignore[assignment]
    genus = make_name(make_taxon())
    query.filter.return_value = [genus]
    assert any(
        "adding NominalGenus" in str(msg)
        for msg in taxon_lint.check_nominal_genus(taxon, CFG)
    )
