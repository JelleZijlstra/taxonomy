from collections.abc import Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call

import pytest

import taxonomy
from taxonomy.apis import zoobank
from taxonomy.db.constants import NomenclatureStatus, Status
from taxonomy.db.models.location import Location
from taxonomy.db.models.name import NameTag, TypeTag
from taxonomy.db.models.name.name import Name


def _get_tags(tags: tuple[object, ...], tag_cls: type[object]) -> Iterable[object]:
    return (tag for tag in tags if isinstance(tag, tag_cls))


def _get_expected(value: str) -> str:
    return f"- _{value.replace('_', ' ')}_:"


def test_add_type_tag_sorts_tags() -> None:
    existing = TypeTag.TextualOriginalRank("species")
    name = SimpleNamespace(type_tags=(existing,))
    tag = TypeTag.ImpreciseLocality(comment="Only the country is known.")

    Name.add_type_tag(name, tag)  # type: ignore[arg-type]

    assert name.type_tags == tuple(sorted((existing, tag)))


def test_partial_type_locality_serializes_location_reference() -> None:
    location = Location(123)

    assert TypeTag.PartialTypeLocality(location).serialize() == [76, 123]


EXCLUDED = {
    # NameTag
    "MappedClassificationEntry",
    "HMW",
    # TypeTag
    "ImpreciseLocality",
    "TypeLocality",
    "TypeSpecimenLink",
    "_RawCollector",
    "StratigraphyDetail",
    "NoOriginalParent",
    "Habitat",
    "IgnoreLint",
    "NoAge",
    "DifferentAuthority",
    "NoLocation",
    "NoOrgan",
    "RejectedLSID",
    "NoCollector",
    "NoGender",
    "IncorrectGrammar",
    "NoEtymology",
    "NoSpecimen",
    "NoDate",
    "IgnorePotentialCitationFrom",
    "InterpretedTypeLocality",
    "InterpretedTypeSpecimen",
    "InterpretedTypeTaxon",
    "NomenclatureComments",
    # Fields
    "type_tags",
    "tags",
    "target",
}


def test_docs() -> None:
    assert taxonomy.__file__ is not None
    docs_root = Path(taxonomy.__file__).parent.parent / "docs"
    name_docs = (docs_root / "name.md").read_text()

    missing_statuses = {
        status for status in Status if _get_expected(status.name) not in name_docs
    }
    assert (
        not missing_statuses
    ), f"Missing documentation for Status values: {missing_statuses}"

    missing_nomenclature_statuses = {
        status
        for status in NomenclatureStatus
        if _get_expected(status.name) not in name_docs
    }
    assert (
        not missing_nomenclature_statuses
    ), f"Missing documentation for NomenclatureStatus values: {missing_nomenclature_statuses}"

    for tag_cls in (NameTag, TypeTag):
        missing_tags = {
            tag
            for tag in tag_cls._members
            if tag not in EXCLUDED and _get_expected(tag) not in name_docs
        }
        assert (
            not missing_tags
        ), f"Missing documentation for {tag_cls.__name__} values: {missing_tags}"

    missing_fields = {
        field
        for field in Name.clirm_fields
        if _get_expected(field) not in name_docs and field not in EXCLUDED
    }
    assert (
        not missing_fields
    ), f"Missing documentation for Name fields: {missing_fields}"


def test_clear_zoobank_caches_is_adt_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Name, "original_citation", None)
    name = object.__new__(Name)

    callbacks = name.get_adt_callbacks()

    assert callbacks["clear_zoobank_caches"] == name.clear_zoobank_caches


def test_clear_zoobank_caches_clears_name_and_act_lsid_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_cache = Mock()
    monkeypatch.setattr(zoobank, "clear_zoobank_act_cache", clear_cache)
    name = cast(
        Name,
        SimpleNamespace(
            corrected_original_name="Pseudovespertiliavus parva",
            type_tags=(
                TypeTag.LSID(
                    "urn:lsid:zoobank.org:act:fc07acbe-03f7-414a-bb64-1bb0711766bf"
                ),
            ),
            get_tags=_get_tags,
        ),
    )

    Name.clear_zoobank_caches(name)

    assert clear_cache.call_count == 4
    clear_cache.assert_has_calls(
        [
            call("Pseudovespertiliavus_parva"),
            call("urn:lsid:zoobank.org:act:fc07acbe-03f7-414a-bb64-1bb0711766bf"),
            call("FC07ACBE-03F7-414A-BB64-1BB0711766BF"),
            call("fc07acbe-03f7-414a-bb64-1bb0711766bf"),
        ],
        any_order=True,
    )


def test_knowledge_level_includes_desired_tags_and_source_matching(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from taxonomy.db.constants import (
        AgeClass,
        ArticleKind,
        CoverageLevel,
        Group,
        Rank,
        TypeSpeciesDesignation,
    )
    from taxonomy.db.models import Article, Person, Taxon
    from taxonomy.db.models.name_complex import NameComplex
    from taxonomy.db.models.person import AuthorTag
    from taxonomy.db.models.tags import TaxonTag

    original = Article.virtual(
        name="original.pdf", kind=ArticleKind.electronic, tags=()
    )
    nam = Name.virtual(
        group=Group.genus,
        root_name="Example",
        original_name="Example",
        corrected_original_name="Example",
        original_rank=Rank.genus,
        author_tags=(AuthorTag.Author(Person.virtual(family_name="Example")),),
        year="2000",
        page_described="1",
        original_citation=original,
        nomenclature_status=NomenclatureStatus.available,
        status=Status.valid,
        taxon=Taxon.virtual(
            age=AgeClass.extant, tags=(TaxonTag.Coverage(CoverageLevel.low),)
        ),
        name_complex=NameComplex.virtual(),
        genus_type_kind=TypeSpeciesDesignation.undesignated,
        type_tags=(TypeTag.NoEtymology(original), TypeTag.GenusCoelebs()),
        tags=(),
    )
    assert nam.knowledge_level() == 2
    # Pending details and high-coverage requirements affect completeness even
    # when lint would not enforce them in this low-coverage taxon.
    nam.type_tags = (TypeTag.GenusCoelebs(),)
    assert nam.knowledge_level(verbose=True) == 1
    assert "type tag EtymologyDetail" in capsys.readouterr().out
    nam.type_tags = (TypeTag.NoEtymology(original),)
    assert nam.knowledge_level() == 1
    later = Article.virtual(name="later.pdf")
    nam.type_tags = (TypeTag.NoEtymology(later), TypeTag.GenusCoelebs())
    assert nam.knowledge_level() == 1
    nam.type_tags = (TypeTag.NoEtymology(original), TypeTag.GenusCoelebs())
    assert nam.knowledge_level() == 2
    nam.year = None
    assert nam.knowledge_level() == 1
    nam.original_citation = None
    assert nam.knowledge_level() == 0
