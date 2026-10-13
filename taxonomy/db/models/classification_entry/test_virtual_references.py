"""Lint must read the reviewed proposal rather than stale query results."""

from unittest.mock import patch

import pytest
from clirm import substitute_virtual_models

from taxonomy.db.constants import Group, NomenclatureStatus, Rank, Status
from taxonomy.db.models import Article, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryStatus,
    ClassificationEntryTag,
)
from taxonomy.db.models.classification_entry.lint import (
    check_mapped_name,
    check_mapped_name_inference,
    check_missing_mapped_name,
    check_needs_auxiliary_name,
    check_parent_rank,
    get_filtered_possible_mapped_names,
    infer_page_from_mapped_name,
)
from taxonomy.db.models.lint_types import LintIssue
from taxonomy.db.models.name import Name, NameTag, TypeTag
from taxonomy.db.models.name.lint.ce_mapping import infer_tags_from_mapped_entries
from taxonomy.db.models.name.lint.variants import (
    check_duplicate_variants,
    mark_incorrect_subsequent_spelling_as_name_combination,
)


class _Query[T](list[T]):
    def filter(self, *args: object, **kwargs: object) -> _Query[T]:
        return self


def test_changed_date_does_not_redirect_name_to_own_origin() -> None:
    original = Name(999990003)
    proposed = Name.virtual(
        group=Group.species,
        root_name="example",
        corrected_original_name="Example example",
        nomenclature_status=NomenclatureStatus.name_combination,
        original_rank=Rank.species,
        taxon=Taxon.virtual(valid_name="Example example"),
        year="1876",
        tags=(),
    )
    proposed._clirm_virtual_origin = original
    with (
        substitute_virtual_models([proposed]),
        patch.object(Name, "select_valid", return_value=_Query([original])),
        patch.object(Name, "resolve_variant", return_value=proposed),
    ):
        assert list(check_duplicate_variants.linter(proposed, LintConfig())) == []


def test_mapping_candidate_resolves_proposed_name() -> None:
    original = Name(999990001)
    proposed = Name.virtual(group=Group.genus, corrected_original_name="Example")
    proposed._clirm_virtual_origin = original
    ce = ClassificationEntry.virtual(
        name="Example", rank=Rank.genus, mapped_name=proposed, tags=()
    )
    with (
        substitute_virtual_models([proposed]),
        patch(
            "taxonomy.db.models.classification_entry.lint.get_possible_mapped_names",
            return_value=[original],
        ),
    ):
        assert list(get_filtered_possible_mapped_names(ce)) == [proposed]
        assert list(check_mapped_name_inference.linter(ce, LintConfig())) == []


def test_auxiliary_inference_does_not_use_own_persisted_origin() -> None:
    original_name = Name.virtual(group=Group.genus)
    emendation = Name.virtual(
        group=Group.genus, tags=(NameTag.UnjustifiedEmendationOf(original_name),)
    )
    source = Article.virtual(name="source")
    original = ClassificationEntry(999990001)
    proposed = ClassificationEntry.virtual(
        article=source,
        name="Example",
        rank=Rank.genus,
        parent=None,
        mapped_name=emendation,
        status=ClassificationEntryStatus.valid,
        tags=(),
    )
    proposed._clirm_virtual_origin = original
    with (
        substitute_virtual_models([proposed]),
        patch.object(
            ClassificationEntry, "select_valid", return_value=_Query([original])
        ),
    ):
        assert list(check_needs_auxiliary_name.linter(proposed, LintConfig())) == []


@pytest.mark.parametrize("change", ["quote", "mapping", "removal"])
def test_detail_transfer_reads_repaired_ce_quote(change: str) -> None:
    source = Article.virtual(name="source")
    name = Name.virtual(
        root_name="example",
        group=Group.species,
        status=Status.valid,
        original_citation=source,
        tags=(),
        type_tags=(),
    )
    original = ClassificationEntry(999990002)
    proposed = ClassificationEntry.virtual(
        article=source,
        name="Example example",
        rank=Rank.species,
        mapped_name=name,
        status=ClassificationEntryStatus.valid,
        type_locality=None,
        citation=None,
        tags=(ClassificationEntryTag.TypeSpecimenData("verified quotation"),),
    )
    proposed._clirm_virtual_origin = original
    if change == "mapping":
        proposed.mapped_name = Name.virtual()
    elif change == "removal":
        proposed.status = ClassificationEntryStatus.removed
    with (
        substitute_virtual_models([proposed]),
        patch.object(Name, "get_classification_entries", return_value=[original]),
        patch.object(Name, "__str__", return_value="test name"),
        patch.object(Name, "get_mapped_classification_entry", return_value=None),
    ):
        issues = list(infer_tags_from_mapped_entries.linter(name, LintConfig()))
        if change != "quote":
            assert issues == []
            assert name.type_tags == ()
            return
        (issue,) = issues
        assert isinstance(issue, LintIssue)
        assert issue.fix is not None
        assert issue.fix.apply()
    (detail,) = name.get_tags(name.type_tags, TypeTag.SpecimenDetail)
    assert detail.text == "verified quotation"
    assert detail.classification_entry == proposed


def test_earlier_spelling_does_not_become_usage_of_own_persisted_origin() -> None:
    original = Name(999990004)
    taxon = Taxon.virtual(valid_name="Nycteris example")
    base = Name.virtual(root_name="exampleii")
    fields = {
        "group": Group.species,
        "root_name": "examplei",
        "corrected_original_name": "Nycteris examplei",
        "nomenclature_status": NomenclatureStatus.incorrect_subsequent_spelling,
        "original_rank": Rank.species,
        "status": Status.synonym,
        "taxon": taxon,
        "tags": (NameTag.IncorrectSubsequentSpellingOf(base),),
    }
    proposed = Name.virtual(year="1882", **fields)
    proposed._clirm_virtual_origin = original
    later = Name.virtual(year="1912", **fields)
    with (
        substitute_virtual_models([proposed]),
        patch.object(Name, "select_valid", return_value=_Query([original, later])),
    ):
        assert (
            list(
                mark_incorrect_subsequent_spelling_as_name_combination.linter(
                    proposed, LintConfig()
                )
            )
            == []
        )


@pytest.mark.parametrize(
    "status",
    [
        NomenclatureStatus.unjustified_emendation,
        NomenclatureStatus.justified_emendation,
        NomenclatureStatus.name_combination,
    ],
)
def test_exact_mapping_does_not_use_future_emendation(
    status: NomenclatureStatus,
) -> None:
    taxon = Taxon.virtual(valid_name="Pteropus example")
    base = Name.virtual(
        group=Group.species,
        root_name="paliatus",
        corrected_original_name="Pteropus paliatus",
        status=Status.synonym,
        nomenclature_status=NomenclatureStatus.available,
        taxon=taxon,
        tags=(),
    )
    candidate = Name.virtual(
        group=Group.species,
        root_name="palliatus",
        corrected_original_name="Cephalotes palliatus",
        status=Status.synonym,
        nomenclature_status=status,
        taxon=taxon,
        year="1901",
        tags=(),
        original_citation=None,
        original_rank=Rank.species,
    )
    ce = ClassificationEntry.virtual(
        name="Cephalotes palliatus",
        rank=Rank.species,
        mapped_name=base,
        tags=(),
        page="87",
        article=Article.virtual(name="source", year="1899"),
    )
    with (
        patch.object(Name, "select_valid", return_value=_Query([candidate])),
        patch.object(Name, "resolve_variant", return_value=base),
        patch.object(Name, "__str__", return_value="candidate name"),
        patch(
            "taxonomy.db.models.classification_entry.lint._matches_species_root_name",
            return_value=False,
        ),
    ):
        issues = list(check_mapped_name.linter(ce, LintConfig(interactive=False)))
    assert any("change to" in str(issue) for issue in issues) is (
        status is NomenclatureStatus.name_combination
    )
    with patch(
        "taxonomy.db.models.classification_entry.lint.get_possible_mapped_names",
        return_value=[candidate],
    ):
        inferred = list(get_filtered_possible_mapped_names(ce))
    assert inferred == (
        [candidate] if status is NomenclatureStatus.name_combination else []
    )


@pytest.mark.parametrize("page", ["200", "@pl18B", "200, 201"])
def test_name_page_links_only_transfer_to_matching_source_pages(page: str) -> None:
    article = Article.virtual(name="source")
    name = Name.virtual(
        original_citation=article,
        type_tags=(
            TypeTag.AuthorityPageLink(
                url="https://www.biodiversitylibrary.org/page/123",
                confirmed=False,
                page="200",
            ),
        ),
    )
    ce = ClassificationEntry.virtual(
        article=article, page=page, mapped_name=name, tags=()
    )
    with patch.object(Name, "__str__", return_value="mapped name"):
        issues = list(infer_page_from_mapped_name.linter(ce, LintConfig()))
    assert bool(issues) is (page != "@pl18B")


@pytest.mark.parametrize("ancestor_rank", [Rank.species, Rank.genus])
def test_parent_rank_checks_past_informal_source_heading(ancestor_rank: Rank) -> None:
    ancestor = ClassificationEntry.virtual(rank=ancestor_rank)
    heading = ClassificationEntry.virtual(rank=Rank.informal, parent=ancestor)
    ce = ClassificationEntry.virtual(
        name="Canis familiaris example", rank=Rank.subspecies, parent=heading, tags=()
    )
    with patch.object(ClassificationEntry, "get_children", return_value=[]):
        issues = list(check_parent_rank.linter(ce, LintConfig()))
    assert bool(issues) is (ancestor_rank is Rank.genus)


@pytest.mark.parametrize("removed", [False, True])
def test_name_original_ce_respects_proposed_evidence(*, removed: bool) -> None:
    original = ClassificationEntry(999990008)
    article = Article.virtual(name="source")
    name = Name.virtual(original_citation=article)
    proposed = ClassificationEntry.virtual(
        article=article,
        mapped_name=name,
        status=(
            ClassificationEntryStatus.removed
            if removed
            else ClassificationEntryStatus.valid
        ),
        tags=(),
    )
    proposed._clirm_virtual_origin = original
    with (
        substitute_virtual_models([proposed]),
        patch.object(
            ClassificationEntry, "select_valid", return_value=_Query([original])
        ),
    ):
        assert name.get_mapped_classification_entry() is (None if removed else proposed)


@pytest.mark.parametrize("authority", [None, "Wagner", "Bechstein", "Dwigubski"])
def test_missing_mapping_respects_explicit_authority(authority: str | None) -> None:
    candidate = Name.virtual(root_name="vulgaris")
    base = Name.virtual(root_name="original")
    ce = ClassificationEntry.virtual(mapped_name=None, authority=authority, tags=())
    with (
        patch(
            "taxonomy.db.models.classification_entry.lint.must_have_mapped_name",
            return_value=True,
        ),
        patch(
            "taxonomy.db.models.classification_entry.lint.get_filtered_possible_mapped_names",
            return_value=[candidate],
        ),
        patch.object(Name, "resolve_variant", return_value=base),
        patch.object(
            Name,
            "taxonomic_authority",
            lambda self: "Wagner" if self is candidate else "Bechstein",
        ),
        patch.object(Name, "__str__", return_value="candidate"),
    ):
        issues = list(check_missing_mapped_name.linter(ce, LintConfig()))
    assert len(issues) == 1
    assert bool(getattr(issues[0], "fix", None)) is (authority != "Dwigubski")


@pytest.mark.parametrize(
    ("rank", "expanded", "expected"),
    [
        (
            Rank.subspecies,
            "Canis familiaris himalayensis",
            "Canis familiaris himalayensis",
        ),
        (
            Rank.variety,
            "Canis familiaris himalayensis",
            "Canis familiaris himalayensis",
        ),
        (Rank.species, "Canis familiaris himalayensis", "Canis himalayensis"),
        (Rank.subspecies, "Vulpes familiaris himalayensis", "Canis himalayensis"),
        (Rank.subspecies, "Canis familiaris example", "Canis himalayensis"),
    ],
)
def test_infraspecific_original_name_uses_exact_source_context(
    rank: Rank, expanded: str, expected: str
) -> None:
    article = Article.virtual(name="source")
    name = Name.virtual(
        original_name="Canis Himalayensis",
        original_rank=rank,
        group=Group.species,
        original_citation=article,
        nomenclature_status=NomenclatureStatus.nomen_nudum,
    )
    ce = ClassificationEntry.virtual(
        name="Canis Himalayensis",
        article=article,
        rank=rank,
        tags=(ClassificationEntryTag.NormalizedName(expanded),),
    )
    with patch.object(Name, "get_mapped_classification_entry", return_value=ce):
        assert name.infer_corrected_original_name() == expected
