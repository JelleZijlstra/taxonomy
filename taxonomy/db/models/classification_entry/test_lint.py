from unittest.mock import patch

from taxonomy.db.constants import Group, Rank, Status
from taxonomy.db.models import Article, Taxon
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.classification_entry.lint import (
    _is_misspelling_of,
    check_auxiliary_name,
    check_needs_auxiliary_name,
    check_parent_cycle,
    check_parent_rank,
    check_verbatim_parent,
)
from taxonomy.db.models.name import Name, NameTag

_ARTICLE = Article.virtual(name="classification source")


def _make_ce(
    *, parent: ClassificationEntry | None, auxiliary: bool
) -> ClassificationEntry:
    tags = (ClassificationEntryTag.AuxiliaryName,) if auxiliary else ()
    return ClassificationEntry.virtual(
        article=_ARTICLE,
        name="classification entry",
        rank=Rank.genus,
        parent=parent,
        tags=tags,
    )


def _make_source_ce(
    *, parent: ClassificationEntry, verbatim_parent: ClassificationEntry
) -> ClassificationEntry:
    return ClassificationEntry.virtual(
        article=_ARTICLE,
        name="source entry",
        rank=Rank.genus,
        parent=parent,
        tags=(ClassificationEntryTag.VerbatimParent(verbatim_parent),),
    )


def _make_name(root_name: str, *tags: NameTag) -> Name:
    taxon = Taxon.virtual(rank=Rank.species, valid_name=root_name)
    return Name.virtual(
        group=Group.species,
        root_name=root_name,
        status=Status.valid,
        taxon=taxon,
        author_tags=(),
        tags=tags,
    )


def test_verbatim_parent_is_valid() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    ce = _make_source_ce(parent=parent, verbatim_parent=auxiliary)

    assert list(check_verbatim_parent(ce, LintConfig())) == []


def test_verbatim_parent_must_be_auxiliary() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    target = _make_ce(parent=parent, auxiliary=False)
    ce = _make_source_ce(parent=parent, verbatim_parent=target)

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        f"VerbatimParent target {target} is not an AuxiliaryName CE "
        "[verbatim_parent]"
    )


def test_verbatim_parent_must_have_same_parent() -> None:
    auxiliary = _make_ce(parent=_make_ce(parent=None, auxiliary=False), auxiliary=True)
    ce = _make_source_ce(
        parent=_make_ce(parent=None, auxiliary=False), verbatim_parent=auxiliary
    )

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        f"VerbatimParent target {auxiliary} does not have the same parent "
        "[verbatim_parent]"
    )


def test_auxiliary_name_requires_same_rank_parent() -> None:
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = correct_name
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.species
    auxiliary.mapped_name = misspelling

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "AuxiliaryName CE has parent of different rank genus [auxiliary_name]"
    )


def test_auxiliary_name_requires_misspelling_mapping() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = _make_name("correct")
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = _make_name("not a misspelling")

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "AuxiliaryName CE does not map to a misspelling of its parent name "
        "[auxiliary_name]"
    )


def test_auxiliary_name_accepts_misspelling_mapping() -> None:
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectOriginalSpellingOf(correct_name)
    )
    parent = _make_ce(parent=None, auxiliary=False)
    parent.mapped_name = correct_name
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = misspelling

    assert list(check_auxiliary_name(auxiliary, LintConfig())) == []


def test_parent_rank_allows_same_rank_auxiliary_subspecies() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    parent.rank = Rank.subspecies
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.subspecies

    assert list(check_parent_rank(auxiliary, LintConfig())) == []


def test_parent_cycle_lint() -> None:
    first = _make_ce(parent=None, auxiliary=False)
    second = _make_ce(parent=first, auxiliary=False)
    first.parent = second

    assert list(check_parent_cycle.linter(first, LintConfig())) == [
        "parent cycle detected"
    ]


def test_parent_cycle_lint_accepts_acyclic_tree() -> None:
    root = _make_ce(parent=None, auxiliary=False)
    child = _make_ce(parent=root, auxiliary=False)

    assert list(check_parent_cycle.linter(child, LintConfig())) == []


def test_is_misspelling_of() -> None:
    correct = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct)
    )

    assert _is_misspelling_of(misspelling, correct)


def test_needs_auxiliary_name_finds_misspelled_sibling() -> None:
    parent = _make_ce(parent=None, auxiliary=False)
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    correct_ce = _make_ce(parent=parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = misspelling

    with patch(
        "taxonomy.db.models.classification_entry.lint._get_same_level_ces",
        return_value=[misspelled_ce, correct_ce],
    ):
        messages = list(
            check_needs_auxiliary_name(misspelled_ce, LintConfig(autofix=False))
        )

    assert len(messages) == 1
    assert "convert to AuxiliaryName under sibling CE" in messages[0]


def test_needs_auxiliary_name_autofix() -> None:
    original_parent = _make_ce(parent=None, auxiliary=False)
    correct_name = _make_name("correct")
    misspelling = _make_name(
        "misspelling", NameTag.IncorrectSubsequentSpellingOf(correct_name)
    )
    correct_ce = _make_ce(parent=original_parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=original_parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = misspelling
    child = _make_ce(parent=misspelled_ce, auxiliary=False)

    with (
        patch(
            "taxonomy.db.models.classification_entry.lint._get_same_level_ces",
            return_value=[misspelled_ce, correct_ce],
        ),
        patch.object(misspelled_ce, "get_children", return_value=[child]),
    ):
        messages = list(check_needs_auxiliary_name(misspelled_ce, LintConfig()))

    assert len(messages) == 1
    assert misspelled_ce.has_tag(ClassificationEntryTag.AuxiliaryName)
    assert misspelled_ce.parent is correct_ce
    assert child.parent is correct_ce
    assert [
        tag.ce
        for tag in child.get_tags(child.tags, ClassificationEntryTag.VerbatimParent)
    ] == [misspelled_ce]
