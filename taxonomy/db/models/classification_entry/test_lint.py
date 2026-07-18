from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from taxonomy.db.constants import Rank
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.classification_entry.lint import (
    _is_misspelling_of,
    check_auxiliary_name,
    check_needs_auxiliary_name,
    check_parent_rank,
    check_verbatim_parent,
)
from taxonomy.db.models.name import Name, NameTag


def _make_ce(*, parent: object, auxiliary: bool) -> ClassificationEntry:
    tags = (ClassificationEntryTag.AuxiliaryName,) if auxiliary else ()
    ce = SimpleNamespace(parent=parent, tags=tags, children=[])
    ce.has_tag = lambda tag_cls: any(tag._tag == tag_cls._tag for tag in ce.tags)
    ce.get_tags = lambda tags, tag_cls: (
        tag for tag in tags if isinstance(tag, tag_cls)
    )
    ce.add_tag = lambda tag: setattr(ce, "tags", (*ce.tags, tag))
    ce.get_children = lambda: iter(ce.children)
    return cast(ClassificationEntry, ce)


def _make_source_ce(
    *, parent: object, verbatim_parent: ClassificationEntry
) -> ClassificationEntry:
    ce = SimpleNamespace(
        parent=parent, tags=(ClassificationEntryTag.VerbatimParent(verbatim_parent),)
    )
    ce.get_tags = lambda tags, tag_cls: (
        tag for tag in tags if isinstance(tag, tag_cls)
    )
    return cast(ClassificationEntry, ce)


def test_verbatim_parent_is_valid() -> None:
    parent = object()
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    ce = _make_source_ce(parent=parent, verbatim_parent=auxiliary)

    assert list(check_verbatim_parent(ce, LintConfig())) == []


def test_verbatim_parent_must_be_auxiliary() -> None:
    parent = object()
    target = _make_ce(parent=parent, auxiliary=False)
    ce = _make_source_ce(parent=parent, verbatim_parent=target)

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        f"VerbatimParent target {target} is not an AuxiliaryName CE "
        "[verbatim_parent]"
    )


def test_verbatim_parent_must_have_same_parent() -> None:
    auxiliary = _make_ce(parent=object(), auxiliary=True)
    ce = _make_source_ce(parent=object(), verbatim_parent=auxiliary)

    messages = list(check_verbatim_parent(ce, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        f"VerbatimParent target {auxiliary} does not have the same parent "
        "[verbatim_parent]"
    )


def test_auxiliary_name_requires_same_rank_parent() -> None:
    correct_name = cast(Name, object())
    misspelling = SimpleNamespace(
        get_tag_target=lambda tag_cls: (
            correct_name if tag_cls is NameTag.IncorrectSubsequentSpellingOf else None
        )
    )
    parent = SimpleNamespace(rank=Rank.genus, mapped_name=correct_name)
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.species
    auxiliary.mapped_name = cast(Name, misspelling)

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "AuxiliaryName CE has parent of different rank genus [auxiliary_name]"
    )


def test_auxiliary_name_requires_misspelling_mapping() -> None:
    parent = SimpleNamespace(rank=Rank.genus, mapped_name=cast(Name, object()))
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = cast(
        Name, SimpleNamespace(get_tag_target=lambda tag_cls: None)
    )

    messages = list(check_auxiliary_name(auxiliary, LintConfig()))

    assert len(messages) == 1
    assert messages[0].endswith(
        "AuxiliaryName CE does not map to a misspelling of its parent name "
        "[auxiliary_name]"
    )


def test_auxiliary_name_accepts_misspelling_mapping() -> None:
    correct_name = cast(Name, object())
    misspelling = SimpleNamespace(
        get_tag_target=lambda tag_cls: (
            correct_name if tag_cls is NameTag.IncorrectOriginalSpellingOf else None
        )
    )
    parent = SimpleNamespace(rank=Rank.genus, mapped_name=correct_name)
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.genus
    auxiliary.mapped_name = cast(Name, misspelling)

    assert list(check_auxiliary_name(auxiliary, LintConfig())) == []


def test_parent_rank_allows_same_rank_auxiliary_subspecies() -> None:
    parent = SimpleNamespace(rank=Rank.subspecies)
    auxiliary = _make_ce(parent=parent, auxiliary=True)
    auxiliary.rank = Rank.subspecies

    assert list(check_parent_rank(auxiliary, LintConfig())) == []


def test_is_misspelling_of() -> None:
    correct = cast(Name, object())
    misspelling = SimpleNamespace()

    def get_tag_target(tag_cls: Any) -> Name | None:
        if tag_cls is NameTag.IncorrectSubsequentSpellingOf:
            return correct
        return None

    misspelling.get_tag_target = get_tag_target

    assert _is_misspelling_of(cast(Name, misspelling), correct)


def test_needs_auxiliary_name_finds_misspelled_sibling() -> None:
    parent = object()
    correct_name = cast(Name, object())
    misspelling = SimpleNamespace()

    def get_tag_target(tag_cls: Any) -> Name | None:
        if tag_cls is NameTag.IncorrectSubsequentSpellingOf:
            return correct_name
        return None

    misspelling.get_tag_target = get_tag_target
    correct_ce = _make_ce(parent=parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = cast(Name, misspelling)

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
    original_parent = object()
    correct_name = cast(Name, object())
    misspelling = SimpleNamespace()

    def get_tag_target(tag_cls: Any) -> Name | None:
        if tag_cls is NameTag.IncorrectSubsequentSpellingOf:
            return correct_name
        return None

    misspelling.get_tag_target = get_tag_target
    correct_ce = _make_ce(parent=original_parent, auxiliary=False)
    correct_ce.rank = Rank.genus
    correct_ce.mapped_name = correct_name
    misspelled_ce = _make_ce(parent=original_parent, auxiliary=False)
    misspelled_ce.rank = Rank.genus
    misspelled_ce.mapped_name = cast(Name, misspelling)
    child = _make_ce(parent=misspelled_ce, auxiliary=False)
    cast(Any, misspelled_ce).children.append(child)

    with patch(
        "taxonomy.db.models.classification_entry.lint._get_same_level_ces",
        return_value=[misspelled_ce, correct_ce],
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
