from collections.abc import Iterable

import pytest

from taxonomy.db.constants import ArticleKind, ArticleType
from taxonomy.db.models import Article, CitationGroup, Name, Person, TypeTag
from taxonomy.db.models.article.article import ArticleTag
from taxonomy.db.models.classification_entry.ce import (
    ClassificationEntry,
    ClassificationEntryStatus,
)
from taxonomy.db.models.person import AuthorTag

from .citation_match import CitationIndex, page_or_range


def _group(name: str = "Journal", *, book: bool = False) -> CitationGroup:
    return CitationGroup.virtual(
        name=name, type=ArticleType.BOOK if book else ArticleType.JOURNAL, tags=()
    )


def _authors(*names: str) -> tuple[AuthorTag, ...]:
    return tuple(AuthorTag.Author(Person.virtual(family_name=name)) for name in names)


def _article(
    group: CitationGroup | None,
    *,
    name: str = "source.pdf",
    title: str = "An account of the mammals of the world",
    year: str | None = "1900",
    authors: tuple[str, ...] = ("Smith",),
    start: str | None = "100",
    end: str | None = "120",
    volume: str | None = None,
    series: str | None = None,
    pages: str | None = None,
    parent: Article | None = None,
    kind: ArticleKind = ArticleKind.electronic,
    type: ArticleType = ArticleType.JOURNAL,
    tags: tuple[ArticleTag, ...] = (),
) -> Article:
    return Article.virtual(
        name=name,
        title=title,
        year=year,
        author_tags=_authors(*authors),
        citation_group=group,
        start_page=start,
        end_page=end,
        volume=volume,
        series=series,
        pages=pages,
        parent=parent,
        kind=kind,
        type=type,
        tags=tags,
    )


def _name(
    group: CitationGroup | None,
    *,
    page: str | None = "110",
    year: str | None = "1900",
    authors: tuple[str, ...] = ("Smith",),
    tags: tuple[TypeTag, ...] = (),
    citation: str | None = None,
) -> Name:
    return Name.virtual(
        original_name="Mus example",
        corrected_original_name="Mus example",
        root_name="example",
        year=year,
        author_tags=_authors(*authors),
        citation_group=group,
        original_citation=None,
        page_described=page,
        type_tags=tags,
        verbatim_citation=citation,
    )


def _found(
    index: CitationIndex, name: Name, *, aggressive: bool = True
) -> list[Article]:
    return [match.article for match in index.find(name, aggressive=aggressive)]


@pytest.mark.parametrize("page", ["110", "110-115", "110, 115", "110 (fig. 2)"])
def test_pages_and_lists(page: str) -> None:
    group = _group()
    article = _article(group)
    name = _name(group, page=page)
    assert _found(CitationIndex([article]), name, aggressive=False) == [article]


def test_book_with_mixed_preliminary_and_arabic_pages() -> None:
    group = _group("London", book=True)
    article = _article(group, type=ArticleType.BOOK, start=None, end=None, pages="420")
    name = _name(group, page="xv, 286, 362")
    matches = CitationIndex([article]).find(name, aggressive=False)
    assert [match.article for match in matches] == [article]
    assert "some page references cannot be compared" in matches[0].reasons


def test_figure_lists_do_not_supply_numeric_pages() -> None:
    name = _name(_group(), page="pl. VIII, fig. 4, 5, 9, 10")
    assert page_or_range(name) == (None, None)


def test_page_lists_check_every_numeric_page() -> None:
    group = _group()
    article = _article(group)
    name = _name(group, page="110, 121")
    assert _found(CitationIndex([article]), name) == []
    assert page_or_range(name) == (110, 121)


def test_roman_pages_are_a_separate_sequence() -> None:
    group = _group()
    article = _article(group, start="x", end="xx")
    index = CitationIndex([article])
    assert _found(index, _name(group, page="xv"), aggressive=False) == [article]
    assert _found(index, _name(group, page="xxi")) == []
    assert _found(index, _name(group, page="15"), aggressive=False) == []
    arabic_article = _article(group, start="10", end="20")
    assert _found(CitationIndex([arabic_article]), _name(group, page="xv")) == []


def test_raw_pages_are_not_treated_as_arabic_pages() -> None:
    group = _group()
    article = _article(group)
    assert (
        _found(CitationIndex([article]), _name(group, page="@110"), aggressive=False)
        == []
    )


def test_structured_pages_used_when_name_page_missing() -> None:
    group = _group()
    article = _article(group)
    tag = TypeTag.StructuredVerbatimCitation(start_page="110", end_page="115")
    assert _found(
        CitationIndex([article]), _name(group, page=None, tags=(tag,)), aggressive=False
    ) == [article]
    assert _found(CitationIndex([article]), _name(group, page="121", tags=(tag,))) == []


def test_article_pages_field_can_be_a_range() -> None:
    group = _group()
    article = _article(group, start=None, end=None, pages="351-914")
    index = CitationIndex([article])
    assert _found(index, _name(group, page="400"), aggressive=False) == [article]
    assert _found(index, _name(group, page="334")) == []


def test_missing_pages_require_aggressive_and_unambiguous_candidate() -> None:
    group = _group()
    article = _article(group)
    other = _article(group, name="other.pdf", title="A different work")
    name = _name(group, page=None)
    assert _found(CitationIndex([article]), name, aggressive=False) == []
    assert _found(CitationIndex([article]), name) == [article]
    assert _found(CitationIndex([article, other]), name) == []
    assert _found(CitationIndex([article]), _name(group, page=None, authors=())) == []


def test_missing_pages_can_use_explicit_title_or_source_tag() -> None:
    group = _group()
    article = _article(group)
    other = _article(group, name="other.pdf", title="A different work")
    index = CitationIndex([article, other])
    name = _name(
        group,
        page=None,
        citation="Smith. 1900. An account of the mammals of the world. London.",
    )
    assert _found(index, name) == [article]
    name = _name(
        group, page=None, tags=(TypeTag.LocationDetail("The locality.", article),)
    )
    assert _found(index, name) == [article]


def test_chapter_inherits_parent_group() -> None:
    group = _group("London", book=True)
    parent = _article(group, type=ArticleType.BOOK)
    chapter = _article(None, type=ArticleType.CHAPTER, parent=parent)
    matches = CitationIndex([chapter]).find(_name(group), aggressive=False)
    assert [match.article for match in matches] == [chapter]
    assert "citation group inherited from parent" in matches[0].reasons


def test_matching_chapter_preferred_to_parent_book() -> None:
    group = _group("London", book=True)
    parent = _article(group, name="book.pdf", type=ArticleType.BOOK)
    chapter = _article(
        None, name="chapter.pdf", type=ArticleType.CHAPTER, parent=parent
    )
    assert _found(CitationIndex([parent, chapter]), _name(group)) == [chapter]


def test_book_group_fallback_does_not_cross_into_journals() -> None:
    generic = _group("book", book=True)
    place = _group("London", book=True)
    book = _article(place, type=ArticleType.BOOK)
    journal = _article(_group(), name="journal.pdf")
    index = CitationIndex([book, journal])
    assert _found(index, _name(generic)) == [book]
    assert _found(index, _name(generic), aggressive=False) == []
    assert _found(index, _name(_group("Another journal"))) == []
    assert _found(index, _name(_group("Paris", book=True))) == []


def test_names_without_groups_require_author_evidence() -> None:
    article = _article(_group())
    index = CitationIndex([article])
    assert _found(index, _name(None)) == [article]
    assert _found(index, _name(None), aggressive=False) == []
    assert _found(index, _name(None, authors=())) == []
    assert _found(index, _name(None, page=None)) == []


def test_known_page_with_unknown_article_pagination_needs_support() -> None:
    group = _group()
    article = _article(group, start=None, end=None)
    index = CitationIndex([article])
    assert _found(index, _name(group)) == []
    name = _name(group, citation=article.title)
    assert _found(index, name) == [article]


def test_volume_year_discrepancy_and_translation_counterexample() -> None:
    group = _group()
    journal = _article(group, year="1901", volume="1900")
    translation = _article(group, year="1901", volume="46", name="translation.pdf")
    index = CitationIndex([journal, translation])
    assert _found(index, _name(group)) == [journal]
    assert _found(index, _name(group), aggressive=False) == []
    assert (
        _found(
            CitationIndex([_article(group, year="1902", volume="1900")]), _name(group)
        )
        == []
    )


def test_structured_volume_matching_still_allows_different_authors_and_years() -> None:
    group = _group()
    article = _article(group, year="1903", volume="7", series="2", authors=("Jones",))
    tag = TypeTag.StructuredVerbatimCitation(volume="7", series="2")
    name = _name(group, tags=(tag,))
    assert _found(CitationIndex([article]), name, aggressive=False) == [article]
    wrong = _article(group, year="1903", volume="7", series="3", authors=("Jones",))
    assert _found(CitationIndex([wrong]), name) == []


def test_attributed_author_requires_exact_mapped_name_and_page() -> None:
    group = _group("London", book=True)
    name = _name(group)
    book = _article(group, authors=("Jones",), type=ArticleType.BOOK)
    part = _article(None, authors=("Jones",), type=ArticleType.PART, parent=book)
    index = CitationIndex([part])
    entry = ClassificationEntry.virtual(
        article=part,
        mapped_name=name,
        name="Mus example",
        page="110",
        status=ClassificationEntryStatus.valid,
    )
    assert index.find(name) == []
    assert index.find(name, aggressive=False, classification_entries=[entry]) == []
    assert [
        match.article for match in index.find(name, classification_entries=[entry])
    ] == [part]
    entry.page = "111"
    assert index.find(name, classification_entries=[entry]) == []
    entry.page = "110"
    entry.name = "Mus other"
    assert index.find(name, classification_entries=[entry]) == []


@pytest.mark.parametrize(
    ("kind", "tags"),
    [
        (ArticleKind.no_copy, ()),
        (ArticleKind.redirect, ()),
        (ArticleKind.removed, ()),
        (ArticleKind.electronic, (ArticleTag.NonOriginal(),)),
        (ArticleKind.electronic, (ArticleTag.Incomplete(),)),
        (ArticleKind.electronic, (ArticleTag.MustUseChildren,)),
    ],
)
def test_unusable_articles_excluded(
    kind: ArticleKind, tags: tuple[ArticleTag, ...]
) -> None:
    group = _group()
    assert (
        _found(CitationIndex([_article(group, kind=kind, tags=tags)]), _name(group))
        == []
    )


def test_ignored_pairs_and_already_linked_names_excluded() -> None:
    group = _group()
    article = _article(group)
    index = CitationIndex([article])
    name = _name(group, tags=(TypeTag.IgnorePotentialCitationFrom(article),))
    assert _found(index, name) == []
    name = _name(group)
    name.original_citation = article
    assert _found(index, name) == []


def test_missing_years_are_not_matches() -> None:
    group = _group()
    assert (
        _found(CitationIndex([_article(group, year=None)]), _name(group, year=None))
        == []
    )


def test_classification_entries_are_only_read_when_needed() -> None:
    def entries() -> Iterable[ClassificationEntry]:
        pytest.fail("Ordinary author/year matches must not load classification entries")
        yield  # type: ignore[misc]

    group = _group()
    article = _article(group)
    assert CitationIndex([article]).find(_name(group), classification_entries=entries())


def test_shell_read_only_review_explains_matches_and_respects_aggressive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from taxonomy import getinput, shell

    group = _group()
    name = _name(group, page=None)
    index = CitationIndex([_article(group)])
    monkeypatch.setattr(Name, "load", lambda self: None)
    monkeypatch.setattr(Name, "display", lambda self: None)
    monkeypatch.setattr(Name, "get_classification_entries", lambda self: ())
    monkeypatch.setattr(getinput, "print_header", lambda obj: None)
    monkeypatch.setattr(Article, "__repr__", lambda self: self.name)

    def unexpected_edit(*args: object, **kwargs: object) -> None:
        pytest.fail("Read-only citation discovery must not open files or edit Names")

    monkeypatch.setattr(Article, "openf", unexpected_edit)
    monkeypatch.setattr(Name, "edit", unexpected_edit)
    for aggressive, expected in [(False, 0), (True, 1)]:
        assert (
            shell._find_potential_citations_for_names(
                [name], index, fix=False, aggressive=aggressive, label="Journal"
            )
            == expected
        )
    output = capsys.readouterr().out
    assert "pages missing or not comparable" in output
    assert "unique author/year candidate" in output
    assert name.original_citation is None
