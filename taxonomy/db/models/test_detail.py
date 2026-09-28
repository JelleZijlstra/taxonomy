from collections.abc import Iterable

import pytest

from taxonomy.db import detail_pages
from taxonomy.db.models import Article, ClassificationEntry, Name
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag
from taxonomy.db.models.detail import infer_detail_pages, infer_detail_pages_from_text
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name import TypeTag
from taxonomy.db.test_detail_pages import QUOTE


def _apply(issues: Iterable[LintResult]) -> int:
    count = 0
    for issue in issues:
        assert not isinstance(issue, str)
        assert issue.fix is not None
        assert issue.fix.apply()
        count += 1
    return count


@pytest.fixture
def article() -> Article:
    return Article.virtual(name="source.pdf", url=None, start_page="1", end_page="20")


@pytest.fixture(autouse=True)
def _display(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Name, "__str__", lambda _: "name")
    monkeypatch.setattr(ClassificationEntry, "__str__", lambda _: "entry")
    monkeypatch.setattr(Name, "get_classification_entries", lambda _: [])


def test_annotation_fix_and_conflict(article: Article) -> None:
    tag = TypeTag.LocationDetail("Locality [p. 12]", article)
    nam = Name.virtual(type_tags=(tag,))
    assert _apply(infer_detail_pages(nam)) == 1
    assert nam.type_tags == (tag.replace(text="Locality", page="12"),)
    assert list(infer_detail_pages(nam)) == []
    nam.type_tags = (tag.replace(page="13"),)
    issues = list(infer_detail_pages(nam))
    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert nam.type_tags == (tag.replace(page="13"),)


def test_ce_annotation_and_substring(article: Article) -> None:
    ce = ClassificationEntry.virtual(
        article=article,
        tags=(
            ClassificationEntryTag.EtymologyDetail(
                "Named for the northern river [p. 12]"
            ),
        ),
    )
    assert _apply(infer_detail_pages(ce)) == 1
    ce.tags = (
        *ce.tags,
        ClassificationEntryTag.DescriptionDetail("Named for the northern river"),
    )
    assert _apply(infer_detail_pages(ce)) == 1
    assert ce.tags[1].page == "12"


def test_substring_requires_same_source_and_entry(article: Article) -> None:
    ce = ClassificationEntry.virtual()
    other_ce = ClassificationEntry.virtual()
    target = TypeTag.LocationDetail(
        "Northern bank of the river", article, classification_entry=ce
    )
    donor = TypeTag.SpecimenDetail(
        "Holotype from the Northern bank of the river.",
        article,
        page="12",
        classification_entry=other_ce,
    )
    nam = Name.virtual(type_tags=(target, donor))
    assert list(infer_detail_pages(nam)) == []
    nam.type_tags = (
        target,
        donor.replace(
            classification_entry=ce, source=Article.virtual(name="other.pdf")
        ),
    )
    assert list(infer_detail_pages(nam)) == []
    nam.type_tags = (target, donor.replace(classification_entry=ce))
    assert _apply(infer_detail_pages(nam)) == 1
    assert nam.type_tags[0].page == "12"


def test_text_search_resources_and_bhl_scope(
    article: Article, monkeypatch: pytest.MonkeyPatch
) -> None:
    ce = ClassificationEntry.virtual(article=article, page="12", tags=())
    nam = Name.virtual(
        original_citation=article,
        page_described="10-11",
        type_tags=(
            TypeTag.SpecimenDetail(QUOTE, article, classification_entry=ce),
            TypeTag.LocationDetail("Locality", article, page="13"),
        ),
    )
    calls = []
    monkeypatch.setattr(detail_pages, "cached_pdf_pages", lambda _: ())

    def bhl(
        pages: set[str], links: object, url: object
    ) -> tuple[detail_pages.PageText, ...]:
        calls.append(pages)
        return (
            detail_pages.PageText(
                "12", QUOTE, "https://www.biodiversitylibrary.org/page/123"
            ),
        )

    monkeypatch.setattr(detail_pages, "cached_bhl_pages", bhl)
    assert (
        list(
            infer_detail_pages_from_text(
                nam, LintConfig(available_resources=frozenset())
            )
        )
        == []
    )
    assert not calls
    assert (
        _apply(
            infer_detail_pages_from_text(
                nam, LintConfig(available_resources=frozenset({LintResource.SLOW}))
            )
        )
        == 1
    )
    assert calls == [{"10", "11", "12", "13"}]
    assert nam.type_tags[0].page == "12"
    assert nam.type_tags[0].page_link == "https://www.biodiversitylibrary.org/page/123"


@pytest.mark.parametrize(
    "pdf_pages",
    [
        [detail_pages.PageText("12", QUOTE), detail_pages.PageText(None, QUOTE)],
        [detail_pages.PageText("13", QUOTE)],
    ],
)
def test_pdf_ambiguity_or_conflict_blocks_bhl(
    article: Article,
    monkeypatch: pytest.MonkeyPatch,
    pdf_pages: list[detail_pages.PageText],
) -> None:
    ce = ClassificationEntry.virtual(
        article=article,
        page="12",
        tags=(ClassificationEntryTag.TypeSpecimenData(QUOTE),),
    )
    monkeypatch.setattr(detail_pages, "cached_pdf_pages", lambda _: pdf_pages)
    monkeypatch.setattr(
        detail_pages,
        "cached_bhl_pages",
        lambda *_: (detail_pages.PageText("12", QUOTE),),
    )
    assert (
        list(
            infer_detail_pages_from_text(
                ce, LintConfig(available_resources=frozenset({LintResource.SLOW}))
            )
        )
        == []
    )


def test_pdf_inference_preserves_text_and_metadata(
    article: Article, monkeypatch: pytest.MonkeyPatch
) -> None:
    tag = ClassificationEntryTag.TypeSpecimenData(
        QUOTE, comment="Keep", translation="Keep translation"
    )
    ce = ClassificationEntry.virtual(article=article, page=None, tags=(tag,))
    monkeypatch.setattr(
        detail_pages,
        "cached_pdf_pages",
        lambda _: (detail_pages.PageText("12", QUOTE),),
    )
    assert (
        _apply(
            infer_detail_pages_from_text(
                ce, LintConfig(available_resources=frozenset({LintResource.SLOW}))
            )
        )
        == 1
    )
    assert ce.tags == (tag.replace(page="12"),)


def test_explicit_annotation_not_overridden_by_text_search(
    article: Article, monkeypatch: pytest.MonkeyPatch
) -> None:
    ce = ClassificationEntry.virtual(
        article=article,
        page=None,
        tags=(ClassificationEntryTag.TypeSpecimenData(f"{QUOTE} [p. 13]"),),
    )
    monkeypatch.setattr(
        detail_pages,
        "cached_pdf_pages",
        lambda _: (detail_pages.PageText("12", QUOTE),),
    )
    assert (
        list(
            infer_detail_pages_from_text(
                ce, LintConfig(available_resources=frozenset({LintResource.SLOW}))
            )
        )
        == []
    )


def test_annotation_keeps_linked_type_locality_in_sync(article: Article) -> None:
    text = "Locality [p. 12]"
    ce = ClassificationEntry.virtual(article=article, type_locality=text)
    tag = TypeTag.LocationDetail(text, article, classification_entry=ce)
    nam = Name.virtual(type_tags=(tag,))
    assert _apply(infer_detail_pages(nam)) == 1
    assert nam.type_tags == (tag.replace(text="Locality", page="12"),)
    assert ce.type_locality == "Locality"


def test_annotation_keeps_linked_detail_in_sync(article: Article) -> None:
    text = "Quotation [p. 12]"
    ce_tag = ClassificationEntryTag.TypeSpecimenData(text, comment="Preserve")
    ce = ClassificationEntry.virtual(article=article, tags=(ce_tag,))
    tag = TypeTag.SpecimenDetail(text, article, classification_entry=ce)
    nam = Name.virtual(type_tags=(tag,))
    assert _apply(infer_detail_pages(nam)) == 1
    assert nam.type_tags == (tag.replace(text="Quotation", page="12"),)
    assert ce.tags == (ce_tag.replace(text="Quotation", page="12"),)


def test_annotation_does_not_overwrite_linked_detail_conflict(article: Article) -> None:
    text = "Quotation [p. 12]"
    ce_tag = ClassificationEntryTag.TypeSpecimenData(text, page="13")
    ce = ClassificationEntry.virtual(article=article, tags=(ce_tag,))
    tag = TypeTag.SpecimenDetail(text, article, classification_entry=ce)
    nam = Name.virtual(type_tags=(tag,))
    issues = list(infer_detail_pages(nam))
    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert nam.type_tags == (tag,)
    assert ce.tags == (ce_tag,)


@pytest.mark.parametrize("checklist_page", ["516", "398"])
def test_leading_checklist_reference_is_not_a_source_page(checklist_page: str) -> None:
    source = Article.virtual(name="Moreau 1946", start_page="387", end_page="447")
    text = (
        f"[p. {checklist_page}] ONOTRAGUS LECHE AMBOELLENSIS Sokolowsky. Kubango Basin."
    )
    tag = TypeTag.SpecimenDetail(text, source)
    nam = Name.virtual(type_tags=(tag,))
    assert list(infer_detail_pages(nam)) == []
    assert nam.type_tags == (tag,)


@pytest.mark.parametrize("page", ["516", "445-516"])
def test_out_of_range_annotation_is_not_removed_or_inferred(page: str) -> None:
    source = Article.virtual(name="Moreau 1946", start_page="387", end_page="447")
    tag = TypeTag.SpecimenDetail(f"Quotation [p. {page}]", source)
    nam = Name.virtual(type_tags=(tag,))
    issues = list(infer_detail_pages(nam))
    assert len(issues) == 1
    assert isinstance(issues[0], str)
    assert "outside its source's page range" in issues[0]
    assert nam.type_tags == (tag,)


def test_out_of_range_donor_does_not_propagate_to_location() -> None:
    source = Article.virtual(name="Moreau 1946", start_page="387", end_page="447")
    location = "Kubango (Okabango) Basin-between Kulio and Quatiri"
    donor = TypeTag.SpecimenDetail(
        f"ONOTRAGUS LECHE AMBOELLENSIS Sokolowsky. {location}", source, page="516"
    )
    target = TypeTag.LocationDetail(location, source)
    nam = Name.virtual(type_tags=(donor, target))
    assert list(infer_detail_pages(nam)) == []
    assert nam.type_tags == (donor, target)
