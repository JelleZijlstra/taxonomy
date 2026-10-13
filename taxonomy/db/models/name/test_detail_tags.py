from collections.abc import Generator, Iterable
from typing import Any

import pytest

from taxonomy.db.constants import ArticleKind, ArticleType, Group, Rank, Status
from taxonomy.db.models import Article
from taxonomy.db.models import detail as detail_lint
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.classification_entry.ce import (
    DETAIL_METADATA_FIELDS,
    DETAIL_TAGS,
    ClassificationEntry,
    ClassificationEntryTag,
)
from taxonomy.db.models.classification_entry.lint import (
    check_duplicate_details as check_duplicate_ce_details,
)
from taxonomy.db.models.classification_entry.lint import (
    check_tags,
    infer_data_from_mapped,
)
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.lint import ce_mapping, type_tags
from taxonomy.db.models.name.name import (
    DETAIL_TAG_TO_CE_TAG,
    SOURCE_TAGS,
    Name,
    TypeTag,
)

_CFG = LintConfig(autofix=False, interactive=False, available_resources=frozenset())
_METADATA = {
    "comment": "A comment",
    "page": "12",
    "translation": "A translation",
    "page_link": "https://example.com/12",
}


@pytest.mark.parametrize(
    ("tag_type", "tag_id"),
    [
        (TypeTag.SpecimenDetail, 17),
        (TypeTag.LocationDetail, 18),
        (TypeTag.TypeSpeciesDetail, 22),
        (TypeTag.CollectionDetail, 24),
        (TypeTag.CitationDetail, 25),
        (TypeTag.DefinitionDetail, 26),
        (TypeTag.EtymologyDetail, 27),
        (TypeTag.NomenclatureDetail, 41),
        (TypeTag.SourceDetail, 57),
        (TypeTag.DescriptionDetail, 60),
    ],
)
def test_detail_serialization_preserves_stored_positions(
    tag_type: type[TypeTag], tag_id: int
) -> None:
    old = [tag_id, "verbatim", 123]
    tag = TypeTag.unserialize(old)
    assert type(tag) is tag_type
    assert tag.text == "verbatim"
    assert tag.source.id == 123
    assert tag.serialize() == old
    enriched = tag.replace(classification_entry=ClassificationEntry(456), **_METADATA)
    suffix: list[Any] = ["A comment", "12", "A translation", "https://example.com/12"]
    if tag_type is TypeTag.SpecimenDetail:
        suffix = [456, *suffix]
        old.append(456)
        assert TypeTag.unserialize(old).classification_entry.id == 456
        assert TypeTag.unserialize(old).comment is None
    else:
        suffix.append(456)
    assert enriched.serialize() == [tag_id, "verbatim", 123, *suffix]
    assert TypeTag.unserialize(enriched.serialize()) == enriched
    assert TypeTag.unserialize([9, "legacy"]).serialize() == [9, "legacy"]


@pytest.mark.parametrize(
    ("tag_id", "tag_type"),
    [
        (5, ClassificationEntryTag.TypeSpecimenData),
        (21, ClassificationEntryTag.EtymologyDetail),
    ],
)
def test_ce_detail_old_serialization(
    tag_id: int, tag_type: type[ClassificationEntryTag]
) -> None:
    old = [tag_id, "verbatim"]
    tag = ClassificationEntryTag.unserialize(old)
    assert type(tag) is tag_type
    assert tag.serialize() == old
    assert all(getattr(tag, field) is None for field in DETAIL_METADATA_FIELDS)
    enriched = tag.replace(**_METADATA)
    assert ClassificationEntryTag.unserialize(enriched.serialize()) == enriched


def _models(monkeypatch: pytest.MonkeyPatch) -> tuple[Name, ClassificationEntry]:
    article = Article.virtual(
        name="source.pdf",
        kind=ArticleKind.electronic,
        type=ArticleType.JOURNAL,
        url=None,
        start_page="1",
        end_page="20",
        tags=(),
    )
    name = Name.virtual(
        group=Group.species,
        status=Status.valid,
        original_citation=article,
        type_tags=(),
        collection=None,
    )
    ce = ClassificationEntry.virtual(
        article=article,
        name="Species example",
        rank=Rank.species,
        parent=None,
        mapped_name=name,
        type_locality="Locality",
        citation=None,
        tags=(),
    )
    monkeypatch.setattr(Name, "__str__", lambda _: "test name")
    monkeypatch.setattr(ClassificationEntry, "__str__", lambda _: "test CE")
    monkeypatch.setattr(Name, "resolve_variant", lambda self, **_: self)
    monkeypatch.setattr(Name, "get_mapped_classification_entry", lambda _: ce)
    monkeypatch.setattr(Name, "get_classification_entries", lambda _: [ce])
    name.type_tags = (
        TypeTag.LocationDetail("Locality", article, classification_entry=ce),
    )
    return name, ce


def _apply(issues: Iterable[LintResult]) -> None:
    for issue in issues:
        assert not isinstance(issue, str), issue
        assert issue.fix is not None, issue
        assert issue.fix.apply()


def _finish(
    generator: Generator[LintResult, None, Any],
) -> tuple[list[LintResult], Any]:
    issues = []
    while True:
        try:
            issues.append(next(generator))
        except StopIteration as exc:
            return issues, exc.value


@pytest.mark.parametrize(("name_type", "ce_type"), DETAIL_TAG_TO_CE_TAG.items())
@pytest.mark.parametrize("existing", [False, True])
def test_detail_sync_both_directions_and_idempotence(
    monkeypatch: pytest.MonkeyPatch,
    name_type: type[TypeTag],
    ce_type: type[ClassificationEntryTag],
    *,
    existing: bool,
) -> None:
    name, ce = _models(monkeypatch)
    ce.tags = (ce_type("quotation", **_METADATA),)  # type: ignore[assignment]
    if existing:
        name.type_tags = (*name.type_tags, name_type("quotation", ce.article))  # type: ignore[assignment]
    issues = list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
    assert len(issues) == 1
    _apply(issues)
    (detail,) = name.get_tags(name.type_tags, name_type)
    assert detail.classification_entry == ce
    assert all(getattr(detail, field) == value for field, value in _METADATA.items())
    assert list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG)) == []
    ce.tags = (ce_type("quotation"),) if existing else ()  # type: ignore[assignment]
    _apply(infer_data_from_mapped.linter(ce, _CFG))
    assert ce.tags == (ce_type("quotation", **_METADATA),)
    assert list(infer_data_from_mapped.linter(ce, _CFG)) == []


@pytest.mark.parametrize(("name_type", "ce_type"), DETAIL_TAG_TO_CE_TAG.items())
@pytest.mark.parametrize("clean_name", [False, True])
@pytest.mark.parametrize("reverse_order", [False, True])
def test_normalized_duplicate_details_stay_removed(
    monkeypatch: pytest.MonkeyPatch,
    name_type: type[TypeTag],
    ce_type: type[ClassificationEntryTag],
    *,
    clean_name: bool,
    reverse_order: bool,
) -> None:
    name, ce = _models(monkeypatch)
    # Name 46519 and CE 365126 had both forms, with a page link on only
    # the straight-quote version. Linting either side must break the loop.
    raw = "Plée’s monkey was called “carita blanca.”"
    clean = 'Plée\'s monkey was called "carita blanca."'
    name_tags = [
        name_type(raw, ce.article, classification_entry=ce, page="12"),
        name_type(clean, ce.article, classification_entry=ce, **_METADATA),
    ]
    ce_tags = [ce_type(raw, page="12"), ce_type(clean, **_METADATA)]
    if reverse_order:
        name_tags.reverse()
        ce_tags.reverse()
    name.type_tags = (*name.type_tags, *name_tags)  # type: ignore[assignment]
    ce.tags = tuple(ce_tags)  # type: ignore[assignment]
    owner = name if clean_name else ce
    before = name.type_tags if clean_name else ce.tags
    issues = list(
        type_tags.check_duplicate_details.linter(name, _CFG)
        if clean_name
        else check_duplicate_ce_details.linter(ce, _CFG)
    )
    assert (name.type_tags if clean_name else ce.tags) == before
    _apply(issues)

    for _ in range(3):
        _apply(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
        _apply(infer_data_from_mapped.linter(ce, _CFG))
        assert list(detail_lint.clean_duplicate_details(owner)) == []
        tags = (
            list(name.get_tags(name.type_tags, name_type))
            if clean_name
            else list(ce.get_tags(ce.tags, ce_type))
        )
        assert len(tags) == 1
        assert tags[0].text == clean
        assert all(
            getattr(tags[0], field) == value for field, value in _METADATA.items()
        )

    _apply(detail_lint.clean_duplicate_details(ce if clean_name else name))
    assert list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG)) == []
    assert list(infer_data_from_mapped.linter(ce, _CFG)) == []
    assert list(type_tags.check_type_tags_for_name.linter(name, _CFG)) == []


@pytest.mark.parametrize("on_name", [False, True])
@pytest.mark.parametrize(
    "difference", ["text", "source", "classification_entry", *DETAIL_METADATA_FIELDS]
)
def test_duplicate_cleanup_preserves_distinct_details(
    monkeypatch: pytest.MonkeyPatch, difference: str, *, on_name: bool
) -> None:
    name, ce = _models(monkeypatch)
    owner: Name | ClassificationEntry
    if on_name:
        owner = name
        field = "type_tags"
        first = TypeTag.EtymologyDetail(
            "quotation", ce.article, classification_entry=ce, **_METADATA
        )
    else:
        if difference in ("source", "classification_entry"):
            return  # These fields exist only on Name details.
        owner = ce
        field = "tags"
        first = ClassificationEntryTag.EtymologyDetail("quotation", **_METADATA)
    if difference == "source":
        second = first.replace(source=Article.virtual())
    elif difference == "classification_entry":
        second = first.replace(classification_entry=ClassificationEntry.virtual())
    else:
        second = first.replace(**{difference: "different"})
    setattr(owner, field, (first, second))
    assert list(detail_lint.clean_duplicate_details(owner)) == []
    assert getattr(owner, field) == (first, second)


@pytest.mark.parametrize("from_name", [False, True])
def test_new_detail_transfer_normalizes_text(
    monkeypatch: pytest.MonkeyPatch, *, from_name: bool
) -> None:
    name, ce = _models(monkeypatch)
    if from_name:
        name.type_tags = (  # type: ignore[assignment]
            *name.type_tags,
            TypeTag.EtymologyDetail("“quoted”", ce.article, classification_entry=ce),
        )
        _apply(infer_data_from_mapped.linter(ce, _CFG))
        (tag,) = ce.tags
    else:
        ce.tags = (ClassificationEntryTag.EtymologyDetail("“quoted”"),)  # type: ignore[assignment]
        _apply(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
        (tag,) = name.get_tags(name.type_tags, TypeTag.EtymologyDetail)
    assert tag.text == '"quoted"'
    assert list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG)) == []
    assert list(infer_data_from_mapped.linter(ce, _CFG)) == []


def test_original_citation_detail_survives_lint_and_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, ce = _models(monkeypatch)
    ce_tag = ClassificationEntryTag.CitationDetail("quotation", **_METADATA)
    ce.tags = (ce_tag,)  # type: ignore[assignment]
    _apply(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
    (detail,) = name.get_tags(name.type_tags, TypeTag.CitationDetail)
    assert detail.source == name.original_citation
    issues, checked = _finish(type_tags._check_all_type_tags(detail, name, _CFG, {}))
    assert issues == []
    assert checked == [detail]
    assert detail.classification_entry == ce
    assert all(getattr(detail, field) == value for field, value in _METADATA.items())
    ce.tags = ()  # type: ignore[assignment]
    _apply(infer_data_from_mapped.linter(ce, _CFG))
    assert list(ce.tags) == [ce_tag]
    assert list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG)) == []
    assert list(infer_data_from_mapped.linter(ce, _CFG)) == []


@pytest.mark.parametrize("tag_type", SOURCE_TAGS)
def test_detail_page_cleanup_inference_and_validation(
    monkeypatch: pytest.MonkeyPatch, tag_type: type[TypeTag]
) -> None:
    name, ce = _models(monkeypatch)
    tag = tag_type("quotation", ce.article, comment="p. 12", translation="translation")
    issues, tags = _finish(type_tags._check_detail_type_tag(tag, name, _CFG, {}))
    assert issues == []
    (tag,) = tags
    assert tag.page == "12"
    assert tag.comment is None
    assert tag.translation == "translation"
    ce.article.url = "https://www.biodiversitylibrary.org/item/1"
    monkeypatch.setattr(type_tags, "infer_bhl_page_id", lambda *_: (12345, "test"))
    issues, tags = _finish(type_tags._check_detail_type_tag(tag, name, _CFG, {}))
    assert issues == []
    assert tags[0].page_link == "https://www.biodiversitylibrary.org/page/12345"
    issues, _ = _finish(
        type_tags._check_detail_type_tag(tag.replace(page="30"), name, _CFG, {})
    )
    assert issues


@pytest.mark.parametrize("ce_type", DETAIL_TAGS)
def test_ce_detail_page_cleanup(
    monkeypatch: pytest.MonkeyPatch, ce_type: type[ClassificationEntryTag]
) -> None:
    _, ce = _models(monkeypatch)
    ce.tags = (ce_type("quotation", comment="p. 12", translation="translation"),)  # type: ignore[assignment]
    _apply(check_tags.linter(ce, _CFG))
    assert ce.tags == (ce_type("quotation", page="12", translation="translation"),)


@pytest.mark.parametrize(("name_type", "ce_type"), DETAIL_TAG_TO_CE_TAG.items())
def test_detail_checks_ce_provenance_text_and_metadata(
    monkeypatch: pytest.MonkeyPatch,
    name_type: type[TypeTag],
    ce_type: type[ClassificationEntryTag],
) -> None:
    name, ce = _models(monkeypatch)
    ce.tags = (ce_type("quotation", **_METADATA),)  # type: ignore[assignment]
    tag = name_type("quotation", ce.article, classification_entry=ce)
    issues, tags = _finish(type_tags._check_detail_type_tag(tag, name, _CFG, {}))
    assert issues == []
    assert tags == [tag.replace(**_METADATA)]
    issues, tags = _finish(
        type_tags._check_detail_type_tag(tag.replace(page="13"), name, _CFG, {})
    )
    assert any("does not match classification entry" in str(issue) for issue in issues)
    assert tags[0].page == "13"
    wrong_source = Article.virtual(name="other.pdf", url=None)
    ce.mapped_name = Name.virtual()
    ce.tags = (ce_type("different quotation"),)  # type: ignore[assignment]
    issues, _ = _finish(
        type_tags._check_detail_type_tag(
            tag.replace(source=wrong_source), name, _CFG, {}
        )
    )
    assert len(issues) == 3
    assert any("does not match source" in str(issue) for issue in issues)
    assert any("does not match name" in str(issue) for issue in issues)
    assert any("does not match text" in str(issue) for issue in issues)


def test_all_specimen_quotations_are_synced(monkeypatch: pytest.MonkeyPatch) -> None:
    name, ce = _models(monkeypatch)
    ce.tags = (
        ClassificationEntryTag.TypeSpecimenData("First"),  # type: ignore[assignment]
        ClassificationEntryTag.TypeSpecimenData("Second"),
    )
    _apply(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
    assert {
        tag.text for tag in name.get_tags(name.type_tags, TypeTag.SpecimenDetail)
    } == {"First", "Second"}


def test_definition_cleanup_preserves_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    name, ce = _models(monkeypatch)
    tag = TypeTag.DefinitionDetail(
        "Te defnition", ce.article, classification_entry=ce, **_METADATA
    )
    issues, tags = _finish(type_tags._check_all_type_tags(tag, name, _CFG, {}))
    assert issues == []
    assert tags == [tag.replace(text="The definition")]


@pytest.mark.parametrize(("name_type", "ce_type"), DETAIL_TAG_TO_CE_TAG.items())
def test_sync_preserves_conflicting_metadata(
    monkeypatch: pytest.MonkeyPatch,
    name_type: type[TypeTag],
    ce_type: type[ClassificationEntryTag],
) -> None:
    name, ce = _models(monkeypatch)
    detail = name_type("quotation", ce.article, classification_entry=ce, page="13")
    name.type_tags = (*name.type_tags, detail)  # type: ignore[assignment]
    ce.tags = (ce_type("quotation", page="12"),)  # type: ignore[assignment]
    name_issues = list(ce_mapping.infer_tags_from_mapped_entries.linter(name, _CFG))
    ce_issues = list(infer_data_from_mapped.linter(ce, _CFG))
    assert len(name_issues) == len(ce_issues) == 1
    assert all(isinstance(issue, str) for issue in [*name_issues, *ce_issues])
    assert detail in name.type_tags
    assert ce.tags == (ce_type("quotation", page="12"),)


def test_sync_does_not_copy_quotation_from_another_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, ce = _models(monkeypatch)
    other_ce = ClassificationEntry.virtual(article=ce.article)
    detail = TypeTag.SpecimenDetail(
        "quotation", ce.article, classification_entry=other_ce
    )
    name.type_tags = (*name.type_tags, detail)  # type: ignore[assignment]
    assert list(infer_data_from_mapped.linter(ce, _CFG)) == []


@pytest.mark.parametrize("extra_field", [None, *DETAIL_METADATA_FIELDS])
def test_redundant_detail_cleanup_preserves_metadata(
    monkeypatch: pytest.MonkeyPatch, extra_field: str | None
) -> None:
    name, ce = _models(monkeypatch)
    unlinked = TypeTag.SpecimenDetail("quotation", ce.article)
    if extra_field is not None:
        unlinked = unlinked.replace(**{extra_field: _METADATA[extra_field]})
    linked = TypeTag.SpecimenDetail("quotation", ce.article, classification_entry=ce)
    issues, tags = _finish(
        type_tags._check_detail_type_tag(
            unlinked, name, _CFG, {TypeTag.SpecimenDetail: [unlinked, linked]}
        )
    )
    if extra_field is None:
        assert len(issues) == 1
        assert tags == []
    else:
        assert issues == []
        assert tags == [unlinked]


@pytest.mark.parametrize("reverse_order", [False, True])
@pytest.mark.parametrize("prefix", ["", "From ", "fRoM ", "in ", "IN "])
@pytest.mark.parametrize("suffix", ["", "."])
def test_location_detail_substring_cleanup_autofix(
    monkeypatch: pytest.MonkeyPatch, prefix: str, suffix: str, *, reverse_order: bool
) -> None:
    name, ce = _models(monkeypatch)
    text = "Pecos River at Carlsbad (Eddy), New Mexico"
    ce.type_locality = f"the {text}"
    unlinked = TypeTag.LocationDetail(f"{prefix}{text}{suffix}", ce.article)
    linked = TypeTag.LocationDetail(
        ce.type_locality, ce.article, classification_entry=ce, **_METADATA
    )
    original_tags = (linked, unlinked) if reverse_order else (unlinked, linked)
    name.type_tags = original_tags  # type: ignore[assignment]

    issues = list(type_tags.check_type_tags_for_name.linter(name, _CFG))

    assert name.type_tags == original_tags
    assert any("remove redundant tag" in str(issue) for issue in issues)
    _apply(issue for issue in issues if not isinstance(issue, str))
    assert name.type_tags == (linked,)  # type: ignore[comparison-overlap]
    assert list(type_tags.check_type_tags_for_name.linter(name, _CFG)) == []


@pytest.mark.parametrize(
    "difference",
    [
        "source",
        "no_ce",
        "both_ce",
        "reverse_substring",
        "text",
        *DETAIL_METADATA_FIELDS,
    ],
)
def test_location_detail_substring_cleanup_preserves_distinct_details(
    monkeypatch: pytest.MonkeyPatch, difference: str
) -> None:
    name, ce = _models(monkeypatch)
    ce.type_locality = "the Pecos River"
    unlinked = TypeTag.LocationDetail("From Pecos River.", ce.article)
    linked = TypeTag.LocationDetail(
        ce.type_locality, ce.article, classification_entry=ce
    )
    if difference == "source":
        unlinked = unlinked.replace(
            source=Article.virtual(
                name="other.pdf", kind=ArticleKind.electronic, url=None
            )
        )
    elif difference == "no_ce":
        linked = linked.replace(classification_entry=None)
    elif difference == "both_ce":
        unlinked = unlinked.replace(classification_entry=ce)
    elif difference == "reverse_substring":
        unlinked = unlinked.replace(text="the Pecos River at Carlsbad")
    elif difference == "text":
        unlinked = unlinked.replace(text="another river")
    else:
        unlinked = unlinked.replace(**{difference: _METADATA[difference]})
    original_tags = (unlinked, linked)
    name.type_tags = original_tags  # type: ignore[assignment]

    issues = list(type_tags.check_type_tags_for_name.linter(name, _CFG))

    assert all(isinstance(issue, str) for issue in issues)
    assert name.type_tags == original_tags


@pytest.mark.parametrize("text", ["FromPecos River", "inPecos River", "From .", "."])
def test_location_detail_cleanup_requires_nonempty_text_and_whole_prefix_word(
    monkeypatch: pytest.MonkeyPatch, text: str
) -> None:
    name, ce = _models(monkeypatch)
    unlinked = TypeTag.LocationDetail(text, ce.article)
    linked = TypeTag.LocationDetail("Pecos River", ce.article, classification_entry=ce)

    issues, tags = _finish(
        type_tags._check_detail_type_tag(
            unlinked, name, _CFG, {TypeTag.LocationDetail: [unlinked, linked]}
        )
    )

    assert issues == []
    assert tags == [unlinked]
