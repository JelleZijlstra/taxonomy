import copy
import hashlib
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy.applicator import article as recommendations
from taxonomy.config import Options
from taxonomy.db.constants import ArticleKind, ArticleType, DateSource
from taxonomy.db.models import Article, CitationGroup, Person
from taxonomy.db.models.article.article import ArticleTag
from taxonomy.db.models.person import VirtualPerson


def _pdf_bytes() -> bytes:
    return b"%PDF-1.4\nminimal test fixture\n%%EOF\n"


_CITATION_GROUP = CitationGroup.virtual(
    name="Journal of Mollusks", type=ArticleType.JOURNAL, tags=()
)
_BOOK_CITATION_GROUP = CitationGroup.virtual(
    name="Riga", type=ArticleType.BOOK, tags=()
)


def _row(pdf: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "create_article",
        "confidence": "high",
        "reason": "The staged PDF and DOI identify the reviewed work.",
        "evidence": [
            {"kind": "landing_page", "text": "https://doi.org/10.1234/example"}
        ],
        "article": {
            "name": "Endodontidae.pdf",
            "doi": "10.1234/example",
            "fields": {"title": "Reviewed title"},
            "citation_group": {"id": _CITATION_GROUP.id, "name": "Journal of Mollusks"},
        },
        "file": {
            "source_path": "download.pdf",
            "destination_folder": "Mollusca",
            "sha256": hashlib.sha256(pdf).hexdigest(),
            "size": len(pdf),
        },
    }


def _citation_group() -> CitationGroup:
    return _CITATION_GROUP


def _book_citation_group() -> CitationGroup:
    return _BOOK_CITATION_GROUP


def _volume_row() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "create_article",
        "confidence": "high",
        "reason": "The title and copyright pages identify the edited volume.",
        "evidence": [{"kind": "title_page", "text": "Volume III, 2017."}],
        "article": {
            "name": "Wallacea, New Guinea-biodiversity (Telnov et al. 2017) (3)",
            "type": "BOOK",
            "fields": {
                "year": "2017",
                "title": (
                    "Biodiversity, Biogeography and Nature Conservation in Wallacea and New Guinea. Volume III"
                ),
                "publisher": "Entomological Society of Latvia",
                "pages": "658",
            },
            "authors": [
                {"family_name": "Telnov", "given_names": "Dmitry"},
                {"family_name": "Barclay", "given_names": "Maxwell V. L."},
                {"family_name": "Pauwels", "given_names": "Olivier S. G."},
            ],
            "citation_group": {"id": _BOOK_CITATION_GROUP.id, "name": "Riga"},
        },
    }


def _chapter_row(pdf: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "create_article",
        "confidence": "high",
        "reason": "The chapter first page and volume contents agree.",
        "evidence": [{"kind": "first_page", "text": "Tumbrinck and Skejo."}],
        "article": {
            "name": "Ophiotettix 33nov.pdf",
            "type": "CHAPTER",
            "fields": {
                "year": "2017",
                "title": "Taxonomic revision of Ophiotettix",
                "start_page": "525",
                "end_page": "580",
            },
            "authors": [
                {"family_name": "Tumbrinck", "given_names": "Josef"},
                {"family_name": "Skejo", "given_names": "Josip"},
            ],
            "parent": {
                "name": "Wallacea, New Guinea-biodiversity (Telnov et al. 2017) (3)"
            },
        },
        "file": {
            "source_path": "TumbrinckSkejo.pdf",
            "destination_folder": "Insecta",
            "sha256": hashlib.sha256(pdf).hexdigest(),
            "size": len(pdf),
        },
    }


def _supplement_row(workbook: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "create_article",
        "confidence": "high",
        "reason": "The publisher identifies the staged workbook as a supplement.",
        "evidence": [
            {"kind": "acquisition_url", "text": "https://example.org/table-s1.xlsx"}
        ],
        "article": {
            "name": "Endodontidae (supplement).xlsx",
            "type": "SUPPLEMENT",
            "fields": {"year": "1976", "title": "Supplementary Table S1"},
            "parent": {"id": 10, "name": "Endodontidae.pdf"},
        },
        "file": {
            "source_path": "table-s1.xlsx",
            "destination_folder": "Mollusca",
            "sha256": hashlib.sha256(workbook).hexdigest(),
            "size": len(workbook),
        },
    }


def _build_volume_and_chapter(tmp_path: Path) -> recommendations.RecommendationPlan:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Insecta").mkdir(parents=True)
    (new_path / "TumbrinckSkejo.pdf").write_bytes(pdf)
    rows = (
        recommendations.parse_recommendation(_volume_row(), 1),
        recommendations.parse_recommendation(_chapter_row(pdf), 2),
    )
    return recommendations.build_plan(
        rows,
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: _book_citation_group(),
        expand_doi=lambda _doi: {},
    )


def _build(tmp_path: Path) -> recommendations.RecommendationPlan:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf)
    row = recommendations.parse_recommendation(_row(pdf), 1)
    return recommendations.build_plan(
        (row,),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: _citation_group(),
        expand_doi=lambda _doi: {
            "type": ArticleType.JOURNAL,
            "title": "CrossRef title",
            "journal": "Journal of Mollusks",
            "author_tags": [VirtualPerson(family_name="Solem", given_names="Alan")],
            "tags": [
                ArticleTag.PublicationDate(source=DateSource.doi_published, date="1976")
            ],
        },
    )


def test_review_expands_article_creation_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    row = recommendations.parse_recommendation(_volume_row(), 1)

    recommendations.print_review_table([row])

    output = capsys.readouterr().out
    assert "    - article type: BOOK" in output
    assert "    - field: year='2017'" in output
    assert "    - field: title='Biodiversity, Biogeography" in output
    assert "    - author 1: family_name='Telnov', given_names='Dmitry'" in output
    assert "    - citation group: CitationGroupSpec(" in output


def test_build_plan_expands_doi_and_applies_explicit_overrides(tmp_path: Path) -> None:
    plan = _build(tmp_path)

    action = plan.actions[0]
    assert action.article_type is ArticleType.JOURNAL
    assert action.fields["title"] == "Reviewed title"
    assert action.authors[0].family_name == "Solem"
    assert action.crossref_journal == "Journal of Mollusks"
    assert not action.already_applied


def test_build_plan_canonicalizes_tags_and_infers_jstor(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    row_data = _row(pdf)
    row_data["article"]["doi"] = "10.2307/1445633"
    row = recommendations.parse_recommendation(row_data, 1)
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf)
    date_tag = ArticleTag.PublicationDate(
        source=DateSource.doi_published, date="1989-02-27"
    )

    plan = recommendations.build_plan(
        (row,),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: _citation_group(),
        expand_doi=lambda _doi: {
            "type": ArticleType.JOURNAL,
            "tags": [date_tag, date_tag],
        },
    )

    assert plan.actions[0].tags == tuple(
        sorted((ArticleTag.JSTOR("1445633"), date_tag))
    )


def test_virtual_article_includes_legacy_string_defaults(tmp_path: Path) -> None:
    plan = _build(tmp_path)

    class RecordingBuilder:
        def __init__(self) -> None:
            self.article_values: dict[str, Any] | None = None

        def copy(self, obj: Any, *, context: str) -> Any:
            return obj

        def create(self, model: type[Any], *, context: str, **values: Any) -> Any:
            if model is Person:
                return Person.virtual(**values)
            if model is Article:
                self.article_values = values
                return Article.virtual(**values)
            raise AssertionError(f"unexpected model {model}")

    builder = RecordingBuilder()
    recommendations.add_virtual_models(plan, cast(Any, builder))

    assert builder.article_values is not None
    assert builder.article_values["_location"] == "None"
    assert builder.article_values["misc_data"] == "None"


def test_existing_person_author_is_guarded_and_reused_virtually(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    row_data = _row(pdf)
    row_data["article"]["authors"] = [{"person": {"id": 74061, "name": "Schmidt"}}]
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf)
    person = cast(
        Person,
        SimpleNamespace(
            id=74061,
            family_name="Schmidt",
            given_names="Karl Patterson",
            initials=None,
            tussenvoegsel=None,
            suffix=None,
            is_invalid=lambda: False,
        ),
    )
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(row_data, 1),),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        get_person_by_id=lambda _id: person,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: _citation_group(),
        expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
    )

    class RecordingBuilder:
        def __init__(self) -> None:
            self.copied: list[Any] = []

        def copy(self, obj: Any, *, context: str) -> Any:
            self.copied.append(obj)
            return obj

        def create(self, model: type[Any], *, context: str, **values: Any) -> Any:
            assert model is Article
            return cast(Any, SimpleNamespace(**values))

    builder = RecordingBuilder()
    recommendations.add_virtual_models(plan, cast(Any, builder))

    assert plan.actions[0].resolved_people == (person,)
    assert builder.copied[-1] is person


def test_existing_person_author_rejects_changed_family_name(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    row_data = _row(pdf)
    row_data["article"]["authors"] = [{"person": {"id": 74061, "name": "Changed"}}]
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf)
    person = cast(
        Person,
        SimpleNamespace(id=74061, family_name="Schmidt", is_invalid=lambda: False),
    )

    with pytest.raises(
        recommendations.RecommendationError,
        match="not valid with family name 'Changed'",
    ):
        recommendations.build_plan(
            (recommendations.parse_recommendation(row_data, 1),),
            options=Options(new_path=new_path, library_path=library_path),
            get_article=lambda _name: None,
            get_person_by_id=lambda _id: person,
            articles_with_doi=lambda _doi: (),
            is_catalog_folder=lambda _path: True,
            get_citation_group=lambda _id: _citation_group(),
            expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
        )


def test_build_plan_rejects_changed_staged_file(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf + b"changed")
    row = recommendations.parse_recommendation(_row(pdf), 1)

    with pytest.raises(recommendations.RecommendationError, match="size changed"):
        recommendations.build_plan(
            (row,),
            options=Options(new_path=new_path, library_path=library_path),
            get_article=lambda _name: None,
            articles_with_doi=lambda _doi: (),
            is_catalog_folder=lambda _path: True,
            get_citation_group=lambda _id: _citation_group(),
            expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
        )


def test_build_plan_accepts_parented_non_pdf_supplement(tmp_path: Path) -> None:
    workbook = b"PK\x03\x04minimal workbook fixture"
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "table-s1.xlsx").write_bytes(workbook)
    parent = cast(
        Article,
        SimpleNamespace(
            id=10,
            name="Endodontidae.pdf",
            type=ArticleType.JOURNAL,
            is_invalid=lambda: False,
        ),
    )

    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(_supplement_row(workbook), 1),),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        get_article_by_id=lambda _id: parent,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        expand_doi=lambda _doi: {},
    )

    action = plan.actions[0]
    assert action.article_type is ArticleType.SUPPLEMENT
    assert action.parent_article is parent
    assert action.destination_path == (
        library_path / "Mollusca/Endodontidae (supplement).xlsx"
    )


def test_build_plan_rejects_non_pdf_ordinary_article(tmp_path: Path) -> None:
    workbook = b"PK\x03\x04minimal workbook fixture"
    row_data = _supplement_row(workbook)
    row_data["article"]["type"] = "MISCELLANEOUS"
    row_data["article"].pop("parent")
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "table-s1.xlsx").write_bytes(workbook)

    with pytest.raises(
        recommendations.RecommendationError,
        match="only SUPPLEMENT Articles may use non-PDF files",
    ):
        recommendations.build_plan(
            (recommendations.parse_recommendation(row_data, 1),),
            options=Options(new_path=new_path, library_path=library_path),
            get_article=lambda _name: None,
            articles_with_doi=lambda _doi: (),
            is_catalog_folder=lambda _path: True,
            expand_doi=lambda _doi: {},
        )


def test_run_auxiliary_skips_pdf_processing_for_non_pdf_supplement() -> None:
    calls: list[object] = []
    article = cast(
        Article,
        SimpleNamespace(
            name="Endodontidae (supplement).xlsx",
            store_pdf_content=lambda: calls.append("store"),
            index_pdf_for_search=lambda: calls.append("index"),
            add_to_history=lambda *args: calls.append(args),
        ),
    )

    recommendations._run_auxiliary(article, add_history=True)

    assert calls == [(), ("name",)]


def test_execute_installs_pdf_then_runs_auxiliary_and_removes_source(
    tmp_path: Path,
) -> None:
    plan = _build(tmp_path)
    created: list[tuple[str, dict[str, Any]]] = []
    auxiliary: list[tuple[object, bool]] = []
    fake_article = object()

    def create_article(name: str, values: Mapping[str, Any]) -> Article:
        created.append((name, dict(values)))
        return cast(Article, fake_article)

    recommendations.execute_plan(
        plan,
        apply=True,
        get_or_create_person=lambda **_kwargs: Person.virtual(),
        create_article=create_article,
        run_auxiliary=lambda article, *, add_history: auxiliary.append(
            (article, add_history)
        ),
    )

    action = plan.actions[0]
    assert action.destination_path is not None
    assert action.source_path is not None
    assert action.destination_path.read_bytes() == _pdf_bytes()
    assert not action.source_path.exists()
    assert created[0][0] == "Endodontidae.pdf"
    assert auxiliary == [(fake_article, True)]


def test_parse_rejects_path_fields_with_wrong_types() -> None:
    pdf = _pdf_bytes()
    row = _row(pdf)
    row["file"]["size"] = "large"

    with pytest.raises(recommendations.RecommendationError, match="must be an integer"):
        recommendations.parse_recommendation(row, 1)


def test_inline_citation_group_is_planned_and_created(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    row_data = _row(pdf)
    row_data["article"]["citation_group"] = {
        "name": "New Journal of Mollusks",
        "type": "JOURNAL",
        "region": {"id": 7, "name": "Fiji"},
        "tags": [],
    }
    second_row_data = copy.deepcopy(row_data)
    second_row_data["article"]["name"] = "Endodontidae Pacific.pdf"
    second_row_data["article"]["doi"] = "10.1234/example2"
    second_row_data["file"]["source_path"] = "download2.pdf"
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Mollusca").mkdir(parents=True)
    (new_path / "download.pdf").write_bytes(pdf)
    (new_path / "download2.pdf").write_bytes(pdf)
    region = cast(Any, SimpleNamespace(id=7, name="Fiji"))
    plan = recommendations.build_plan(
        (
            recommendations.parse_recommendation(row_data, 1),
            recommendations.parse_recommendation(second_row_data, 2),
        ),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        citation_groups_named=lambda _name: (),
        get_region=lambda _id: region,
        expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
    )
    created_groups: list[recommendations.PlannedCitationGroup] = []
    created_articles: list[str] = []
    fake_group = CitationGroup.virtual(name="New Journal of Mollusks")

    def create_citation_group(
        planned: recommendations.PlannedCitationGroup,
    ) -> CitationGroup:
        created_groups.append(planned)
        return fake_group

    def create_article(name: str, values: Mapping[str, Any]) -> Article:
        created_articles.append(name)
        return Article.virtual(name=name, **values)

    recommendations.execute_plan(
        plan,
        apply=True,
        create_citation_group=create_citation_group,
        create_article=create_article,
        run_auxiliary=lambda _article, *, add_history: None,
    )

    assert [group.spec.name for group in created_groups] == ["New Journal of Mollusks"]
    assert created_articles == ["Endodontidae.pdf", "Endodontidae Pacific.pdf"]


def test_completed_exact_state_is_idempotent(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    destination_dir = library_path / "Mollusca"
    destination_dir.mkdir(parents=True)
    (destination_dir / "Endodontidae.pdf").write_bytes(pdf)
    cg = _citation_group()
    person = SimpleNamespace(
        family_name="Solem",
        given_names="Alan",
        initials=None,
        tussenvoegsel=None,
        suffix=None,
    )
    tag = ArticleTag.PublicationDate(source=DateSource.doi_published, date="1976")
    existing = cast(
        Article,
        SimpleNamespace(
            id=99,
            name="Endodontidae.pdf",
            kind=ArticleKind.electronic,
            path="Mollusca",
            doi="10.1234/example",
            type=ArticleType.JOURNAL,
            title="Reviewed title",
            citation_group=cg,
            parent=None,
            tags=(tag,),
            get_authors=lambda: [person],
        ),
    )

    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(_row(pdf), 1),),
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: existing,
        articles_with_doi=lambda _doi: (existing,),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: cg,
        expand_doi=lambda _doi: {
            "type": ArticleType.JOURNAL,
            "title": "CrossRef title",
            "author_tags": [VirtualPerson(family_name="Solem", given_names="Alan")],
            "tags": [tag],
        },
    )

    assert plan.actions[0].already_applied


def test_completed_exact_state_removes_downloads_source_on_apply(
    tmp_path: Path,
) -> None:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    downloads_path = tmp_path / "downloads"
    library_path = tmp_path / "library"
    new_path.mkdir()
    downloads_path.mkdir()
    source = downloads_path / "download.pdf"
    source.write_bytes(pdf)
    destination_dir = library_path / "Mollusca"
    destination_dir.mkdir(parents=True)
    (destination_dir / "Endodontidae.pdf").write_bytes(pdf)
    row_data = _row(pdf)
    row_data["file"]["source_root"] = "downloads"
    cg = _citation_group()
    person = SimpleNamespace(
        family_name="Solem",
        given_names="Alan",
        initials=None,
        tussenvoegsel=None,
        suffix=None,
    )
    tag = ArticleTag.PublicationDate(source=DateSource.doi_published, date="1976")
    existing = cast(
        Article,
        SimpleNamespace(
            id=99,
            name="Endodontidae.pdf",
            kind=ArticleKind.electronic,
            path="Mollusca",
            doi="10.1234/example",
            type=ArticleType.JOURNAL,
            title="Reviewed title",
            citation_group=cg,
            parent=None,
            tags=(tag,),
            get_authors=lambda: [person],
        ),
    )
    plan = recommendations.build_plan(
        (recommendations.parse_recommendation(row_data, 1),),
        options=Options(
            new_path=new_path, downloads_path=downloads_path, library_path=library_path
        ),
        get_article=lambda _name: existing,
        articles_with_doi=lambda _doi: (existing,),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: cg,
        expand_doi=lambda _doi: {
            "type": ArticleType.JOURNAL,
            "title": "CrossRef title",
            "author_tags": [VirtualPerson(family_name="Solem", given_names="Alan")],
            "tags": [tag],
        },
    )

    recommendations.execute_plan(
        plan, apply=True, run_auxiliary=lambda _article, *, add_history: None
    )

    assert plan.actions[0].already_applied
    assert not source.exists()


def test_build_plan_supports_no_copy_volume_and_parented_chapter(
    tmp_path: Path,
) -> None:
    plan = _build_volume_and_chapter(tmp_path)

    volume, chapter = plan.actions
    assert volume.kind is ArticleKind.no_copy
    assert volume.source_path is None
    assert volume.destination_path is None
    assert chapter.kind is ArticleKind.electronic
    assert chapter.article_type is ArticleType.CHAPTER
    assert chapter.planned_parent_name == volume.recommendation.name


def test_execute_creates_volume_before_chapter_and_only_installs_chapter_pdf(
    tmp_path: Path,
) -> None:
    plan = _build_volume_and_chapter(tmp_path)
    created: list[tuple[str, dict[str, Any], Article]] = []
    histories: list[Article] = []
    auxiliary: list[Article] = []

    def create_article(name: str, values: Mapping[str, Any]) -> Article:
        article = Article.virtual(name=name, **values)
        created.append((name, dict(values), article))
        return article

    recommendations.execute_plan(
        plan,
        apply=True,
        get_or_create_person=lambda **kwargs: Person.virtual(**kwargs),
        create_article=create_article,
        run_auxiliary=lambda article, *, add_history: auxiliary.append(article),
        add_article_history=histories.append,
    )

    volume_action, chapter_action = plan.actions
    volume = created[0][2]
    assert [name for name, _values, _article in created] == [
        volume_action.recommendation.name,
        chapter_action.recommendation.name,
    ]
    assert created[0][1]["kind"] is ArticleKind.no_copy
    assert created[0][1]["path"] is None
    assert created[1][1]["parent"] is volume
    assert histories == [volume]
    assert auxiliary == [created[1][2]]
    assert chapter_action.destination_path is not None
    assert chapter_action.destination_path.read_bytes() == _pdf_bytes()
    assert chapter_action.source_path is not None
    assert not chapter_action.source_path.exists()


def test_build_plan_rejects_forward_planned_parent_reference(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Insecta").mkdir(parents=True)
    (new_path / "TumbrinckSkejo.pdf").write_bytes(pdf)
    rows = (
        recommendations.parse_recommendation(_chapter_row(pdf), 1),
        recommendations.parse_recommendation(_volume_row(), 2),
    )

    with pytest.raises(recommendations.RecommendationError, match="must be an earlier"):
        recommendations.build_plan(
            rows,
            options=Options(new_path=new_path, library_path=library_path),
            get_article=lambda _name: None,
            articles_with_doi=lambda _doi: (),
            is_catalog_folder=lambda _path: True,
            get_citation_group=lambda _id: _book_citation_group(),
            expand_doi=lambda _doi: {},
        )


def test_build_plan_orders_forward_typed_parent_reference(tmp_path: Path) -> None:
    pdf = _pdf_bytes()
    new_path = tmp_path / "new"
    library_path = tmp_path / "library"
    new_path.mkdir()
    (library_path / "Insecta").mkdir(parents=True)
    (new_path / "TumbrinckSkejo.pdf").write_bytes(pdf)
    chapter_data = _chapter_row(pdf)
    chapter_data["article"]["ref"] = "chapter"
    chapter_data["article"]["parent"] = {
        "model": "Article",
        "ref": "volume",
        "label": _volume_row()["article"]["name"],
    }
    volume_data = _volume_row()
    volume_data["article"]["ref"] = "volume"
    rows = (
        recommendations.parse_recommendation(chapter_data, 1),
        recommendations.parse_recommendation(volume_data, 2),
    )

    plan = recommendations.build_plan(
        rows,
        options=Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        get_citation_group=lambda _id: _book_citation_group(),
        expand_doi=lambda _doi: {},
    )

    assert [action.recommendation.ref for action in plan.actions] == [
        "volume",
        "chapter",
    ]


def test_author_refs_share_article_people_in_proposals_and_execution(
    tmp_path: Path,
) -> None:
    from taxonomy.applicator.proposals import ProposalBuilder

    row_data = _volume_row()
    row_data["article"]["ref"] = "volume"
    row_data["article"]["authors"][0]["ref"] = "author:telnov"
    row = recommendations.parse_recommendation(row_data, 1)
    plan = recommendations.build_plan(
        [row],
        options=Options(new_path=tmp_path, library_path=tmp_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        get_citation_group=lambda _id: _book_citation_group(),
        expand_doi=lambda _doi: {},
    )
    proposed = recommendations.add_virtual_models(plan, ProposalBuilder())
    volume = proposed["volume"]
    assert isinstance(volume, Article)
    assert proposed["author:telnov"] is volume.get_authors()[0]

    created_people: list[Person] = []

    def person(**kwargs: Any) -> Person:
        result = Person.virtual(**kwargs)
        created_people.append(result)
        return result

    actual = recommendations.execute_plan(
        plan,
        apply=True,
        get_or_create_person=person,
        create_article=lambda name, values: Article.virtual(name=name, **values),
        add_article_history=lambda _article: None,
    )
    actual_volume = actual["volume"]
    assert isinstance(actual_volume, Article)
    assert actual["author:telnov"] is actual_volume.get_authors()[0]
    assert len(created_people) == 3
    # A resumed plan must export the already installed Article's People too.
    resumed = replace(
        plan,
        actions=(
            replace(plan.actions[0], article=actual_volume, already_applied=True),
        ),
    )
    reused = recommendations.execute_plan(resumed, apply=True)
    assert reused["author:telnov"] is created_people[0]
    resumed_builder = ProposalBuilder()
    virtual_reused = recommendations.add_virtual_models(resumed, resumed_builder)
    resumed_builder.build()
    reused_volume = virtual_reused["volume"]
    assert isinstance(reused_volume, Article)
    # A cumulative review compares already applied dependent fields/tags against
    # these refs. A fresh virtual copy has a different ID and fails those guards.
    assert reused_volume is actual_volume
    assert virtual_reused["author:telnov"] is created_people[0]
    assert virtual_reused["author:telnov"] is reused_volume.get_authors()[0]
    assert row.authors is not None
    assert "ref" not in row.authors[0].as_kwargs()


@pytest.mark.parametrize("conflict", ["volume", "author:duplicate"])
def test_article_and_author_refs_share_one_namespace(
    tmp_path: Path, conflict: str
) -> None:
    data = _volume_row()
    data["article"]["ref"] = "volume"
    data["article"]["authors"][0]["ref"] = conflict
    data["article"]["authors"][1]["ref"] = conflict
    row = recommendations.parse_recommendation(data, 1)
    with pytest.raises(recommendations.RecommendationError, match=r"duplicate .* ref"):
        recommendations.build_plan(
            [row],
            options=Options(new_path=tmp_path, library_path=tmp_path),
            get_article=lambda _name: None,
            articles_with_doi=lambda _doi: (),
            get_citation_group=lambda _id: _book_citation_group(),
            expand_doi=lambda _doi: {},
        )
