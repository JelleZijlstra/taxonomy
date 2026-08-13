import copy
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy.applicator import article as recommendations
from taxonomy.db.constants import ArticleKind, ArticleType, DateSource
from taxonomy.db.models import Article, CitationGroup, Person
from taxonomy.db.models.article.article import ArticleTag
from taxonomy.db.models.person import VirtualPerson


@dataclass(frozen=True)
class _Options:
    new_path: Path
    library_path: Path


def _pdf_bytes() -> bytes:
    return b"%PDF-1.4\nminimal test fixture\n%%EOF\n"


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
            "citation_group": {"id": 12, "name": "Journal of Mollusks"},
        },
        "file": {
            "source_path": "download.pdf",
            "destination_folder": "Mollusca",
            "sha256": hashlib.sha256(pdf).hexdigest(),
            "size": len(pdf),
        },
    }


def _citation_group() -> CitationGroup:
    return cast(
        CitationGroup,
        SimpleNamespace(
            id=12,
            name="Journal of Mollusks",
            type=ArticleType.JOURNAL,
            tags=(),
            is_invalid=lambda: False,
        ),
    )


def _book_citation_group() -> CitationGroup:
    return cast(
        CitationGroup,
        SimpleNamespace(
            id=22, name="Riga", type=ArticleType.BOOK, tags=(), is_invalid=lambda: False
        ),
    )


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
            "citation_group": {"id": 22, "name": "Riga"},
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
        options=_Options(new_path=new_path, library_path=library_path),
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
        options=_Options(new_path=new_path, library_path=library_path),
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
        options=_Options(new_path=new_path, library_path=library_path),
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
                return cast(Person, SimpleNamespace(**values))
            if model is Article:
                self.article_values = values
            return cast(Any, SimpleNamespace(**values))

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
        options=_Options(new_path=new_path, library_path=library_path),
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
            options=_Options(new_path=new_path, library_path=library_path),
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
            options=_Options(new_path=new_path, library_path=library_path),
            get_article=lambda _name: None,
            articles_with_doi=lambda _doi: (),
            is_catalog_folder=lambda _path: True,
            get_citation_group=lambda _id: _citation_group(),
            expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
        )


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
        get_or_create_person=lambda **_kwargs: cast(Person, object()),
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
        options=_Options(new_path=new_path, library_path=library_path),
        get_article=lambda _name: None,
        articles_with_doi=lambda _doi: (),
        is_catalog_folder=lambda _path: True,
        citation_groups_named=lambda _name: (),
        get_region=lambda _id: region,
        expand_doi=lambda _doi: {"type": ArticleType.JOURNAL},
    )
    created_groups: list[recommendations.PlannedCitationGroup] = []
    created_articles: list[str] = []
    fake_group = cast(CitationGroup, SimpleNamespace(name="New Journal of Mollusks"))

    def create_citation_group(
        planned: recommendations.PlannedCitationGroup,
    ) -> CitationGroup:
        created_groups.append(planned)
        return fake_group

    def create_article(name: str, values: Mapping[str, Any]) -> Article:
        created_articles.append(name)
        return cast(Article, SimpleNamespace(name=name, **values))

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
        options=_Options(new_path=new_path, library_path=library_path),
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
        article = cast(Article, SimpleNamespace(name=name, **values))
        created.append((name, dict(values), article))
        return article

    recommendations.execute_plan(
        plan,
        apply=True,
        get_or_create_person=lambda **kwargs: cast(Person, SimpleNamespace(**kwargs)),
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
            options=_Options(new_path=new_path, library_path=library_path),
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
        options=_Options(new_path=new_path, library_path=library_path),
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
