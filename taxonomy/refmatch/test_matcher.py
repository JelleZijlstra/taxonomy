import csv
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from taxonomy.db.constants import ArticleType
from taxonomy.refmatch import matcher, parse


class StaticDatabase:
    def __init__(
        self,
        *,
        citations: dict[int, str],
        original_citations: dict[str, tuple[int, ...]] | None = None,
    ) -> None:
        self.citations = citations
        self.original_citations = original_citations or {}

    def get_article(self, article_id: int) -> Any:
        return SimpleNamespace(doi="", citation_group=None)

    def get_article_citation(self, article_id: int) -> str:
        return self.citations[article_id]

    def original_citation_ids_for_taxon(self, label: str) -> tuple[int, ...]:
        return self.original_citations.get(label, ())

    def get_citation_group(self, citation_group_id: int) -> Any:
        raise AssertionError("BHL lookups should be disabled in this test")


def make_record(
    *,
    article_id: int = 1,
    title: str = "A very specific mammal paper",
    authors: tuple[str, ...] = ("smith",),
    year: int = 2001,
    citation_group_id: int = 10,
    citation_group: str = "Journal of Mammalogy",
    volume: str = "12",
    start_page: str = "34",
    end_page: str = "40",
) -> matcher.ArticleRecord:
    title_key = matcher.normalize_text(title)
    aliases = (citation_group,)
    return matcher.ArticleRecord(
        id=article_id,
        name=f"article-{article_id}.pdf",
        type=ArticleType.JOURNAL,
        type_name=ArticleType.JOURNAL.name,
        kind_name="electronic",
        year=str(year),
        year_num=year,
        title=title,
        title_key=title_key,
        title_tokens=matcher.title_tokens(title),
        authors=authors,
        author_aliases=tuple(frozenset({author}) for author in authors),
        author_key=authors[0],
        citation_group_id=citation_group_id,
        citation_group=citation_group,
        citation_group_aliases=aliases,
        citation_group_keys=frozenset(
            matcher.normalize_text(alias) for alias in aliases
        ),
        volume=volume,
        issue="",
        start_page=start_page,
        end_page=end_page,
        pages=f"{start_page}-{end_page}",
        doi="",
        url="",
    )


def make_row(**overrides: str) -> dict[str, str]:
    row = parse.make_empty_stage2_row()
    row.update(
        {
            "section": "References",
            "reference_type": "journal_article",
            "authors": "Smith, J.",
            "year": "2001",
            "title": "A very specific mammal paper",
            "container_title": "Journal of Mammalogy",
            "volume": "12",
            "pages": "34-40",
            "raw_reference": (
                "Smith, J. 2001. A very specific mammal paper. "
                "Journal of Mammalogy 12:34-40."
            ),
        }
    )
    row.update(overrides)
    return row


def test_run_match_csv_with_static_index_and_database(tmp_path: Path) -> None:
    input_path = tmp_path / "stage2.csv"
    output_path = tmp_path / "stage3.csv"
    with input_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, parse.STAGE2_FIELDS)
        writer.writeheader()
        writer.writerow(make_row())

    index = matcher.build_article_index_from_records([make_record()])
    database = StaticDatabase(citations={1: "Smith 2001 citation"})
    matcher.run_match_csv(
        input_path,
        output_path,
        doi_mode="off",
        bhl_mode="off",
        database=database,
        index=index,
        include_slow_links=False,
    )

    with output_path.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["taxonomy_match_status"] == "matched"
    assert row["taxonomy_article_id"] == "1"
    assert row["taxonomy_article_name"] == "article-1.pdf"
    assert row["taxonomy_citation"] == "Smith 2001 citation"
    assert row["doi"] == ""
    assert row["bhl_url"] == ""


def test_scientific_description_uses_injected_original_citation_lookup() -> None:
    record = make_record(article_id=5, start_page="100", end_page="120")
    index = matcher.build_article_index_from_records([record])
    database = StaticDatabase(
        citations={5: "Smith 2001 citation"},
        original_citations={"Newtaxon example": (5,)},
    )
    row = make_row(
        reference_type="scientific_description",
        described_taxa="Newtaxon example",
        pages="",
        title="",
        container_title="",
        raw_reference="Smith, J. (2001). Journal of Mammalogy 12:100 [Newtaxon example].",
    )

    evaluation = matcher.evaluate_row(
        1,
        row,
        index,
        database,
        doi_mode="off",
        bhl_mode="off",
        include_slow_links=False,
    )

    assert evaluation.status == "matched"
    assert evaluation.match is not None
    assert evaluation.match.article.id == 5
    assert "described_taxon:Newtaxon example" in evaluation.match.methods


def test_journal_page_conflict_blocks_automatic_match() -> None:
    record = make_record(article_id=8, start_page="34", end_page="40")
    index = matcher.build_article_index_from_records([record])
    database = StaticDatabase(citations={8: "Smith 2001 citation"})
    row = make_row(pages="50-60")

    evaluation = matcher.evaluate_row(
        1,
        row,
        index,
        database,
        doi_mode="off",
        bhl_mode="off",
        include_slow_links=False,
    )

    assert evaluation.status == "unmatched"
    assert evaluation.scored
    assert "start page conflicts" in evaluation.scored[0].reasons
    assert "end page conflicts" in evaluation.scored[0].reasons
