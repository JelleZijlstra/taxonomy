import csv
from dataclasses import replace
from pathlib import Path

import pytest

from taxonomy.db.models import Article, Name
from taxonomy.db.models.name import TypeTag

from .taxon_literature_most_wanted import (
    Candidate,
    apply_batlit_review,
    bibliography_entries,
    is_bhl_url,
    is_candidate,
    write_bibliography,
    write_csv,
)


def write_review(path: Path, rows: list[dict[str, str | int]]) -> None:
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def review_row(record: Candidate, status: str) -> dict[str, str | int]:
    return {
        "name_id": record.name_id,
        "verbatim_citation": record.verbatim_citation,
        "year": record.year,
        "batlit_status": status,
        "batlit_url": (
            "https://www.zotero.org/groups/bat_literature_project/items/ABCDEFGH"
        ),
        "full_text_url": "https://zenodo.org/records/123/files/work.pdf",
    }


def test_batlit_review_keeps_unconfirmed_and_metadata_only(tmp_path: Path) -> None:
    records = [candidate(i, f"Aus {i}", f"Work {i}", "Alpha", 1901) for i in range(5)]
    rows = [
        review_row(record, status)
        for record, status in zip(
            records,
            ["confirmed_available", "metadata_only", "other_edition", "possible_match"],
            strict=False,
        )
    ]
    path = tmp_path / "review.csv"
    write_review(path, rows)
    assert apply_batlit_review(records, path) == records[1:]


@pytest.mark.parametrize("field", ["verbatim_citation", "year"])
def test_batlit_review_rejects_stale_evidence(tmp_path: Path, field: str) -> None:
    record = candidate(1, "Aus primus", "First edition", "Alpha", 1901)
    row = review_row(record, "confirmed_available") | {field: "changed"}
    path = tmp_path / "review.csv"
    write_review(path, [row])
    with pytest.raises(ValueError, match="Stale BatLit review"):
        apply_batlit_review([record], path)


def test_batlit_review_rejects_missing_evidence_and_duplicates(tmp_path: Path) -> None:
    record = candidate(1, "Aus primus", "Work", "Alpha", 1901)
    row = review_row(record, "confirmed_available")
    path = tmp_path / "review.csv"
    write_review(path, [row | {"full_text_url": ""}])
    with pytest.raises(ValueError, match="lacks availability evidence"):
        apply_batlit_review([record], path)
    write_review(path, [row, row])
    with pytest.raises(ValueError, match="Duplicate BatLit review"):
        apply_batlit_review([record], path)


def candidate(
    name_id: int, name: str, citation: str, author: str, year: int
) -> Candidate:
    return Candidate(
        requested_taxon="Chiroptera",
        name_id=name_id,
        name_url=f"https://hesperomys.com/n/{name_id}",
        current_taxon="Chiroptera",
        original_name=name,
        corrected_original_name=name,
        authority=author,
        year=str(year),
        numeric_year=year,
        page_described="1",
        citation_group="Journal",
        structured_series="",
        structured_volume="1",
        structured_issue="",
        structured_start_page="1",
        structured_end_page="2",
        verbatim_citation=citation,
        sort_author=author,
    )


def test_candidate_rule_is_exact() -> None:
    assert is_candidate(Name.virtual(original_citation=None, type_tags=()))
    assert not is_candidate(
        Name.virtual(original_citation=Article.virtual(), type_tags=())
    )
    assert not is_candidate(
        Name.virtual(
            original_citation=None,
            type_tags=(
                TypeTag.AuthorityPageLink(
                    url="https://www.biodiversitylibrary.org/page/123",
                    page="1",
                    confirmed=True,
                ),
            ),
        )
    )
    assert not is_candidate(
        Name.virtual(
            original_citation=None,
            type_tags=(
                TypeTag.StructuredVerbatimCitation(
                    volume="1", url="https://biodiversitylibrary.org/item/123"
                ),
            ),
        )
    )
    assert is_candidate(
        Name.virtual(
            original_citation=None,
            type_tags=(TypeTag.StructuredVerbatimCitation(volume="1"),),
        )
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://books.google.com/books?id=example",
        "https://archive.org/details/example",
        "https://example.com/work.pdf",
        "https://biodiversitylibrary.org.example.com/page/1",
        "https://example.com/?url=https://biodiversitylibrary.org/page/1",
        "https://[malformed",
    ],
)
def test_other_links_do_not_exclude_names(url: str) -> None:
    for tag in (
        TypeTag.AuthorityPageLink(url=url, page="1", confirmed=True),
        TypeTag.StructuredVerbatimCitation(url=url),
    ):
        assert is_candidate(Name.virtual(original_citation=None, type_tags=(tag,)))


def test_bhl_link_excludes_even_with_other_links() -> None:
    assert is_bhl_url("HTTP://WWW.BIODIVERSITYLIBRARY.ORG/page/123")
    assert not is_candidate(
        Name.virtual(
            original_citation=None,
            type_tags=(
                TypeTag.AuthorityPageLink(
                    url="https://example.com/page", confirmed=True, page="1"
                ),
                TypeTag.StructuredVerbatimCitation(
                    url="https://www.biodiversitylibrary.org/page/123"
                ),
            ),
        )
    )


def test_retrieval_leads_survive_export(tmp_path: Path) -> None:
    record = replace(
        candidate(1, "Aus primus", "A work", "Alpha", 1901),
        authority_page_urls="https://example.com/page",
        structured_url="https://example.com/work",
    )
    other = replace(record, name_id=2, corrected_original_name="Aus secundus")
    taxon = type("TaxonStub", (), {"valid_name": "Chiroptera"})()
    bibliography = tmp_path / "bibliography.md"
    write_bibliography(bibliography, taxon, [record, other])
    text = bibliography.read_text()
    assert text.count("https://example.com/page") == 1
    assert text.count("https://example.com/work") == 1
    assert "Retrieval leads" in text
    csv = tmp_path / "wanted.csv"
    write_csv(csv, [record])
    assert "authority_page_urls,structured_url" in csv.read_text()
    assert "https://example.com/page,https://example.com/work" in csv.read_text()


def test_bibliography_groups_and_sorts(tmp_path: Path) -> None:
    shared = "Zulu, Z. 1902. Shared work."
    candidates = [
        candidate(2, "Aus secundus", shared, "Zulu", 1902),
        candidate(1, "Aus primus", shared, "Zulu", 1902),
        candidate(3, "Bus primus", "Alpha, A. 1901. First work", "Alpha", 1901),
    ]
    entries = bibliography_entries(candidates)
    assert [entry[1] for entry in entries] == ["Alpha, A. 1901. First work.", shared]
    assert entries[1][2] == ["Aus primus", "Aus secundus"]

    path = tmp_path / "bibliography.md"
    taxon = type("TaxonStub", (), {"valid_name": "Chiroptera"})()
    write_bibliography(path, taxon, candidates)
    text = path.read_text()
    assert "(Original citation of *Aus primus* and *Aus secundus*.)" in text
    assert "Alpha, A. 1901. First work." in text


def test_missing_citations_keep_retrieval_leads_separate(tmp_path: Path) -> None:
    records = [
        replace(
            candidate(1, "Aus primus", "", "Alpha", 1901),
            structured_url="https://example.com/first",
        ),
        replace(
            candidate(2, "Aus secundus", "", "Alpha", 1901),
            structured_url="https://example.com/second",
        ),
    ]
    taxon = type("TaxonStub", (), {"valid_name": "Chiroptera"})()
    path = tmp_path / "bibliography.md"
    write_bibliography(path, taxon, records)
    first, second = path.read_text().split("2. [Citation not recorded].")
    assert "https://example.com/first" in first
    assert "https://example.com/second" not in first
    assert "https://example.com/second" in second
    assert "https://example.com/first" not in second
