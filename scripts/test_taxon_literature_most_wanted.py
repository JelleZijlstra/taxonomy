from pathlib import Path

from taxonomy.db.models import Article, Name
from taxonomy.db.models.name import TypeTag

from .taxon_literature_most_wanted import (
    Candidate,
    bibliography_entries,
    is_candidate,
    write_bibliography,
)


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
                    url="https://example.com/page", page="1", confirmed=True
                ),
            ),
        )
    )
    assert not is_candidate(
        Name.virtual(
            original_citation=None,
            type_tags=(
                TypeTag.StructuredVerbatimCitation(
                    volume="1", url="https://example.com/work"
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
