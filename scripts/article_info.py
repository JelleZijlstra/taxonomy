import argparse
import contextlib
import itertools
import json
import sys
from pathlib import Path
from typing import Any

from taxonomy import config
from taxonomy.db import models

MAX_REPORTED_NAMES = 50
MAX_CANDIDATES = 25


def normalize_reference(reference: str) -> str:
    """Turn a brace-delimited Article reference into its database name."""
    name = reference.strip()
    if len(name) >= 2 and name.startswith("{") and name.endswith("}"):
        name = name[1:-1].strip()
    return name


def find_article(reference: str) -> tuple[models.Article | None, list[models.Article]]:
    """Find an exact Article, with a conservative filename-suffix fallback."""
    name = normalize_reference(reference)
    if not name:
        return None, []
    exact = list(models.Article.select().filter(models.Article.name == name))
    if len(exact) == 1:
        return exact[0], []
    if len(exact) > 1:
        return None, exact

    if not name.casefold().endswith(".pdf"):
        pdf_name = f"{name}.pdf"
        exact_pdf = list(
            models.Article.select().filter(models.Article.name == pdf_name)
        )
        if len(exact_pdf) == 1:
            return exact_pdf[0], []
        if len(exact_pdf) > 1:
            return None, exact_pdf

    candidates = list(
        itertools.islice(
            models.Article.select_valid().filter(models.Article.name.contains(name)),
            MAX_CANDIDATES + 1,
        )
    )
    if len(candidates) == 1:
        return candidates[0], []
    return None, candidates


def resolve_file_article(
    article: models.Article,
) -> tuple[models.Article | None, list[models.Article]]:
    """Follow parents until reaching the record backed by a physical file."""
    chain: list[models.Article] = []
    seen: set[int] = set()
    current: models.Article | None = article
    while current is not None:
        if current.id in seen:
            raise ValueError(f"cycle in Article parent chain at {current.id}")
        seen.add(current.id)
        chain.append(current)
        if current.kind.is_electronic():
            return current, chain
        current = current.parent
    return None, chain


def article_summary(article: models.Article) -> dict[str, Any]:
    citation_group = article.get_citation_group()
    return {
        "id": article.id,
        "name": article.name,
        "kind": article.kind.name,
        "type": article.type.name if article.type is not None else None,
        "title": article.title,
        "authors": [person.get_full_name() for person in article.get_authors()],
        "year": article.year,
        "citation_group": citation_group.name if citation_group is not None else None,
        "series": article.series,
        "volume": article.volume,
        "issue": article.issue,
        "start_page": article.start_page,
        "end_page": article.end_page,
        "pages": article.pages,
        "doi": article.doi,
        "url": article.url,
        "parent_id": article.parent.id if article.parent is not None else None,
        "parent_name": article.parent.name if article.parent is not None else None,
    }


def name_summary(name: models.Name) -> dict[str, Any]:
    return {
        "id": name.id,
        "original_name": name.original_name,
        "corrected_original_name": name.corrected_original_name,
        "status": name.status.name,
        "taxon": name.taxon.valid_name,
    }


def build_report(article: models.Article, *, extract: bool) -> dict[str, Any]:
    file_article, chain = resolve_file_article(article)
    warnings: list[str] = []
    file_path: Path | None = None
    text_path: Path | None = None

    if file_article is None:
        warnings.append("No file-backed Article occurs in the parent chain.")
    else:
        file_path = file_article.get_path()
        text_path = config.get_options().pdf_text_path / f"{file_article.id}.txt"
        if not file_path.exists():
            warnings.append("The resolved file path does not exist on disk.")
        if extract and not text_path.exists():
            if file_article.ispdf() and file_path.exists():
                with contextlib.redirect_stdout(sys.stderr):
                    extracted = file_article.store_pdf_content()
                if extracted is not None:
                    text_path = extracted
            else:
                warnings.append(
                    "Text extraction requires a present, direct electronic PDF Article."
                )
        if not text_path.exists():
            warnings.append(
                "No cached PDF text exists; rerun with --extract or process the PDF "
                "with the PDF skill."
            )

    new_names_query = article.get_new_names()
    new_name_count = new_names_query.count()
    new_names = [
        name_summary(name)
        for name in itertools.islice(new_names_query, MAX_REPORTED_NAMES)
    ]
    citation: str | None
    try:
        citation = article.cite()
    except Exception as exc:
        citation = None
        warnings.append(f"Could not format citation: {exc}")

    return {
        "query_article": article_summary(article),
        "citation": citation,
        "parent_chain": [article_summary(item) for item in chain],
        "file": {
            "article_id": file_article.id if file_article is not None else None,
            "article_name": file_article.name if file_article is not None else None,
            "path": str(file_path) if file_path is not None else None,
            "exists": file_path.exists() if file_path is not None else False,
        },
        "extracted_text": {
            "article_id": file_article.id if file_article is not None else None,
            "path": str(text_path) if text_path is not None else None,
            "exists": text_path.exists() if text_path is not None else False,
        },
        "new_names": {
            "count": new_name_count,
            "reported": new_names,
            "truncated": new_name_count > len(new_names),
        },
        "classification_entry_count": article.get_classification_entries().count(),
        "warnings": warnings,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve a taxonomy Article name or {brace-delimited reference} to its "
            "database metadata, physical file, and cached extracted text."
        )
    )
    parser.add_argument("article", help="Article.name, optionally enclosed in braces")
    parser.add_argument(
        "--extract",
        action="store_true",
        help="create the cached pdftotext file when it is missing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    article, candidates = find_article(args.article)
    if article is None:
        normalized = normalize_reference(args.article)
        report = {
            "error": f"No unique Article matched {normalized!r}",
            "candidates": [
                article_summary(candidate) for candidate in candidates[:MAX_CANDIDATES]
            ],
            "candidates_truncated": len(candidates) > MAX_CANDIDATES,
        }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2

    report = {
        "input": args.article,
        "normalized_name": normalize_reference(args.article),
        **build_report(article, extract=args.extract),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
