import argparse
import csv
from collections.abc import Iterable
from pathlib import Path
from typing import TypedDict

from taxonomy.db.models import Article


class Row(TypedDict):
    ce_name: str
    ce_rank: str
    ce_page: str
    taxon_valid_name: str
    taxon_authority: str
    taxon_year: str


def get_rows(article: Article) -> Iterable[Row]:
    for ce in sorted(
        article.get_classification_entries(),
        key=lambda ce: (ce.numeric_page(), ce.rank.name, ce.name, ce.id),
    ):
        mapped_name = ce.mapped_name
        taxon = mapped_name.taxon if mapped_name is not None else None
        base_name = taxon.base_name if taxon is not None else None
        yield {
            "ce_name": ce.name,
            "ce_rank": ce.rank.name,
            "ce_page": ce.page or "",
            "taxon_valid_name": "" if taxon is None else taxon.valid_name,
            "taxon_authority": (
                "" if base_name is None else base_name.taxonomic_authority()
            ),
            "taxon_year": "" if base_name is None else str(base_name.numeric_year()),
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export classification entries for an Article, together with the current "
            "valid Taxon and that Taxon's authority and year."
        )
    )
    parser.add_argument("article_name", help="Exact Article.name value")
    parser.add_argument("output_file", help="CSV file to write")
    args = parser.parse_args()

    article = Article.getter("name")(args.article_name)
    assert article is not None, f"could not find article {args.article_name!r}"

    with Path(args.output_file).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, list(Row.__annotations__))
        writer.writeheader()
        for row in get_rows(article):
            writer.writerow(row)


if __name__ == "__main__":
    main()
