import argparse
import csv
from collections import Counter
from collections.abc import Iterable
from typing import TypedDict

from zipp import Path

from taxonomy.db import export
from taxonomy.db.constants import AgeClass, ArticleType, Group, Rank
from taxonomy.db.models import Taxon
from taxonomy.db.models.citation_group import CitationGroup


class CitationGroupRow(TypedDict):
    cg_id: str
    cg_name: str
    cg_num_extant_mammals: str
    cg_start_year: str
    cg_end_year: str
    cg_bhl_bibliography: str


def get_rows() -> Iterable[CitationGroupRow]:
    taxon = Taxon.getter("valid_name")("Mammalia")
    assert taxon is not None
    hesp_names = export.get_names_for_export(
        taxon,
        ages={AgeClass.extant, AgeClass.recently_extinct},
        group=Group.species,
        min_rank_for_age_filtering=Rank.species,
    )
    cg_count = Counter(
        nam.citation_group.id for nam in hesp_names if nam.citation_group is not None
    )
    for cg in CitationGroup.select_valid().filter(
        CitationGroup.type == ArticleType.JOURNAL
    ):
        year_range = cg.get_active_year_range()
        if year_range is None:
            start_year = end_year = ""
        else:
            start_year, end_year = year_range
        title_ids = cg.get_bhl_title_ids()
        if cg.id not in cg_count:
            continue
        yield {
            "cg_id": str(cg.id),
            "cg_name": cg.name,
            "cg_num_extant_mammals": str(cg_count[cg.id]),
            "cg_start_year": str(start_year),
            "cg_end_year": str(end_year),
            "cg_bhl_bibliography": " | ".join(
                f"https://www.biodiversitylibrary.org/bibliography/{title_id}"
                for title_id in title_ids
            ),
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv")
    args = parser.parse_args()
    if args.csv is not None:
        with Path(args.csv).open("w") as f:
            writer = csv.DictWriter(f, list(CitationGroupRow.__annotations__))
            writer.writeheader()
            for row in get_rows():
                writer.writerow(row)


if __name__ == "__main__":
    main()
