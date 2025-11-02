"""

Code for inferring matches with the BatLit corpus.

Actions to take:
- When there is a new BatLit release, consider downloading the Zenodo refs csv and updating BATLIT_CSV_ARTICLE_ID
- Run run_all() to find more matches; use add_tags=True to add tags to matched articles
- Run find_dupes() to find articles that have the same BatLit tag; use interactive=True to help resolve them

"""

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TypedDict, cast

import Levenshtein

from taxonomy import getinput
from taxonomy.db import helpers
from taxonomy.db.models.article.article import Article, ArticleTag

BATLIT_CSV_ARTICLE_ID = 72150  # Chiroptera-BatLit 0.8 Zenodo refs.csv


class BatLitRow(TypedDict):
    authors: str
    date: str
    title: str
    journal: str
    type: str
    volume: str
    issue: str
    pages: str
    doi: str
    id: str  # https://www.zotero.org/groups/bat_literature_project/items/A5RNP5YH
    attachment: str
    attachmentId: str
    corpusId: str
    alternativeDoi: str  # 10.5281/zenodo.14822460
    alternativeDoiUrl: str
    zenodoResponseCorpusId: str


@dataclass(frozen=True, kw_only=True)
class BatLitIndex:
    by_doi: dict[str, list[BatLitRow]]
    by_title: dict[str, list[BatLitRow]]
    by_journal_volume: dict[tuple[str, str], list[BatLitRow]]
    by_id: dict[str, BatLitRow]


@dataclass(frozen=True)
class BatLitMatch:
    doi_matches: bool
    title_distance: int
    normalized_title_distance: float
    is_prefix: bool
    journal_matches: bool
    volume_matches: bool
    authors_distance: int
    year_distance: int
    pages_match: bool

    def is_acceptable(self) -> bool:
        if self.doi_matches:
            if self.title_distance < 100 and self.year_distance < 2:
                return True
        if (
            self.normalized_title_distance < 0.1 or self.is_prefix
        ) and self.year_distance < 2:
            return True
        if (
            (self.normalized_title_distance < 0.1 or self.is_prefix)
            and self.year_distance < 1
            and self.authors_distance < 2
        ):
            return True
        if (
            (self.normalized_title_distance < 0.2 or self.is_prefix)
            and self.year_distance < 1
            and self.journal_matches
            and self.volume_matches
            and self.pages_match
        ):
            return True
        if (
            self.authors_distance == 0
            and self.journal_matches
            and self.volume_matches
            and self.pages_match
        ):
            return True
        return False


@cache
def build_batlit_index() -> BatLitIndex:
    art = Article(BATLIT_CSV_ARTICLE_ID)
    path = art.get_path()

    by_doi: dict[str, list[BatLitRow]] = defaultdict(list)
    by_title: dict[str, list[BatLitRow]] = defaultdict(list)
    by_journal_volume: dict[tuple[str, str], list[BatLitRow]] = defaultdict(list)
    by_id: dict[str, BatLitRow] = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            batlit_row = cast(BatLitRow, row)
            by_doi[row["doi"].casefold()].append(batlit_row)
            by_title[helpers.simplify_string(row["title"])].append(batlit_row)
            if row["journal"] and row["volume"]:
                by_journal_volume[
                    (helpers.simplify_string(row["journal"]), row["volume"])
                ].append(batlit_row)
            by_id[row["id"]] = batlit_row

    return BatLitIndex(
        by_doi=by_doi,
        by_title=by_title,
        by_journal_volume=by_journal_volume,
        by_id=by_id,
    )


def cite_row(row: BatLitRow) -> str:
    parts = []
    if row["authors"]:
        parts.append(row["authors"].replace(" | ", ", "))
    if row["date"]:
        parts.append(f"({row['date']})")
    if row["title"]:
        parts.append(f'"{row["title"]}."')
    if row["journal"]:
        parts.append(f"{row['journal']}")
    if row["volume"]:
        vol_issue = row["volume"]
        if row["issue"]:
            vol_issue += f"({row['issue']})"
        parts.append(vol_issue)
    if row["pages"]:
        parts.append(f": {row['pages']}.")
    if row["doi"]:
        parts.append(f"https://doi.org/{row['doi']}")
    parts.append(row["id"])
    return " ".join(parts)


def match_article(art: Article, row: BatLitRow) -> BatLitMatch:
    doi_matches = art.doi is not None and art.doi.casefold() == row["doi"].casefold()
    art_title = helpers.simplify_string(art.title or "")
    row_title = helpers.simplify_string(row["title"])
    title_distance = Levenshtein.distance(art_title, row_title)
    if art.citation_group is not None:
        journal_name = helpers.simplify_string(art.citation_group.get_citable_name())
    else:
        journal_name = ""
    journal_matches = journal_name == helpers.simplify_string(row["journal"])
    volume_matches = (art.volume or "") == (row["volume"] or "")
    row_authors = [a.strip() for a in row["authors"].split("|")]
    row_authors = [a for a in row_authors if a]
    art_authors = [person.family_name for person in art.get_authors()]
    authors_distance = Levenshtein.distance(row_authors, art_authors)

    match = re.search(r"\b\d{4}\b", row["date"])
    row_year = int(match.group()) if match else 0
    art_year = art.numeric_year()
    year_distance = abs(art_year - row_year)
    pages_match = (art.start_page or "") + "-" + (art.end_page or "") == (
        row["pages"] or ""
    )

    is_prefix = False
    if len(art_title) > 10 and row_title.startswith(art_title):
        is_prefix = True
    elif len(row_title) > 10 and art_title.startswith(row_title):
        is_prefix = True

    return BatLitMatch(
        doi_matches=doi_matches,
        title_distance=title_distance,
        normalized_title_distance=title_distance
        / (len(art_title) + len(row_title) + 1),
        is_prefix=is_prefix,
        journal_matches=journal_matches,
        volume_matches=volume_matches,
        authors_distance=authors_distance,
        year_distance=year_distance,
        pages_match=pages_match,
    )


def find_matches(art: Article) -> list[tuple[BatLitRow, BatLitMatch]]:
    index = build_batlit_index()
    candidates: list[BatLitRow] = []

    if art.doi:
        candidates.extend(index.by_doi.get(art.doi.casefold(), []))

    simplified_title = helpers.simplify_string(art.title or "")
    candidates.extend(index.by_title.get(simplified_title, []))

    if art.citation_group is not None:
        journal_name = art.citation_group.get_citable_name()
    else:
        journal_name = ""
    if art.volume:
        candidates.extend(
            index.by_journal_volume.get(
                (helpers.simplify_string(journal_name), art.volume), []
            )
        )

    unique_candidates = {row["id"]: row for row in candidates}.values()

    matches: list[tuple[BatLitRow, BatLitMatch]] = []
    for row in unique_candidates:
        match = match_article(art, row)
        matches.append((row, match))

    return matches


def run_all(
    *, add_tags: bool = False, verbose: bool = False, dry_run: bool = False
) -> None:
    dupes: list[tuple[Article, list[BatLitRow]]] = []
    art_to_matches: dict[Article, list[tuple[BatLitRow, BatLitMatch]]] = {}
    used_ids: set[str] = set()
    for art in Article.select_valid():
        if art.has_tag(ArticleTag.BatLit):
            for tag in art.get_tags(art.tags, ArticleTag.BatLit):
                used_ids.add(tag.zotero_id)
            continue
        matches = find_matches(art)
        matches = [(row, match) for row, match in matches if match.is_acceptable()]
        if len(matches) > 1 or (verbose and matches):
            getinput.print_header(repr(art))
            for row, match in matches:
                print(cite_row(row), match)
            dupes.append((art, [row for row, match in matches]))
        if matches:
            art_to_matches[art] = matches

    if dupes:
        with Path("batlit_dupes.csv").open("w") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "hesp_ref",
                    "batlit_id1",
                    "batlit_cite1",
                    "batlit_id2",
                    "batlit_cite2",
                    "batlit_id3",
                    "batlit_cite3",
                ],
            )
            writer.writeheader()
            for art, rows in dupes:
                writer.writerow(
                    {  # static analysis: ignore[incompatible_argument]
                        "hesp_ref": art.cite(),
                        **{
                            f"batlit_id{i+1}": row["id"]
                            for i, row in enumerate(rows[:3])
                        },
                        **{
                            f"batlit_cite{i+1}": cite_row(row)
                            for i, row in enumerate(rows[:3])
                        },
                    }
                )

    print(f"Found {len(art_to_matches)} articles with matches")

    if add_tags:
        for art, matches in art_to_matches.items():
            best_row, best_match = min(
                matches,
                key=lambda pair: (
                    0 if pair[1].doi_matches else 1,
                    pair[1].title_distance,
                    pair[1].year_distance,
                    pair[1].authors_distance,
                ),
            )
            if best_row["id"] in used_ids:
                print(
                    f"Skipping article {art} because BatLit id {best_row['id']} is already used"
                )
                continue
            getinput.print_header(repr(art))
            print("Best match:")
            print(cite_row(best_row), best_match)
            tag = ArticleTag.BatLit(
                zotero_id=best_row["id"], zenodo_doi=best_row["alternativeDoi"]
            )
            print("Adding tag:", tag)
            if not dry_run:
                art.add_tag(tag)


def find_dupes(*, interactive: bool = True) -> None:
    index = build_batlit_index()
    zotero_id_to_arts: dict[str, list[Article]] = defaultdict(list)
    for art in Article.select_valid():
        if not art.has_tag(ArticleTag.BatLit):
            continue
        for tag in art.get_tags(art.tags, ArticleTag.BatLit):
            if tag.zotero_id not in index.by_id:
                print(f"Article {art} has BatLit tag with unknown id {tag.zotero_id}")
                continue
            zotero_id_to_arts[tag.zotero_id].append(art)
    for zotero_id, arts in zotero_id_to_arts.items():
        deduped_arts = {art.resolve_redirect() for art in arts}
        if len(deduped_arts) > 1:
            getinput.print_header(
                f"Zotero id {zotero_id} is used by multiple articles:"
            )
            for art in arts:
                print(" ", repr(art))
            print(cite_row(index.by_id[zotero_id]))
            if interactive:

                def open_all(arts: list[Article] = arts) -> None:
                    for art in arts:
                        art.openf()

                while True:
                    line = getinput.choose_one_by_name(
                        arts,
                        history_key=zotero_id,
                        callbacks={"open all": open_all},
                        display_fn=lambda art: art.name,
                    )
                    if not line:
                        break
                    line.edit()
