import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

# Mammals of South America (The University of Chicago Press; multiple volumes).
# This parser expects a decolumnized plain-text export of the relevant volume.
# Configure the input text filename below to point at that export.

SOURCE = lib.Source("southamerica-decolumnized.txt", "South America.pdf")


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()
    last_genus: str | None = None

    for page_no, lines in pages:
        it = lib.PeekingIterator(lines)
        for raw in it:
            line = raw.rstrip()
            if not line:
                continue

            # Higher taxa headers
            if m := re.match(
                r"^ *(Order|Suborder|Family|Subfamily|Tribe|Subtribe) +([A-Z][a-zA-Z\-]+)\b",
                line,
            ):
                rank = Rank[m.group(1).lower()]
                name = m.group(2)
                yield {"name": name, "rank": rank, "page": str(page_no), "article": art}
                continue

            # Genus header: a capitalized word standing alone or followed by author/comma
            if re.match(r"^([A-Z][a-z]+)(,|$)", line) and len(line.split()) <= 3:
                genus = re.match(r"^([A-Z][a-z]+)", line).group(1)  # type: ignore[union-attr]
                last_genus = genus
                yield {
                    "name": genus,
                    "rank": Rank.genus,
                    "page": str(page_no),
                    "article": art,
                }
                continue

            # Species entry lines commonly begin with either "G. epithet" or "Genus epithet"
            if m := re.match(r"^([A-Z])\. +([a-z][a-z\-]+)\b", line):
                initial, epithet = m.groups()
                if last_genus and last_genus[0] == initial:
                    yield {
                        "name": f"{last_genus} {epithet}",
                        "rank": Rank.species,
                        "page": str(page_no),
                        "article": art,
                    }
                    continue

            if m := re.match(r"^([A-Z][a-z]+) +([a-z][a-z\-]+)\b", line):
                genus, epithet = m.groups()
                # Avoid misclassifying higher headers as species
                if genus and epithet and genus[0].isupper() and epithet.islower():
                    last_genus = genus
                    yield {
                        "name": f"{genus} {epithet}",
                        "rank": Rank.species,
                        "page": str(page_no),
                        "article": art,
                    }
                    continue

            # Subspecies lines often look like three-part names
            if m := re.match(
                r"^([A-Z][a-z]+) +([a-z][a-z\-]+) +([a-z][a-z\-]+)\b", line
            ):
                g, s, ss = m.groups()
                yield {
                    "name": f"{g} {s} {ss}",
                    "rank": Rank.subspecies,
                    "page": str(page_no),
                    "article": art,
                }
                last_genus = g
                continue


def main() -> None:
    lines = lib.get_text(SOURCE)
    pages = lib.extract_pages(lines)
    pages = lib.validate_pages(pages, verbose=False)
    names: Iterable[lib.CEDict] = list(extract_names(pages))
    names = lib.add_parents(names)
    names = lib.no_childless_ces(names)
    # Write a quick CSV snapshot for verification
    lib.create_csv("southamerica_ce.csv", list(names))
    # Optionally, add to the database directly:
    # lib.add_classification_entries(names, dry_run=False)


if __name__ == "__main__":
    main()
