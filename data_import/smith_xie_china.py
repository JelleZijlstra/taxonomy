import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank
from taxonomy.db.models.article.article import Article

SOURCE = lib.Source(
    "mammalia-china-smith-xie-.txt", "Mammalia China (Smith & Xie 2008).pdf"
)


def format_pages(pages: lib.PagesT) -> lib.PagesT:
    for page_no, lines in pages:
        yield page_no, format_page(page_no, lines)


def format_page(page_no: int, lines: list[str]) -> list[str]:
    new_lines = []
    in_key = False
    for line in lines:
        if not line.strip():
            in_key = False
        elif re.search(r"^Key to ", line):
            in_key = True
        if not in_key:
            new_lines.append(line)
    return new_lines


def _get_species(line: str, art: Article, page_no: int) -> lib.CEDict | None:
    species_match = re.search(
        r"^(?P<name>[A-Z][a-z]+ [a-z]+)\s*\(?(?P<author>[^\d]+), (?P<year>\d{4})\)?\s+MAP \d+$",
        line,
    )
    if species_match:
        return {
            "name": species_match.group("name"),
            "rank": Rank.species,
            "authority": species_match.group("author"),
            "year": species_match.group("year"),
            "article": art,
            "page": str(page_no),
        }
    return None


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()

    last_line: str = ""
    last_species: lib.CEDict | None = None
    distribution_lines: list[str] = []
    for page_no, lines in pages:
        for line in lines:
            line = line.lstrip()
            higher_match = re.search(
                r"^\s*(?P<rank>CLASS|ORDER|Order|Suborder|SUBORDER|FAMILY|Family|Subfamily|Genus) (?P<name>[A-Z][a-zA-Z]{2,20})",
                line,
            )
            if higher_match:
                assert not distribution_lines, line
                yield {
                    "name": higher_match.group("name").title(),
                    "rank": Rank[
                        higher_match.group("rank").lower().replace("class", "class_")
                    ],
                    "article": art,
                    "page": str(page_no),
                }
                last_species = None
                continue
            if sp := _get_species(line, art, page_no):
                assert not distribution_lines, line
                last_species = sp
                yield sp
            elif re.search(r"MAP \d+$", line):
                assert not distribution_lines, line
                combined_line = last_line + " " + line
                if sp := _get_species(combined_line, art, page_no):
                    last_species = sp
                    yield sp
                else:
                    print(f"Failed to parse species: {combined_line}")

            if distribution_lines and line.startswith(
                ("Natural History:", "Comments:")
            ):
                text = " ".join(distribution_lines)
                assert last_species is not None, line
                genus, species = last_species["name"].split()
                rgx = rf"{genus[0]}\.\s+{species[0]}\.\s+(?P<epithet>[a-z]+)\s+\(?(?P<author>[^\d]+),\s+(?P<year>\d{{4}})\)?"
                for match in re.finditer(rgx, text):
                    yield {
                        "name": f"{genus} {species} {match.group('epithet')}",
                        "rank": Rank.subspecies,
                        "authority": match.group("author"),
                        "year": match.group("year"),
                        "article": art,
                        "page": str(page_no),
                    }
                distribution_lines.clear()
            if line.startswith("Distribution:") or distribution_lines:
                distribution_lines.append(line)
            last_line = line


def main() -> None:
    text = lib.get_text(SOURCE)
    pages = lib.extract_pages(text)
    pages = format_pages(pages)
    pages = lib.align_columns(pages, single_column_pages={313})
    names = extract_names(pages)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
