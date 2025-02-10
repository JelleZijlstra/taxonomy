import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank

SOURCE = lib.Source("geomyidae2025.txt", "Geomyidae (Alvarez & Segura 2025).pdf")


# This initially written by o1-pro
def extract_taxa_from_text(text: Iterable[str]) -> Iterable[lib.CEDict]:
    """
    Given the text of a paper (string with multiple lines),
    yield dictionaries representing the taxa covered in the text.
    """
    # Compile regexes for efficiency
    # 1) Page number lines: lines that contain only digits
    page_pattern = re.compile(r"^(\d+)$")

    # 2) Genus lines of the form:
    #      Genus Megascapheus Elliot 1903
    #    This regex captures:
    #      group(1) = "Megascapheus"
    #      group(2) = "Elliot"
    #      group(3) = "1903"
    genus_pattern = re.compile(
        r"^(?:Genus\s+)?([A-Z][a-z]+)\s+([A-Za-z]+(?:-Neuwied)?),?\s+(\d{4})\s*$"
    )

    subgenus_pattern = re.compile(
        r"^Subg?enus\s+([A-Z][a-z]+)\s+\(?([A-Za-z]+(?:-Neuwied)?),?\s+(\d{4})\s*\)?$"
    )

    # 3) Taxon names with author/year in parentheses
    #    Examples:
    #       "Megascapheus atrovarius (Allen, 1898)"
    #       "1. M. a. atrovarius (Allen, 1898). For type locality see..."
    #
    #    We'll do a "findall" approach on each line, capturing:
    #      group(1) = entire name portion (e.g. "Megascapheus atrovarius" or "M. a. atrovarius")
    #      group(2) = author portion (everything before the comma)
    #      group(3) = year portion (the digits after the comma)
    species_pattern = re.compile(
        r"^([A-Z][a-z]+(?: \([A-Z][a-z]+\))? [a-z]+\w)"  # The name portion (e.g. "Megascapheus atrovarius")
        r"\s*\(?"  # Opening parenthesis
        r"([^,]+)"  # Author portion (up to the comma), e.g. "Allen" or "Eydoux and Gervais"
        r",\s*(\d{4})\)?$"  # Comma, spaces, then 4-digit year, e.g. "1898"
    )

    subspecies_pattern = re.compile(
        r"^\d+\.\s*?"  # "1." or "2." prefix
        r"([A-Z]\. [a-z]\. [a-z]+\w)"  # The name portion (e.g. "M. a. atrovarius")
        r"\s*\(?"  # Opening parenthesis
        r"([^,]+)"  # Author portion (up to the comma), e.g. "Allen" or "Eydoux and Gervais"
        r",\s*(\d{4})\)?"  # Comma, spaces, then 4-digit year, e.g. "1898"
    )

    current_page = 54  # Start at page 54

    for line in text:
        line = line.strip()

        # 1) Check if the line is just a page number
        match_page = page_pattern.match(line)
        if match_page:
            current_page = int(match_page.group(1)) + 1
            continue

        # 2) Check if this is a "Genus ..." line
        match_genus = genus_pattern.match(line)
        if match_genus:
            genus_name = match_genus.group(1)
            author = match_genus.group(2).strip()
            year = match_genus.group(3)
            yield {
                "rank": Rank.genus,
                "name": genus_name,
                "authority": author,
                "year": year,
                "page": str(current_page),
                "article": SOURCE.get_source(),
            }
            # We can continue since each line typically has one main match
            continue

        match_subgenus = subgenus_pattern.match(line)
        if match_subgenus:
            subgenus_name = match_subgenus.group(1)
            author = match_subgenus.group(2).strip()
            year = match_subgenus.group(3)
            yield {
                "rank": Rank.subgenus,
                "name": subgenus_name,
                "authority": author,
                "year": year,
                "page": str(current_page),
                "article": SOURCE.get_source(),
            }
            continue

        match_species = species_pattern.match(line)
        if match_species:
            name_portion, author, year = match_species.groups()

            yield {
                "rank": Rank.species,
                "name": name_portion,
                "authority": author.strip(),
                "year": year,
                "page": str(current_page),
                "article": SOURCE.get_source(),
            }
            continue

        match_subspecies = subspecies_pattern.match(line)
        if match_subspecies:
            name_portion, author, year = match_subspecies.groups()

            yield {
                "rank": Rank.subspecies,
                "name": name_portion,
                "authority": author.strip(),
                "year": year,
                "page": str(current_page),
                "article": SOURCE.get_source(),
            }
            continue


def main() -> None:
    text = lib.get_text(SOURCE)
    names = extract_taxa_from_text(text)
    names = lib.add_parents(names)
    names = lib.expand_abbreviations(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
