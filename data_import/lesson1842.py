import re
from collections.abc import Iterable

from data_import import lib
from taxonomy.db import helpers
from taxonomy.db.constants import AgeClass, Rank

SOURCE = lib.Source("mammalia-lesson-.txt", "Mammalia (Lesson 1842).pdf")


def extract_names(pages: lib.PagesT) -> Iterable[lib.CEDict]:
    art = SOURCE.get_source()

    current_species = 0
    current_genus_counter = 0
    current_family_counter = 0
    subgenus_counter = 0
    current_genus = None
    for page_no, lines in pages:
        for line in lines:
            line = line.strip()

            # Species
            if match := re.search(r"^(\d+)\. ", line):
                species_no = int(match.group(1))
                if current_species + 1 != species_no:
                    # 5 often gets mis-OCRed as 3
                    if species_no == current_species + 3 and str(species_no)[-1] == "5":
                        species_no -= 2
                        # 59 and 60 are missing
                    elif (current_species, species_no) != (58, 61):
                        print(f"jump from {current_species} to {species_no}: {line}")
                current_species = species_no
                genus = r"(?P<genus>[A-Z]E?[a-zœïëæ]+|—|–|-)"
                species = r"(?P<species>(d')?[A-Z]?[a-zA-Zæœëï\-]+)"
                regexes = [
                    rf"^\d+\.\s+{genus}\s+{species}\s*,\s*(?P<author>.*?\.)  ",
                    rf"^\d+\.\s+{genus}\s+{species}\.\s{{3,}}[^\.]+\.$",
                    rf"^\d+\.\s+{genus}\s+{species}\s*,\s*(?P<author>.*?\.) \S[^\.]*\.$",
                    rf"^\d+\.\s+{genus}\s+{species}, (?P<author>[A-Z][a-z]*\.)$",
                ]
                for regex in regexes:
                    full_match = re.search(regex, line)
                    if full_match is not None:
                        break
                else:
                    print(f"no match: {line}")
                    continue
                genus = full_match.group("genus")
                if not genus.isalpha():
                    if current_genus is None:
                        print(f"no current genus: {line}")
                        continue
                    genus = current_genus
                else:
                    current_genus = genus
                species = full_match.group("species")
                data: lib.CEDict = {
                    "article": art,
                    "page": str(page_no),
                    "name": f"{genus} {species}",
                    "rank": Rank.species,
                }
                try:
                    data["authority"] = full_match.group("author")
                except IndexError:
                    pass
                yield data
            # Genus
            elif match := re.search(r"^([IVXLC]+)\.. ", line):
                subgenus_counter = 0
                genus_no = helpers.parse_roman_numeral(match.group(1))
                if current_genus_counter + 1 != genus_no:
                    if (current_genus_counter, genus_no) not in (
                        # Numbers are discontinuous twice
                        (65, 65),
                        (80, 85),
                    ):
                        print(
                            f"jump from {current_genus_counter} to {genus_no}: {line}"
                        )
                current_genus_counter = genus_no
                genus_match = re.search(
                    (
                        r"^[IVXLC]+\.. [\-–—] (?P<genus>[A-Z][A-Za-zÆæ]+)\s*,\s*(?P<author>.*?\.)  "
                    ),
                    line,
                )
                if genus_match is None:
                    print(f"no genus match: {line}")
                    continue
                data = {
                    "article": art,
                    "page": str(page_no),
                    "name": genus_match.group("genus").title(),
                    "rank": Rank.genus,
                }
                try:
                    data["authority"] = genus_match.group("author")
                except IndexError:
                    pass
                yield data
            elif "Famille" in line:
                family_match = re.search(
                    r"^(?P<foss>Foss\. : )?(?P<number>\d+).{1,2} Famille[,\.] [\-–—] (?P<name>[A-ZæÉÆa-z]+)\.",
                    line,
                )
                if family_match is None:
                    print(f"no family match: {line}")
                    continue
                family_no = int(family_match.group("number"))
                if current_family_counter + 1 != family_no:
                    print(f"jump from {current_family_counter} to {family_no}: {line}")
                current_family_counter = family_no
                name = family_match.group("name").title()
                if name.endswith("ae"):
                    name = name[:-2] + "æ"
                data = {
                    "article": art,
                    "page": str(page_no),
                    "name": name,
                    "rank": Rank.family,
                }
                if family_match.group("foss"):
                    data["age_class"] = AgeClass.fossil
                yield data
            elif match := re.search(r"^F[Oo][Ss][Ss]\.\s*:", line):
                line = line[match.span()[1] :].strip()
                if match := re.search(
                    r"^(?P<sous>S\.-)?Genre : (?P<name>[A-Z][a-zA-Zïœ]+)\s*,\s+(?P<author>.*?\.) ( |[A-Za-z ]+\.$)",
                    line,
                ):
                    yield {
                        "article": art,
                        "page": str(page_no),
                        "name": match.group("name").title(),
                        "authority": match.group("author"),
                        "rank": Rank.subgenus if match.group("sous") else Rank.genus,
                        "age_class": AgeClass.fossil,
                    }
                elif match := re.search(
                    r"^(?P<name>[A-Z][a-zœ]+ [A-Z]?[a-z]+)\s*, (?P<author>.*?\.)  ",
                    line,
                ):
                    yield {
                        "article": art,
                        "page": str(page_no),
                        "name": match.group("name"),
                        "authority": match.group("author"),
                        "rank": Rank.species,
                        "age_class": AgeClass.fossil,
                    }
                elif "genre" in line.lower():
                    print(line)
                    # we give up on most fossil species
            elif "S.-GENRE" in line.upper():
                if match := re.search(
                    r"^(?P<number>\d+)\.?.{1,3}\s+S\.-GENRE [:,] (?P<name>[A-Za-z]+)(\s*, (?P<author>.*)|\.)$",
                    line,
                ):
                    subgenus_no = int(match.group("number"))
                    if subgenus_counter + 1 != subgenus_no:
                        print(f"jump from {subgenus_counter} to {subgenus_no}: {line}")
                    subgenus_counter = subgenus_no
                    author = match.group("author")
                    if author and "  " in author:
                        author, _ = author.split("  ", maxsplit=1)
                    name = match.group("name").title()
                    if name == "Oedipus":
                        name = "OEdipus"
                    yield {
                        "article": art,
                        "page": str(page_no),
                        "name": name,
                        "authority": author,
                        "rank": Rank.subgenus,
                    }
                else:
                    print(f"no subgenus match: {line}")


def main() -> None:
    text = lib.get_text(SOURCE)
    pages = lib.extract_pages(text)
    names = extract_names(pages)
    names = lib.add_parents(names)
    names = lib.validate_ce_parents(names)
    names = lib.add_classification_entries(names, dry_run=False)
    lib.print_ce_summary(names)
    lib.format_ces(SOURCE)


if __name__ == "__main__":
    main()
